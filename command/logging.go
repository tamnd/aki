package command

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file implements structured logging from doc 20 section 8.3. aki writes to
// stderr by default or to a logfile, at or above loglevel, in the Redis
// traditional format or as JSON. syslog is optional. SIGHUP reopens the logfile so
// logrotate can rename it and have aki start writing to a fresh file.

// Log severities. The numeric order matches the spec: debug is the lowest and
// loudest, nothing silences everything.
const (
	logDebug = iota
	logVerbose
	logNotice
	logWarning
	logNothing
)

// logLevelNum maps a loglevel name to its numeric severity. An unknown name falls
// back to notice, the default.
func logLevelNum(name string) int {
	switch strings.ToLower(name) {
	case "debug":
		return logDebug
	case "verbose":
		return logVerbose
	case "notice":
		return logNotice
	case "warning":
		return logWarning
	case "nothing":
		return logNothing
	default:
		return logNotice
	}
}

// logLevelName is the inverse of logLevelNum, used by the JSON format.
func logLevelName(level int) string {
	switch level {
	case logDebug:
		return "debug"
	case logVerbose:
		return "verbose"
	case logWarning:
		return "warning"
	default:
		return "notice"
	}
}

// logLevelChar is the one-character marker the Redis format prints for a level.
func logLevelChar(level int) byte {
	switch level {
	case logDebug:
		return '.'
	case logVerbose:
		return '-'
	case logWarning:
		return '#'
	default:
		return '*'
	}
}

// logField is one structured key value pair attached to a log line. The Redis
// format appends it as " key=value"; the JSON format adds it as a top-level key.
type logField struct {
	key string
	val any
}

// lf builds a log field.
func lf(key string, val any) logField { return logField{key, val} }

// The syslog sink lives in logging_syslog.go (unix) and logging_nosyslog.go
// (windows), because log/syslog is a unix-only package. Both files define the
// syslogSink type and newSyslogSink so this file stays platform neutral.

// logger holds the logging state. The stream sink (stderr or an open file) and the
// optional syslog sink are guarded by the mutex so a CONFIG SET that reopens the
// file cannot race a concurrent log line.
type logger struct {
	mu     sync.Mutex
	out    io.Writer // current stream sink, stderr or the open file
	file   *os.File  // non-nil when out is a file aki opened
	path   string    // current logfile path, "" for stderr
	sys    *syslogSink
	pid    int
	level  int         // cached minimum level from loglevel
	format string      // "redis" or "json", cached from log-format
	roleCh func() byte // returns the current role character, M or S
}

// logInit sets the logger defaults: stderr sink, this process id, the role
// character source, and the cached level and format from config. It runs in New
// and cannot fail, so a dispatcher always has a working logger even before the
// server opens a logfile.
func (d *Dispatcher) logInit() {
	d.log.pid = os.Getpid()
	d.log.out = os.Stderr
	d.log.roleCh = func() byte {
		if d.roleMaster.Load() {
			return 'M'
		}
		return 'S'
	}
	d.logApplyConfig()
}

// logStart opens the logfile if one is set and connects to syslog if enabled. The
// server command calls it at startup; a failure to open the file or reach syslog
// is returned so startup can report it. Tests that only need stderr skip it.
func (d *Dispatcher) logStart() error {
	if err := d.logReopen(); err != nil {
		return err
	}
	if d.confBool("syslog-enabled", false) {
		sink, err := newSyslogSink(d.confValue("syslog-ident", "aki"), d.confValue("syslog-facility", "local0"))
		if err != nil {
			return err
		}
		d.log.mu.Lock()
		d.log.sys = sink
		d.log.mu.Unlock()
	}
	return nil
}

// LogStart opens the logfile and connects to syslog. The server command calls it
// at startup.
func (d *Dispatcher) LogStart() error { return d.logStart() }

// LogClose releases the log file and syslog handles. The server command defers it.
func (d *Dispatcher) LogClose() { d.logClose() }

// ReopenLog closes and reopens the logfile. The server command calls it on SIGHUP
// so logrotate can rotate the file.
func (d *Dispatcher) ReopenLog() error { return d.logReopen() }

// LogNotice writes a notice-level line. The server command uses it for startup and
// shutdown messages. The variadic arguments are alternating key and value pairs.
func (d *Dispatcher) LogNotice(msg string, kv ...any) {
	d.logNotice(msg, pairFields(kv)...)
}

// LogWarn writes a warning line, taking alternating key value pairs like
// LogNotice. The background commit paths use it to surface a failed checkpoint.
func (d *Dispatcher) LogWarn(msg string, kv ...any) {
	d.logWarning(msg, pairFields(kv)...)
}

// SetConfig validates and applies one directive the same way CONFIG SET does, then
// runs its side effects. The server command uses it to apply command-line flags
// like logfile and loglevel before logging starts.
func (d *Dispatcher) SetConfig(name, value string) error {
	def, ok := d.conf.defs[name]
	if !ok {
		return fmt.Errorf("unknown config directive %q", name)
	}
	canon, ok := validateValue(def, value)
	if !ok {
		return fmt.Errorf("invalid value for %q: %q", name, value)
	}
	d.conf.set(name, canon)
	switch name {
	case "loglevel", "log-format":
		d.logApplyConfig()
	case "logfile":
		return d.logReopen()
	case "appendonly", "appendfsync":
		// Retune the pager checkpoint cadence so a startup --appendonly/--appendfsync
		// flag has the same effect as the matching CONFIG SET. Toggling the AOF flips
		// whether it carries the always guarantee, which changes the policy too. This
		// also recomputes the hash overlay gate.
		d.applyCommitPolicy()
	case "aki-hash-overlay":
		// Turn the in-memory hash write overlay on or off so a startup directive has
		// the same effect as CONFIG SET aki-hash-overlay.
		d.applyHashOverlay()
	}
	return nil
}

// pairFields turns alternating key value arguments into log fields. A trailing
// odd argument is dropped.
func pairFields(kv []any) []logField {
	out := make([]logField, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			continue
		}
		out = append(out, lf(key, kv[i+1]))
	}
	return out
}

// logApplyConfig refreshes the cached level and format from the config store. It
// runs at init and after a CONFIG SET that touches loglevel or log-format.
func (d *Dispatcher) logApplyConfig() {
	level := logLevelNum(d.confValue("loglevel", "notice"))
	format := strings.ToLower(d.confValue("log-format", "redis"))
	d.log.mu.Lock()
	d.log.level = level
	d.log.format = format
	d.log.mu.Unlock()
}

// logReopen points the stream sink at the current logfile, closing any file it had
// open. An empty logfile means stderr. SIGHUP and a CONFIG SET of logfile both
// call it, which is what lets logrotate rename the file and signal aki to continue
// in a fresh one.
func (d *Dispatcher) logReopen() error {
	path := d.confValue("logfile", "")
	d.log.mu.Lock()
	defer d.log.mu.Unlock()
	if path == "" {
		if d.log.file != nil {
			_ = d.log.file.Close()
			d.log.file = nil
		}
		d.log.out = os.Stderr
		d.log.path = ""
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if d.log.file != nil {
		_ = d.log.file.Close()
	}
	d.log.file = f
	d.log.out = f
	d.log.path = path
	return nil
}

// logClose releases the file and syslog handles. The server calls it on shutdown.
func (d *Dispatcher) logClose() {
	d.log.mu.Lock()
	defer d.log.mu.Unlock()
	if d.log.file != nil {
		_ = d.log.file.Close()
		d.log.file = nil
	}
	if d.log.sys != nil {
		_ = d.log.sys.Close()
		d.log.sys = nil
	}
}

// logAt writes one line at the given level if it clears the configured minimum.
// Most callers go through the named helpers below.
func (d *Dispatcher) logAt(level int, msg string, fields ...logField) {
	l := &d.log
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.out == nil || level < l.level || l.level >= logNothing {
		return
	}
	now := time.Now()
	var line string
	if l.format == "json" {
		line = l.formatJSON(now, level, d.roleStr(), msg, fields)
	} else {
		line = l.formatRedis(now, level, msg, fields)
	}
	_, _ = io.WriteString(l.out, line)
	if l.sys != nil {
		l.sys.write(level, msg)
	}
}

// logDebugMsg, logNotice, logWarning write at a fixed level.
func (d *Dispatcher) logDebugMsg(msg string, fields ...logField) { d.logAt(logDebug, msg, fields...) }
func (d *Dispatcher) logNotice(msg string, fields ...logField)   { d.logAt(logNotice, msg, fields...) }
func (d *Dispatcher) logWarning(msg string, fields ...logField)  { d.logAt(logWarning, msg, fields...) }

// roleStr returns the role name for the JSON format.
func (d *Dispatcher) roleStr() string {
	if d.roleMaster.Load() {
		return "master"
	}
	return "slave"
}

// formatRedis renders a line in the Redis traditional format:
// <pid>:<role> <timestamp> <level-char> <message> with any fields appended as
// key=value pairs. The caller holds the logger lock.
func (l *logger) formatRedis(now time.Time, level int, msg string, fields []logField) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(l.pid))
	b.WriteByte(':')
	if l.roleCh != nil {
		b.WriteByte(l.roleCh())
	} else {
		b.WriteByte('M')
	}
	b.WriteByte(' ')
	b.WriteString(now.Format("02 Jan 2006 15:04:05.000"))
	b.WriteByte(' ')
	b.WriteByte(logLevelChar(level))
	b.WriteByte(' ')
	b.WriteString(msg)
	for _, f := range fields {
		b.WriteByte(' ')
		b.WriteString(f.key)
		b.WriteByte('=')
		b.WriteString(fieldString(f.val))
	}
	b.WriteByte('\n')
	return b.String()
}

// formatJSON renders a line as a single JSON object with a stable set of base keys
// plus one key per field. The caller holds the logger lock.
func (l *logger) formatJSON(now time.Time, level int, role, msg string, fields []logField) string {
	obj := map[string]any{
		"ts":    now.UTC().Format("2006-01-02T15:04:05.000Z"),
		"pid":   l.pid,
		"role":  role,
		"level": logLevelName(level),
		"msg":   msg,
	}
	for _, f := range fields {
		obj[f.key] = f.val
	}
	enc, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	return string(enc) + "\n"
}

// fieldString renders a field value for the Redis format. Strings pass through;
// everything else uses the default Go formatting.
func fieldString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		enc, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(enc)
	}
}

// confBool reads a bool directive off the config store, falling back to def when
// the directive is missing or unparseable.
func (d *Dispatcher) confBool(name string, def bool) bool {
	v := d.confValue(name, "")
	switch strings.ToLower(v) {
	case "yes", "true", "1":
		return true
	case "no", "false", "0":
		return false
	default:
		return def
	}
}
