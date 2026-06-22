package command

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureLog points the dispatcher's log sink at a buffer the test can read.
func captureLog(d *Dispatcher) *bytes.Buffer {
	var buf bytes.Buffer
	d.log.mu.Lock()
	d.log.out = &buf
	d.log.mu.Unlock()
	return &buf
}

// TestLogRedisFormat checks a notice line in the Redis traditional format carries
// the pid, role char, level marker, message, and appended fields.
func TestLogRedisFormat(t *testing.T) {
	d := newMetricsDispatcher(t)
	buf := captureLog(d)

	d.logNotice("Server started", lf("aki_version", "0.1.0"))

	line := buf.String()
	if !strings.Contains(line, ":M ") {
		t.Fatalf("line missing role char: %q", line)
	}
	if !strings.Contains(line, " * Server started") {
		t.Fatalf("line missing notice marker and message: %q", line)
	}
	if !strings.Contains(line, "aki_version=0.1.0") {
		t.Fatalf("line missing field: %q", line)
	}
}

// TestLogJSONFormat checks the JSON format emits a parseable object with the base
// keys and the extra field.
func TestLogJSONFormat(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.conf.set("log-format", "json")
	d.logApplyConfig()
	buf := captureLog(d)

	d.logWarning("disk full", lf("free_bytes", 0))

	var obj map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &obj); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, buf.String())
	}
	if obj["level"] != "warning" || obj["msg"] != "disk full" || obj["role"] != "master" {
		t.Fatalf("unexpected base fields: %v", obj)
	}
	if _, ok := obj["ts"]; !ok {
		t.Fatalf("missing ts: %v", obj)
	}
	if obj["free_bytes"].(float64) != 0 {
		t.Fatalf("missing or wrong field: %v", obj)
	}
}

// TestLogLevelFilter checks a line below the configured level is dropped and one at
// or above it is written.
func TestLogLevelFilter(t *testing.T) {
	d := newMetricsDispatcher(t)
	buf := captureLog(d)

	// Default level is notice, so debug is dropped and warning is kept.
	d.logDebugMsg("internal detail")
	if buf.Len() != 0 {
		t.Fatalf("debug line written at notice level: %q", buf.String())
	}
	d.logWarning("something wrong")
	if !strings.Contains(buf.String(), "something wrong") {
		t.Fatalf("warning line dropped: %q", buf.String())
	}
}

// TestLogLevelChange checks CONFIG SET loglevel takes effect at once.
func TestLogLevelChange(t *testing.T) {
	d := newMetricsDispatcher(t)
	if err := d.SetConfig("loglevel", "warning"); err != nil {
		t.Fatalf("SetConfig loglevel: %v", err)
	}
	buf := captureLog(d)

	d.logNotice("startup notice")
	if buf.Len() != 0 {
		t.Fatalf("notice written at warning level: %q", buf.String())
	}
	d.logWarning("a warning")
	if !strings.Contains(buf.String(), "a warning") {
		t.Fatalf("warning dropped: %q", buf.String())
	}
}

// TestLogFileReopen checks logfile writes go to the file and a reopen after a
// rename starts a fresh file, the SIGHUP and logrotate path.
func TestLogFileReopen(t *testing.T) {
	d := newMetricsDispatcher(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "aki.log")

	if err := d.SetConfig("logfile", path); err != nil {
		t.Fatalf("SetConfig logfile: %v", err)
	}
	d.logNotice("first line")

	// Simulate logrotate moving the file aside, then SIGHUP reopening the path.
	rotated := filepath.Join(dir, "aki.log.1")
	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := d.ReopenLog(); err != nil {
		t.Fatalf("ReopenLog: %v", err)
	}
	d.logNotice("second line")
	d.logClose()

	old, err := os.ReadFile(rotated)
	if err != nil {
		t.Fatalf("read rotated: %v", err)
	}
	fresh, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fresh: %v", err)
	}
	if !strings.Contains(string(old), "first line") || strings.Contains(string(old), "second line") {
		t.Fatalf("rotated file = %q", old)
	}
	if !strings.Contains(string(fresh), "second line") || strings.Contains(string(fresh), "first line") {
		t.Fatalf("fresh file = %q", fresh)
	}
}

// TestLogRoleChar checks the role marker follows the replication role.
func TestLogRoleChar(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.roleMaster.Store(false)
	buf := captureLog(d)

	d.logNotice("as replica")
	if !strings.Contains(buf.String(), ":S ") {
		t.Fatalf("replica role char missing: %q", buf.String())
	}
}
