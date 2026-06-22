package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file implements the append-only-file emulation (spec 2064 doc 17 section
// 7). aki's WAL is the real durability log, so there is no separate appendonly
// file during normal operation. When appendonly is on, aki exports the Redis 7
// multi-part AOF layout: an appendonlydir with a base RDB, an incremental command
// log, and a manifest tying them together. BGREWRITEAOF rewrites the base and
// starts a fresh incremental file. Writes are appended to the incremental file as
// canonical RESP commands so the directory replays on a real Redis.

// aofState holds the AOF bookkeeping. Every field is guarded by mu because the
// command handlers, the background rewrite trigger, and INFO all touch it.
type aofState struct {
	mu sync.Mutex

	rewriteInProgress bool
	scheduled         bool
	lastStatus        string // "ok" or "err", empty before the first rewrite
	lastWriteStatus   string // "ok" or "err", empty before the first write
	lastTimeSec       float64
	curStartUnix      int64

	seq      int    // sequence number of the current base and incr files
	baseSize int64  // size of the base RDB at the last rewrite
	incrSize int64  // bytes written to the current incr file
	incrPath string // path of the current incr file, empty when not initialized

	incrFile       *os.File // open handle on the incr file for appends
	lastSelectedDB int      // database last written into the incr file, -1 if none

	pendingSync bool      // bytes written since the last fsync, drives everysec
	lastSync    time.Time // time of the last incr-file fsync

	loading bool // true while replaying the AOF, suppresses re-propagation
}

// aofCommands registers BGREWRITEAOF.
func aofCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "bgrewriteaof", Group: GroupServer, Since: "1.0.0",
			Arity: 1, Flags: FlagAdmin | FlagNoScript, Handler: handleBgrewriteaof},
	}
}

// handleBgrewriteaof triggers an AOF rewrite. With no fork, aki rewrites inline
// and replies right away. If a rewrite is already running it schedules one and
// says so, matching redis.
func handleBgrewriteaof(ctx *Ctx) {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return
	}

	ctx.d.aof.mu.Lock()
	if ctx.d.aof.rewriteInProgress {
		ctx.d.aof.scheduled = true
		ctx.d.aof.mu.Unlock()
		ctx.enc().WriteStatus("Background append only file rewriting scheduled")
		return
	}
	ctx.d.aof.mu.Unlock()

	if err := ctx.d.rewriteAOF(); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	ctx.enc().WriteStatus("Background append only file rewriting started")
}

// aofEnabled reports whether appendonly is configured on.
func (d *Dispatcher) aofEnabled() bool {
	if d.conf == nil {
		return false
	}
	return confValue(d.conf, "appendonly", "no") == "yes"
}

// aofDir returns the appendonlydir path under the data directory.
func (d *Dispatcher) aofDir() string {
	dir := "."
	sub := "appendonlydir"
	if d.conf != nil {
		dir = confValue(d.conf, "dir", ".")
		sub = confValue(d.conf, "appenddirname", "appendonlydir")
	}
	return filepath.Join(dir, sub)
}

// aofBasename returns the base file name, "appendonly.aof" by default.
func (d *Dispatcher) aofBasename() string {
	if d.conf == nil {
		return "appendonly.aof"
	}
	return confValue(d.conf, "appendfilename", "appendonly.aof")
}

// initAOF sets up the AOF on startup when appendonly is on. If an appendonlydir
// with a manifest already exists, it loads the base RDB, replays the incremental
// files, and reopens the incr file for appends so new writes continue the log.
// Otherwise it does a fresh rewrite: a base RDB plus an empty incr file plus the
// manifest.
func (d *Dispatcher) initAOF() {
	if d.engine == nil || !d.aofEnabled() {
		return
	}
	d.aof.mu.Lock()
	already := d.aof.incrPath != ""
	d.aof.mu.Unlock()
	if already {
		return
	}
	if d.aofManifestExists() {
		if err := d.loadAOF(); err == nil {
			return
		}
		// A load that fails leaves the keyspace as it is; fall through to a fresh
		// rewrite so the server still comes up with a consistent AOF directory.
	}
	_ = d.rewriteAOF()
}

// rewriteAOF writes a new base RDB from a snapshot, opens a fresh incr file, and
// updates the manifest, then removes the previous base and incr files. When
// appendonly is off it is a no-op rewrite that only records the status, the same
// observable result Redis gives for BGREWRITEAOF with AOF disabled.
func (d *Dispatcher) rewriteAOF() error {
	d.aof.mu.Lock()
	if d.aof.rewriteInProgress {
		d.aof.mu.Unlock()
		return fmt.Errorf("background append only file rewriting already in progress")
	}
	d.aof.rewriteInProgress = true
	d.aof.curStartUnix = time.Now().Unix()
	oldSeq := d.aof.seq
	newSeq := oldSeq + 1
	oldIncr := d.aof.incrFile
	d.aof.mu.Unlock()

	start := time.Now()

	if !d.aofEnabled() {
		d.aof.mu.Lock()
		d.aof.rewriteInProgress = false
		d.aof.curStartUnix = 0
		d.aof.lastStatus = "ok"
		d.aof.lastTimeSec = time.Since(start).Seconds()
		d.aof.mu.Unlock()
		return nil
	}

	snap, err := d.snapshotForRDB()
	if err != nil {
		d.finishRewrite(false, start)
		return err
	}

	dir := d.aofDir()
	if mkerr := os.MkdirAll(dir, 0o755); mkerr != nil {
		d.finishRewrite(false, start)
		return mkerr
	}

	basename := d.aofBasename()
	baseName := fmt.Sprintf("%s.%d.base.rdb", basename, newSeq)
	incrName := fmt.Sprintf("%s.%d.incr.aof", basename, newSeq)

	checksum := d.conf == nil || confValue(d.conf, "rdbchecksum", "yes") != "no"
	if werr := writeRDBFile(snap, dir, baseName, checksum); werr != nil {
		d.finishRewrite(false, start)
		return werr
	}
	baseInfo, statErr := os.Stat(filepath.Join(dir, baseName))
	var baseSize int64
	if statErr == nil {
		baseSize = baseInfo.Size()
	}

	incrPath := filepath.Join(dir, incrName)
	incrFile, ferr := os.OpenFile(incrPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if ferr != nil {
		d.finishRewrite(false, start)
		return ferr
	}

	// The base RDB carries the function libraries in its FUNCTION2 records, so the
	// fresh incr file starts empty. Later FUNCTION writes stream into it on their own.
	if merr := d.writeManifest(dir, baseName, incrName, newSeq); merr != nil {
		_ = incrFile.Close()
		d.finishRewrite(false, start)
		return merr
	}

	// Swap in the new files, then close and remove the old ones.
	d.aof.mu.Lock()
	d.aof.seq = newSeq
	d.aof.baseSize = baseSize
	d.aof.incrSize = 0
	d.aof.incrPath = incrPath
	d.aof.incrFile = incrFile
	d.aof.lastSelectedDB = -1
	d.aof.rewriteInProgress = false
	d.aof.curStartUnix = 0
	d.aof.lastStatus = "ok"
	d.aof.lastTimeSec = time.Since(start).Seconds()
	scheduled := d.aof.scheduled
	d.aof.scheduled = false
	d.aof.mu.Unlock()

	if oldIncr != nil {
		_ = oldIncr.Close()
	}
	if oldSeq > 0 {
		_ = os.Remove(filepath.Join(dir, fmt.Sprintf("%s.%d.base.rdb", basename, oldSeq)))
		_ = os.Remove(filepath.Join(dir, fmt.Sprintf("%s.%d.incr.aof", basename, oldSeq)))
	}
	_ = scheduled // a scheduled follow-up rewrite is satisfied by this one
	return nil
}

// finishRewrite records a failed rewrite and clears the in-progress flag.
func (d *Dispatcher) finishRewrite(ok bool, start time.Time) {
	d.aof.mu.Lock()
	d.aof.rewriteInProgress = false
	d.aof.curStartUnix = 0
	d.aof.lastTimeSec = time.Since(start).Seconds()
	if ok {
		d.aof.lastStatus = "ok"
	} else {
		d.aof.lastStatus = "err"
	}
	d.aof.mu.Unlock()
}

// writeManifest writes the multi-part AOF manifest atomically.
func (d *Dispatcher) writeManifest(dir, base, incr string, seq int) error {
	content := fmt.Sprintf("file %s seq %d type b\nfile %s seq %d type i\n", base, seq, incr, seq)
	name := d.aofBasename() + ".manifest"
	target := filepath.Join(dir, name)
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(content); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, target)
}

// appendAOF writes one command (preceded by a SELECT when the database changed)
// to the current incr file.
func (d *Dispatcher) appendAOF(db int, argv [][]byte) {
	d.aof.mu.Lock()
	defer d.aof.mu.Unlock()
	if d.aof.incrFile == nil || d.aof.loading {
		return
	}
	var buf []byte
	if db != d.aof.lastSelectedDB {
		buf = appendRESPCommand(buf, [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))})
		d.aof.lastSelectedDB = db
	}
	buf = appendRESPCommand(buf, argv)
	n, err := d.aof.incrFile.Write(buf)
	if err != nil {
		d.aof.lastWriteStatus = "err"
		return
	}
	d.aof.incrSize += int64(n)
	d.aof.lastWriteStatus = "ok"
	d.aof.pendingSync = true
	// The always policy makes every write durable before the reply, so fsync the
	// incr file inline. The everysec policy defers to syncAOFCron and the no policy
	// leaves it to the OS, so both only mark the write pending here.
	if d.aofFsyncPolicy() == "always" && !d.aofSyncBlockedByRewrite() {
		if err := d.aof.incrFile.Sync(); err != nil {
			d.aof.lastWriteStatus = "err"
			return
		}
		d.aof.pendingSync = false
		d.aof.lastSync = time.Now()
	}
}

// aofFsyncPolicy returns the configured appendfsync policy, one of "always",
// "everysec", or "no".
func (d *Dispatcher) aofFsyncPolicy() string {
	return confValue(d.conf, "appendfsync", "everysec")
}

// aofSyncBlockedByRewrite reports whether a background fsync should be held off
// because a rewrite is running and no-appendfsync-on-rewrite is set, which is how
// Redis avoids blocking on a fsync while the rewrite is also doing disk IO. The
// caller must hold d.aof.mu.
func (d *Dispatcher) aofSyncBlockedByRewrite() bool {
	return d.aof.rewriteInProgress && d.confBool("no-appendfsync-on-rewrite", false)
}

// syncAOFCron runs from the background cron. Under the everysec policy it fsyncs
// the incr file when there are unsynced writes and a second has passed since the
// last fsync. The always policy syncs inline in appendAOF and the no policy leaves
// syncing to the OS, so both make this a no-op.
func (d *Dispatcher) syncAOFCron() {
	if d.aofFsyncPolicy() != "everysec" {
		return
	}
	d.aof.mu.Lock()
	defer d.aof.mu.Unlock()
	if d.aof.incrFile == nil || !d.aof.pendingSync || d.aofSyncBlockedByRewrite() {
		return
	}
	now := time.Now()
	if !d.aof.lastSync.IsZero() && now.Sub(d.aof.lastSync) < time.Second {
		return
	}
	if err := d.aof.incrFile.Sync(); err != nil {
		d.aof.lastWriteStatus = "err"
		return
	}
	d.aof.pendingSync = false
	d.aof.lastSync = now
}

// appendRESPCommand encodes one command as a RESP array of bulk strings.
func appendRESPCommand(b []byte, argv [][]byte) []byte {
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(argv)), 10)
	b = append(b, '\r', '\n')
	for _, a := range argv {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	return b
}

// rewriteForAOF returns the command to write to the AOF for a given write
// command. Most commands are propagated verbatim. The commands that carry a
// relative expiry are rewritten so the expiry is an absolute millisecond
// timestamp, otherwise a delayed replay would set a different expiry than the
// master did. This is what real Redis does for AOF and replication.
func rewriteForAOF(name string, argv [][]byte) [][]byte {
	// FUNCTION LOAD/DELETE/FLUSH/RESTORE propagate as themselves, except LOAD gets
	// the REPLACE flag so a replay over an existing library does not error. The
	// command verb sits in argv[0]; the dispatched subcommand name is just the bare
	// word ("load"), which is too generic to switch on here.
	if len(argv) >= 2 && strings.EqualFold(string(argv[0]), "function") {
		return rewriteFunctionForAOF(argv)
	}
	switch strings.ToLower(name) {
	case "expire", "pexpire", "expireat":
		if len(argv) < 3 {
			return argv
		}
		n, ok := parseInteger(argv[2])
		if !ok {
			return argv
		}
		var absMs int64
		switch strings.ToLower(name) {
		case "expire":
			absMs = time.Now().UnixMilli() + n*1000
		case "pexpire":
			absMs = time.Now().UnixMilli() + n
		case "expireat":
			absMs = n * 1000
		}
		return [][]byte{[]byte("PEXPIREAT"), argv[1], []byte(strconv.FormatInt(absMs, 10))}
	case "setex", "psetex":
		// SETEX key seconds value and PSETEX key milliseconds value both become
		// SET key value PXAT <absolute-ms>, the same shape real Redis propagates.
		if len(argv) < 4 {
			return argv
		}
		n, ok := parseInteger(argv[2])
		if !ok {
			return argv
		}
		absMs := time.Now().UnixMilli() + n*1000
		if strings.ToLower(name) == "psetex" {
			absMs = time.Now().UnixMilli() + n
		}
		return setPxat(argv[1], argv[3], absMs)
	case "set":
		// A SET that carries EX, PX, EXAT or PXAT is rewritten to
		// SET key value PXAT <absolute-ms>. The NX, XX and GET flags are dropped:
		// the master already decided the write happened, and replaying the
		// condition could behave differently. A SET without an expiry, including
		// one with KEEPTTL, is propagated verbatim.
		if len(argv) < 3 {
			return argv
		}
		absMs, ok := setAbsExpiry(argv[3:])
		if !ok {
			return argv
		}
		return setPxat(argv[1], argv[2], absMs)
	case "getex":
		// GETEX with an expiry option propagates as PEXPIREAT, with PERSIST as
		// PERSIST. GETEX with no option does not change anything so it never
		// reaches this path.
		if len(argv) < 3 {
			return argv
		}
		if strings.EqualFold(string(argv[2]), "persist") {
			return [][]byte{[]byte("PERSIST"), argv[1]}
		}
		absMs, ok := setAbsExpiry(argv[2:])
		if !ok {
			return argv
		}
		return [][]byte{[]byte("PEXPIREAT"), argv[1], []byte(strconv.FormatInt(absMs, 10))}
	default:
		return argv
	}
}

// rewriteFunctionForAOF rewrites a FUNCTION admin command for the AOF and the
// replication stream. FUNCTION LOAD <src> becomes FUNCTION LOAD REPLACE <src> so
// a replay over a library that already exists overwrites it instead of failing.
// A LOAD that already carries REPLACE, and every other FUNCTION write, is
// propagated verbatim.
func rewriteFunctionForAOF(argv [][]byte) [][]byte {
	if len(argv) >= 3 && strings.EqualFold(string(argv[1]), "load") {
		if strings.EqualFold(string(argv[2]), "replace") {
			return argv
		}
		out := make([][]byte, 0, len(argv)+1)
		out = append(out, argv[0], argv[1], []byte("REPLACE"))
		out = append(out, argv[2:]...)
		return out
	}
	return argv
}

// setPxat builds SET key value PXAT ms.
func setPxat(key, value []byte, absMs int64) [][]byte {
	return [][]byte{[]byte("SET"), key, value, []byte("PXAT"), []byte(strconv.FormatInt(absMs, 10))}
}

// setAbsExpiry scans SET-style options for an expiry token and returns its
// absolute millisecond timestamp. The second result is false when there is no
// expiry option, which means the command should be propagated verbatim.
func setAbsExpiry(opts [][]byte) (int64, bool) {
	for i := 0; i < len(opts); i++ {
		opt := strings.ToLower(string(opts[i]))
		switch opt {
		case "ex", "px", "exat", "pxat":
			if i+1 >= len(opts) {
				return 0, false
			}
			n, ok := parseInteger(opts[i+1])
			if !ok {
				return 0, false
			}
			switch opt {
			case "ex":
				return time.Now().UnixMilli() + n*1000, true
			case "px":
				return time.Now().UnixMilli() + n, true
			case "exat":
				return n * 1000, true
			case "pxat":
				return n, true
			}
		}
	}
	return 0, false
}

// checkAOFRewrite runs from the background cron. It starts an automatic rewrite
// when the incr file has grown past both the minimum size and the configured
// growth percentage versus the base.
func (d *Dispatcher) checkAOFRewrite() {
	if d.engine == nil || d.conf == nil || !d.aofEnabled() {
		return
	}
	d.aof.mu.Lock()
	inProgress := d.aof.rewriteInProgress
	incrSize := d.aof.incrSize
	baseSize := d.aof.baseSize
	inited := d.aof.incrPath != ""
	d.aof.mu.Unlock()
	if inProgress || !inited {
		return
	}

	pct, _ := strconv.ParseInt(confValue(d.conf, "auto-aof-rewrite-percentage", "100"), 10, 64)
	if pct <= 0 {
		return
	}
	minSize, _ := strconv.ParseInt(confValue(d.conf, "auto-aof-rewrite-min-size", "67108864"), 10, 64)
	if incrSize < minSize {
		return
	}
	if baseSize <= 0 {
		_ = d.rewriteAOF()
		return
	}
	if incrSize*100/baseSize >= pct {
		_ = d.rewriteAOF()
	}
}

// forceSyncAOF fsyncs the current incr file so the data written so far is durable
// on the local disk. WAITAOF uses it to satisfy a numlocal of 1 regardless of the
// configured appendfsync policy. It returns true when a fsync succeeded.
func (d *Dispatcher) forceSyncAOF() bool {
	d.aof.mu.Lock()
	defer d.aof.mu.Unlock()
	if d.aof.incrFile == nil {
		return false
	}
	if err := d.aof.incrFile.Sync(); err != nil {
		d.aof.lastWriteStatus = "err"
		return false
	}
	d.aof.pendingSync = false
	d.aof.lastSync = time.Now()
	return true
}

// closeAOF closes the incr file handle. The server calls it on shutdown.
func (d *Dispatcher) closeAOF() {
	d.aof.mu.Lock()
	defer d.aof.mu.Unlock()
	if d.aof.incrFile != nil {
		_ = d.aof.incrFile.Close()
		d.aof.incrFile = nil
	}
}
