package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tamnd/aki/networking"
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

	// incrBuf holds AOF records appended since the last flush to incrFile. Under
	// the everysec and no policies a record is appended here and the buffer is
	// written to the file in one syscall at the end of a connection's drain pass
	// (aki's beforeSleep) and by the cron, so a pipeline of N writes costs one
	// write() instead of N. The always policy flushes inline to stay durable
	// before the reply. flushIncrLocked is the only place these bytes reach the OS.
	incrBuf []byte

	pendingSync bool      // bytes written since the last fsync, drives everysec
	lastSync    time.Time // time of the last incr-file fsync

	// Group-commit bookkeeping for the always policy. writeSeq is bumped under mu
	// for every appended record; syncedSeq is the highest seq a completed fsync has
	// made durable. While syncing is true one goroutine is inside fsync with mu
	// released, and the others wait on syncCond. A single fsync makes every record
	// written before it began durable, so N concurrent writers share one fsync
	// instead of paying one each. syncCond is created lazily under mu.
	writeSeq  uint64
	syncedSeq uint64
	syncing   bool
	syncCond  *sync.Cond

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

// aofEnabled reports whether appendonly is configured on. It reads the config
// store's atomic mirror, so this stays lock-free on the per-command write path.
func (d *Dispatcher) aofEnabled() bool {
	return d.conf != nil && d.conf.appendOnly()
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

	// Swap in the new files, then close and remove the old ones. Flush any
	// records still buffered for the old incr file first: the new base RDB already
	// captures the keyspace, but flushing keeps the old file consistent for the
	// brief window before it is removed and matches the pre-buffering behaviour
	// where each record was written to the old file as it arrived. The fresh incr
	// file starts with an empty buffer.
	d.aof.mu.Lock()
	d.flushIncrLocked()
	d.aof.incrBuf = d.aof.incrBuf[:0]
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

// aofBufFlushThreshold bounds how many buffered AOF bytes accumulate before
// appendAOF flushes them to the incr file mid-pipeline. A single huge pipeline
// would otherwise pin the whole thing in memory until its drain pass ends.
const aofBufFlushThreshold = 1 << 20 // 1 MiB

// appendAOF records one command (preceded by a SELECT when the database changed)
// for the current incr file. The record is appended to incrBuf; flushIncrLocked
// later writes the buffer to the file. The always policy flushes and fsyncs
// inline so the write is durable before its reply, matching Redis. The everysec
// and no policies leave the record in the buffer for the drain-end flush and the
// cron, which turns a pipeline of N writes into one write() syscall.
func (d *Dispatcher) appendAOF(db int, argv [][]byte) {
	d.aof.mu.Lock()
	defer d.aof.mu.Unlock()
	if d.aof.incrFile == nil || d.aof.loading {
		return
	}
	// aof-timestamp-enabled prefixes each record with a #TS:<unix_ms> comment line.
	// The loader skips comment lines, so the annotation does not change the replay.
	if d.conf != nil && d.conf.aofTimestampEnabled() {
		d.aof.incrBuf = append(d.aof.incrBuf, "#TS:"...)
		d.aof.incrBuf = strconv.AppendInt(d.aof.incrBuf, time.Now().UnixMilli(), 10)
		d.aof.incrBuf = append(d.aof.incrBuf, '\r', '\n')
	}
	if db != d.aof.lastSelectedDB {
		d.aof.incrBuf = appendRESPCommand(d.aof.incrBuf, [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))})
		d.aof.lastSelectedDB = db
	}
	d.aof.incrBuf = appendRESPCommand(d.aof.incrBuf, argv)
	d.aof.pendingSync = true
	d.aof.writeSeq++
	mySeq := d.aof.writeSeq

	if d.aofFsyncPolicy() == "always" && !d.aofSyncBlockedByRewrite() {
		// Durable before the reply: write the buffered records and fsync now.
		// Concurrent writers still share one fsync through groupSyncLocked, so a
		// burst pays one fsync, not one each.
		d.flushIncrLocked()
		d.groupSyncLocked(mySeq)
		return
	}
	// Bound the buffer so one enormous pipeline cannot hold unbounded memory.
	if len(d.aof.incrBuf) >= aofBufFlushThreshold {
		d.flushIncrLocked()
	}
}

// bufferAOFRecord appends one command's AOF record to the connection's own buffer
// without taking the AOF lock. It mirrors appendAOF's encoding (an optional #TS
// timestamp, a SELECT when the database changed, then the command) but writes into
// sess.aofBuf instead of the shared incr buffer. spliceSessionAOFLocked later
// moves the whole buffer into the shared buffer under one lock, so a pipeline of N
// writes contends for the AOF lock once instead of N times.
//
// Each freshly emptied buffer starts with sess.aofBufDB == -1, so its first record
// always leads with a SELECT. That makes every spliced segment self-describing:
// it sets its own database regardless of what the previous connection's segment
// left selected, which is what keeps interleaved multi-connection writes replaying
// into the right databases. The cost is one redundant SELECT per drain, negligible
// against a pipeline and absent from the common single-drain-per-command path only
// in that it adds bytes, never wrong ones.
//
// Caller must use this only for online connections under a deferred policy: the
// flush happens in OnBatchComplete, which a script or replay connection never
// reaches, and the always policy must stay durable before its reply.
func (d *Dispatcher) bufferAOFRecord(sess *session, db int, argv [][]byte) {
	if d.conf != nil && d.conf.aofTimestampEnabled() {
		sess.aofBuf = append(sess.aofBuf, "#TS:"...)
		sess.aofBuf = strconv.AppendInt(sess.aofBuf, time.Now().UnixMilli(), 10)
		sess.aofBuf = append(sess.aofBuf, '\r', '\n')
	}
	if db != sess.aofBufDB {
		sess.aofBuf = appendRESPCommand(sess.aofBuf, [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))})
		sess.aofBufDB = db
	}
	sess.aofBuf = appendRESPCommand(sess.aofBuf, argv)
}

// spliceSessionAOFLocked moves a connection's buffered AOF records into the shared
// incr buffer and resets the connection buffer. The caller holds d.aof.mu. It sets
// the shared lastSelectedDB to the segment's final database, so a later inline
// appendAOF on the same database skips a redundant SELECT, and advances writeSeq so
// the everysec cron treats the spliced records as unsynced and fsyncs them.
func (d *Dispatcher) spliceSessionAOFLocked(sess *session) {
	if sess == nil || len(sess.aofBuf) == 0 || d.aof.incrFile == nil || d.aof.loading {
		// Drop the buffer when there is no live incr file (AOF off or loading): the
		// records have no destination and must not survive into a later session.
		if sess != nil {
			sess.aofBuf = sess.aofBuf[:0]
			sess.aofBufDB = -1
		}
		return
	}
	d.aof.incrBuf = append(d.aof.incrBuf, sess.aofBuf...)
	d.aof.lastSelectedDB = sess.aofBufDB
	d.aof.pendingSync = true
	d.aof.writeSeq++
	sess.aofBuf = sess.aofBuf[:0]
	sess.aofBufDB = -1
}

// flushSessionAOF splices a connection's buffered records into the shared buffer
// and leaves the shared buffer ready for the next sync. Commands that must observe
// their own connection's earlier pipelined writes as durable (WAITAOF, DEBUG
// LOADAOF) call it before they read or sync the incr file. It takes the AOF lock.
func (d *Dispatcher) flushSessionAOF(sess *session) {
	d.aof.mu.Lock()
	d.spliceSessionAOFLocked(sess)
	d.aof.mu.Unlock()
}

// flushIncrLocked writes the buffered AOF records to the incr file in a single
// syscall and clears the buffer. The caller holds d.aof.mu. It is the only point
// where buffered records reach the OS, so every fsync and close path calls it
// first to keep "flush precedes sync" true. On a short or failed write it keeps
// the unwritten tail so the next flush retries rather than dropping records.
func (d *Dispatcher) flushIncrLocked() {
	if len(d.aof.incrBuf) == 0 || d.aof.incrFile == nil {
		return
	}
	n, err := d.aof.incrFile.Write(d.aof.incrBuf)
	if n > 0 {
		d.aof.incrSize += int64(n)
	}
	if err != nil {
		d.aof.lastWriteStatus = "err"
		d.aof.incrBuf = d.aof.incrBuf[:copy(d.aof.incrBuf, d.aof.incrBuf[n:])]
		return
	}
	d.aof.lastWriteStatus = "ok"
	d.aof.incrBuf = d.aof.incrBuf[:0]
}

// FlushAOF writes any buffered AOF records to the incr file. The networking serve
// loop calls it once after draining each batch of pipelined commands, before the
// connection blocks for more input, so a pipeline's records reach the file in one
// write() syscall. It is aki's beforeSleep flush.
func (d *Dispatcher) FlushAOF() {
	d.aof.mu.Lock()
	d.flushIncrLocked()
	d.aof.mu.Unlock()
}

// OnBatchComplete satisfies networking.BatchHandler. It runs once per connection
// after a pipeline batch is drained: it splices the connection's buffered AOF
// records into the shared incr buffer and writes the buffer to the incr file, both
// under a single AOF lock. So a pipeline of N writes pays one lock acquisition and
// one write() syscall here instead of one per command.
func (d *Dispatcher) OnBatchComplete(c *networking.Conn) {
	sess, _ := c.Session().(*session)
	// Apply any increments deferred during this drain before the AOF splice, so their
	// replies land at the tail of the pipeline and their propagation records are in
	// the session buffer the splice below picks up.
	if sess != nil && len(sess.incrPend) > 0 {
		d.flushIncrPending(c, sess)
	}
	if sess != nil && len(sess.pushPend) > 0 {
		d.flushPushPending(c, sess)
	}
	d.aof.mu.Lock()
	d.spliceSessionAOFLocked(sess)
	d.flushIncrLocked()
	// Under the always policy the batch must be durable before its replies leave
	// the socket. serve() runs this hook before it flushes the reply buffer, so a
	// single group-commit fsync here makes the whole drained pipeline durable at
	// once. That matches Redis, which fsyncs the AOF once per event-loop iteration
	// in beforeSleep and only then writes the replies, instead of paying one fsync
	// per command. groupSyncLocked is a no-op when there is nothing new to sync, so
	// a read-only batch costs one comparison.
	if d.aofFsyncPolicy() == "always" && !d.aofSyncBlockedByRewrite() {
		d.groupSyncLocked(d.aof.writeSeq)
	}
	d.aof.mu.Unlock()
}

// The serve loop calls OnBatchComplete through this interface, so a mismatch must
// fail the build rather than silently fall back to cron-only flushing.
var _ networking.BatchHandler = (*Dispatcher)(nil)

// groupSyncLocked blocks until the incr file has been fsynced through at least
// seq, coalescing concurrent callers. While one goroutine performs the fsync with
// the mutex released, the others wait on syncCond; the fsync makes every record
// written before it began (writeSeq at its start) durable, so they all return
// together. Caller holds d.aof.mu and it is still held on return.
func (d *Dispatcher) groupSyncLocked(seq uint64) {
	if d.aof.syncCond == nil {
		d.aof.syncCond = sync.NewCond(&d.aof.mu)
	}
	for d.aof.syncedSeq < seq {
		if d.aof.syncing {
			d.aof.syncCond.Wait()
			continue
		}
		// Become the syncer for everything written so far.
		d.aof.syncing = true
		target := d.aof.writeSeq
		f := d.aof.incrFile
		d.aof.mu.Unlock()
		serr := f.Sync()
		d.aof.mu.Lock()
		d.aof.syncing = false
		if serr != nil {
			d.aof.lastWriteStatus = "err"
			// A failed fsync did not advance durability. Wake the waiters so one of
			// them retries as syncer rather than blocking forever, and give up this
			// caller's wait: its write is not durable and lastWriteStatus records it.
			d.aof.syncCond.Broadcast()
			return
		}
		if target > d.aof.syncedSeq {
			d.aof.syncedSeq = target
		}
		d.aof.pendingSync = d.aof.writeSeq > d.aof.syncedSeq
		d.aof.lastSync = time.Now()
		d.aof.syncCond.Broadcast()
	}
}

// aofFsyncPolicy returns the configured appendfsync policy, one of "always",
// "everysec", or "no". It reads the config store's atomic mirror, so the
// per-command write path that consults the policy stays lock-free.
func (d *Dispatcher) aofFsyncPolicy() string {
	if d.conf == nil {
		return "everysec"
	}
	return d.conf.fsyncPolicy()
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
	d.flushIncrLocked()
	if err := d.aof.incrFile.Sync(); err != nil {
		d.aof.lastWriteStatus = "err"
		return
	}
	d.aof.pendingSync = false
	d.aof.syncedSeq = d.aof.writeSeq
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
	d.flushIncrLocked()
	if err := d.aof.incrFile.Sync(); err != nil {
		d.aof.lastWriteStatus = "err"
		return false
	}
	d.aof.pendingSync = false
	d.aof.syncedSeq = d.aof.writeSeq
	d.aof.lastSync = time.Now()
	return true
}

// closeAOF closes the incr file handle. The server calls it on shutdown.
func (d *Dispatcher) closeAOF() {
	d.aof.mu.Lock()
	defer d.aof.mu.Unlock()
	if d.aof.incrFile != nil {
		d.flushIncrLocked()
		_ = d.aof.incrFile.Close()
		d.aof.incrFile = nil
	}
}
