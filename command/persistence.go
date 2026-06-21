package command

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tamnd/aki/rdb"
)

// This file implements the RDB save surface (spec 2064 doc 17 section 6): SAVE,
// BGSAVE, LASTSAVE, the automatic save points, and the INFO persistence state.
// aki does not fork. SAVE writes the dump.rdb inline, BGSAVE copies the dataset
// under the engine lock and writes the file from a background goroutine while new
// writes proceed.

// firstSyntheticPID is where the fake BGSAVE child PID counter starts. aki has no
// child process, but INFO persistence reports rdb_child_pid, so each BGSAVE hands
// out the next number from here.
const firstSyntheticPID = 10000

// persistState holds the save bookkeeping. Every field is guarded by mu because
// the background BGSAVE goroutine and the command handlers both touch it.
type persistState struct {
	mu sync.Mutex

	dirty        int64  // writes since the last successful save
	lastSaveUnix int64  // unix seconds of the last successful save, 0 if never
	saves        int64  // total successful SAVE and BGSAVE operations
	inProgress   bool   // a BGSAVE is running
	lastStatus   string // "ok" or "err", empty before the first save
	lastTimeSec  float64
	curStartUnix int64 // unix seconds the current BGSAVE began, 0 if none
	nextPID      int
}

// markDirty records one write since the last save. The save-point check reads the
// counter to decide whether an automatic BGSAVE is due.
func (p *persistState) markDirty() {
	p.mu.Lock()
	p.dirty++
	p.mu.Unlock()
}

// beginSave marks a save as started and returns false if one is already running.
// dirtyAtStart is the dirty count captured so it can be subtracted on success, so
// writes that land during the save are not lost from the next save's trigger.
func (p *persistState) beginSave() (started bool, dirtyAtStart int64, pid int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inProgress {
		return false, 0, 0
	}
	p.inProgress = true
	p.curStartUnix = time.Now().Unix()
	if p.nextPID == 0 {
		p.nextPID = firstSyntheticPID
	}
	pid = p.nextPID
	p.nextPID++
	return true, p.dirty, pid
}

// finishSave records the outcome of a save and clears the in-progress flag.
func (p *persistState) finishSave(ok bool, dirtyAtStart int64, elapsed time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inProgress = false
	p.curStartUnix = 0
	p.lastTimeSec = elapsed.Seconds()
	if ok {
		p.lastStatus = "ok"
		p.dirty -= dirtyAtStart
		if p.dirty < 0 {
			p.dirty = 0
		}
		p.lastSaveUnix = time.Now().Unix()
		p.saves++
	} else {
		p.lastStatus = "err"
	}
}

// persistenceCommands registers SAVE, BGSAVE, and LASTSAVE.
func persistenceCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "save", Group: GroupServer, Since: "1.0.0",
			Arity: 1, Flags: FlagAdmin | FlagNoScript, Handler: handleSave},
		{Name: "bgsave", Group: GroupServer, Since: "1.0.0",
			Arity: -1, Flags: FlagAdmin | FlagNoScript, Handler: handleBgsave},
		{Name: "lastsave", Group: GroupServer, Since: "1.0.0",
			Arity: 1, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleLastsave},
	}
}

// handleSave writes the dump.rdb synchronously and replies +OK, or an error if a
// background save is already running or the write fails.
func handleSave(ctx *Ctx) {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return
	}
	started, dirtyAtStart, _ := ctx.d.persist.beginSave()
	if !started {
		ctx.enc().WriteError("ERR Background save already in progress")
		return
	}
	start := time.Now()
	err := ctx.d.writeRDB()
	ctx.d.persist.finishSave(err == nil, dirtyAtStart, time.Since(start))
	if err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	ctx.enc().WriteStatus("OK")
}

// handleBgsave starts a background save and replies right away. The SCHEDULE
// keyword is accepted; when a save is already running it reports that the save is
// scheduled rather than erroring, matching redis.
func handleBgsave(ctx *Ctx) {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return
	}
	schedule := false
	if len(ctx.Argv) == 2 && strings.EqualFold(string(ctx.Argv[1]), "SCHEDULE") {
		schedule = true
	} else if len(ctx.Argv) > 1 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}

	started := ctx.d.startBgsave()
	if !started {
		if schedule {
			ctx.enc().WriteStatus("Background saving scheduled")
			return
		}
		ctx.enc().WriteError("ERR Background save already in progress")
		return
	}
	ctx.enc().WriteStatus("Background saving started")
}

// startBgsave snapshots the dataset under the engine lock and writes it from a
// goroutine. It reports false when a save is already running. The snapshot copy is
// taken before the goroutine starts so the on-disk file reflects the dataset at
// the moment BGSAVE was issued.
func (d *Dispatcher) startBgsave() bool {
	started, dirtyAtStart, _ := d.persist.beginSave()
	if !started {
		return false
	}
	snap, err := d.engine.snapshotAll()
	if err != nil {
		d.persist.finishSave(false, dirtyAtStart, 0)
		return true
	}
	dir, file := d.rdbPath()
	checksum := d.conf == nil || confValue(d.conf, "rdbchecksum", "yes") != "no"
	start := time.Now()
	go func() {
		werr := writeRDBFile(snap, dir, file, checksum)
		d.persist.finishSave(werr == nil, dirtyAtStart, time.Since(start))
	}()
	return true
}

// writeRDB builds the snapshot and writes the dump.rdb inline. SAVE uses it.
func (d *Dispatcher) writeRDB() error {
	snap, err := d.engine.snapshotAll()
	if err != nil {
		return err
	}
	dir, file := d.rdbPath()
	checksum := d.conf == nil || confValue(d.conf, "rdbchecksum", "yes") != "no"
	return writeRDBFile(snap, dir, file, checksum)
}

// rdbPath returns the configured directory and dump file name.
func (d *Dispatcher) rdbPath() (dir, file string) {
	dir = "."
	file = "dump.rdb"
	if d.conf != nil {
		dir = confValue(d.conf, "dir", ".")
		file = confValue(d.conf, "dbfilename", "dump.rdb")
	}
	return dir, file
}

// writeRDBFile marshals the snapshot and writes it to dir/file atomically: it
// writes a temp file in the same directory, fsyncs it, then renames over the
// target so a reader never sees a half-written dump.
func writeRDBFile(snap rdb.Snapshot, dir, file string, checksum bool) error {
	blob, err := rdb.MarshalFile(snap)
	if err != nil {
		return err
	}
	if !checksum {
		// rdbchecksum no means the trailing 8 bytes are zero and not validated.
		for i := len(blob) - 8; i < len(blob); i++ {
			blob[i] = 0
		}
	}
	target := filepath.Join(dir, file)
	tmp, err := os.CreateTemp(dir, file+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(blob); err != nil {
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
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// handleLastsave returns the unix timestamp of the last successful save, 0 if none.
func handleLastsave(ctx *Ctx) {
	ctx.d.persist.mu.Lock()
	ts := ctx.d.persist.lastSaveUnix
	ctx.d.persist.mu.Unlock()
	ctx.enc().WriteInteger(ts)
}

// checkSavePoints runs from the background cron. It triggers a BGSAVE when any
// configured save point is satisfied: at least min-changes writes since the last
// save and at least interval seconds elapsed since the last save.
func (d *Dispatcher) checkSavePoints() {
	if d.engine == nil || d.conf == nil {
		return
	}
	points := parseSavePoints(confValue(d.conf, "save", ""))
	if len(points) == 0 {
		return
	}
	d.persist.mu.Lock()
	if d.persist.inProgress {
		d.persist.mu.Unlock()
		return
	}
	dirty := d.persist.dirty
	last := d.persist.lastSaveUnix
	d.persist.mu.Unlock()

	if dirty == 0 {
		return
	}
	// Before the first save, measure the interval from process start so a fresh
	// server still saves once its first save point window passes.
	ref := last
	if ref == 0 {
		ref = d.startTime.Unix()
	}
	now := time.Now().Unix()
	for _, sp := range points {
		if dirty >= sp.changes && now-ref >= sp.seconds {
			d.startBgsave()
			return
		}
	}
}

// savePoint is one "save <seconds> <changes>" rule.
type savePoint struct {
	seconds int64
	changes int64
}

// parseSavePoints reads the space-separated "save" directive into rules. An empty
// string, or the explicit "" disable form, yields no rules.
func parseSavePoints(s string) []savePoint {
	fields := strings.Fields(s)
	var out []savePoint
	for i := 0; i+1 < len(fields); i += 2 {
		sec, err1 := strconv.ParseInt(fields[i], 10, 64)
		chg, err2 := strconv.ParseInt(fields[i+1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, savePoint{seconds: sec, changes: chg})
	}
	return out
}

// confValue reads a directive value with a fallback, the package-level form of
// Ctx.confStr for code paths that have only the dispatcher.
func confValue(cs *configStore, name, def string) string {
	if v, ok := cs.get(name); ok {
		return v
	}
	return def
}
