package command

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/resp"
)

// This file implements loading a dataset back from the appendonlydir (spec 2064
// doc 17 section 8 and section 9.1). The layout written by aof.go is read here:
// the manifest names a base RDB and one or more incremental files, the base is
// loaded as a snapshot, and each incremental file is replayed as a stream of RESP
// commands. DEBUG LOADAOF and startup both go through loadAOF.

// manifestEntry is one line of the AOF manifest: a file name, its sequence
// number, and its type ("b" for the base RDB, "i" for an incremental file).
type manifestEntry struct {
	name string
	seq  int
	typ  string
}

// aofManifestExists reports whether the appendonlydir holds a manifest, the
// signal that there is an AOF to load rather than create.
func (d *Dispatcher) aofManifestExists() bool {
	manifest := filepath.Join(d.aofDir(), d.aofBasename()+".manifest")
	_, err := os.Stat(manifest)
	return err == nil
}

// parseManifest reads the manifest into its entries. Each line is "file <name>
// seq <n> type <b|i>". Blank lines are skipped and any other shape is an error so
// a corrupt manifest does not load a partial dataset.
func parseManifest(data []byte) ([]manifestEntry, error) {
	var entries []manifestEntry
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 6 || fields[0] != "file" || fields[2] != "seq" || fields[4] != "type" {
			return nil, fmt.Errorf("bad manifest line %q", line)
		}
		seq, err := strconv.Atoi(fields[3])
		if err != nil {
			return nil, fmt.Errorf("bad manifest seq %q", fields[3])
		}
		entries = append(entries, manifestEntry{name: fields[1], seq: seq, typ: fields[5]})
	}
	return entries, nil
}

// loadAOF flushes the dataset, loads the base RDB, and replays the incremental
// files named by the manifest. After a successful load it points the AOF state at
// the latest incr file and reopens it for appends so new writes continue the log.
func (d *Dispatcher) loadAOF() error {
	if d.engine == nil {
		return errors.New("this server has no keyspace")
	}
	dir := d.aofDir()
	manifest := filepath.Join(dir, d.aofBasename()+".manifest")
	data, err := os.ReadFile(manifest)
	if err != nil {
		return err
	}
	entries, err := parseManifest(data)
	if err != nil {
		return err
	}

	d.aof.mu.Lock()
	d.aof.loading = true
	d.aof.mu.Unlock()
	defer func() {
		d.aof.mu.Lock()
		d.aof.loading = false
		d.aof.mu.Unlock()
	}()

	// Start from an empty dataset so the AOF is the authoritative source for this
	// load, the same effect a crash recovery from the AOF would have.
	if err := d.flushAllDatabases(); err != nil {
		return err
	}

	for _, e := range entries {
		if e.typ != "b" {
			continue
		}
		if err := d.loadAOFBase(filepath.Join(dir, e.name)); err != nil {
			return err
		}
	}

	conn := networking.NewOfflineConn()
	sess := &session{authenticated: true}
	conn.SetSession(sess)
	ctx := &Ctx{Conn: conn, d: d, sess: sess}
	var lastIncr manifestEntry
	for _, e := range entries {
		if e.typ != "i" {
			continue
		}
		blob, rerr := os.ReadFile(filepath.Join(dir, e.name))
		if rerr != nil {
			return rerr
		}
		if perr := d.replayAOF(ctx, blob); perr != nil {
			return perr
		}
		lastIncr = e
	}

	d.adoptLoadedAOF(dir, entries, lastIncr)
	return nil
}

// loadAOFBase loads one base RDB file into every database, replacing whatever is
// there. The base is written by the same file codec SAVE uses, so it round-trips
// the snapshot exactly.
func (d *Dispatcher) loadAOFBase(path string) error {
	blob, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		return err
	}
	return d.engine.updateKeyspace(func(ks *keyspace.Keyspace) error {
		_, lerr := LoadSnapshot(ks, snap, -1, true)
		return lerr
	})
}

// flushAllDatabases empties every database under the engine lock.
func (d *Dispatcher) flushAllDatabases() error {
	return d.engine.updateKeyspace(func(ks *keyspace.Keyspace) error {
		for i := range ks.DBCount() {
			db, err := ks.DB(i)
			if err != nil {
				return err
			}
			db.Flush()
		}
		return nil
	})
}

// replayAOF parses a buffer of RESP commands and applies each one through the
// normal command path on the replay connection. The loading flag set by the
// caller keeps these writes from being propagated back into the incr file.
func (d *Dispatcher) replayAOF(ctx *Ctx, data []byte) error {
	pos := 0
	for pos < len(data) {
		val, next, err := resp.Decode(data, pos)
		if err != nil {
			if errors.Is(err, resp.ErrNeedMore) {
				return fmt.Errorf("truncated AOF command at offset %d", pos)
			}
			return err
		}
		pos = next
		if val.Type != resp.TypeArray || len(val.Elems) == 0 {
			continue
		}
		argv := make([][]byte, len(val.Elems))
		for i, elem := range val.Elems {
			argv[i] = elem.Str
		}
		d.replayOne(ctx, argv)
	}
	return nil
}

// replayOne dispatches a single replayed command. An unknown command or a bad
// arity is skipped rather than aborting the whole load, matching the lenient
// stance Redis takes when its own AOF holds a command an older build does not
// know.
func (d *Dispatcher) replayOne(ctx *Ctx, argv [][]byte) {
	name := strings.ToLower(string(argv[0]))
	cmd, err := d.table.lookup(name, argv)
	if err != nil {
		return
	}
	if !checkArity(cmd, len(argv)) {
		return
	}
	ctx.Argv = argv
	d.runCommand(ctx, cmd)
}

// adoptLoadedAOF records the sequence number, base size, and incr file the load
// finished with, and reopens that incr file for appends so new writes extend it.
func (d *Dispatcher) adoptLoadedAOF(dir string, entries []manifestEntry, lastIncr manifestEntry) {
	var baseSize int64
	seq := 0
	for _, e := range entries {
		if e.seq > seq {
			seq = e.seq
		}
		if e.typ == "b" {
			if info, err := os.Stat(filepath.Join(dir, e.name)); err == nil {
				baseSize = info.Size()
			}
		}
	}

	d.aof.mu.Lock()
	defer d.aof.mu.Unlock()
	if d.aof.incrFile != nil {
		_ = d.aof.incrFile.Close()
		d.aof.incrFile = nil
	}
	d.aof.seq = seq
	d.aof.baseSize = baseSize
	d.aof.lastSelectedDB = -1
	if lastIncr.name == "" {
		return
	}
	incrPath := filepath.Join(dir, lastIncr.name)
	if info, err := os.Stat(incrPath); err == nil {
		d.aof.incrSize = info.Size()
	}
	if f, err := os.OpenFile(incrPath, os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		d.aof.incrPath = incrPath
		d.aof.incrFile = f
	}
}
