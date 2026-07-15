// Lab: the INCR int64 shadow, on and off (spec 2064/sqlo1 doc 05
// section 2, milestone T1 lab 03).
//
// Doc 05 keeps int-shaped values as their canonical decimal string and
// gives keys that have seen INCR-family ops a header-cached int64 shadow,
// so a hot INCR is an add instead of parse, add, format. T1 slice 4
// bakes that design, and this lab prices what the shadow actually buys:
// the noshadow arm parses and reformats the decimal bytes on every op,
// the shadow arm adds to a cached int64 and pays one format per key per
// drain. Both arms drain identical decimal strings, so the store cost is
// shared and the delta is the shadow.
//
// Hot runs with every touched key resident, which is where the shadow
// can win; cold runs a capped resident model over a much larger
// keyspace, where the store read on every miss should drown the parse
// and the shadow should buy nothing. If the hot delta is small too,
// slice 4 ships without the shadow and its invalidation rules.
//
// B3 re-points the suite at Track B: the same counters model drives
// either arm through a narrow store surface, selected by -store. The
// A arm is the SQLite path as before; the B arm (store_b.go) keeps
// each counter as a plain record under its user key with the decimal
// bytes as the value, turns a flush into one DrainBatch, and points
// the checkpoint cadence at the store's own checkpoint, so one sweep
// prices the shadow on the backend that will actually carry it.
package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"
)

const (
	schemaSQL = `CREATE TABLE IF NOT EXISTS kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK (id = 0),
  hw INTEGER) WITHOUT ROWID;
INSERT OR IGNORE INTO meta (id, hw) VALUES (0, 0);`

	getSQL   = `SELECT v FROM kv WHERE k = ?1`
	putSQL   = `INSERT INTO kv (k, t, exp, gen, v, crc) VALUES (?1, 0, 0, 0, ?2, 0) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
	setHWSQL = `UPDATE meta SET hw = ?1 WHERE id = 0`
)

type config struct {
	dir      string
	store    string
	arm      string
	dist     string
	keys     int
	hotKeys  int
	hotOps   int
	coldOps  int
	resident int
	flushAt  int
}

// store is the backend arm under the counters: Track A rows or Track B
// records, one narrow surface so the shared model above prices both.
// get feeds the cold miss path and the oracle readback, flush lands one
// drain-shaped write set, and checkpoint is whatever the arm's WAL trim
// verb is.
type store interface {
	get(ki int) (v []byte, found bool, err error)
	flush(fs *flushSet) error
	checkpoint() error
	dataMB() float64
	close() error
}

// flushSet is one drain cycle as the model hands it down: dirty
// counters as their decimal bytes and the high-water sequence that
// must land atomically with them. Values may alias model buffers, so
// an arm must not retain them past flush.
type flushSet struct {
	seq  int64
	vals []valRow
}

type valRow struct {
	ki  int
	val []byte
}

func openStore(cfg config, path string, keys [][]byte) (store, error) {
	switch cfg.store {
	case "a":
		return openA(path, keys)
	case "b":
		return openB(path, keys)
	}
	return nil, fmt.Errorf("unknown store arm %q", cfg.store)
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	keys  [][]byte
	get1  *sqlite3.Stmt
	put1  *sqlite3.Stmt
	hw1   *sqlite3.Stmt
	stmts []*sqlite3.Stmt
}

func openA(path string, keys [][]byte) (*db, error) {
	conn, err := sqlite3.Open(path)
	if err != nil {
		return nil, err
	}
	pragmas := []string{
		"PRAGMA page_size = 8192",
		"PRAGMA auto_vacuum = INCREMENTAL",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = OFF",
		"PRAGMA wal_autocheckpoint = 0",
		"PRAGMA cache_size = -32768",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 0",
		"PRAGMA busy_timeout = 10000",
	}
	for _, p := range pragmas {
		if err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}
	if err := conn.Exec(schemaSQL); err != nil {
		conn.Close()
		return nil, err
	}
	d := &db{conn: conn, path: path, keys: keys}
	for _, s := range []struct {
		dst **sqlite3.Stmt
		sql string
	}{
		{&d.get1, getSQL},
		{&d.put1, putSQL},
		{&d.hw1, setHWSQL},
	} {
		stmt, _, err := conn.Prepare(s.sql)
		if err != nil {
			conn.Close()
			return nil, err
		}
		*s.dst = stmt
		d.stmts = append(d.stmts, stmt)
	}
	return d, nil
}

func (d *db) close() error {
	for _, s := range d.stmts {
		s.Close()
	}
	return d.conn.Close()
}

func stepReset(s *sqlite3.Stmt) (found bool, err error) {
	found = s.Step()
	err = s.Err()
	if rerr := s.Reset(); err == nil {
		err = rerr
	}
	return found, err
}

// get returns the stored decimal bytes, false for a missing key. The
// copy is deliberate: the blob column's bytes die at Reset.
func (d *db) get(ki int) ([]byte, bool, error) {
	if err := d.get1.BindBlob(1, d.keys[ki]); err != nil {
		return nil, false, err
	}
	var v []byte
	found := d.get1.Step()
	if found {
		v = slices.Clone(d.get1.ColumnBlob(0, nil))
	}
	err := d.get1.Err()
	if rerr := d.get1.Reset(); err == nil {
		err = rerr
	}
	return v, found, err
}

// flush is one drain-shaped transaction: every dirty counter's decimal
// bytes and the high-water row, committed together.
func (d *db) flush(fs *flushSet) error {
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return err
	}
	fail := func(err error) error { txn.Rollback(); return err }
	for _, v := range fs.vals {
		if err := d.put1.BindBlob(1, d.keys[v.ki]); err != nil {
			return fail(err)
		}
		if err := d.put1.BindBlob(2, v.val); err != nil {
			return fail(err)
		}
		if _, err := stepReset(d.put1); err != nil {
			return fail(err)
		}
	}
	if err := d.hw1.BindInt64(1, fs.seq); err != nil {
		return fail(err)
	}
	if _, err := stepReset(d.hw1); err != nil {
		return fail(err)
	}
	return txn.Commit()
}

func (d *db) checkpoint() error {
	return d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
}

func (d *db) dataMB() float64 { return fileMB(d.path) }

// entry is one resident key. The shadow arm keeps the counter as an
// int64 and formats at drain; the noshadow arm keeps the canonical
// decimal bytes and reparses them on every INCR, which is exactly the
// doc 05 trade under test.
type entry struct {
	shadow int64
	str    []byte
	dirty  bool
}

// counters is the resident model over the store: uncapped for the hot
// phase, capped with random clean eviction (dirty entries pinned until
// their flush) for the cold phase, the same shape sqlocache pinned.
// seq continues from wherever the preload left the high-water mark,
// because the store treats a stale sequence as an already-applied
// batch.
type counters struct {
	st      store
	arm     string
	m       map[int]*entry
	cap     int
	dirty   int
	flushAt int

	seq        int64
	flushes    int
	flushDur   []time.Duration
	storeReads int
}

func newCounters(st store, arm string, capN, flushAt int, seq int64) *counters {
	return &counters{st: st, arm: arm, m: map[int]*entry{}, cap: capN, flushAt: flushAt, seq: seq}
}

func (c *counters) evictOne() {
	for k, e := range c.m {
		if !e.dirty {
			delete(c.m, k)
			return
		}
	}
}

// incr is the operator under test. Misses read the row and parse either
// way; the arms differ in what a resident hit costs.
func (c *counters) incr(ki int) error {
	e, ok := c.m[ki]
	if !ok {
		v, found, err := c.st.get(ki)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("key %d missing from store", ki)
		}
		c.storeReads++
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return fmt.Errorf("key %d not an integer: %w", ki, err)
		}
		e = &entry{shadow: n, str: v}
		if c.cap > 0 && len(c.m) >= c.cap {
			c.evictOne()
		}
		c.m[ki] = e
	}
	if c.arm == "shadow" {
		e.shadow++
	} else {
		n, err := strconv.ParseInt(string(e.str), 10, 64)
		if err != nil {
			return err
		}
		e.str = strconv.AppendInt(e.str[:0], n+1, 10)
	}
	if !e.dirty {
		e.dirty = true
		c.dirty++
	}
	if c.dirty >= c.flushAt {
		return c.flush()
	}
	return nil
}

// flush drains every dirty counter as its decimal string in one write
// set; the shadow arm pays its one format per key here.
func (c *counters) flush() error {
	if c.dirty == 0 {
		return nil
	}
	t0 := time.Now()
	fs := flushSet{seq: c.seq + 1}
	for ki, e := range c.m {
		if !e.dirty {
			continue
		}
		if c.arm == "shadow" {
			e.str = strconv.AppendInt(e.str[:0], e.shadow, 10)
		}
		fs.vals = append(fs.vals, valRow{ki: ki, val: e.str})
		e.dirty = false
	}
	if err := c.st.flush(&fs); err != nil {
		return err
	}
	c.seq++
	c.dirty = 0
	if c.flushes++; c.flushes%8 == 0 {
		if err := c.st.checkpoint(); err != nil {
			return err
		}
	}
	c.flushDur = append(c.flushDur, time.Since(t0))
	return nil
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.StringVar(&cfg.store, "store", "a", "backend arm: a (SQLite rows) or b (Track B records)")
	flag.StringVar(&cfg.arm, "arm", "shadow", "arm: shadow or noshadow")
	flag.StringVar(&cfg.dist, "dist", "zipf", "key distribution: zipf or uniform")
	flag.IntVar(&cfg.keys, "keys", 1000000, "preloaded key count (cold keyspace)")
	flag.IntVar(&cfg.hotKeys, "hotkeys", 65536, "hot-phase keyspace (fully resident)")
	flag.IntVar(&cfg.hotOps, "hotops", 2000000, "hot-phase INCR count")
	flag.IntVar(&cfg.coldOps, "coldops", 200000, "cold-phase INCR count")
	flag.IntVar(&cfg.resident, "resident", 4096, "cold-phase resident entry cap")
	flag.IntVar(&cfg.flushAt, "flushat", 8192, "dirty counters per flush")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.hotKeys, cfg.hotOps, cfg.coldOps, cfg.resident, cfg.flushAt =
			20000, 1024, 50000, 5000, 256, 1024
	}
	if cfg.arm != "shadow" && cfg.arm != "noshadow" {
		fmt.Fprintln(os.Stderr, "intshadow: -arm must be shadow or noshadow")
		os.Exit(1)
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "intshadow:", err)
		os.Exit(1)
	}
}

type row struct {
	workload string
	ops      int
	dur      time.Duration
	p50, p99 time.Duration
	maxLat   time.Duration
	fileMB   float64
	vmhwmMB  float64
}

func runAll(cfg config, out io.Writer) error {
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "intshadow")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("intshadow-%s-%s.db", cfg.store, cfg.arm))
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".aki-wal"} {
		os.Remove(p)
	}

	keys := make([][]byte, cfg.keys)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "n:%09d", i)
	}
	st, err := openStore(cfg, path, keys)
	if err != nil {
		return err
	}
	defer st.close()

	// Preload every key with a random counter so the parse under test
	// works on realistic digit counts, not "0". The counters models
	// below continue from this sequence.
	start := time.Now()
	rng := rand.New(rand.NewSource(67))
	const batch = 4096
	seq := int64(0)
	for off := 0; off < cfg.keys; off += batch {
		seq++
		fs := flushSet{seq: seq}
		for i := off; i < min(off+batch, cfg.keys); i++ {
			fs.vals = append(fs.vals,
				valRow{ki: i, val: strconv.AppendInt(nil, rng.Int63n(1_000_000_000), 10)})
		}
		if err := st.flush(&fs); err != nil {
			return err
		}
		if (off/batch)%8 == 7 {
			if err := st.checkpoint(); err != nil {
				return err
			}
		}
	}
	if err := st.checkpoint(); err != nil {
		return err
	}
	emit(cfg, out, row{workload: "load", ops: cfg.keys, dur: time.Since(start),
		fileMB: st.dataMB()})

	// Hot phase: every touched key resident (warmed first), ops timed in
	// blocks of 1024 because a hot INCR is tens of nanoseconds and per-op
	// clock reads would drown it. Flush stalls land in the block tail.
	c := newCounters(st, cfg.arm, 0, cfg.flushAt, seq)
	hotPick := picker(cfg.dist, cfg.hotKeys, 89)
	for ki := range cfg.hotKeys {
		if err := c.incr(ki); err != nil {
			return err
		}
	}
	if err := c.flush(); err != nil {
		return err
	}
	c.flushDur = nil
	const block = 1024
	var blockLat []time.Duration
	start = time.Now()
	for done := 0; done < cfg.hotOps; done += block {
		n := min(block, cfg.hotOps-done)
		t0 := time.Now()
		for range n {
			if err := c.incr(hotPick()); err != nil {
				return err
			}
		}
		blockLat = append(blockLat, time.Since(t0)/time.Duration(n))
	}
	hotWall := time.Since(start)
	if err := c.flush(); err != nil {
		return err
	}
	p50, p99, maxLat := percentiles(blockLat)
	emit(cfg, out, row{workload: "hot-incr", ops: cfg.hotOps, dur: hotWall,
		p50: p50, p99: p99, maxLat: maxLat})
	emitFlush(cfg, out, "hot-flush", c, st)

	// Cold phase: a capped resident model over the whole keyspace, so
	// most INCRs pay the store read and parse before the add. Per-op
	// timing is fine at microsecond scale.
	c = newCounters(st, cfg.arm, cfg.resident, cfg.flushAt, c.seq)
	coldPick := picker(cfg.dist, cfg.keys, 97)
	var lats []time.Duration
	start = time.Now()
	for range cfg.coldOps {
		t0 := time.Now()
		if err := c.incr(coldPick()); err != nil {
			return err
		}
		lats = append(lats, time.Since(t0))
	}
	coldWall := time.Since(start)
	if err := c.flush(); err != nil {
		return err
	}
	p50, p99, maxLat = percentiles(lats)
	emit(cfg, out, row{workload: "cold-incr", ops: cfg.coldOps, dur: coldWall,
		p50: p50, p99: p99, maxLat: maxLat})
	fmt.Fprintf(os.Stderr, "cold store reads: %d of %d ops\n", c.storeReads, cfg.coldOps)
	emitFlush(cfg, out, "cold-flush", c, st)
	return nil
}

func emitFlush(cfg config, out io.Writer, name string, c *counters, st store) {
	var total, worst time.Duration
	for _, fd := range c.flushDur {
		total += fd
		if fd > worst {
			worst = fd
		}
	}
	emit(cfg, out, row{workload: name, ops: len(c.flushDur), dur: total,
		maxLat: worst, fileMB: st.dataMB(), vmhwmMB: vmhwmMB()})
}

func picker(dist string, n int, seed int64) func() int {
	rng := rand.New(rand.NewSource(seed))
	if dist == "zipf" {
		z := rand.NewZipf(rng, 1.1, 1, uint64(n-1))
		return func() int { return int(z.Uint64()) }
	}
	return func() int { return rng.Intn(n) }
}

func percentiles(all []time.Duration) (p50, p99, max time.Duration) {
	if len(all) == 0 {
		return 0, 0, 0
	}
	s := slices.Clone(all)
	slices.Sort(s)
	return s[len(s)/2], s[len(s)*99/100], s[len(s)-1]
}

func fileMB(path string) float64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(fi.Size()) / (1 << 20)
}

// vmhwmMB reads the process's peak resident set from /proc on Linux and
// reports zero elsewhere; the gate box is the only place the number is
// read for a verdict.
func vmhwmMB() float64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			fields := strings.Fields(rest)
			if len(fields) >= 1 {
				kb, err := strconv.ParseFloat(fields[0], 64)
				if err == nil {
					return kb / 1024
				}
			}
		}
	}
	return 0
}

func emit(cfg config, out io.Writer, r row) {
	nsPerOp := float64(r.dur.Nanoseconds()) / float64(max(r.ops, 1))
	opsPerS := float64(r.ops) / max(r.dur.Seconds(), 1e-9)
	fmt.Fprintf(out, "%s,%s,%s,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.1f\n",
		cfg.store, cfg.arm, cfg.dist, cfg.keys,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.fileMB, r.vmhwmMB)
}
