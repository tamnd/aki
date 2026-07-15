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
// keyspace, where the SQL read on every miss should drown the parse and
// the shadow should buy nothing. If the hot delta is small too, slice 4
// ships without the shadow and its invalidation rules.
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
	arm      string
	dist     string
	keys     int
	hotKeys  int
	hotOps   int
	coldOps  int
	resident int
	flushAt  int
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	get1  *sqlite3.Stmt
	put1  *sqlite3.Stmt
	hw1   *sqlite3.Stmt
	stmts []*sqlite3.Stmt
}

func openDB(path string) (*db, error) {
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
	d := &db{conn: conn, path: path}
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

func (d *db) get(key []byte) ([]byte, bool, error) {
	if err := d.get1.BindBlob(1, key); err != nil {
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
type counters struct {
	d       *db
	arm     string
	m       map[int]*entry
	keys    [][]byte
	cap     int
	dirty   int
	flushAt int

	seq      int64
	flushes  int
	flushDur []time.Duration
	sqlReads int
}

func newCounters(d *db, arm string, keys [][]byte, capN, flushAt int) *counters {
	return &counters{d: d, arm: arm, m: map[int]*entry{}, keys: keys, cap: capN, flushAt: flushAt}
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
		v, found, err := c.d.get(c.keys[ki])
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("key %d missing from store", ki)
		}
		c.sqlReads++
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

// flush drains every dirty counter as its decimal string in one
// transaction; the shadow arm pays its one format per key here.
func (c *counters) flush() error {
	if c.dirty == 0 {
		return nil
	}
	t0 := time.Now()
	txn, err := c.d.conn.BeginImmediate()
	if err != nil {
		return err
	}
	fail := func(err error) error { txn.Rollback(); return err }
	for ki, e := range c.m {
		if !e.dirty {
			continue
		}
		if c.arm == "shadow" {
			e.str = strconv.AppendInt(e.str[:0], e.shadow, 10)
		}
		if err := c.d.put1.BindBlob(1, c.keys[ki]); err != nil {
			return fail(err)
		}
		if err := c.d.put1.BindBlob(2, e.str); err != nil {
			return fail(err)
		}
		if _, err := stepReset(c.d.put1); err != nil {
			return fail(err)
		}
		e.dirty = false
	}
	c.seq++
	if err := c.d.hw1.BindInt64(1, c.seq); err != nil {
		return fail(err)
	}
	if _, err := stepReset(c.d.hw1); err != nil {
		return fail(err)
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	c.dirty = 0
	if c.flushes++; c.flushes%8 == 0 {
		if err := c.d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
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
	path := filepath.Join(cfg.dir, fmt.Sprintf("intshadow-%s.db", cfg.arm))
	os.Remove(path)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	d, err := openDB(path)
	if err != nil {
		return err
	}
	defer d.close()

	keys := make([][]byte, cfg.keys)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "n:%09d", i)
	}

	// Preload every key with a random counter so the parse under test
	// works on realistic digit counts, not "0".
	start := time.Now()
	rng := rand.New(rand.NewSource(67))
	var buf []byte
	const batch = 4096
	seq := int64(0)
	for off := 0; off < cfg.keys; off += batch {
		txn, err := d.conn.BeginImmediate()
		if err != nil {
			return err
		}
		fail := func(err error) error { txn.Rollback(); return err }
		for i := off; i < min(off+batch, cfg.keys); i++ {
			buf = strconv.AppendInt(buf[:0], rng.Int63n(1_000_000_000), 10)
			if err := d.put1.BindBlob(1, keys[i]); err != nil {
				return fail(err)
			}
			if err := d.put1.BindBlob(2, buf); err != nil {
				return fail(err)
			}
			if _, err := stepReset(d.put1); err != nil {
				return fail(err)
			}
		}
		seq++
		if err := d.hw1.BindInt64(1, seq); err != nil {
			return fail(err)
		}
		if _, err := stepReset(d.hw1); err != nil {
			return fail(err)
		}
		if err := txn.Commit(); err != nil {
			return err
		}
		if (off/batch)%8 == 7 {
			if err := d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				return err
			}
		}
	}
	if err := d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}
	emit(cfg, out, row{workload: "load", ops: cfg.keys, dur: time.Since(start),
		fileMB: fileMB(path)})

	// Hot phase: every touched key resident (warmed first), ops timed in
	// blocks of 1024 because a hot INCR is tens of nanoseconds and per-op
	// clock reads would drown it. Flush stalls land in the block tail.
	c := newCounters(d, cfg.arm, keys, 0, cfg.flushAt)
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
	emitFlush(cfg, out, "hot-flush", c, path)

	// Cold phase: a capped resident model over the whole keyspace, so
	// most INCRs pay the SQL read and parse before the add. Per-op
	// timing is fine at microsecond scale.
	c = newCounters(d, cfg.arm, keys, cfg.resident, cfg.flushAt)
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
	fmt.Fprintf(os.Stderr, "cold sql reads: %d of %d ops\n", c.sqlReads, cfg.coldOps)
	emitFlush(cfg, out, "cold-flush", c, path)
	return nil
}

func emitFlush(cfg config, out io.Writer, name string, c *counters, path string) {
	var total, worst time.Duration
	for _, fd := range c.flushDur {
		total += fd
		if fd > worst {
			worst = fd
		}
	}
	emit(cfg, out, row{workload: name, ops: len(c.flushDur), dur: total,
		maxLat: worst, fileMB: fileMB(path), vmhwmMB: vmhwmMB()})
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
	fmt.Fprintf(out, "%s,%s,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.1f\n",
		cfg.arm, cfg.dist, cfg.keys,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.fileMB, r.vmhwmMB)
}
