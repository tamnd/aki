// Lab: hot-tier-first vs page-cache-only RAM split (spec 2064/sqlo1 doc
// 02 section 6, milestone A2 lab 04).
//
// Two arms at the same total cache budget. The hot arm splits it the doc
// 04 way: the arena share goes to a record-granular cache in front of
// the store and the small store share (~15 points against the arena's
// 55) goes to SQLite's cache_size. The page arm hands the whole budget
// to SQLite. The claim under test (doc 02 section 6) is that
// record-granular-first beats giving SQLite everything, because a
// record costs its bytes while a page costs 8 KiB however little of it
// is hot.
//
// The record cache is a lab-side model, not the engine's HotTable: the
// real one is inseparable from its evictor and drain machinery, which
// are package-internal and not what this lab prices. The model charges
// doc 04's per-entry overhead (71 B) plus key and value bytes against
// the arena share, promotes read misses at p=0.5 (sampled promotion,
// doc 02 section 6), is write-aware (writes populate it), and evicts at
// random. Random eviction only underestimates the hot arm against the
// engine's sampled scoring, so a hot-arm win here is a safe verdict and
// a loss is not final.
//
// Both arms batch writes identically (1024-row drain-shaped
// transactions with checkpoints), so the arms differ only in where the
// read RAM sits. The store shape and pragma posture are the apragma
// lab's; sqlo1a.Open cannot take a cache_size until slice 5 bakes one,
// which is part of what this verdict feeds.
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

	getSQL   = `SELECT t, exp, gen, v, crc FROM kv WHERE k = ?1`
	putSQL   = `INSERT INTO kv (k, t, exp, gen, v, crc) VALUES (?1, 0, 0, 0, ?2, 0) ON CONFLICT (k) DO UPDATE SET v = excluded.v, crc = excluded.crc`
	setHWSQL = `UPDATE meta SET hw = ?1 WHERE id = 0`

	// Per-entry overhead of the engine's hot table (doc 04 section 15
	// and engine/sqlo1 budget.go: 48 B header, 15 B map slot, 4 B dirty
	// ring, 4 B free stack). The model charges it so the arms compare at
	// honest byte counts.
	entryOverhead = 71
)

type config struct {
	dir       string
	arm       string
	budgetMiB int
	keys      int
	val       int
	ops       int
	dist      string
	writePct  int
}

// recCache is the record-granular model: bytes-capped, write-aware,
// random-evicting. Dirty entries are pinned until the write batch that
// holds them flushes, mirroring the engine rule that dirty records
// cannot be evicted.
type recCache struct {
	m     map[int][]byte
	dirty map[int]bool
	bytes int
	cap   int
	unit  int
}

func newRecCache(capBytes, unit int) *recCache {
	return &recCache{m: map[int][]byte{}, dirty: map[int]bool{}, cap: capBytes, unit: unit}
}

func (c *recCache) put(k int, v []byte, dirty bool) {
	if _, ok := c.m[k]; !ok {
		c.bytes += c.unit
	}
	c.m[k] = v
	if dirty {
		c.dirty[k] = true
	}
	for c.bytes > c.cap {
		evicted := false
		for victim := range c.m {
			if c.dirty[victim] {
				continue
			}
			delete(c.m, victim)
			c.bytes -= c.unit
			evicted = true
			break
		}
		if !evicted {
			return // everything resident is dirty; the flush will unpin
		}
	}
}

func (c *recCache) get(k int) ([]byte, bool) {
	v, ok := c.m[k]
	return v, ok
}

func (c *recCache) clean(k int) { delete(c.dirty, k) }

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir; keep it to reuse the preload)")
	flag.StringVar(&cfg.arm, "arm", "hot", "RAM split arm: hot or page")
	flag.IntVar(&cfg.budgetMiB, "budget", 64, "total cache budget in MiB")
	flag.IntVar(&cfg.keys, "keys", 2000000, "loaded key count (size past the budget)")
	flag.IntVar(&cfg.val, "val", 128, "value size in bytes")
	flag.IntVar(&cfg.ops, "ops", 1000000, "mixed ops in the measured phase")
	flag.StringVar(&cfg.dist, "dist", "zipf", "key distribution: zipf or uniform")
	flag.IntVar(&cfg.writePct, "writepct", 10, "write percentage of the mix")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.ops, cfg.budgetMiB = 20000, 20000, 1
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sqlocache:", err)
		os.Exit(1)
	}
}

type row struct {
	workload string
	ops      int
	dur      time.Duration
	p50, p99 time.Duration
	maxLat   time.Duration
	hitPct   float64
	fileMB   float64
	vmhwmMB  float64
}

// split returns the record-cache and SQLite cache_size shares for the
// arm. The 55:15 ratio is the doc 04 arena and store shares, rescaled so
// both arms spend the same total.
func split(arm string, budget int) (recBytes, sqlKiB int, err error) {
	switch arm {
	case "hot":
		return budget * 55 / 70, budget * 15 / 70 / 1024, nil
	case "page":
		return 0, budget / 1024, nil
	default:
		return 0, 0, fmt.Errorf("unknown -arm %q", arm)
	}
}

func runAll(cfg config, out io.Writer) error {
	recBytes, sqlKiB, err := split(cfg.arm, cfg.budgetMiB<<20)
	if err != nil {
		return err
	}
	if cfg.dir == "" {
		dir, mkErr := os.MkdirTemp("", "sqlocache")
		if mkErr != nil {
			return mkErr
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("sqlocache-k%d-v%d.db", cfg.keys, cfg.val))

	conn, err := sqlite3.Open(path)
	if err != nil {
		return err
	}
	defer conn.Close()
	pragmas := []string{
		"PRAGMA page_size = 8192",
		"PRAGMA auto_vacuum = INCREMENTAL",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = OFF",
		"PRAGMA wal_autocheckpoint = 0",
		fmt.Sprintf("PRAGMA cache_size = -%d", sqlKiB),
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 0",
		"PRAGMA busy_timeout = 10000",
	}
	for _, p := range pragmas {
		if err := conn.Exec(p); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	if err := conn.Exec(schemaSQL); err != nil {
		return err
	}
	var get1, put1, hw1 *sqlite3.Stmt
	for _, s := range []struct {
		dst **sqlite3.Stmt
		sql string
	}{{&get1, getSQL}, {&put1, putSQL}, {&hw1, setHWSQL}} {
		stmt, _, err := conn.Prepare(s.sql)
		if err != nil {
			return err
		}
		defer stmt.Close()
		*s.dst = stmt
	}

	keys := makeKeys(cfg.keys)
	val := makeVal(cfg.val)
	stepReset := func(s *sqlite3.Stmt) error {
		s.Step()
		if err := s.Err(); err != nil {
			return err
		}
		return s.Reset()
	}
	flush := func(batch []int, seq int64) error {
		txn, err := conn.BeginImmediate()
		if err != nil {
			return err
		}
		for _, k := range batch {
			if err := put1.BindBlob(1, keys[k]); err != nil {
				txn.Rollback()
				return err
			}
			if err := put1.BindBlob(2, val); err != nil {
				txn.Rollback()
				return err
			}
			if err := stepReset(put1); err != nil {
				txn.Rollback()
				return err
			}
		}
		if err := hw1.BindInt64(1, seq); err != nil {
			txn.Rollback()
			return err
		}
		if err := stepReset(hw1); err != nil {
			txn.Rollback()
			return err
		}
		return txn.Commit()
	}

	// Preload once per work dir: reruns against a kept -dir skip it, so
	// a sweep pays the 2M-row build once.
	var seq int64
	loaded, err := rowCount(conn)
	if err != nil {
		return err
	}
	if loaded < cfg.keys {
		batch := make([]int, 0, 4096)
		for off := 0; off < cfg.keys; off += 4096 {
			batch = batch[:0]
			for i := off; i < min(off+4096, cfg.keys); i++ {
				batch = append(batch, i)
			}
			seq++
			if err := flush(batch, seq); err != nil {
				return err
			}
			if seq%8 == 0 {
				if err := conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
					return err
				}
			}
		}
		if err := conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			return err
		}
	}

	// Mixed phase: one loop, writePct writes, the rest reads, keys by
	// the swept distribution. The hot arm answers reads from the record
	// cache when it can and promotes misses at p=0.5; both arms batch
	// writes into identical 1024-row transactions.
	cache := newRecCache(recBytes, entryOverhead+len(keys[0])+cfg.val)
	useCache := cfg.arm == "hot"
	var pick func() int
	switch cfg.dist {
	case "zipf":
		pick = zipfPicker(cfg.keys, 7)
	case "uniform":
		pick = uniformPicker(cfg.keys, 7)
	default:
		return fmt.Errorf("unknown -dist %q", cfg.dist)
	}
	rng := rand.New(rand.NewSource(99))
	sqlGet := func(k int) error {
		if err := get1.BindBlob(1, keys[k]); err != nil {
			return err
		}
		if get1.Step() {
			_ = get1.ColumnRawBlob(3)
		}
		if err := get1.Err(); err != nil {
			return err
		}
		return get1.Reset()
	}

	var reads, writes, hits int
	var pending []int
	var samples []time.Duration
	flushPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		seq++
		if err := flush(pending, seq); err != nil {
			return err
		}
		for _, k := range pending {
			cache.clean(k)
		}
		pending = pending[:0]
		if seq%8 == 0 {
			return conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		}
		return nil
	}

	start := time.Now()
	for i := range cfg.ops {
		k := pick()
		if rng.Intn(100) < cfg.writePct {
			if useCache {
				cache.put(k, val, true)
			}
			pending = append(pending, k)
			writes++
			if len(pending) >= 1024 {
				if err := flushPending(); err != nil {
					return err
				}
			}
			continue
		}
		t0 := time.Now()
		if useCache {
			if _, ok := cache.get(k); ok {
				hits++
			} else {
				if err := sqlGet(k); err != nil {
					return err
				}
				if rng.Intn(2) == 0 {
					cache.put(k, val, false)
				}
			}
		} else {
			if err := sqlGet(k); err != nil {
				return err
			}
		}
		if i%8 == 0 {
			samples = append(samples, time.Since(t0))
		}
		reads++
	}
	dur := time.Since(start)
	if err := flushPending(); err != nil {
		return err
	}

	hitPct := 0.0
	if reads > 0 {
		hitPct = 100 * float64(hits) / float64(reads)
	}
	p50, p99, maxLat := percentiles(samples)
	emit(cfg, out, row{workload: "mixed-read", ops: reads, dur: dur,
		p50: p50, p99: p99, maxLat: maxLat, hitPct: hitPct,
		fileMB: fileMB(path), vmhwmMB: vmhwmMB()})
	emit(cfg, out, row{workload: "mixed-write", ops: writes, dur: dur})
	return nil
}

func rowCount(conn *sqlite3.Conn) (int, error) {
	stmt, _, err := conn.Prepare(`SELECT count(*) FROM kv`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if !stmt.Step() {
		return 0, stmt.Err()
	}
	return stmt.ColumnInt(0), nil
}

func percentiles(all []time.Duration) (p50, p99, max time.Duration) {
	if len(all) == 0 {
		return 0, 0, 0
	}
	slices.Sort(all)
	return all[len(all)/2], all[len(all)*99/100], all[len(all)-1]
}

func makeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "k:%013d", i)
	}
	return keys
}

func makeVal(n int) []byte {
	val := make([]byte, n)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	return val
}

func uniformPicker(n int, seed int64) func() int {
	rng := rand.New(rand.NewSource(seed))
	return func() int { return rng.Intn(n) }
}

func zipfPicker(n int, seed int64) func() int {
	rng := rand.New(rand.NewSource(seed))
	z := rand.NewZipf(rng, 1.1, 1, uint64(n-1))
	return func() int { return int(z.Uint64()) }
}

func fileMB(path string) float64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(fi.Size()) / (1 << 20)
}

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
	fmt.Fprintf(out, "%d,%s,%s,%d,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.1f,%.1f\n",
		cfg.budgetMiB, cfg.arm, cfg.dist, cfg.keys, cfg.val,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.hitPct, r.fileMB, r.vmhwmMB)
}
