// Lab: pragma sweep for the frozen Track A driver (spec 2064/sqlo1 doc 02
// section 5, milestone A2 lab 01).
//
// The drivershoot lab froze ncruces at page_size 8192 on datasets the page
// cache could swallow, and said 16384 would have to earn its way back on
// datasets that beat the cache. This lab is that rematch, plus the two
// knobs slice 5 still has to bake: cache_size and the checkpoint cadence
// that makes wal_autocheckpoint = 0 safe. One binary, one configuration
// per run; run.sh sweeps page size, cache budget, and cadence into a CSV.
//
// The workload is the production shape: a drain writer committing
// batch-sized upsert transactions with the high-water meta write inside,
// a read pool on separate readonly connections, and a keyspace sized well
// past cache_size so the cache split matters. Readers sample latency, so
// each cell reports p50/p99/max beside the rates, and the writer times
// every checkpoint call, so cadence buys its latency cost in the open.
// Peak RSS (VmHWM) rides along on Linux because the memory bar is part of
// every sqlo1 verdict.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// The store shape under test is the doc 02 section 4 kv table plus the
// meta row, and the statements mirror the sqlo1a catalog: the drain upsert
// binds all six columns and every drain transaction moves the high-water
// mark, so a batch here costs what an ApplyBatch costs at the SQL layer.
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
)

type config struct {
	dir      string
	page     int
	cacheKiB int
	ckpt     int
	val      int
	keys     int
	ops      int
	batch    int
	readers  int
	poolDur  time.Duration
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	cfg   config
	get1  *sqlite3.Stmt
	put1  *sqlite3.Stmt
	hw1   *sqlite3.Stmt
	stmts []*sqlite3.Stmt
}

func createPragmas(pageSize int) []string {
	return []string{
		fmt.Sprintf("PRAGMA page_size = %d", pageSize),
		"PRAGMA auto_vacuum = INCREMENTAL",
	}
}

func writerPragmas(cacheKiB int) []string {
	return append([]string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = OFF",
		"PRAGMA wal_autocheckpoint = 0",
	}, readerPragmas(cacheKiB)...)
}

func readerPragmas(cacheKiB int) []string {
	return []string{
		fmt.Sprintf("PRAGMA cache_size = -%d", cacheKiB),
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 0",
		"PRAGMA busy_timeout = 10000",
	}
}

func openDB(cfg config, path string) (*db, error) {
	conn, err := sqlite3.Open(path)
	if err != nil {
		return nil, err
	}
	for _, p := range append(createPragmas(cfg.page), writerPragmas(cfg.cacheKiB)...) {
		if err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}
	if err := conn.Exec(schemaSQL); err != nil {
		conn.Close()
		return nil, err
	}
	d := &db{conn: conn, path: path, cfg: cfg}
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

func (d *db) pageSize() (int, error) {
	stmt, _, err := d.conn.Prepare("PRAGMA page_size")
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if !stmt.Step() {
		return 0, stmt.Err()
	}
	return stmt.ColumnInt(0), nil
}

func stepReset(s *sqlite3.Stmt) (found bool, err error) {
	found = s.Step()
	err = s.Err()
	if rerr := s.Reset(); err == nil {
		err = rerr
	}
	return found, err
}

func (d *db) get(key []byte) (bool, error) {
	if err := d.get1.BindBlob(1, key); err != nil {
		return false, err
	}
	return stepReset(d.get1)
}

// drain is one ApplyBatch-shaped transaction: the upserts and the
// high-water move commit together.
func (d *db) drain(keys [][]byte, val []byte, seq int64) error {
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := d.put1.BindBlob(1, k); err != nil {
			txn.Rollback()
			return err
		}
		if err := d.put1.BindBlob(2, val); err != nil {
			txn.Rollback()
			return err
		}
		if _, err := stepReset(d.put1); err != nil {
			txn.Rollback()
			return err
		}
	}
	if err := d.hw1.BindInt64(1, seq); err != nil {
		txn.Rollback()
		return err
	}
	if _, err := stepReset(d.hw1); err != nil {
		txn.Rollback()
		return err
	}
	return txn.Commit()
}

func (d *db) checkpoint() error {
	return d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
}

type reader struct {
	conn *sqlite3.Conn
	get1 *sqlite3.Stmt
}

func (d *db) newReader() (*reader, error) {
	conn, err := sqlite3.OpenFlags(d.path, sqlite3.OPEN_READONLY)
	if err != nil {
		return nil, err
	}
	for _, p := range readerPragmas(d.cfg.cacheKiB) {
		if err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, err
		}
	}
	stmt, _, err := conn.Prepare(getSQL)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &reader{conn: conn, get1: stmt}, nil
}

func (r *reader) get(key []byte) (bool, error) {
	if err := r.get1.BindBlob(1, key); err != nil {
		return false, err
	}
	return stepReset(r.get1)
}

func (r *reader) close() error {
	r.get1.Close()
	return r.conn.Close()
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.IntVar(&cfg.page, "page", 8192, "SQLite page size")
	flag.IntVar(&cfg.cacheKiB, "cache", 32768, "SQLite cache_size in KiB")
	flag.IntVar(&cfg.ckpt, "ckpt", 8, "drain batches per checkpoint")
	flag.IntVar(&cfg.val, "val", 128, "value size in bytes")
	flag.IntVar(&cfg.keys, "keys", 2000000, "loaded key count (size past the cache)")
	flag.IntVar(&cfg.ops, "ops", 200000, "point ops per read arm")
	flag.IntVar(&cfg.batch, "batch", 4096, "rows per drain transaction")
	flag.IntVar(&cfg.readers, "readers", 4, "read pool size")
	flag.DurationVar(&cfg.poolDur, "pooldur", 10*time.Second, "read-pool arm duration")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.ops, cfg.poolDur = 20000, 20000, 300*time.Millisecond
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "apragma:", err)
		os.Exit(1)
	}
}

// row is one CSV line; zero-valued latency and size fields mean the arm
// does not measure them.
type row struct {
	workload  string
	ops       int
	dur       time.Duration
	p50, p99  time.Duration
	maxLat    time.Duration
	fileMB    float64
	walMB     float64
	vmhwmMB   float64
	extraRate float64
}

func runAll(cfg config, out io.Writer) error {
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "apragma")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("apragma-p%d-c%d-k%d.db", cfg.page, cfg.cacheKiB, cfg.ckpt))
	os.Remove(path)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	d, err := openDB(cfg, path)
	if err != nil {
		return err
	}
	if got, err := d.pageSize(); err != nil || got != cfg.page {
		d.close()
		return fmt.Errorf("page_size is %d, swept value %d did not take (%v)", got, cfg.page, err)
	}

	keys := makeKeys(cfg.keys)
	val := makeVal(cfg.val)

	// load: the drain shape into a cold file, checkpointing on the swept
	// cadence exactly as the pool arm will.
	start := time.Now()
	seq, batches := int64(0), 0
	for off := 0; off < len(keys); off += cfg.batch {
		n := min(cfg.batch, len(keys)-off)
		seq++
		if err := d.drain(keys[off:off+n], val, seq); err != nil {
			d.close()
			return fmt.Errorf("load: %w", err)
		}
		if batches++; batches%cfg.ckpt == 0 {
			if err := d.checkpoint(); err != nil {
				d.close()
				return fmt.Errorf("load checkpoint: %w", err)
			}
		}
	}
	if err := d.checkpoint(); err != nil {
		d.close()
		return err
	}
	emit(cfg, out, row{workload: "load", ops: cfg.keys, dur: time.Since(start),
		fileMB: fileMB(path), walMB: fileMB(path + "-wal")})

	// get-zipf: the hot set fits the page cache even when the file does
	// not, which is the arm cache_size is supposed to win.
	zpick := zipfPicker(cfg.keys, 11)
	for range cfg.ops / 4 {
		if _, err := d.get(keys[zpick()]); err != nil {
			d.close()
			return err
		}
	}
	start = time.Now()
	for range cfg.ops {
		if ok, err := d.get(keys[zpick()]); err != nil {
			d.close()
			return err
		} else if !ok {
			d.close()
			return fmt.Errorf("get-zipf: loaded key missing")
		}
	}
	emit(cfg, out, row{workload: "get-zipf", ops: cfg.ops, dur: time.Since(start)})

	// get-cold: reopen drops the SQLite cache (not the OS file cache;
	// cross-cell ratios are the read) and uniform keys defeat what is
	// left of it.
	if err := d.close(); err != nil {
		return err
	}
	d, err = openDB(cfg, path)
	if err != nil {
		return err
	}
	defer d.close()
	upick := uniformPicker(cfg.keys, 23)
	start = time.Now()
	for range cfg.ops {
		if ok, err := d.get(keys[upick()]); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("get-cold: loaded key missing")
		}
	}
	emit(cfg, out, row{workload: "get-cold", ops: cfg.ops, dur: time.Since(start)})

	// pool: the production shape. Readers sample latency; the writer
	// times every checkpoint the cadence schedules.
	if err := runPool(cfg, d, keys, val, seq, out); err != nil {
		return err
	}
	return nil
}

func runPool(cfg config, d *db, keys [][]byte, val []byte, seq int64, out io.Writer) error {
	var stop atomic.Bool
	errs := make(chan error, cfg.readers)
	lats := make([][]time.Duration, cfg.readers)
	counts := make([]int, cfg.readers)
	var wg sync.WaitGroup
	for i := range cfg.readers {
		rd, err := d.newReader()
		if err != nil {
			stop.Store(true)
			wg.Wait()
			return err
		}
		wg.Add(1)
		go func(slot int, seed int64) {
			defer wg.Done()
			defer rd.close()
			pick := uniformPicker(cfg.keys, seed)
			n := 0
			var samples []time.Duration
			for !stop.Load() {
				t0 := time.Now()
				if _, err := rd.get(keys[pick()]); err != nil {
					errs <- err
					return
				}
				if n%8 == 0 {
					samples = append(samples, time.Since(t0))
				}
				n++
			}
			lats[slot], counts[slot] = samples, n
		}(i, int64(i)*131+41)
	}

	writes, batches := 0, 0
	var ckptDur []time.Duration
	var walMax float64
	deadline := time.Now().Add(cfg.poolDur)
	for time.Now().Before(deadline) {
		off := writes % cfg.keys
		n := min(cfg.batch, cfg.keys-off)
		seq++
		if err := d.drain(keys[off:off+n], val, seq); err != nil {
			stop.Store(true)
			wg.Wait()
			return err
		}
		writes += n
		if wm := fileMB(d.path + "-wal"); wm > walMax {
			walMax = wm
		}
		if batches++; batches%cfg.ckpt == 0 {
			t0 := time.Now()
			if err := d.checkpoint(); err != nil {
				stop.Store(true)
				wg.Wait()
				return err
			}
			ckptDur = append(ckptDur, time.Since(t0))
		}
	}
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}

	reads := 0
	var all []time.Duration
	for i := range lats {
		reads += counts[i]
		all = append(all, lats[i]...)
	}
	p50, p99, maxLat := percentiles(all)
	emit(cfg, out, row{workload: fmt.Sprintf("pool-read-r%d", cfg.readers), ops: reads,
		dur: cfg.poolDur, p50: p50, p99: p99, maxLat: maxLat, vmhwmMB: vmhwmMB()})
	emit(cfg, out, row{workload: fmt.Sprintf("pool-drain-r%d", cfg.readers), ops: writes,
		dur: cfg.poolDur, walMB: walMax})
	if len(ckptDur) > 0 {
		var total time.Duration
		worst := ckptDur[0]
		for _, cd := range ckptDur {
			total += cd
			if cd > worst {
				worst = cd
			}
		}
		emit(cfg, out, row{workload: "pool-ckpt", ops: len(ckptDur),
			dur: total, maxLat: worst})
	}
	return nil
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
	fmt.Fprintf(out, "%d,%d,%d,%d,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.1f,%.1f\n",
		cfg.page, cfg.cacheKiB, cfg.ckpt, cfg.keys, cfg.val,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.fileMB, r.walMB, r.vmhwmMB)
}
