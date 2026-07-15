// Lab: SQLite driver shootout for Track A (spec 2064/sqlo1 doc 02 section
// 2, milestone A1 lab 01).
//
// cgo is banned, so sqlo1a links zombiezen, ncruces, or modernc, and the
// published 2026 benchmarks disagree about which is fastest because they
// measure different shapes. This lab measures ours: point GET and SET at
// Redis-like value sizes on the doc 02 kv schema, the helem elem-table
// shape, one write connection plus a read pool, prepared statements only,
// WAL mode with the doc 02 section 5 pragmas, uniform and zipfian keys,
// cold and cache-hot arms, big multi-row upsert transactions (the drain
// shape), and the isolated statement-step cost that bounds the SQL tax
// the rkv result warns about.
//
// Each driver builds behind its own tag (drv_modernc, drv_zombiezen,
// drv_ncruces) so a binary carries exactly one; run.sh builds all three
// and sweeps page size, value size, and distribution. The adapters speak
// each driver's native API, except modernc, which is measured through
// database/sql because that is its documented posture and the
// compatibility floor the doc names. Output is CSV on stdout, one row per
// workload: driver, workload, page, val, dist, ops, ns/op, ops/s.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// The doc 02 section 4 shapes this lab exercises: kv is the point-op
// table, helem the composite-PK element table. Statements are the ones
// sqlo1a will run; no query building anywhere.
const (
	schemaSQL = `CREATE TABLE IF NOT EXISTS kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS helem (k BLOB, f BLOB, v BLOB, exp INTEGER,
  PRIMARY KEY (k, f)) WITHOUT ROWID;`

	getSQL      = "SELECT v FROM kv WHERE k = ?1"
	setSQL      = "INSERT INTO kv (k, t, exp, gen, v, crc) VALUES (?1, ?2, 0, 0, ?3, 0) ON CONFLICT(k) DO UPDATE SET v = excluded.v, t = excluded.t"
	helemGetSQL = "SELECT v FROM helem WHERE k = ?1 AND f = ?2"
	helemSetSQL = "INSERT INTO helem (k, f, v, exp) VALUES (?1, ?2, ?3, 0) ON CONFLICT(k, f) DO UPDATE SET v = excluded.v"
	// stepSQL prices one prepared bind-step-reset round trip through the
	// driver with the B-tree out of the picture: the SQL tax floor.
	stepSQL = "SELECT ?1"
)

// The doc 02 section 5 posture, split by where each pragma belongs.
// createPragmas run once on the connection that creates the file,
// before the schema: page_size and auto_vacuum are header decisions.
// Replaying them on later connections is not just wasted work; measured
// here, auto_vacuum on every pooled modernc connection made each new
// connection queue behind the writer for a lock and collapsed the read
// pool to single-digit reads per second.
func createPragmas(pageSize int) []string {
	return []string{
		fmt.Sprintf("PRAGMA page_size = %d", pageSize),
		"PRAGMA auto_vacuum = INCREMENTAL",
	}
}

// writerPragmas run on the write connection. wal_autocheckpoint = 0
// only works because the harness checkpoints on the drain cadence like
// the real drain loop will; an unbounded WAL is a read-amplification
// problem the doc calls out.
func writerPragmas(cacheKiB int) []string {
	return append([]string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = OFF",
		"PRAGMA wal_autocheckpoint = 0",
	}, readerPragmas(cacheKiB)...)
}

// readerPragmas are the read-safe subset for pool connections; a
// readonly connection cannot run the header or journal pragmas (ncruces
// refuses with a readonly-database error, correctly).
func readerPragmas(cacheKiB int) []string {
	return []string{
		fmt.Sprintf("PRAGMA cache_size = -%d", cacheKiB),
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 0",
		"PRAGMA busy_timeout = 10000",
	}
}

// shootDB is what each driver adapter implements. get reports the value
// length instead of the bytes so adapters can use whatever zero-copy
// column access their driver offers; copying costs are still theirs to
// pay where their API forces a copy, which is part of what is measured.
type shootDB interface {
	get(key []byte) (int, bool, error)
	set(key, val []byte) error
	drain(keys, vals [][]byte) error
	helemGet(k, f []byte) (int, bool, error)
	helemDrain(k []byte, fs, vs [][]byte) error
	step() error
	checkpoint() error
	newReader() (shootReader, error)
	close() error
}

type shootReader interface {
	get(key []byte) (int, bool, error)
	close() error
}

type config struct {
	dir      string
	page     int
	cacheKiB int
	val      int
	keys     int
	ops      int
	batch    int
	readers  int
	fields   int
	dist     string
	poolDur  time.Duration
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.IntVar(&cfg.page, "page", 4096, "SQLite page size")
	flag.IntVar(&cfg.cacheKiB, "cache", 65536, "SQLite cache_size in KiB")
	flag.IntVar(&cfg.val, "val", 128, "value size in bytes")
	flag.IntVar(&cfg.keys, "keys", 200000, "loaded key count")
	flag.IntVar(&cfg.ops, "ops", 500000, "point ops per read arm")
	flag.IntVar(&cfg.batch, "batch", 4096, "rows per drain transaction")
	flag.IntVar(&cfg.readers, "readers", 4, "read pool size")
	flag.IntVar(&cfg.fields, "fields", 64, "fields per helem key")
	flag.StringVar(&cfg.dist, "dist", "uniform", "key distribution: uniform or zipf")
	flag.DurationVar(&cfg.poolDur, "pooldur", 2*time.Second, "read-pool arm duration")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.ops, cfg.poolDur = 4000, 20000, 200*time.Millisecond
	}
	if err := runAll(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "drivershoot:", err)
		os.Exit(1)
	}
}

func runAll(cfg config) error {
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "drivershoot")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("shoot-%s-p%d.db", driverName, cfg.page))
	os.Remove(path)
	db, err := openShootDB(path, cfg.page, cfg.cacheKiB)
	if err != nil {
		return err
	}

	keys := makeKeys(cfg.keys)
	val := makeVal(cfg.val)
	pick := picker(cfg.dist, cfg.keys)

	// Load is the first measurement: the drain shape against a cold
	// empty file, batch rows per transaction.
	start := time.Now()
	if err := drainAll(db, keys, val, cfg.batch); err != nil {
		return fmt.Errorf("load: %w", err)
	}
	if err := db.checkpoint(); err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	report(cfg, "load", cfg.keys, time.Since(start))

	// step: the prepared statement round trip with no table behind it.
	start = time.Now()
	for range cfg.ops {
		if err := db.step(); err != nil {
			return fmt.Errorf("step: %w", err)
		}
	}
	report(cfg, "step", cfg.ops, time.Since(start))

	// get-hot: one touch pass warms the page cache, then the timed run.
	for _, k := range keys {
		if _, _, err := db.get(k); err != nil {
			return fmt.Errorf("warm: %w", err)
		}
	}
	start = time.Now()
	for range cfg.ops {
		if _, ok, err := db.get(keys[pick()]); err != nil {
			return fmt.Errorf("get-hot: %w", err)
		} else if !ok {
			return fmt.Errorf("get-hot: loaded key missing")
		}
	}
	report(cfg, "get-hot", cfg.ops, time.Since(start))

	// get-cold: reopen drops the driver's page cache; the OS file cache
	// stays warm, which is equal treatment across drivers and noted in
	// the README.
	if err := db.close(); err != nil {
		return err
	}
	db, err = openShootDB(path, cfg.page, cfg.cacheKiB)
	if err != nil {
		return err
	}
	defer db.close()
	start = time.Now()
	for range cfg.ops {
		if _, ok, err := db.get(keys[pick()]); err != nil {
			return fmt.Errorf("get-cold: %w", err)
		} else if !ok {
			return fmt.Errorf("get-cold: loaded key missing")
		}
	}
	report(cfg, "get-cold", cfg.ops, time.Since(start))

	// set: single-statement autocommit upserts, the shape SQLite is
	// worst at and the reason the drain exists.
	setOps := cfg.ops / 10
	start = time.Now()
	for i := range setOps {
		if err := db.set(keys[i%cfg.keys], val); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}
	report(cfg, "set", setOps, time.Since(start))

	// drain: the same rows through batch-sized transactions.
	start = time.Now()
	if err := drainAll(db, keys, val, cfg.batch); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	if err := db.checkpoint(); err != nil {
		return fmt.Errorf("drain checkpoint: %w", err)
	}
	report(cfg, "drain", cfg.keys, time.Since(start))

	// helem: the composite-PK element shape, fields per key, point gets
	// and batch upserts.
	hkeys := cfg.keys / cfg.fields
	if hkeys == 0 {
		hkeys = 1
	}
	fields := makeKeys(cfg.fields)
	vals := make([][]byte, cfg.fields)
	for i := range vals {
		vals[i] = val
	}
	start = time.Now()
	for i := range hkeys {
		if err := db.helemDrain(keys[i], fields, vals); err != nil {
			return fmt.Errorf("helem-drain: %w", err)
		}
	}
	if err := db.checkpoint(); err != nil {
		return fmt.Errorf("helem checkpoint: %w", err)
	}
	report(cfg, "helem-drain", hkeys*cfg.fields, time.Since(start))
	hpick := picker(cfg.dist, hkeys)
	start = time.Now()
	for range cfg.ops {
		if _, ok, err := db.helemGet(keys[hpick()], fields[hpick()%cfg.fields]); err != nil {
			return fmt.Errorf("helem-get: %w", err)
		} else if !ok {
			return fmt.Errorf("helem-get: loaded field missing")
		}
	}
	report(cfg, "helem-get", cfg.ops, time.Since(start))

	// pool: readers hammer point gets while the owner drains, the
	// WAL-mode concurrency story sqlo1a depends on.
	reads, writes, err := runPool(db, cfg, keys, val)
	if err != nil {
		return fmt.Errorf("pool: %w", err)
	}
	report(cfg, fmt.Sprintf("pool-read-r%d", cfg.readers), reads, cfg.poolDur)
	report(cfg, fmt.Sprintf("pool-drain-r%d", cfg.readers), writes, cfg.poolDur)
	return nil
}

func runPool(db shootDB, cfg config, keys [][]byte, val []byte) (int, int, error) {
	var stop atomic.Bool
	var reads atomic.Int64
	errs := make(chan error, cfg.readers)
	var wg sync.WaitGroup
	for r := range cfg.readers {
		rd, err := db.newReader()
		if err != nil {
			stop.Store(true)
			wg.Wait()
			return 0, 0, err
		}
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			defer rd.close()
			pick := seededPicker(cfg.dist, cfg.keys, seed)
			n := 0
			for !stop.Load() {
				if _, _, err := rd.get(keys[pick()]); err != nil {
					errs <- err
					return
				}
				n++
			}
			reads.Add(int64(n))
		}(int64(r) + 7)
	}
	// The writer checkpoints every few batches, the drain-cadence
	// discipline doc 02 section 5 requires with autocheckpoint off.
	writes, batches := 0, 0
	deadline := time.Now().Add(cfg.poolDur)
	for time.Now().Before(deadline) {
		end := writes % cfg.keys
		n := min(cfg.batch, cfg.keys-end)
		if err := db.drain(keys[end:end+n], repeatVal(val, n)); err != nil {
			stop.Store(true)
			wg.Wait()
			return 0, 0, err
		}
		writes += n
		if batches++; batches%8 == 0 {
			if err := db.checkpoint(); err != nil {
				stop.Store(true)
				wg.Wait()
				return 0, 0, err
			}
		}
	}
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-errs:
		return 0, 0, err
	default:
	}
	return int(reads.Load()), writes, nil
}

func drainAll(db shootDB, keys [][]byte, val []byte, batch int) error {
	for off := 0; off < len(keys); off += batch {
		n := min(batch, len(keys)-off)
		if err := db.drain(keys[off:off+n], repeatVal(val, n)); err != nil {
			return err
		}
	}
	return nil
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

func repeatVal(val []byte, n int) [][]byte {
	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = val
	}
	return vals
}

func picker(dist string, n int) func() int {
	return seededPicker(dist, n, 1)
}

func seededPicker(dist string, n int, seed int64) func() int {
	rng := rand.New(rand.NewSource(seed))
	if dist == "zipf" {
		z := rand.NewZipf(rng, 1.1, 1, uint64(n-1))
		return func() int { return int(z.Uint64()) }
	}
	return func() int { return rng.Intn(n) }
}

func report(cfg config, workload string, ops int, d time.Duration) {
	nsPerOp := float64(d.Nanoseconds()) / float64(ops)
	fmt.Printf("%s,%s,%d,%d,%s,%d,%.0f,%.0f\n",
		driverName, workload, cfg.page, cfg.val, cfg.dist,
		ops, nsPerOp, float64(ops)/d.Seconds())
}
