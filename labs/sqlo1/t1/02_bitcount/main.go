// Lab: popcount cache on and off (spec 2064/sqlo1 doc 05 sections 3.2
// and 8, milestone T1 lab 02).
//
// Doc 05 claims BITCOUNT over a cold bitmap must cost the popcount cache,
// not the bitmap: on Track A the cache is the pc column on chunk rows, so
// a whole-key count is sum(pc) plus edge chunks instead of reading every
// blob. This lab measures cache against scan across bitmap sizes and
// range shapes, cold and hot, and the cold curve is the verdict: cache
// time must scale like row visits while scan time scales like bytes.
//
// It also sweeps the one Track A constant doc 05 leaves implicit: column
// order. SQLite stores record bytes in declared order, so with chunk
// (k, cid, v, pc) the pc bytes sit at the end of a 32 KiB blob's overflow
// chain and sum(pc) may have to walk it; with pc ahead of v it lives in
// the leaf-local prefix. The -layout flag builds the same store both ways
// and the delta tells T1 slice 7 whether the shipped schema needs pc
// moved ahead of the blob.
//
// B3 re-points the suite at Track B: the same count paths drive either
// arm through a narrow store surface, selected by -store. The A arm is
// the SQLite schema above; the B arm (store_b.go) maps chunks to
// segment subkey records under a minted rooth and keeps the popcount
// in the doc 05 section 3.2 kind 2 cache segments written in the same
// DrainBatch, so cache mode reads cache segments plus edge chunks and
// scan mode reads every chunk record. Column order is a SQLite
// question, so -store b accepts only the default layout, and one sweep
// prices the cache on the backend that will actually carry it.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// Two DDLs, one per column order; every statement names its columns so
// they run against either. The kv and meta tables ride along so the file
// is the doc 02 shape a rope key lives in.
const (
	schemaPCLastSQL = `CREATE TABLE IF NOT EXISTS kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS chunk (k BLOB, cid INTEGER, v BLOB, pc INTEGER,
  PRIMARY KEY (k, cid)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK (id = 0),
  hw INTEGER) WITHOUT ROWID;
INSERT OR IGNORE INTO meta (id, hw) VALUES (0, 0);`

	schemaPCFirstSQL = `CREATE TABLE IF NOT EXISTS kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS chunk (k BLOB, cid INTEGER, pc INTEGER, v BLOB,
  PRIMARY KEY (k, cid)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK (id = 0),
  hw INTEGER) WITHOUT ROWID;
INSERT OR IGNORE INTO meta (id, hw) VALUES (0, 0);`

	chunkPutSQL = `INSERT INTO chunk (k, cid, v, pc) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (k, cid) DO UPDATE SET v = excluded.v, pc = excluded.pc`
	chunkGetSQL = `SELECT v FROM chunk WHERE k = ?1 AND cid = ?2`
	sumPCSQL    = `SELECT coalesce(sum(pc), 0), count(*) FROM chunk WHERE k = ?1 AND cid BETWEEN ?2 AND ?3`
	scanSQL     = `SELECT cid, v FROM chunk WHERE k = ?1 AND cid BETWEEN ?2 AND ?3 ORDER BY cid`
	setHWSQL    = `UPDATE meta SET hw = ?1 WHERE id = 0`
)

type config struct {
	dir     string
	store   string
	chunk   int
	layout  string
	sizeMB  int
	reps    int
	hotReps int
}

// store is the backend arm under the count paths: Track A rows or
// Track B records, one narrow surface so the shared model prices both.
// chunkGet feeds the edge reads, sumPC is the cache-mode interior read
// over chunks [c0, c1] inclusive, scanChunks visits every stored chunk
// in cid order for scan mode (img is only valid during the visit),
// flush lands one preload write set, and checkpoint is whatever the
// arm's WAL trim verb is.
type store interface {
	chunkGet(cid int64) ([]byte, error)
	sumPC(c0, c1 int64) (int64, error)
	scanChunks(c0, c1 int64, visit func(cid int64, img []byte) error) error
	flush(fs *flushSet) error
	checkpoint() error
	close() error
}

// flushSet is one preload batch as the model hands it down: chunk rows
// with their pc and the high-water sequence that must land atomically
// with them. The model advances seq by the chunk count per flush.
type flushSet struct {
	seq    int64
	chunks []chunkRow
}

type chunkRow struct {
	cid int64
	row []byte
	pc  int64
}

// openStore picks the arm; create matters only to the b arm, whose
// open verb differs between a fresh file and a cold-rep reopen.
func openStore(cfg config, path string, key []byte, create bool) (store, error) {
	switch cfg.store {
	case "a":
		return openA(path, cfg.layout, key)
	case "b":
		return openB(path, create)
	}
	return nil, fmt.Errorf("unknown store arm %q", cfg.store)
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	key   []byte
	cput  *sqlite3.Stmt
	cget  *sqlite3.Stmt
	sum1  *sqlite3.Stmt
	scan1 *sqlite3.Stmt
	hw1   *sqlite3.Stmt
	stmts []*sqlite3.Stmt
}

func openA(path, layout string, key []byte) (*db, error) {
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
	schema := schemaPCLastSQL
	if layout == "pcfirst" {
		schema = schemaPCFirstSQL
	}
	if err := conn.Exec(schema); err != nil {
		conn.Close()
		return nil, err
	}
	d := &db{conn: conn, path: path, key: key}
	for _, s := range []struct {
		dst **sqlite3.Stmt
		sql string
	}{
		{&d.cput, chunkPutSQL},
		{&d.cget, chunkGetSQL},
		{&d.sum1, sumPCSQL},
		{&d.scan1, scanSQL},
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

func (d *db) chunkGet(cid int64) ([]byte, error) {
	if err := d.cget.BindBlob(1, d.key); err != nil {
		return nil, err
	}
	if err := d.cget.BindInt64(2, cid); err != nil {
		return nil, err
	}
	var v []byte
	if d.cget.Step() {
		v = slices.Clone(d.cget.ColumnBlob(0, nil))
	}
	err := d.cget.Err()
	if rerr := d.cget.Reset(); err == nil {
		err = rerr
	}
	return v, err
}

// sumPC sums the pc column over chunks [c0, c1], the cache-mode
// interior read.
func (d *db) sumPC(c0, c1 int64) (int64, error) {
	if err := d.sum1.BindBlob(1, d.key); err != nil {
		return 0, err
	}
	if err := d.sum1.BindInt64(2, c0); err != nil {
		return 0, err
	}
	if err := d.sum1.BindInt64(3, c1); err != nil {
		return 0, err
	}
	var total int64
	if d.sum1.Step() {
		total = d.sum1.ColumnInt64(0)
	}
	err := d.sum1.Err()
	if rerr := d.sum1.Reset(); err == nil {
		err = rerr
	}
	return total, err
}

// scanChunks walks stored chunks [c0, c1] in cid order; img aliases the
// statement's blob column and dies at Reset, so a visit must consume it
// before returning.
func (d *db) scanChunks(c0, c1 int64, visit func(cid int64, img []byte) error) error {
	if err := d.scan1.BindBlob(1, d.key); err != nil {
		return err
	}
	if err := d.scan1.BindInt64(2, c0); err != nil {
		return err
	}
	if err := d.scan1.BindInt64(3, c1); err != nil {
		return err
	}
	var verr error
	for verr == nil && d.scan1.Step() {
		verr = visit(d.scan1.ColumnInt64(0), d.scan1.ColumnBlob(1, nil))
	}
	err := d.scan1.Err()
	if rerr := d.scan1.Reset(); err == nil {
		err = rerr
	}
	if verr != nil {
		return verr
	}
	return err
}

// flush is one preload transaction: chunk rows and the high-water row
// committed together, the same shape a drain writes.
func (d *db) flush(fs *flushSet) error {
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return err
	}
	fail := func(err error) error { txn.Rollback(); return err }
	for _, c := range fs.chunks {
		if err := d.cput.BindBlob(1, d.key); err != nil {
			return fail(err)
		}
		if err := d.cput.BindInt64(2, c.cid); err != nil {
			return fail(err)
		}
		if err := d.cput.BindBlob(3, c.row); err != nil {
			return fail(err)
		}
		if err := d.cput.BindInt64(4, c.pc); err != nil {
			return fail(err)
		}
		if _, err := stepReset(d.cput); err != nil {
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

// cacheCount is the doc 05 shape: the cache read over interior chunks,
// the one or two edge chunks read raw and popcounted over the addressed
// bytes. The byte range is [b0, b1).
func cacheCount(st store, chunk int, b0, b1 int64) (int64, error) {
	c0, c1 := b0/int64(chunk), (b1-1)/int64(chunk)
	edge := func(cid, lo, hi int64) (int64, error) {
		img, err := st.chunkGet(cid)
		if err != nil {
			return 0, err
		}
		if img == nil {
			return 0, nil
		}
		lo = min(lo, int64(len(img)))
		hi = min(hi, int64(len(img)))
		return int64(popcount(img[lo:hi])), nil
	}
	if c0 == c1 {
		return edge(c0, b0-c0*int64(chunk), b1-c0*int64(chunk))
	}
	total, err := edge(c0, b0-c0*int64(chunk), int64(chunk))
	if err != nil {
		return 0, err
	}
	last, err := edge(c1, 0, b1-c1*int64(chunk))
	if err != nil {
		return 0, err
	}
	total += last
	if c0+1 <= c1-1 {
		interior, err := st.sumPC(c0+1, c1-1)
		if err != nil {
			return 0, err
		}
		total += interior
	}
	return total, nil
}

// scanCount is the cache-off arm: every overlapping chunk read and
// popcounted, edges trimmed to the addressed bytes.
func scanCount(st store, chunk int, b0, b1 int64) (int64, error) {
	c0, c1 := b0/int64(chunk), (b1-1)/int64(chunk)
	var total int64
	err := st.scanChunks(c0, c1, func(cid int64, img []byte) error {
		lo, hi := int64(0), int64(len(img))
		if cid == c0 {
			lo = min(b0-c0*int64(chunk), hi)
		}
		if cid == c1 {
			hi = min(b1-c1*int64(chunk), hi)
		}
		if lo < hi {
			total += int64(popcount(img[lo:hi]))
		}
		return nil
	})
	return total, err
}

func popcount(b []byte) int {
	n := 0
	for len(b) >= 8 {
		n += bits.OnesCount64(binary.LittleEndian.Uint64(b))
		b = b[8:]
	}
	for _, x := range b {
		n += bits.OnesCount8(x)
	}
	return n
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	chunkKiB := flag.Int("chunk", 32, "rope chunk size in KiB")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.StringVar(&cfg.store, "store", "a", "backend arm: a (SQLite rows) or b (Track B records)")
	flag.StringVar(&cfg.layout, "layout", "pclast", "chunk column order: pclast (shipped schema) or pcfirst")
	flag.IntVar(&cfg.sizeMB, "size", 128, "bitmap size in MiB")
	flag.IntVar(&cfg.reps, "reps", 5, "cold reps per cell (each behind a fresh open)")
	flag.IntVar(&cfg.hotReps, "hotreps", 50, "hot reps per cell")
	flag.Parse()
	cfg.chunk = *chunkKiB << 10
	if *quick {
		cfg.sizeMB, cfg.reps, cfg.hotReps = 2, 2, 5
	}
	if cfg.layout != "pclast" && cfg.layout != "pcfirst" {
		fmt.Fprintln(os.Stderr, "bitcount: -layout must be pclast or pcfirst")
		os.Exit(1)
	}
	if cfg.store == "b" && cfg.layout != "pclast" {
		fmt.Fprintln(os.Stderr, "bitcount: -layout is a SQLite column-order question; -store b runs the default pclast only")
		os.Exit(1)
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "bitcount:", err)
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
		dir, err := os.MkdirTemp("", "bitcount")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("bitcount-%s-%s-%dmb.db", cfg.store, cfg.layout, cfg.sizeMB))
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".aki-wal"} {
		os.Remove(p)
	}

	key := []byte("b:0000")
	st, err := openStore(cfg, path, key, true)
	if err != nil {
		return err
	}
	size := int64(cfg.sizeMB) << 20
	nChunks := size / int64(cfg.chunk)

	// Preload: random bytes so popcounts are unpredictable, pc computed
	// per chunk exactly as a drain would, batched write sets, one
	// checkpoint at the end so the measured file is the settled shape.
	start := time.Now()
	rng := rand.New(rand.NewSource(41))
	var expected int64
	const batch = 512
	batches := 0
	for cid := int64(0); cid < nChunks; {
		fs := flushSet{}
		for range batch {
			if cid >= nChunks {
				break
			}
			row := make([]byte, cfg.chunk)
			rng.Read(row)
			pc := int64(popcount(row))
			expected += pc
			fs.chunks = append(fs.chunks, chunkRow{cid: cid, row: row, pc: pc})
			cid++
		}
		fs.seq = cid
		if err := st.flush(&fs); err != nil {
			st.close()
			return err
		}
		if batches++; batches%8 == 0 {
			if err := st.checkpoint(); err != nil {
				st.close()
				return err
			}
		}
	}
	if err := st.checkpoint(); err != nil {
		st.close()
		return err
	}
	if err := st.close(); err != nil {
		return err
	}
	emit(cfg, out, row{workload: "load", ops: int(nChunks), dur: time.Since(start),
		fileMB: fileMB(path)})

	// Range shapes: whole key, a 64 KiB span in the middle, and an
	// unaligned middle half. Small stays inside the file at quick sizes.
	shapes := []struct {
		name   string
		b0, b1 int64
	}{
		{"full", 0, size},
		{"small", size/2 + 13, min(size/2+13+64<<10, size)},
		{"half", size/4 + 7, 3*size/4 + 7},
	}
	arms := []struct {
		name  string
		count func(store, int64, int64) (int64, error)
	}{
		{"cache", func(st store, b0, b1 int64) (int64, error) { return cacheCount(st, cfg.chunk, b0, b1) }},
		{"scan", func(st store, b0, b1 int64) (int64, error) { return scanCount(st, cfg.chunk, b0, b1) }},
	}

	answers := map[string]int64{}
	for _, shape := range shapes {
		for _, arm := range arms {
			// Cold: every rep behind a fresh open. On arm a that empties
			// the SQLite page cache (the apragma OS-cache caveat applies;
			// cross-arm ratios are the read). On arm b the store was
			// closed after the load checkpoint, so each reopen rebuilds
			// its state from the settled file instead of reading the
			// writer's dirty RAM, the closest honest analog.
			var lats []time.Duration
			var total time.Duration
			for range cfg.reps {
				st, err := openStore(cfg, path, key, false)
				if err != nil {
					return err
				}
				t0 := time.Now()
				got, err := arm.count(st, shape.b0, shape.b1)
				lat := time.Since(t0)
				if err != nil {
					st.close()
					return fmt.Errorf("%s-%s cold: %w", shape.name, arm.name, err)
				}
				if err := checkAnswer(answers, shape.name, arm.name, got, expected); err != nil {
					st.close()
					return err
				}
				if err := st.close(); err != nil {
					return err
				}
				lats = append(lats, lat)
				total += lat
			}
			p50, p99, maxLat := percentiles(lats)
			emit(cfg, out, row{workload: shape.name + "-" + arm.name + "-cold", ops: cfg.reps,
				dur: total, p50: p50, p99: p99, maxLat: maxLat, fileMB: fileMB(path)})

			// Hot: one open store, one warm pass, then the timed reps.
			st, err := openStore(cfg, path, key, false)
			if err != nil {
				return err
			}
			if _, err := arm.count(st, shape.b0, shape.b1); err != nil {
				st.close()
				return err
			}
			lats, total = nil, 0
			for range cfg.hotReps {
				t0 := time.Now()
				got, err := arm.count(st, shape.b0, shape.b1)
				lat := time.Since(t0)
				if err != nil {
					st.close()
					return fmt.Errorf("%s-%s hot: %w", shape.name, arm.name, err)
				}
				if err := checkAnswer(answers, shape.name, arm.name, got, expected); err != nil {
					st.close()
					return err
				}
				lats = append(lats, lat)
				total += lat
			}
			if err := st.close(); err != nil {
				return err
			}
			p50, p99, maxLat = percentiles(lats)
			emit(cfg, out, row{workload: shape.name + "-" + arm.name + "-hot", ops: cfg.hotReps,
				dur: total, p50: p50, p99: p99, maxLat: maxLat, vmhwmMB: vmhwmMB()})
		}
	}
	return nil
}

// checkAnswer pins correctness while the clock runs: both arms and every
// rep must agree per shape, and the full count must equal the popcount
// accumulated at preload. A cache that answers fast but wrong would
// otherwise win the sweep.
func checkAnswer(answers map[string]int64, shape, arm string, got, expected int64) error {
	if shape == "full" && got != expected {
		return fmt.Errorf("full-%s: got %d, preload expected %d", arm, got, expected)
	}
	if prev, ok := answers[shape]; ok && got != prev {
		return fmt.Errorf("%s-%s: got %d, other arm said %d", shape, arm, got, prev)
	}
	answers[shape] = got
	return nil
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
	fmt.Fprintf(out, "%s,%d,%s,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.1f\n",
		cfg.store, cfg.chunk>>10, cfg.layout, cfg.sizeMB,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.fileMB, r.vmhwmMB)
}
