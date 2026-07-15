// Lab: rope chunk size sweep (spec 2064/sqlo1 doc 05 section 1.1,
// milestone T1 lab 01).
//
// T1 slice 1 bakes log2chunk, the fixed rope chunk size, and one default
// must serve strings and bitmaps both because Redis says a bitmap IS a
// string. The trade is read locality against small-write amplification:
// a GETRANGE pays one row probe per chunk it overlaps, while a 10-byte
// SETRANGE or a single SETBIT dirties a whole chunk image that the next
// drain writes in full. This lab prices that trade on the real Track A
// chunk schema at 8/16/32/64 KiB across the four doc 05 operator mixes.
//
// The rope model is the doc 05 shape expressed as rows: root length in
// kv, fixed-size chunks in chunk keyed (k, cid), byte offset B in chunk
// B >> log2chunk, absent chunks reading as zeros (lazy fill), and the pc
// popcount column recomputed for dirty chunks when the bitmap mix
// flushes. Writes go through a coalescing dirty-chunk overlay flushed in
// drain-shaped transactions on the engine's byte threshold, because the
// production write path never pays per-op transactions; an oracle test
// pins the model against a flat byte-slice reference. Per-op costs are
// timed per class, so each CSV row is an intrinsic cost (write ops in
// RAM, read ops through overlay then SQL, flushes carrying the IO bill)
// and write amplification is flushed bytes over logical bytes written.
//
// B3 re-points the suite at Track B: the same rope drives either arm
// through a narrow store surface, selected by -store. The A arm is the
// SQLite schema above; the B arm (store_b.go) maps chunks to segment
// subkey records under a minted rooth, keeps the popcount in the doc
// 05 kind 2 cache segments drained alongside the chunks, turns a flush
// into one DrainBatch, and points the checkpoint cadence at the
// store's own checkpoint, so one sweep prices the chunk size on the
// backend that will actually carry it.
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

// The schema is the doc 02 section 4 subset a rope touches: root rows in
// kv, chunk rows with the pc popcount column, and the meta high-water
// row every drain transaction moves. Pragmas are the apragma writer
// posture at the current spec defaults (page 8192, cache 32 MiB); the
// chunk-size crossover is read within a cell, so it does not wait on the
// apragma verdict.
const (
	schemaSQL = `CREATE TABLE IF NOT EXISTS kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS chunk (k BLOB, cid INTEGER, v BLOB, pc INTEGER,
  PRIMARY KEY (k, cid)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK (id = 0),
  hw INTEGER) WITHOUT ROWID;
INSERT OR IGNORE INTO meta (id, hw) VALUES (0, 0);`

	chunkGetSQL   = `SELECT v FROM chunk WHERE k = ?1 AND cid = ?2`
	chunkProbeSQL = `SELECT v, pc FROM chunk WHERE k = ?1 AND cid = ?2`
	chunkPutSQL   = `INSERT INTO chunk (k, cid, v, pc) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (k, cid) DO UPDATE SET v = excluded.v, pc = excluded.pc`
	rootPutSQL    = `INSERT INTO kv (k, t, exp, gen, v, crc) VALUES (?1, 0, 0, 0, ?2, 0) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
	setHWSQL      = `UPDATE meta SET hw = ?1 WHERE id = 0`
)

type config struct {
	dir       string
	store     string
	chunk     int
	mix       string
	dist      string
	keys      int
	valMB     int
	ops       int
	wlen      int
	rlen      int
	threshold int
	ckpt      int
}

// store is the backend arm under the rope: Track A rows or Track B
// records, one narrow surface so the shared model above prices both.
// chunkGet feeds the hot read path, chunkProbe is the oracle test's
// stored-row readback (row bytes plus the pc the arm keeps), flush
// lands one drain-shaped write set, and checkpoint is whatever the
// arm's WAL trim verb is.
type store interface {
	chunkGet(ki int, cid int64) ([]byte, error)
	chunkProbe(ki int, cid int64) (row []byte, pc int64, err error)
	flush(fs *flushSet) error
	checkpoint() error
	dataMB() float64
	walMB() float64
	close() error
}

// flushSet is one drain cycle as the model hands it down: trimmed
// chunk rows with their pc, dirty root lengths, and the high-water
// sequence that must land atomically with them.
type flushSet struct {
	seq    int64
	chunks []chunkRow
	roots  []rootRow
}

type chunkRow struct {
	ki  int
	cid int64
	row []byte
	pc  int64
}

type rootRow struct {
	ki  int
	len int64
}

func openStore(cfg config, path string, keys [][]byte) (store, error) {
	switch cfg.store {
	case "a":
		return openA(path, keys)
	case "b":
		return openB(path, keys, cfg.mix == "setbit")
	}
	return nil, fmt.Errorf("unknown store arm %q", cfg.store)
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	keys  [][]byte
	cget  *sqlite3.Stmt
	pget  *sqlite3.Stmt
	cput  *sqlite3.Stmt
	rput  *sqlite3.Stmt
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
		{&d.cget, chunkGetSQL},
		{&d.pget, chunkProbeSQL},
		{&d.cput, chunkPutSQL},
		{&d.rput, rootPutSQL},
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

// chunkGet returns the stored chunk row or nil for a lazy gap. The copy
// is deliberate: the blob column's bytes die at Reset.
func (d *db) chunkGet(ki int, cid int64) ([]byte, error) {
	if err := d.cget.BindBlob(1, d.keys[ki]); err != nil {
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

// chunkProbe reads the stored row and its pc column for the oracle
// readback; a missing row comes back nil with pc zero.
func (d *db) chunkProbe(ki int, cid int64) ([]byte, int64, error) {
	if err := d.pget.BindBlob(1, d.keys[ki]); err != nil {
		return nil, 0, err
	}
	if err := d.pget.BindInt64(2, cid); err != nil {
		return nil, 0, err
	}
	var v []byte
	var pc int64
	if d.pget.Step() {
		v = slices.Clone(d.pget.ColumnBlob(0, nil))
		pc = d.pget.ColumnInt64(1)
	}
	err := d.pget.Err()
	if rerr := d.pget.Reset(); err == nil {
		err = rerr
	}
	return v, pc, err
}

// flush is one drain-shaped transaction: dirty chunk rows, dirty
// roots, and the high-water row, committed together.
func (d *db) flush(fs *flushSet) error {
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return err
	}
	fail := func(err error) error { txn.Rollback(); return err }
	for _, c := range fs.chunks {
		if err := d.cput.BindBlob(1, d.keys[c.ki]); err != nil {
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
	var rootBuf [8]byte
	for _, rt := range fs.roots {
		binary.LittleEndian.PutUint64(rootBuf[:], uint64(rt.len))
		if err := d.rput.BindBlob(1, d.keys[rt.ki]); err != nil {
			return fail(err)
		}
		if err := d.rput.BindBlob(2, rootBuf[:]); err != nil {
			return fail(err)
		}
		if _, err := stepReset(d.rput); err != nil {
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
func (d *db) walMB() float64  { return fileMB(d.path + "-wal") }

// rope is the doc 05 rope model over the store: fixed-size chunks
// addressed by arithmetic, a coalescing dirty overlay standing in for
// the hot tier, and drain-shaped flush transactions. Overlay images are
// always full chunk size; totalLen decides how much of the last one is
// real, and the flush trims the last row to the logical length.
type rope struct {
	st       store
	cfg      config
	keys     [][]byte
	totalLen []int64

	overlay    map[uint64][]byte
	rootDirty  map[int]bool
	dirtyBytes int

	seq      int64
	flushes  int
	logical  int64
	flushed  int64
	flushDur []time.Duration
	walMaxMB float64
	countPC  bool
}

func newRope(st store, cfg config, keys [][]byte) *rope {
	return &rope{
		st: st, cfg: cfg, keys: keys,
		totalLen:  make([]int64, len(keys)),
		overlay:   map[uint64][]byte{},
		rootDirty: map[int]bool{},
		countPC:   cfg.mix == "setbit",
	}
}

func okey(ki int, cid int64) uint64 { return uint64(ki)<<32 | uint64(cid) }

// writable returns the full-size overlay image for a chunk, pulling the
// stored row as the modify base on first touch (zero-padded when the row
// was short or absent) and charging the dirty budget once per chunk per
// cycle, which is the coalescing the drain queue provides.
func (r *rope) writable(ki int, cid int64) ([]byte, error) {
	k := okey(ki, cid)
	if img, ok := r.overlay[k]; ok {
		return img, nil
	}
	img := make([]byte, r.cfg.chunk)
	row, err := r.st.chunkGet(ki, cid)
	if err != nil {
		return nil, err
	}
	copy(img, row)
	r.overlay[k] = img
	r.dirtyBytes += r.cfg.chunk
	return img, nil
}

// readChunk is overlay first, then the store, then zeros (lazy fill).
func (r *rope) readChunk(ki int, cid int64) ([]byte, error) {
	if img, ok := r.overlay[okey(ki, cid)]; ok {
		return img, nil
	}
	return r.st.chunkGet(ki, cid)
}

func (r *rope) setRange(ki int, off int64, data []byte) error {
	end := off + int64(len(data))
	for pos := off; pos < end; {
		cid := pos >> log2(r.cfg.chunk)
		img, err := r.writable(ki, cid)
		if err != nil {
			return err
		}
		lo := pos - cid*int64(r.cfg.chunk)
		n := copy(img[lo:], data[pos-off:])
		pos += int64(n)
	}
	if end > r.totalLen[ki] {
		r.totalLen[ki] = end
		r.rootDirty[ki] = true
	}
	r.logical += int64(len(data))
	return nil
}

func (r *rope) getRange(ki int, off, n int64) ([]byte, error) {
	if off >= r.totalLen[ki] {
		return nil, nil
	}
	if off+n > r.totalLen[ki] {
		n = r.totalLen[ki] - off
	}
	out := make([]byte, n)
	for pos := off; pos < off+n; {
		cid := pos >> log2(r.cfg.chunk)
		lo := pos - cid*int64(r.cfg.chunk)
		span := min(int64(r.cfg.chunk)-lo, off+n-pos)
		img, err := r.readChunk(ki, cid)
		if err != nil {
			return nil, err
		}
		// A row shorter than lo happens when the value grew past a
		// trimmed last row via a sparse write; those bytes are zeros.
		if img != nil && lo < int64(len(img)) {
			copy(out[pos-off:pos-off+span], img[lo:])
		}
		pos += span
	}
	return out, nil
}

func (r *rope) setBit(ki int, bitOff int64, val bool) error {
	byteOff := bitOff >> 3
	cid := byteOff >> log2(r.cfg.chunk)
	img, err := r.writable(ki, cid)
	if err != nil {
		return err
	}
	mask := byte(1) << (7 - bitOff&7)
	if val {
		img[byteOff-cid*int64(r.cfg.chunk)] |= mask
	} else {
		img[byteOff-cid*int64(r.cfg.chunk)] &^= mask
	}
	if byteOff+1 > r.totalLen[ki] {
		r.totalLen[ki] = byteOff + 1
		r.rootDirty[ki] = true
	}
	r.logical++
	return nil
}

func (r *rope) getBit(ki int, bitOff int64) (bool, error) {
	byteOff := bitOff >> 3
	if byteOff >= r.totalLen[ki] {
		return false, nil
	}
	cid := byteOff >> log2(r.cfg.chunk)
	img, err := r.readChunk(ki, cid)
	if err != nil {
		return false, err
	}
	lo := byteOff - cid*int64(r.cfg.chunk)
	if img == nil || lo >= int64(len(img)) {
		return false, nil
	}
	return img[lo]&(1<<(7-bitOff&7)) != 0, nil
}

// flush drains every dirty chunk and root in one write set, the last
// chunk trimmed to the logical length, pc recomputed for the bitmap mix,
// and the high-water mark moved with the batch. The overlay empties
// afterward, so the next rewrite pulls its base from the store again,
// which is the cold half of the trade the chunk size sets.
func (r *rope) flush() error {
	if len(r.overlay) == 0 && len(r.rootDirty) == 0 {
		return nil
	}
	t0 := time.Now()
	fs := flushSet{seq: r.seq + 1}
	for k, img := range r.overlay {
		ki, cid := int(k>>32), int64(k&0xffffffff)
		rowLen := min(int64(r.cfg.chunk), r.totalLen[ki]-cid*int64(r.cfg.chunk))
		if rowLen <= 0 {
			continue
		}
		row := img[:rowLen]
		pc := int64(0)
		if r.countPC {
			pc = int64(popcount(row))
		}
		fs.chunks = append(fs.chunks, chunkRow{ki: ki, cid: cid, row: row, pc: pc})
		r.flushed += rowLen
	}
	for ki := range r.rootDirty {
		fs.roots = append(fs.roots, rootRow{ki: ki, len: r.totalLen[ki]})
		r.flushed += 8 + int64(len(r.keys[ki]))
	}
	if err := r.st.flush(&fs); err != nil {
		return err
	}
	r.seq++
	clear(r.overlay)
	clear(r.rootDirty)
	r.dirtyBytes = 0
	if wm := r.st.walMB(); wm > r.walMaxMB {
		r.walMaxMB = wm
	}
	if r.flushes++; r.flushes%r.cfg.ckpt == 0 {
		if err := r.st.checkpoint(); err != nil {
			return err
		}
	}
	r.flushDur = append(r.flushDur, time.Since(t0))
	return nil
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

func log2(n int) int { return bits.TrailingZeros(uint(n)) }

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.StringVar(&cfg.store, "store", "a", "backend arm: a (SQLite rows) or b (Track B records)")
	chunkKiB := flag.Int("chunk", 32, "rope chunk size in KiB (power of two)")
	flag.StringVar(&cfg.mix, "mix", "setrange", "operator mix: setrange, append, setbit, getrange")
	flag.StringVar(&cfg.dist, "dist", "uniform", "offset distribution: uniform or zipf over chunks")
	flag.IntVar(&cfg.keys, "keys", 32, "rope key count")
	flag.IntVar(&cfg.valMB, "valmb", 8, "preloaded value size per key in MiB")
	flag.IntVar(&cfg.ops, "ops", 200000, "ops in the measured mix")
	flag.IntVar(&cfg.wlen, "wlen", 64, "SETRANGE and APPEND payload bytes")
	flag.IntVar(&cfg.rlen, "rlen", 4096, "GETRANGE span bytes")
	flag.IntVar(&cfg.threshold, "threshold", 8<<20, "dirty bytes per flush (drain threshold)")
	flag.IntVar(&cfg.ckpt, "ckpt", 8, "flushes per checkpoint")
	flag.Parse()
	cfg.chunk = *chunkKiB << 10
	if *quick {
		cfg.keys, cfg.valMB, cfg.ops, cfg.threshold = 4, 1, 3000, 1<<20
	}
	if cfg.chunk&(cfg.chunk-1) != 0 {
		fmt.Fprintln(os.Stderr, "ropechunk: chunk size must be a power of two")
		os.Exit(1)
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "ropechunk:", err)
		os.Exit(1)
	}
}

type row struct {
	workload string
	ops      int
	dur      time.Duration
	p50, p99 time.Duration
	maxLat   time.Duration
	wa       float64
	fileMB   float64
	walMB    float64
	vmhwmMB  float64
}

func runAll(cfg config, out io.Writer) error {
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "ropechunk")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("ropechunk-%s-c%d-%s.db", cfg.store, cfg.chunk, cfg.mix))
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".aki-wal"} {
		os.Remove(p)
	}

	keys := make([][]byte, cfg.keys)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "r:%04d", i)
	}
	st, err := openStore(cfg, path, keys)
	if err != nil {
		return err
	}
	defer st.close()
	r := newRope(st, cfg, keys)

	// Preload: full values for the in-place mixes, one chunk per key for
	// the append mix so the growth path is what gets measured.
	preload := int64(cfg.valMB) << 20
	if cfg.mix == "append" {
		preload = int64(cfg.chunk)
	}
	start := time.Now()
	pat := make([]byte, 4096)
	for i := range pat {
		pat[i] = byte('a' + i%26)
	}
	for ki := range keys {
		for off := int64(0); off < preload; off += int64(len(pat)) {
			n := min(int64(len(pat)), preload-off)
			if err := r.setRange(ki, off, pat[:n]); err != nil {
				return err
			}
			if r.dirtyBytes >= cfg.threshold {
				if err := r.flush(); err != nil {
					return err
				}
			}
		}
	}
	if err := r.flush(); err != nil {
		return err
	}
	if err := st.checkpoint(); err != nil {
		return err
	}
	emit(cfg, out, row{workload: "load", ops: cfg.keys, dur: time.Since(start),
		fileMB: st.dataMB()})
	r.logical, r.flushed, r.flushDur, r.walMaxMB = 0, 0, nil, 0

	// The measured mix: 90% the heavy op, 10% the light one, per-op cost
	// timed into its class so each row is an intrinsic rate and the flush
	// row carries the amortized IO bill.
	rng := rand.New(rand.NewSource(97))
	pickChunk := chunkPicker(cfg, rng, preload)
	var wLat, rLat []time.Duration
	var wDur, rDur time.Duration
	writes, reads := 0, 0
	for range cfg.ops {
		heavy := rng.Intn(10) > 0
		ki := rng.Intn(cfg.keys)
		var werr error
		t0 := time.Now()
		switch {
		case cfg.mix == "setrange" && heavy, cfg.mix == "getrange" && !heavy:
			off := pickOff(rng, pickChunk, cfg, r.totalLen[ki], int64(cfg.wlen))
			werr = r.setRange(ki, off, pat[:cfg.wlen])
		case cfg.mix == "append" && heavy:
			werr = r.setRange(ki, r.totalLen[ki], pat[:cfg.wlen])
		case cfg.mix == "setbit" && heavy:
			off := pickOff(rng, pickChunk, cfg, r.totalLen[ki], 1)
			werr = r.setBit(ki, off*8+int64(rng.Intn(8)), rng.Intn(2) == 0)
		case cfg.mix == "setbit":
			off := pickOff(rng, pickChunk, cfg, r.totalLen[ki], 1)
			_, werr = r.getBit(ki, off*8+int64(rng.Intn(8)))
		default:
			off := pickOff(rng, pickChunk, cfg, r.totalLen[ki], int64(cfg.rlen))
			_, werr = r.getRange(ki, off, int64(cfg.rlen))
		}
		lat := time.Since(t0)
		if werr != nil {
			return werr
		}
		wrote := heavy != (cfg.mix == "getrange")
		if wrote {
			wLat, wDur, writes = append(wLat, lat), wDur+lat, writes+1
		} else {
			rLat, rDur, reads = append(rLat, lat), rDur+lat, reads+1
		}
		if r.dirtyBytes >= cfg.threshold {
			if err := r.flush(); err != nil {
				return err
			}
		}
	}
	if err := r.flush(); err != nil {
		return err
	}

	wa := 0.0
	if r.logical > 0 {
		wa = float64(r.flushed) / float64(r.logical)
	}
	p50, p99, maxLat := percentiles(wLat)
	emit(cfg, out, row{workload: cfg.mix + "-write", ops: writes, dur: wDur,
		p50: p50, p99: p99, maxLat: maxLat, wa: wa})
	p50, p99, maxLat = percentiles(rLat)
	emit(cfg, out, row{workload: cfg.mix + "-read", ops: reads, dur: rDur,
		p50: p50, p99: p99, maxLat: maxLat})
	var fTotal, fWorst time.Duration
	for _, fd := range r.flushDur {
		fTotal += fd
		if fd > fWorst {
			fWorst = fd
		}
	}
	emit(cfg, out, row{workload: "flush", ops: len(r.flushDur), dur: fTotal,
		maxLat: fWorst, fileMB: st.dataMB(), walMB: r.walMaxMB, vmhwmMB: vmhwmMB()})
	return nil
}

// chunkPicker returns the chunk-index picker for the offset distribution:
// zipf concentrates writes on a hot chunk subset, which is the coalescing
// best case, uniform the worst.
func chunkPicker(cfg config, rng *rand.Rand, valLen int64) func() int64 {
	nChunks := max(valLen/int64(cfg.chunk), 1)
	if cfg.dist == "zipf" {
		z := rand.NewZipf(rng, 1.1, 1, uint64(nChunks-1))
		return func() int64 { return int64(z.Uint64()) }
	}
	return func() int64 { return rng.Int63n(nChunks) }
}

// pickOff picks a byte offset: a chunk by distribution, then a uniform
// position inside it, clamped so a span of n stays inside the value.
func pickOff(rng *rand.Rand, pickChunk func() int64, cfg config, totalLen, n int64) int64 {
	off := pickChunk()*int64(cfg.chunk) + rng.Int63n(int64(cfg.chunk))
	if off+n > totalLen {
		off = max(totalLen-n, 0)
	}
	return off
}

func percentiles(all []time.Duration) (p50, p99, max time.Duration) {
	if len(all) == 0 {
		return 0, 0, 0
	}
	slices.Sort(all)
	return all[len(all)/2], all[len(all)*99/100], all[len(all)-1]
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
	fmt.Fprintf(out, "%s,%d,%s,%s,%d,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.1f,%.1f,%.1f\n",
		cfg.store, cfg.chunk>>10, cfg.mix, cfg.dist, cfg.keys, cfg.valMB,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.wa, r.fileMB, r.walMB, r.vmhwmMB)
}
