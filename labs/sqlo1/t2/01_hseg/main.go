// Lab: hash segment size sweep (spec 2064/sqlo1 doc 06 section 2,
// milestone T2 lab 01).
//
// T2 slice 2 bakes seg_max, the encoded-segment split threshold, and the
// choice is a bandwidth trade: rule W4 says every mutating command costs
// one full segment post-image in the WAL, so a bigger segment makes each
// HSET carry more WAL bytes, while a smaller one means more segments per
// hash, more fence entries, more splits, and more rows written at drain
// for the same field churn. This lab prices that trade at 2016/4032/8064
// bytes across field-size distributions and HSET:HGET ratios.
//
// The model is the doc 06 shape resident: fields partitioned by fh into
// segments found through the fence by binary search, entries sorted by
// fh, splits at the entry-median fh when the encoded size crosses
// seg_max. Segments drain as encoded blobs in drain-shaped transactions
// on the engine's byte threshold; the WAL column is modeled arithmetic
// under W2 and W4 (segment post-image per HSET, root post-image only
// when the fence changed) because the aki WAL is not SQLite's. An oracle
// test pins the model against a reference map, including split coverage
// and root count exactness, through the SQL readback path.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// Roots live in kv and segments in seg keyed (k, segid), the same
// record-per-segment shape the generic engine machinery drains; the meta
// high-water row moves with every batch. Track A proper maps hashes to
// helem rows instead (doc 06 section 7), but seg_max is a constant of
// the shared segment machinery, so the lab prices segment records.
const (
	schemaSQL = `CREATE TABLE IF NOT EXISTS kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS seg (k BLOB, segid INTEGER, v BLOB,
  PRIMARY KEY (k, segid)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK (id = 0),
  hw INTEGER) WITHOUT ROWID;
INSERT OR IGNORE INTO meta (id, hw) VALUES (0, 0);`

	segPutSQL  = `INSERT INTO seg (k, segid, v) VALUES (?1, ?2, ?3) ON CONFLICT (k, segid) DO UPDATE SET v = excluded.v`
	segGetSQL  = `SELECT v FROM seg WHERE k = ?1 AND segid = ?2`
	rootPutSQL = `INSERT INTO kv (k, t, exp, gen, v, crc) VALUES (?1, 4, 0, 0, ?2, 0) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
	setHWSQL   = `UPDATE meta SET hw = ?1 WHERE id = 0`
)

type config struct {
	dir       string
	segMax    int
	fdist     string
	setpct    int
	dist      string
	keys      int
	fields    int
	ops       int
	threshold int
	ckpt      int
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	sput  *sqlite3.Stmt
	sget  *sqlite3.Stmt
	rput  *sqlite3.Stmt
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
		{&d.sput, segPutSQL},
		{&d.sget, segGetSQL},
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

// fh is the field-space partitioning hash: FNV-1a folded through a
// splitmix64 finalizer so short ascii field names still spread across
// the full u64 range the fences partition.
func fh(field []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range field {
		h = (h ^ uint64(b)) * 1099511628211
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	return h ^ h>>31
}

type entry struct {
	fh    uint64
	field []byte
	value []byte
}

func entrySize(e entry) int { return 7 + len(e.field) + len(e.value) }

const segHdrSize = 12  // u16 n, u16 reserved, u64 min_expire_ms
const rootHdrSize = 36 // doc 06 section 2.2 fixed fields
const fenceEntSize = 16

type segment struct {
	id      uint64
	lo      uint64
	entries []entry
	size    int // encoded size, header included
	dirty   bool
}

type fenceEnt struct {
	lo    uint64
	segid uint64
}

// hash is one resident segmented hash: the fence sorted by lo, the
// segments it maps to, and the exact count rule W1 keeps in the root.
type hash struct {
	key       []byte
	fence     []fenceEnt
	segs      map[uint64]*segment
	count     int64
	nextSegid uint64
	rootDirty bool
}

func newHash(key []byte) *hash {
	s := &segment{id: 0, lo: 0}
	s.size = segHdrSize
	return &hash{
		key:       key,
		fence:     []fenceEnt{{lo: 0, segid: 0}},
		segs:      map[uint64]*segment{0: s},
		nextSegid: 1,
		rootDirty: true,
	}
}

func (h *hash) rootSize() int { return rootHdrSize + fenceEntSize*len(h.fence) }

// seg returns the segment covering f per the fence binary search.
func (h *hash) seg(f uint64) *segment {
	i := sort.Search(len(h.fence), func(i int) bool { return h.fence[i].lo > f })
	return h.segs[h.fence[i-1].segid]
}

// find locates field within s: binary search on fh, then field equality
// across an fh collision run.
func (s *segment) find(f uint64, field []byte) (int, bool) {
	i := sort.Search(len(s.entries), func(i int) bool { return s.entries[i].fh >= f })
	for ; i < len(s.entries) && s.entries[i].fh == f; i++ {
		if string(s.entries[i].field) == string(field) {
			return i, true
		}
	}
	return i, false
}

// store owns the resident hashes and the drain bookkeeping: dirty bytes
// against the engine threshold, logical and flushed bytes for WA, and
// the modeled WAL bill under rules W2 and W4.
type store struct {
	d   *db
	cfg config
	hs  []*hash

	dirtyBytes int
	seq        int64
	flushes    int
	logical    int64
	flushed    int64
	walBytes   int64
	splits     int
	flushDur   []time.Duration
	walMaxMB   float64
}

// hset inserts or replaces one field and bills the WAL model: the
// touched segment's full post-image, plus both halves and the root
// post-image when the write split the segment (fence change, rule W2).
func (st *store) hset(ki int, field, value []byte) {
	h := st.hs[ki]
	f := fh(field)
	s := h.seg(f)
	i, found := s.find(f, field)
	if found {
		old := &s.entries[i]
		delta := len(value) - len(old.value)
		s.size += delta
		if s.dirty {
			st.dirtyBytes += delta
		}
		old.value = value
	} else {
		s.entries = slices.Insert(s.entries, i, entry{fh: f, field: field, value: value})
		s.size += entrySize(s.entries[i])
		if s.dirty {
			st.dirtyBytes += entrySize(s.entries[i])
		}
		h.count++
		h.rootDirty = true // rule W1: cardinality change pins the root
	}
	if !s.dirty {
		s.dirty = true
		st.dirtyBytes += s.size
	}
	st.logical += int64(len(field) + len(value))
	if s.size > st.cfg.segMax {
		if ns := st.split(h, s); ns != nil {
			st.walBytes += int64(s.size + ns.size + h.rootSize())
			return
		}
	}
	st.walBytes += int64(s.size)
}

// split cuts s at its entry-median fh, keeps the lower half in place,
// and fences in a new segment for the upper half. A run of identical fh
// values at the median cannot split, which a 64-bit fh never produces in
// practice; the guard just refuses rather than corrupt the fence.
func (st *store) split(h *hash, s *segment) *segment {
	mid := len(s.entries) / 2
	newLo := s.entries[mid].fh
	for mid > 0 && s.entries[mid-1].fh == newLo {
		mid--
	}
	if mid == 0 || newLo <= s.lo {
		return nil
	}
	ns := &segment{id: h.nextSegid, lo: newLo, dirty: true}
	h.nextSegid++
	ns.entries = append(ns.entries, s.entries[mid:]...)
	s.entries = s.entries[:mid]
	moved := 0
	for i := range ns.entries {
		moved += entrySize(ns.entries[i])
	}
	ns.size = segHdrSize + moved
	s.size -= moved
	h.segs[ns.id] = ns
	i := sort.Search(len(h.fence), func(i int) bool { return h.fence[i].lo > newLo })
	h.fence = slices.Insert(h.fence, i, fenceEnt{lo: newLo, segid: ns.id})
	h.rootDirty = true
	// The dirty pool held s at its pre-split size; the split only adds
	// one new segment header on top of the same entry bytes.
	st.dirtyBytes += segHdrSize
	st.splits++
	return ns
}

func (st *store) hget(ki int, field []byte) []byte {
	h := st.hs[ki]
	f := fh(field)
	s := h.seg(f)
	if i, found := s.find(f, field); found {
		return s.entries[i].value
	}
	return nil
}

func encodeSeg(s *segment) []byte {
	buf := make([]byte, 0, s.size)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s.entries)))
	buf = binary.LittleEndian.AppendUint16(buf, 0)
	buf = binary.LittleEndian.AppendUint64(buf, 0)
	for i := range s.entries {
		e := &s.entries[i]
		buf = append(buf, 0)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.field)))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.value)))
		buf = append(buf, e.field...)
		buf = append(buf, e.value...)
	}
	return buf
}

// encodeRoot lays down the doc 06 section 2.2 payload with the fence
// inline; the 16 bit fence-entry meta carries the entry count as the
// fill class, capped, which is what HRANDFIELD weighting reads.
func encodeRoot(h *hash) []byte {
	buf := make([]byte, 0, h.rootSize())
	buf = append(buf, 2, 0, 0, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(h.count))
	buf = binary.LittleEndian.AppendUint64(buf, h.nextSegid)
	buf = binary.LittleEndian.AppendUint64(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(h.fence)))
	for _, fe := range h.fence {
		buf = binary.LittleEndian.AppendUint64(buf, fe.lo)
		fill := min(len(h.segs[fe.segid].entries), 0x7fff)
		buf = binary.LittleEndian.AppendUint64(buf, fe.segid|uint64(fill)<<48)
	}
	return buf
}

// flush drains every dirty segment and root in one transaction, moving
// the high-water mark with the batch; the root drains whenever dirty
// (count or fence changed) per rule W1, coalesced to one write per
// drain per rule W4.
func (st *store) flush() error {
	t0 := time.Now()
	any := false
	var txn sqlite3.Txn
	for _, h := range st.hs {
		for _, s := range h.segs {
			if !s.dirty {
				continue
			}
			if !any {
				var err error
				if txn, err = st.d.conn.BeginImmediate(); err != nil {
					return err
				}
				any = true
			}
			row := encodeSeg(s)
			if err := st.d.sput.BindBlob(1, h.key); err != nil {
				txn.Rollback()
				return err
			}
			if err := st.d.sput.BindInt64(2, int64(s.id)); err != nil {
				txn.Rollback()
				return err
			}
			if err := st.d.sput.BindBlob(3, row); err != nil {
				txn.Rollback()
				return err
			}
			if _, err := stepReset(st.d.sput); err != nil {
				txn.Rollback()
				return err
			}
			st.flushed += int64(len(row))
			s.dirty = false
		}
		if h.rootDirty {
			if !any {
				var err error
				if txn, err = st.d.conn.BeginImmediate(); err != nil {
					return err
				}
				any = true
			}
			row := encodeRoot(h)
			if err := st.d.rput.BindBlob(1, h.key); err != nil {
				txn.Rollback()
				return err
			}
			if err := st.d.rput.BindBlob(2, row); err != nil {
				txn.Rollback()
				return err
			}
			if _, err := stepReset(st.d.rput); err != nil {
				txn.Rollback()
				return err
			}
			st.flushed += int64(len(row))
			h.rootDirty = false
		}
	}
	if !any {
		return nil
	}
	st.seq++
	if err := st.d.hw1.BindInt64(1, st.seq); err != nil {
		txn.Rollback()
		return err
	}
	if _, err := stepReset(st.d.hw1); err != nil {
		txn.Rollback()
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	st.dirtyBytes = 0
	if wm := fileMB(st.d.path + "-wal"); wm > st.walMaxMB {
		st.walMaxMB = wm
	}
	if st.flushes++; st.flushes%st.cfg.ckpt == 0 {
		if err := st.d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			return err
		}
	}
	st.flushDur = append(st.flushDur, time.Since(t0))
	return nil
}

// fieldSizes returns the field and value length bounds for a
// distribution class: small is counters-in-a-hash, med a session store,
// large a document store.
func fieldSizes(fdist string) (fmin, fmax, vmin, vmax int) {
	switch fdist {
	case "small":
		return 8, 16, 8, 32
	case "med":
		return 16, 32, 32, 128
	case "large":
		return 32, 64, 128, 512
	}
	return 0, 0, 0, 0
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.IntVar(&cfg.segMax, "seg", 4032, "encoded segment split threshold in bytes")
	flag.StringVar(&cfg.fdist, "fdist", "med", "field size distribution: small, med, large")
	flag.IntVar(&cfg.setpct, "setpct", 50, "HSET percentage of the mix (rest HGET)")
	flag.StringVar(&cfg.dist, "dist", "zipf", "field pick distribution: zipf or uniform")
	flag.IntVar(&cfg.keys, "keys", 16, "hash key count")
	flag.IntVar(&cfg.fields, "fields", 20000, "fields per hash")
	flag.IntVar(&cfg.ops, "ops", 400000, "ops in the measured mix")
	flag.IntVar(&cfg.threshold, "threshold", 8<<20, "dirty bytes per flush (drain threshold)")
	flag.IntVar(&cfg.ckpt, "ckpt", 8, "flushes per checkpoint")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.fields, cfg.ops, cfg.threshold = 4, 2000, 8000, 1<<20
	}
	if fmin, _, _, _ := fieldSizes(cfg.fdist); fmin == 0 {
		fmt.Fprintln(os.Stderr, "hseg: fdist must be small, med, or large")
		os.Exit(1)
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hseg:", err)
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
	walPerOp float64
	fileMB   float64
	walMB    float64
	vmhwmMB  float64
}

func runAll(cfg config, out io.Writer) error {
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "hseg")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("hseg-s%d-%s.db", cfg.segMax, cfg.fdist))
	os.Remove(path)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	d, err := openDB(path)
	if err != nil {
		return err
	}
	defer d.close()

	st := &store{d: d, cfg: cfg, hs: make([]*hash, cfg.keys)}
	fields := make([][][]byte, cfg.keys)
	rng := rand.New(rand.NewSource(43))
	fmin, fmax, vmin, vmax := fieldSizes(cfg.fdist)
	newValue := func() []byte {
		v := make([]byte, vmin+rng.Intn(vmax-vmin+1))
		for i := range v {
			v[i] = byte('a' + rng.Intn(26))
		}
		return v
	}

	// Preload every field through the same hset path so splits happen
	// the way slice 2 will do them; the measured mix then overwrites in
	// place, which is the steady state the sweep prices.
	start := time.Now()
	for ki := range st.hs {
		st.hs[ki] = newHash(fmt.Appendf(nil, "h:%04d", ki))
		fields[ki] = make([][]byte, cfg.fields)
		for i := range fields[ki] {
			name := fmt.Appendf(nil, "f%04d:%08d:", ki, i)
			for target := fmin + rng.Intn(fmax-fmin+1); len(name) < target; {
				name = append(name, byte('a'+rng.Intn(26)))
			}
			fields[ki][i] = name
			st.hset(ki, name, newValue())
			if st.dirtyBytes >= cfg.threshold {
				if err := st.flush(); err != nil {
					return err
				}
			}
		}
	}
	if err := st.flush(); err != nil {
		return err
	}
	if err := d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}
	segCount := 0
	for _, h := range st.hs {
		segCount += len(h.segs)
	}
	fmt.Fprintf(os.Stderr, "hseg: preload %d splits, %d segments, %.1f fields/segment\n",
		st.splits, segCount, float64(cfg.keys*cfg.fields)/float64(segCount))
	emit(cfg, out, row{workload: "load", ops: cfg.keys * cfg.fields,
		dur: time.Since(start), fileMB: fileMB(path)})
	st.logical, st.flushed, st.walBytes, st.splits = 0, 0, 0, 0
	st.flushDur, st.walMaxMB = nil, 0

	// The measured mix: HSET and HGET per setpct over the preloaded
	// field universe, per-op cost timed into its class, the flush row
	// carrying the amortized IO bill.
	pickField := fieldPicker(cfg, rng)
	var wLat, rLat []time.Duration
	var wDur, rDur time.Duration
	writes, reads := 0, 0
	for range cfg.ops {
		ki := rng.Intn(cfg.keys)
		field := fields[ki][pickField()]
		if rng.Intn(100) < cfg.setpct {
			v := newValue()
			t0 := time.Now()
			st.hset(ki, field, v)
			wLat = append(wLat, time.Since(t0))
			wDur += wLat[len(wLat)-1]
			writes++
		} else {
			t0 := time.Now()
			v := st.hget(ki, field)
			lat := time.Since(t0)
			if v == nil {
				return fmt.Errorf("hget missed a preloaded field %q", field)
			}
			rLat = append(rLat, lat)
			rDur += lat
			reads++
		}
		if st.dirtyBytes >= cfg.threshold {
			if err := st.flush(); err != nil {
				return err
			}
		}
	}
	if err := st.flush(); err != nil {
		return err
	}

	wa, walPerOp := 0.0, 0.0
	if st.logical > 0 {
		wa = float64(st.flushed) / float64(st.logical)
	}
	if writes > 0 {
		walPerOp = float64(st.walBytes) / float64(writes)
	}
	p50, p99, maxLat := percentiles(wLat)
	emit(cfg, out, row{workload: "hset", ops: writes, dur: wDur,
		p50: p50, p99: p99, maxLat: maxLat, wa: wa, walPerOp: walPerOp})
	p50, p99, maxLat = percentiles(rLat)
	emit(cfg, out, row{workload: "hget", ops: reads, dur: rDur,
		p50: p50, p99: p99, maxLat: maxLat})
	var fTotal, fWorst time.Duration
	for _, fd := range st.flushDur {
		fTotal += fd
		if fd > fWorst {
			fWorst = fd
		}
	}
	emit(cfg, out, row{workload: "flush", ops: len(st.flushDur), dur: fTotal,
		maxLat: fWorst, fileMB: fileMB(path), walMB: st.walMaxMB, vmhwmMB: vmhwmMB()})
	return nil
}

// fieldPicker returns the field-index picker: zipf concentrates the mix
// on a hot subset, which is both the W4 coalescing best case and the
// PRED-SQLO1-T2-WAL scenario; uniform spreads it.
func fieldPicker(cfg config, rng *rand.Rand) func() int {
	if cfg.dist == "zipf" {
		z := rand.NewZipf(rng, 1.1, 1, uint64(cfg.fields-1))
		return func() int { return int(z.Uint64()) }
	}
	return func() int { return rng.Intn(cfg.fields) }
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
	fmt.Fprintf(out, "%d,%s,%d,%s,%d,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.0f,%.1f,%.1f,%.1f\n",
		cfg.segMax, cfg.fdist, cfg.setpct, cfg.dist, cfg.keys, cfg.fields,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.wa, r.walPerOp, r.fileMB, r.walMB, r.vmhwmMB)
}
