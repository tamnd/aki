// Lab: hash fence flatness (spec 2064/sqlo1 doc 06 sections 1-2.3,
// milestone T2 lab 02).
//
// The design target of the hash model is that a point op on a
// billion-field hash costs the same records as on a ten-field one:
// root, then (fence-paged only) one fence page, then one segment,
// never more. T2 slice 9 bakes the fence paging and its 3-record cold
// path; PRED-SQLO1-T2-FLAT rides on the curve this lab draws. The lab
// materializes the full ladder (inline root, segmented with the fence
// inline, fence-paged) at 10^2 to 10^9 fields and prices the point
// lookup cold and hot, counting record reads on every op and failing
// the run if any lookup exceeds its mode's ceiling.
//
// Preload builds segments directly at target occupancy instead of
// pushing 10^9 HSETs through a resident model: field hashes are placed
// deterministically inside each segment's fence range, the field name
// is derived from the hash and the value from the name, so any (seg,
// slot) pair can be regenerated for lookup and verified byte-exact
// while the clock runs. RAM per key is reported as measured root and
// fence bytes, which is what a resident hash pins beyond its hot
// segments.
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

// Roots in kv, segments in seg, fence pages in fpage: the rtype 5
// record of doc 06 section 2.3 expressed as its own row table so the
// three reads of the cold path stay three probes here too.
const (
	schemaSQL = `CREATE TABLE IF NOT EXISTS kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS seg (k BLOB, segid INTEGER, v BLOB,
  PRIMARY KEY (k, segid)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS fpage (k BLOB, page INTEGER, v BLOB,
  PRIMARY KEY (k, page)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK (id = 0),
  hw INTEGER) WITHOUT ROWID;
INSERT OR IGNORE INTO meta (id, hw) VALUES (0, 0);`

	rootGetSQL = `SELECT v FROM kv WHERE k = ?1`
	rootPutSQL = `INSERT INTO kv (k, t, exp, gen, v, crc) VALUES (?1, 4, 0, 0, ?2, 0) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
	segGetSQL  = `SELECT v FROM seg WHERE k = ?1 AND segid = ?2`
	segPutSQL  = `INSERT INTO seg (k, segid, v) VALUES (?1, ?2, ?3) ON CONFLICT (k, segid) DO UPDATE SET v = excluded.v`
	pageGetSQL = `SELECT v FROM fpage WHERE k = ?1 AND page = ?2`
	pagePutSQL = `INSERT INTO fpage (k, page, v) VALUES (?1, ?2, ?3) ON CONFLICT (k, page) DO UPDATE SET v = excluded.v`
	setHWSQL   = `UPDATE meta SET hw = ?1 WHERE id = 0`
)

const (
	segHdrSize   = 12
	rootHdrSize  = 36
	fenceEntSize = 16
	pageHdrSize  = 8
	fieldLen     = 17 // "f" + 16 hex digits of fh
	valueLen     = 32
	entSize      = 7 + fieldLen + valueLen
)

type config struct {
	dir         string
	fields      int64
	segMax      int
	inlineMax   int
	inlineCount int
	fenceMax    int
	pageEnts    int
	reps        int
	hotreps     int
	ckpt        int
}

// layout is the arithmetic the ladder derives from a field count: how
// many entries per segment, how many segments, which representation.
type layout struct {
	mode      string // inline, segmented, fence-paged
	fps       int64  // fields per segment
	segs      int64
	pages     int64
	rangeW    uint64 // fence range width per segment
	step      uint64 // fh step between entries in a segment
	wantReads int
}

func planLayout(cfg config) (layout, error) {
	var l layout
	l.fps = int64((cfg.segMax - segHdrSize) / entSize)
	l.segs = (cfg.fields + l.fps - 1) / l.fps
	switch {
	case cfg.fields <= int64(cfg.inlineCount) && rootHdrSize+cfg.fields*entSize <= int64(cfg.inlineMax):
		l.mode, l.segs, l.fps, l.wantReads = "inline", 1, cfg.fields, 1
	case l.segs*fenceEntSize <= int64(cfg.fenceMax):
		l.mode, l.wantReads = "segmented", 2
	default:
		l.mode, l.wantReads = "fence-paged", 3
		l.pages = (l.segs + int64(cfg.pageEnts) - 1) / int64(cfg.pageEnts)
		if l.pages > int64(cfg.pageEnts) {
			return l, fmt.Errorf("%d fence pages exceed the one-level index cap %d (doc 06 keeps a third level out of scope)", l.pages, cfg.pageEnts)
		}
	}
	l.rangeW = ^uint64(0) / uint64(l.segs)
	l.step = l.rangeW / uint64(l.fps)
	return l, nil
}

// fhAt places entry j of segment s deterministically inside the
// segment's fence range; field names and values derive from it, so a
// lookup can regenerate and verify any preloaded field.
func fhAt(l layout, s, j int64) uint64 {
	return l.rangeW*uint64(s) + l.step*uint64(j) + l.step/2
}

func fieldAt(f uint64) []byte {
	return fmt.Appendf(nil, "f%016x", f)
}

func valueAt(f uint64) []byte {
	v := make([]byte, valueLen)
	for i := 0; i < valueLen; i += 8 {
		binary.LittleEndian.PutUint64(v[i:], f^uint64(i))
	}
	return v
}

// segFields returns how many entries segment s holds: the last segment
// takes the remainder.
func segFields(cfg config, l layout, s int64) int64 {
	if s == l.segs-1 {
		return cfg.fields - l.fps*s
	}
	return l.fps
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	rget  *sqlite3.Stmt
	rput  *sqlite3.Stmt
	sget  *sqlite3.Stmt
	sput  *sqlite3.Stmt
	pget  *sqlite3.Stmt
	pput  *sqlite3.Stmt
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
		{&d.rget, rootGetSQL},
		{&d.rput, rootPutSQL},
		{&d.sget, segGetSQL},
		{&d.sput, segPutSQL},
		{&d.pget, pageGetSQL},
		{&d.pput, pagePutSQL},
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

func (d *db) getBlob1(stmt *sqlite3.Stmt, key []byte) ([]byte, error) {
	if err := stmt.BindBlob(1, key); err != nil {
		return nil, err
	}
	var v []byte
	if stmt.Step() {
		v = slices.Clone(stmt.ColumnBlob(0, nil))
	}
	err := stmt.Err()
	if rerr := stmt.Reset(); err == nil {
		err = rerr
	}
	return v, err
}

func (d *db) getBlob2(stmt *sqlite3.Stmt, key []byte, id int64) ([]byte, error) {
	if err := stmt.BindBlob(1, key); err != nil {
		return nil, err
	}
	if err := stmt.BindInt64(2, id); err != nil {
		return nil, err
	}
	var v []byte
	if stmt.Step() {
		v = slices.Clone(stmt.ColumnBlob(0, nil))
	}
	err := stmt.Err()
	if rerr := stmt.Reset(); err == nil {
		err = rerr
	}
	return v, err
}

func encodeSegAt(cfg config, l layout, s int64) []byte {
	n := segFields(cfg, l, s)
	buf := make([]byte, 0, segHdrSize+n*entSize)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(n))
	buf = binary.LittleEndian.AppendUint16(buf, 0)
	buf = binary.LittleEndian.AppendUint64(buf, 0)
	for j := range n {
		f := fhAt(l, s, j)
		field := fieldAt(f)
		buf = append(buf, 0)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(field)))
		buf = binary.LittleEndian.AppendUint32(buf, valueLen)
		buf = append(buf, field...)
		buf = append(buf, valueAt(f)...)
	}
	return buf
}

// preload writes the ladder for the configured field count: segments in
// batched transactions, then fence pages, then the root, checkpointing
// as it goes so the WAL never holds the dataset.
func preload(d *db, cfg config, l layout, key []byte) (rootB int, fenceB int64, err error) {
	if l.mode == "inline" {
		root := make([]byte, 0, rootHdrSize+cfg.fields*entSize)
		root = append(root, 1, 0, 0, 0)
		root = binary.LittleEndian.AppendUint32(root, 0)
		root = binary.LittleEndian.AppendUint64(root, uint64(cfg.fields))
		root = binary.LittleEndian.AppendUint64(root, 0)
		root = binary.LittleEndian.AppendUint64(root, 0)
		root = binary.LittleEndian.AppendUint32(root, 0)
		body := encodeSegAt(cfg, l, 0)
		root = append(root, body[segHdrSize:]...)
		if err := putRoot(d, key, root); err != nil {
			return 0, 0, err
		}
		return len(root), 0, d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	}

	txns := 0
	var txn sqlite3.Txn
	inTxn := false
	begin := func() error {
		var err error
		txn, err = d.conn.BeginImmediate()
		inTxn = err == nil
		return err
	}
	commit := func() error {
		inTxn = false
		if err := txn.Commit(); err != nil {
			return err
		}
		if txns++; txns%cfg.ckpt == 0 {
			return d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		}
		return nil
	}
	fail := func(err error) error {
		if inTxn {
			txn.Rollback()
		}
		return err
	}
	if err := begin(); err != nil {
		return 0, 0, err
	}
	for s := int64(0); s < l.segs; s++ {
		if err := putBlob2(d.sput, key, s, encodeSegAt(cfg, l, s)); err != nil {
			return 0, 0, fail(err)
		}
		if (s+1)%512 == 0 {
			if err := commit(); err != nil {
				return 0, 0, fail(err)
			}
			if err := begin(); err != nil {
				return 0, 0, err
			}
		}
	}

	fenceEnt := func(buf []byte, s int64) []byte {
		buf = binary.LittleEndian.AppendUint64(buf, l.rangeW*uint64(s))
		fill := min(segFields(cfg, l, s), 0x7fff)
		return binary.LittleEndian.AppendUint64(buf, uint64(s)|uint64(fill)<<48)
	}
	root := make([]byte, 0, rootHdrSize)
	hflags := byte(0)
	if l.mode == "fence-paged" {
		hflags = 1
	}
	root = append(root, 2, hflags, 0, 0)
	root = binary.LittleEndian.AppendUint32(root, 0)
	root = binary.LittleEndian.AppendUint64(root, uint64(cfg.fields))
	root = binary.LittleEndian.AppendUint64(root, uint64(l.segs))
	root = binary.LittleEndian.AppendUint64(root, 0)
	root = binary.LittleEndian.AppendUint32(root, uint32(l.segs))
	if l.mode == "segmented" {
		for s := int64(0); s < l.segs; s++ {
			root = fenceEnt(root, s)
		}
	} else {
		root = binary.LittleEndian.AppendUint32(root, uint32(l.pages))
		for p := int64(0); p < l.pages; p++ {
			first := p * int64(cfg.pageEnts)
			last := min(first+int64(cfg.pageEnts), l.segs)
			page := make([]byte, 0, pageHdrSize+(last-first)*fenceEntSize)
			page = binary.LittleEndian.AppendUint32(page, uint32(last-first))
			page = binary.LittleEndian.AppendUint32(page, 0)
			for s := first; s < last; s++ {
				page = fenceEnt(page, s)
			}
			if err := putBlob2(d.pput, key, p, page); err != nil {
				return 0, 0, fail(err)
			}
			fenceB += int64(len(page))
			root = binary.LittleEndian.AppendUint64(root, l.rangeW*uint64(first))
			root = binary.LittleEndian.AppendUint64(root, uint64(p))
		}
	}
	if err := putRoot(d, key, root); err != nil {
		return 0, 0, fail(err)
	}
	if err := commit(); err != nil {
		return 0, 0, fail(err)
	}
	return len(root), fenceB, d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
}

func putRoot(d *db, key, v []byte) error {
	if err := d.rput.BindBlob(1, key); err != nil {
		return err
	}
	if err := d.rput.BindBlob(2, v); err != nil {
		return err
	}
	_, err := stepReset(d.rput)
	return err
}

func putBlob2(stmt *sqlite3.Stmt, key []byte, id int64, v []byte) error {
	if err := stmt.BindBlob(1, key); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, id); err != nil {
		return err
	}
	if err := stmt.BindBlob(3, v); err != nil {
		return err
	}
	_, err := stepReset(stmt)
	return err
}

// searchEnts binary-searches 16 B (lo, packed) entries laid out at off
// inside buf, returning the ordinal of the last entry with lo <= f.
func searchEnts(buf []byte, off, n int, f uint64) int {
	i := sort.Search(n, func(i int) bool {
		return binary.LittleEndian.Uint64(buf[off+i*fenceEntSize:]) > f
	})
	return i - 1
}

// lookup is the cold point path under measurement: root, fence page if
// paged, segment, then an in-segment scan, returning the value and the
// record reads it took.
func lookup(d *db, key []byte, f uint64, field []byte) ([]byte, int, error) {
	root, err := d.getBlob1(d.rget, key)
	if err != nil {
		return nil, 1, fmt.Errorf("root read: %w", err)
	}
	if root == nil {
		return nil, 1, fmt.Errorf("root row missing")
	}
	reads := 1
	if root[0] == 1 { // inline: fields live in the root
		v := scanEntries(root, rootHdrSize, field)
		return v, reads, nil
	}
	var segid uint64
	if root[1]&1 == 0 { // segmented: fence inline in the root
		segCount := int(binary.LittleEndian.Uint32(root[32:]))
		i := searchEnts(root, rootHdrSize, segCount, f)
		segid = binary.LittleEndian.Uint64(root[rootHdrSize+i*fenceEntSize+8:]) & (1<<48 - 1)
	} else { // fence-paged: root page index, then one fence page
		nPages := int(binary.LittleEndian.Uint32(root[rootHdrSize:]))
		pi := searchEnts(root, rootHdrSize+4, nPages, f)
		pageNo := binary.LittleEndian.Uint64(root[rootHdrSize+4+pi*fenceEntSize+8:])
		page, err := d.getBlob2(d.pget, key, int64(pageNo))
		if err != nil {
			return nil, reads, fmt.Errorf("fence page %d read: %w", pageNo, err)
		}
		if page == nil {
			return nil, reads, fmt.Errorf("fence page %d missing", pageNo)
		}
		reads++
		n := int(binary.LittleEndian.Uint32(page))
		i := searchEnts(page, pageHdrSize, n, f)
		segid = binary.LittleEndian.Uint64(page[pageHdrSize+i*fenceEntSize+8:]) & (1<<48 - 1)
	}
	seg, err := d.getBlob2(d.sget, key, int64(segid))
	if err != nil {
		return nil, reads, fmt.Errorf("segment %d read: %w", segid, err)
	}
	if seg == nil {
		return nil, reads, fmt.Errorf("segment %d missing", segid)
	}
	reads++
	return scanEntries(seg, segHdrSize, field), reads, nil
}

func scanEntries(buf []byte, off int, field []byte) []byte {
	for off < len(buf) {
		flen := int(binary.LittleEndian.Uint16(buf[off+1:]))
		vlen := int(binary.LittleEndian.Uint32(buf[off+3:]))
		if flen == len(field) && string(buf[off+7:off+7+flen]) == string(field) {
			return buf[off+7+flen : off+7+flen+vlen]
		}
		off += 7 + flen + vlen
	}
	return nil
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.Int64Var(&cfg.fields, "fields", 1_000_000, "field count (the sweep axis, 1e2 to 1e9)")
	flag.IntVar(&cfg.segMax, "seg", 4032, "encoded segment size in bytes")
	flag.IntVar(&cfg.inlineMax, "inlinemax", 2048, "inline root payload cap in bytes")
	flag.IntVar(&cfg.inlineCount, "inlinecount", 128, "inline root field-count cap")
	flag.IntVar(&cfg.fenceMax, "fencemax", 2048, "inline fence cap in bytes")
	flag.IntVar(&cfg.pageEnts, "pageents", 250, "fence entries per page and pages per index")
	flag.IntVar(&cfg.reps, "reps", 2000, "cold lookups (fresh connection each)")
	flag.IntVar(&cfg.hotreps, "hotreps", 20000, "hot lookups on one warm connection")
	flag.IntVar(&cfg.ckpt, "ckpt", 8, "preload transactions per checkpoint")
	flag.Parse()
	if *quick {
		cfg.fields, cfg.reps, cfg.hotreps = 5000, 100, 1000
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hfence:", err)
		os.Exit(1)
	}
}

type row struct {
	workload string
	ops      int
	dur      time.Duration
	p50, p99 time.Duration
	maxLat   time.Duration
	recReads float64
	rootB    int
	fenceMB  float64
	fileMB   float64
	vmhwmMB  float64
}

func runAll(cfg config, out io.Writer) error {
	l, err := planLayout(cfg)
	if err != nil {
		return err
	}
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "hfence")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, fmt.Sprintf("hfence-f%d.db", cfg.fields))
	os.Remove(path)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	key := []byte("h:fence")
	d, err := openDB(path)
	if err != nil {
		return err
	}
	start := time.Now()
	rootB, fenceB, err := preload(d, cfg, l, key)
	if err != nil {
		d.close()
		return err
	}
	fmt.Fprintf(os.Stderr, "hfence: fields=%d mode=%s segs=%d pages=%d root=%dB fence=%dB\n",
		cfg.fields, l.mode, l.segs, l.pages, rootB, fenceB)
	emit(cfg, out, l, row{workload: "load", ops: int(l.segs), dur: time.Since(start),
		rootB: rootB, fenceMB: float64(fenceB) / (1 << 20), fileMB: fileMB(path)})

	// pick returns a random preloaded field with its expected value
	// regenerated from the layout arithmetic.
	rng := rand.New(rand.NewSource(53))
	pick := func() (uint64, []byte, []byte) {
		s := rng.Int63n(l.segs)
		f := fhAt(l, s, rng.Int63n(segFields(cfg, l, s)))
		return f, fieldAt(f), valueAt(f)
	}
	verify := func(arm string, v []byte, reads int, want []byte) error {
		if !slices.Equal(v, want) {
			return fmt.Errorf("%s lookup returned %d bytes, want the generated value", arm, len(v))
		}
		if reads > l.wantReads {
			return fmt.Errorf("%s lookup took %d record reads, ceiling for %s is %d", arm, reads, l.mode, l.wantReads)
		}
		return nil
	}

	// Cold arm: every lookup on a fresh connection, so each record read
	// is a real B-tree descent with no page cache; the open itself is
	// outside the clock.
	lats := make([]time.Duration, 0, cfg.reps)
	totalReads := 0
	var dur time.Duration
	if err := d.close(); err != nil {
		return err
	}
	for range cfg.reps {
		cd, err := openDB(path)
		if err != nil {
			return err
		}
		f, field, want := pick()
		t0 := time.Now()
		v, reads, err := lookup(cd, key, f, field)
		lat := time.Since(t0)
		cd.close()
		if err != nil {
			return err
		}
		if err := verify("cold", v, reads, want); err != nil {
			return err
		}
		lats = append(lats, lat)
		dur += lat
		totalReads += reads
	}
	p50, p99, maxLat := percentiles(lats)
	emit(cfg, out, l, row{workload: "point-cold", ops: cfg.reps, dur: dur,
		p50: p50, p99: p99, maxLat: maxLat,
		recReads: float64(totalReads) / float64(max(cfg.reps, 1))})

	// Hot arm: one warm connection; the engine proper would also keep
	// the root resident and skip even the first read, so this is the
	// upper bound on a hot point op.
	hd, err := openDB(path)
	if err != nil {
		return err
	}
	defer hd.close()
	lats, totalReads, dur = lats[:0], 0, 0
	for range cfg.hotreps {
		f, field, want := pick()
		t0 := time.Now()
		v, reads, err := lookup(hd, key, f, field)
		lat := time.Since(t0)
		if err != nil {
			return err
		}
		if err := verify("hot", v, reads, want); err != nil {
			return err
		}
		lats = append(lats, lat)
		dur += lat
		totalReads += reads
	}
	p50, p99, maxLat = percentiles(lats)
	emit(cfg, out, l, row{workload: "point-hot", ops: cfg.hotreps, dur: dur,
		p50: p50, p99: p99, maxLat: maxLat,
		recReads: float64(totalReads) / float64(max(cfg.hotreps, 1)),
		rootB:    rootB, fenceMB: float64(fenceB) / (1 << 20),
		fileMB: fileMB(path), vmhwmMB: vmhwmMB()})
	return nil
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

func emit(cfg config, out io.Writer, l layout, r row) {
	nsPerOp := float64(r.dur.Nanoseconds()) / float64(max(r.ops, 1))
	opsPerS := float64(r.ops) / max(r.dur.Seconds(), 1e-9)
	fmt.Fprintf(out, "%d,%s,%s,%d,%.0f,%.0f,%d,%d,%d,%.2f,%d,%.2f,%.1f,%.1f\n",
		cfg.fields, l.mode, r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.recReads, r.rootB, r.fenceMB, r.fileMB, r.vmhwmMB)
}
