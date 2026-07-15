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
//
// B3 re-points the suite at Track B: the same ladder, preload, and
// lookup drive either arm through a narrow store surface, selected by
// -store. The A arm is the SQLite schema below; the B arm
// (store_b.go) keeps the root as a plain record under the user key,
// maps segments and fence pages to subkey records under a minted
// rooth, turns a preload write set into one DrainBatch, and points
// the checkpoint cadence and the cold boundary at the store's own
// verbs, so the flatness curve is drawn on the backend that will
// actually carry it.
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
	store       string
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

// store is the backend arm under the ladder: Track A rows or Track B
// records, one narrow surface so the shared model prices both. The
// three getters are the cold path's three probes and the count the
// per-op ceiling holds against on either arm, flush lands one preload
// write set, checkpoint is the arm's WAL trim verb, and reopen is the
// arm's cold boundary (each arm documents what that drops).
type store interface {
	rootGet() ([]byte, error)
	pageGet(page int64) ([]byte, error)
	segGet(segid int64) ([]byte, error)
	flush(fs *flushSet) error
	checkpoint() error
	reopen() error
	dataMB() float64
	close() error
}

// flushSet is one preload write set as the model hands it down:
// segment rows, fence page rows, the root when the batch carries it,
// and the sequence the b arm lands as its high-water mark.
type flushSet struct {
	seq   int64
	bytes int
	segs  []idRow
	pages []idRow
	root  []byte
}

type idRow struct {
	id  int64
	row []byte
}

func openStore(cfg config, path string, key []byte) (store, error) {
	switch cfg.store {
	case "a":
		return openA(path, key)
	case "b":
		return openB(path, key)
	}
	return nil, fmt.Errorf("unknown store arm %q", cfg.store)
}

type db struct {
	conn  *sqlite3.Conn
	path  string
	key   []byte
	rget  *sqlite3.Stmt
	rput  *sqlite3.Stmt
	sget  *sqlite3.Stmt
	sput  *sqlite3.Stmt
	pget  *sqlite3.Stmt
	pput  *sqlite3.Stmt
	hw1   *sqlite3.Stmt
	stmts []*sqlite3.Stmt
}

func openA(path string, key []byte) (*db, error) {
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
	d := &db{conn: conn, path: path, key: key}
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

func (d *db) rootGet() ([]byte, error)           { return d.getBlob1(d.rget, d.key) }
func (d *db) pageGet(page int64) ([]byte, error) { return d.getBlob2(d.pget, d.key, page) }
func (d *db) segGet(segid int64) ([]byte, error) { return d.getBlob2(d.sget, d.key, segid) }

// flush lands one preload write set in a single transaction: segment
// rows, fence page rows, and the root when the batch carries it. The
// meta hw row stays untouched, as before B3: this lab never replays.
func (d *db) flush(fs *flushSet) error {
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return err
	}
	fail := func(err error) error { txn.Rollback(); return err }
	for _, s := range fs.segs {
		if err := putBlob2(d.sput, d.key, s.id, s.row); err != nil {
			return fail(err)
		}
	}
	for _, p := range fs.pages {
		if err := putBlob2(d.pput, d.key, p.id, p.row); err != nil {
			return fail(err)
		}
	}
	if fs.root != nil {
		if err := putRoot(d, d.key, fs.root); err != nil {
			return fail(err)
		}
	}
	return txn.Commit()
}

func (d *db) checkpoint() error {
	return d.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
}

// reopen is the cold boundary on this arm: a fresh connection, so the
// next lookup's record reads are real B-tree descents with no page
// cache. The model calls it before every cold rep and once before the
// hot arm.
func (d *db) reopen() error {
	if err := d.close(); err != nil {
		return err
	}
	nd, err := openA(d.path, d.key)
	if err != nil {
		return err
	}
	*d = *nd
	return nil
}

func (d *db) dataMB() float64 { return fileMB(d.path) }

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

// flushCap caps one preload write set in bytes: the b arm applies each
// set as a single DrainBatch that must stay well under its 64 MiB WAL
// segment. At the default segment size the 512-row boundary in preload
// closes sets first, so the a arm keeps its pre-B3 transaction shape.
const flushCap = 32 << 20

// preload writes the ladder for the configured field count through the
// store: segments in batched write sets, then fence pages and the root
// riding the final set, checkpointing as it goes so the WAL never
// holds the dataset.
func preload(st store, cfg config, l layout) (rootB int, fenceB int64, err error) {
	txns := 0
	seq := int64(0)
	flush := func(fs *flushSet) error {
		seq++
		fs.seq = seq
		if err := st.flush(fs); err != nil {
			return err
		}
		if txns++; txns%cfg.ckpt == 0 {
			return st.checkpoint()
		}
		return nil
	}

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
		if err := flush(&flushSet{root: root}); err != nil {
			return 0, 0, err
		}
		return len(root), 0, st.checkpoint()
	}

	fs := &flushSet{}
	for s := int64(0); s < l.segs; s++ {
		row := encodeSegAt(cfg, l, s)
		fs.segs = append(fs.segs, idRow{id: s, row: row})
		fs.bytes += len(row)
		if len(fs.segs) >= 512 || fs.bytes >= flushCap {
			if err := flush(fs); err != nil {
				return 0, 0, err
			}
			fs = &flushSet{}
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
			fs.pages = append(fs.pages, idRow{id: p, row: page})
			fenceB += int64(len(page))
			if fs.bytes += len(page); fs.bytes >= flushCap {
				if err := flush(fs); err != nil {
					return 0, 0, err
				}
				fs = &flushSet{}
			}
			root = binary.LittleEndian.AppendUint64(root, l.rangeW*uint64(first))
			root = binary.LittleEndian.AppendUint64(root, uint64(p))
		}
	}
	// The root rides the final set with the tail segments and pages,
	// the same single-transaction tail as before B3.
	fs.root = root
	if err := flush(fs); err != nil {
		return 0, 0, err
	}
	return len(root), fenceB, st.checkpoint()
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
// record reads it took. The counting is model-side, so the per-op
// ceiling holds identically on both arms: three probes, never more.
func lookup(st store, f uint64, field []byte) ([]byte, int, error) {
	root, err := st.rootGet()
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
		page, err := st.pageGet(int64(pageNo))
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
	seg, err := st.segGet(int64(segid))
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
	flag.StringVar(&cfg.store, "store", "a", "backend arm: a (SQLite rows) or b (Track B records)")
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
	path := filepath.Join(cfg.dir, fmt.Sprintf("hfence-%s-f%d.db", cfg.store, cfg.fields))
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".aki-wal"} {
		os.Remove(p)
	}

	key := []byte("h:fence")
	st, err := openStore(cfg, path, key)
	if err != nil {
		return err
	}
	defer st.close()
	start := time.Now()
	rootB, fenceB, err := preload(st, cfg, l)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "hfence: fields=%d mode=%s segs=%d pages=%d root=%dB fence=%dB\n",
		cfg.fields, l.mode, l.segs, l.pages, rootB, fenceB)
	emit(cfg, out, l, row{workload: "load", ops: int(l.segs), dur: time.Since(start),
		rootB: rootB, fenceMB: float64(fenceB) / (1 << 20), fileMB: st.dataMB()})

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

	// Cold arm: every timed lookup runs behind the arm's cold boundary,
	// with the reopen outside the clock. On a that boundary is a fresh
	// connection per lookup, so each record read is a real B-tree
	// descent with no page cache; on b it is one checkpoint, close, and
	// reopen of the store before the phase (store_b.go says what that
	// drops and why per-lookup has no honest analog there).
	lats := make([]time.Duration, 0, cfg.reps)
	totalReads := 0
	var dur time.Duration
	for range cfg.reps {
		if err := st.reopen(); err != nil {
			return err
		}
		f, field, want := pick()
		t0 := time.Now()
		v, reads, err := lookup(st, f, field)
		lat := time.Since(t0)
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

	// Hot arm: one warm handle. On a the reopen swaps in the single
	// warm connection as before B3; on b it is a no-op and the store
	// stays as the cold arm left it. The engine proper would also keep
	// the root resident and skip even the first read, so this is the
	// upper bound on a hot point op.
	if err := st.reopen(); err != nil {
		return err
	}
	lats, totalReads, dur = lats[:0], 0, 0
	for range cfg.hotreps {
		f, field, want := pick()
		t0 := time.Now()
		v, reads, err := lookup(st, f, field)
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
		fileMB: st.dataMB(), vmhwmMB: vmhwmMB()})
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
	fmt.Fprintf(out, "%s,%d,%s,%s,%d,%.0f,%.0f,%d,%d,%d,%.2f,%d,%.2f,%.1f,%.1f\n",
		cfg.store, cfg.fields, l.mode, r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.recReads, r.rootB, r.fenceMB, r.fileMB, r.vmhwmMB)
}
