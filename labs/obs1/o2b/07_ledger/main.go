// ledger measures the remaining doc 08 section 9 ledger rows, the zset,
// list, and stream cells, on the landed engine end to end: a
// durability-booted server builds the corpora over RESP, ballast
// pressure demotes and folds them to a counting sim bucket, and the
// reads run through the rebuilt keymap, the directory, the cold reader,
// and the run planners, the plane the serving node uses. Scores
// PRED-OBS1-O2B-LEDGER, the measured companion to the zsetdual and
// streamcatchup model labs; the list steady row (~0 GETs at the ends)
// belongs to the queue lab and is not remeasured here.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/obs1srv/conformance"
	"github.com/tamnd/aki/obs1srv/drivers"
)

// The collection chunk kind bytes as folded (engine as-built), with the
// footer's chunk marker bit set the way the directory carries them.
const (
	kindZsetMember = 0x02 | 0x80
	kindList       = 0x03 | 0x80
	kindStream     = 0x05 | 0x80
	kindZsetScore  = 0x06 | 0x80
)

// dirChunkCost is the directory's per-collection-chunk resident weight
// as built (#1305): the 24 B ref row plus the u64 fingerprint.
const dirChunkCost = 32

const (
	zsetKey   = "ledger:zset"
	listKey   = "ledger:list"
	streamKey = "ledger:stream"
	permA     = 2654435761 // odd, not a multiple of 5: bijective mod the corpus sizes
)

type cfg struct {
	members int // zset members
	elems   int // list elements
	entries int // stream entries
	samples int // scored ops per point cell
}

func zMember(i int) string { return fmt.Sprintf("zm%05d", i) }
func lValue(i int) string  { return fmt.Sprintf("lv-%011d", i) }
func sValue(i int) string  { return fmt.Sprintf("sv-%011d", i) }

// zScore is member i's score, a multiplicative permutation of 0..n-1 so
// scores are distinct and member i's rank among all n is arithmetic.
func zScore(i, n int) int { return int((int64(i) * permA) % int64(n)) }

type lab struct {
	bucket *sim.Sim
	b      *drivers.Booted
	srv    *drivers.Server
	rc     *conformance.Conn
	nc     net.Conn
}

func boot(dir string) (*lab, error) {
	bucket := sim.New(sim.Config{})
	l := &lab{bucket: bucket}
	srv, err := drivers.Listen(drivers.Options{
		Addr: "127.0.0.1:0", Shards: 4, ArenaBytes: 16 << 20, SegBytes: 4 << 20,
		VlogDir: dir, ColdDir: dir, ResidentCapBytes: 2 << 20,
		Boot: func(rt *shard.Runtime) error {
			b, err := drivers.BootDurability(context.Background(), drivers.BootConfig{
				Store: bucket, Prefix: "p", Node: 0xED, Incarnation: 1,
				FlushAge: 5 * time.Millisecond, FoldAge: 20 * time.Millisecond,
			}, rt)
			if err != nil {
				return err
			}
			l.b = b
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	l.srv = srv
	go func() { _ = srv.Serve() }()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		_ = srv.Close()
		return nil, err
	}
	l.nc = nc
	l.rc = conformance.NewConn(nc)
	return l, nil
}

func (l *lab) close() {
	_ = l.nc.Close()
	_ = l.srv.Close()
	_ = l.b.Close()
}

func (l *lab) do(args ...string) string {
	_ = l.nc.SetDeadline(time.Now().Add(30 * time.Second))
	v, err := l.rc.Do(args)
	if err != nil {
		die("command %v: %v", args, err)
	}
	return conformance.Render(v)
}

// pipeline writes raw commands in batches and checks each reply prefix.
func (l *lab) pipeline(cmds []string, wantPrefix string) {
	const batch = 500
	for base := 0; base < len(cmds); base += batch {
		end := min(base+batch, len(cmds))
		_ = l.nc.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := l.nc.Write([]byte(strings.Join(cmds[base:end], ""))); err != nil {
			die("pipeline write: %v", err)
		}
		for i := base; i < end; i++ {
			line, err := l.rc.R.ReadString('\n')
			if err != nil {
				die("pipeline read: %v", err)
			}
			if !strings.HasPrefix(line, wantPrefix) {
				die("pipeline reply %q, want prefix %q", line, wantPrefix)
			}
			if wantPrefix == "$" {
				if _, err := l.rc.R.ReadString('\n'); err != nil {
					die("pipeline bulk payload read: %v", err)
				}
			}
		}
	}
}

func resp(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	return sb.String()
}

func (l *lab) build(c cfg) {
	var cmds []string
	for i := 0; i < c.members; i++ {
		cmds = append(cmds, resp("ZADD", zsetKey, strconv.Itoa(zScore(i, c.members)), zMember(i)))
	}
	l.pipeline(cmds, ":")
	cmds = cmds[:0]
	for i := 0; i < c.elems; i++ {
		cmds = append(cmds, resp("RPUSH", listKey, lValue(i)))
	}
	l.pipeline(cmds, ":")
	cmds = cmds[:0]
	for i := 0; i < c.entries; i++ {
		cmds = append(cmds, resp("XADD", streamKey, fmt.Sprintf("%d-1", i+1), "f", sValue(i)))
	}
	l.pipeline(cmds, "$")
}

// ballastRaw drives string ballast through the resident cap so demotion
// and fold pressure are real.
func (l *lab) ballastRaw(round int) {
	const keys = 4000
	val := strings.Repeat("b", 48)
	var cmds []string
	for i := 0; i < keys; i++ {
		cmds = append(cmds, resp("SET", "ballast:"+strconv.Itoa(round)+":"+strconv.Itoa(i), val))
	}
	l.pipeline(cmds, "+OK")
}

func (l *lab) settle() {
	deadline := time.Now().Add(30 * time.Second)
	for {
		l.b.Folder.Flush()
		fs := l.b.Folder.Stats()
		if fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut {
			return
		}
		if time.Now().After(deadline) {
			die("fold never settled: %+v", fs)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// pressure runs ballast rounds until all three corpus keys hold chunk
// placements in the published ledger, the coldness proof.
func (l *lab) pressure() int {
	want := []string{zsetKey, listKey, streamKey}
	deadline := time.Now().Add(600 * time.Second)
	round := 0
	for {
		folded := map[string]bool{}
		for _, led := range l.b.Folder.Ledger() {
			for _, p := range led.Places {
				folded[string(p.Key)] = true
			}
		}
		missing := 0
		for _, k := range want {
			if !folded[k] {
				missing++
			}
		}
		if missing == 0 {
			return round
		}
		if time.Now().After(deadline) {
			die("pressure: %d of %d corpus keys never folded", missing, len(want))
		}
		l.ballastRaw(round)
		round++
		for i := 0; i < 50; i++ {
			l.do("EXISTS", zsetKey)
			l.do("EXISTS", listKey)
			l.do("EXISTS", streamKey)
		}
		l.settle()
	}
}

func groupOf(key string) uint16 {
	_, g := drivers.ClusterMapKey([]byte(key))
	return g
}

// plan resolves one collection's kind-restricted refs.
func (l *lab) plan(key string, kind uint8) (uint16, obs1.KeyLoc, []obs1.DirRef) {
	g := groupOf(key)
	fp := obs1.Fingerprint([]byte(key))
	loc, ok := l.b.Keymaps[g].Lookup(fp)
	if !ok {
		die("keymap has no entry for %q", key)
	}
	refs := l.b.Dirs[g].CollChunksKind(loc, fp, kind)
	if len(refs) == 0 {
		die("no %#x runs planned for %q", kind, key)
	}
	return g, loc, refs
}

// fetchRun is the run planners' block fetch against the bucket.
func (l *lab) fetchRun(ref obs1.DirRef) ([]byte, error) {
	off, n := ref.Block.BlockSpan()
	raw, _, err := l.bucket.GetRange(context.Background(), ref.ObjKey, off, n)
	if err != nil {
		return nil, err
	}
	return obs1.ParseSegmentBlock(raw, ref.Block)
}

// fetchFieldKind reads one member through the cold plane, blocking.
func (l *lab) fetchFieldKind(g uint16, key string, loc obs1.KeyLoc, field string, kind uint8) (obs1.ColdField, error) {
	ch := make(chan struct{})
	var f obs1.ColdField
	var err error
	l.b.Cold.FetchFieldKind(g, []byte(key), loc, []byte(field), kind, time.Now().UnixMilli(), func(r obs1.ColdField, e error) {
		f, err = r, e
		close(ch)
	})
	<-ch
	return f, err
}

type cell struct {
	name  string
	ops   int
	gets  int64
	bytes int64
	found int
}

func (c cell) row() string {
	perOp, bytesOp, foundPct := 0.0, 0.0, 0.0
	if c.ops > 0 {
		perOp = float64(c.gets) / float64(c.ops)
		bytesOp = float64(c.bytes) / float64(c.ops)
		foundPct = 100 * float64(c.found) / float64(c.ops)
	}
	return fmt.Sprintf("%s,%d,%d,%.4f,%.1f,%.1f", c.name, c.ops, c.gets, perOp, bytesOp/1024, foundPct)
}

func die(format string, args ...any) {
	panic(fmt.Errorf("ledger: "+format, args...))
}

type share struct {
	name   string
	chunks int
	elems  int
	bPerEl float64
}

type results struct {
	rounds int
	cells  []cell
	shares []share
	cold   obs1.ColdReadStats
	prep   int64 // GETs spent by the disclosed prep walks
}

func run(c cfg) (res results, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
				return
			}
			panic(r)
		}
	}()
	dir, derr := os.MkdirTemp("", "obs1-o2bledger-*")
	if derr != nil {
		return res, derr
	}
	defer func() { _ = os.RemoveAll(dir) }()
	l, berr := boot(dir)
	if berr != nil {
		return res, berr
	}
	defer l.close()

	l.build(c)
	res.rounds = l.pressure()
	now := time.Now().UnixMilli()

	// Prep walks, disclosed: one full stream over each folded projection
	// establishes what folded (the demoters shed coldest-first, ends and
	// hot margins stay resident) and pins the fold-plane content against
	// the built corpus by value. Their GETs are reported, not scored.
	u0 := l.bucket.Usage()

	zg, zloc, zsrefs := l.plan(zsetKey, kindZsetScore)
	var zerr error
	it := obs1.ZsetRunIter(zsrefs, 0, l.fetchRun, nil, now, &zerr)
	type zp struct {
		member string
		bits   uint64
	}
	var zfold []zp
	for {
		p, ok := it()
		if !ok {
			break
		}
		zfold = append(zfold, zp{member: string(p.Member), bits: p.Bits})
	}
	if zerr != nil {
		die("zset prep walk: %v", zerr)
	}
	if len(zfold) < c.samples {
		die("only %d zset members folded, need %d samples", len(zfold), c.samples)
	}
	// Plan order must be exactly score order of the folded subset, so a
	// member's plan position is its rank among the folded members; that
	// rank checks against the full-corpus arithmetic rank order.
	for j := 1; j < len(zfold); j++ {
		if obs1.ZsetScoreKey(math.Float64frombits(zfold[j].bits)) < obs1.ZsetScoreKey(math.Float64frombits(zfold[j-1].bits)) {
			die("zset plan order broken at %d", j)
		}
	}

	lg, lloc, lrefs := l.plan(listKey, kindList)
	_ = lg
	_ = lloc
	var lerr error
	lit := obs1.ListRunIter(lrefs, 0, l.fetchRun, nil, now, &lerr)
	var lfold []string
	for {
		v, ok := lit()
		if !ok {
			break
		}
		lfold = append(lfold, string(v))
	}
	if lerr != nil {
		die("list prep walk: %v", lerr)
	}
	if len(lfold) < c.samples {
		die("only %d list elements folded, need %d samples", len(lfold), c.samples)
	}
	// The folded interior is contiguous (ends stay hot): anchor the plan
	// to the built list by the first folded value, then every element
	// must match in position order.
	var lbase int
	if n, e := fmt.Sscanf(lfold[0], "lv-%d", &lbase); n != 1 || e != nil {
		die("list anchor %q unparseable", lfold[0])
	}
	for j, v := range lfold {
		if v != lValue(lbase+j) {
			die("folded list [%d] = %q, want %q: interior not contiguous", j, v, lValue(lbase+j))
		}
	}

	sg, sloc, srefs := l.plan(streamKey, kindStream)
	_ = sg
	_ = sloc
	var serr error
	sit := obs1.StreamRunIter(srefs, 0, l.fetchRun, nil, &serr)
	type se struct {
		id  obs1.StreamRunID
		val string
	}
	var sfold []se
	for {
		e, ok := sit()
		if !ok {
			break
		}
		if len(e.Fields) != 1 || string(e.Fields[0].Name) != "f" {
			die("stream entry %v carries %d fields", e.ID, len(e.Fields))
		}
		sfold = append(sfold, se{id: e.ID, val: string(e.Fields[0].Value)})
	}
	if serr != nil {
		die("stream prep walk: %v", serr)
	}
	if len(sfold) < c.samples {
		die("only %d stream entries folded, need %d samples", len(sfold), c.samples)
	}
	// Folded stream blocks are the shed prefix in ID order: anchor by the
	// first folded ID and check the arithmetic IDs and values through.
	sbase := int(sfold[0].id.Ms) - 1
	for j, e := range sfold {
		if e.id.Ms != uint64(sbase+j+1) || e.id.Seq != 1 || e.val != sValue(sbase+j) {
			die("folded stream [%d] = %d-%d %q, want %d-1 %q", j, e.id.Ms, e.id.Seq, e.val, sbase+j+1, sValue(sbase+j))
		}
	}

	u1 := l.bucket.Usage()
	res.prep = u1.GetRequests - u0.GetRequests

	// zscore: the member projection through the kind-restricted cold
	// reader, one block per op.
	u0 = l.bucket.Usage()
	zc := cell{name: "zscore", ops: c.samples}
	for i := 0; i < c.samples; i++ {
		p := zfold[(i*7919)%len(zfold)]
		f, ferr := l.fetchFieldKind(zg, zsetKey, zloc, p.member, kindZsetMember)
		if ferr != nil {
			die("zscore %s: %v", p.member, ferr)
		}
		if f.Found && len(f.Value) == 8 && binary.BigEndian.Uint64(f.Value) == p.bits {
			zc.found++
		}
	}
	u1 = l.bucket.Usage()
	zc.gets, zc.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, zc)

	// zscore_miss: a stranger member answers definitively absent for at
	// most one GET, the fp-collision honesty of the field plane.
	u0 = l.bucket.Usage()
	zm := cell{name: "zscore_miss", ops: 100}
	for i := 0; i < 100; i++ {
		f, ferr := l.fetchFieldKind(zg, zsetKey, zloc, "absent-"+strconv.Itoa(i), kindZsetMember)
		if ferr != nil {
			die("zscore miss %d: %v", i, ferr)
		}
		if !f.Found {
			zm.found++
		}
	}
	u1 = l.bucket.Usage()
	zm.gets, zm.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, zm)

	// zrank: the boundary-block half, the ledger's 0-to-1 cell. The score
	// is in hand (the command's other GET is the zscore cell above), the
	// floor is RAM prefix sums, and the position settles in one block.
	u0 = l.bucket.Usage()
	zr := cell{name: "zrank_boundary", ops: c.samples}
	for i := 0; i < c.samples; i++ {
		want := (i * 7919) % len(zfold)
		p := zfold[want]
		key := obs1.ZsetScoreKey(math.Float64frombits(p.bits))
		idx, base := obs1.ZsetRankFloor(zsrefs, key)
		var werr error
		wit := obs1.ZsetRunIter(zsrefs, idx, l.fetchRun, nil, now, &werr)
		got := -1
		for off := 0; ; off++ {
			sp, ok := wit()
			if !ok || sp.Key > key {
				break
			}
			if sp.Key == key && bytes.Equal(sp.Member, []byte(p.member)) {
				got = base + off
				break
			}
		}
		if werr != nil {
			die("zrank stream %s: %v", p.member, werr)
		}
		if got == want {
			zr.found++
		}
	}
	u1 = l.bucket.Usage()
	zr.gets, zr.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, zr)

	// zrangebyscore: floor by prefix sums, then the covering span through
	// the scan plan's coalesced ranges, the 1 + ceil row measured with
	// the span under one coalesced GET at this corpus size.
	u0 = l.bucket.Usage()
	spanLen := len(zfold) / 40
	zs := cell{name: "zrangebyscore", ops: 20}
	for w := 0; w < 20; w++ {
		p0 := (w * (len(zfold) - spanLen)) / 20
		p1 := p0 + spanLen - 1
		lo := obs1.ZsetScoreKey(math.Float64frombits(zfold[p0].bits))
		hi := obs1.ZsetScoreKey(math.Float64frombits(zfold[p1].bits))
		idx, _ := obs1.ZsetRankFloor(zsrefs, lo)
		endIdx, _, ok := obs1.ZsetRunAtRank(zsrefs, p1)
		if !ok {
			die("span %d: end rank %d has no run", w, p1)
		}
		sub := zsrefs[idx : endIdx+1]
		ranges := obs1.ScanRanges(sub, obs1.ScanRangeTargetDefault)
		sf := obs1.NewScanFetcher(context.Background(), l.bucket, ranges, obs1.ScanFanDefault)
		var werr error
		wit := obs1.ZsetRunIter(sub, 0, sf.Fetch, sf.Prefetch, now, &werr)
		got := 0
		for {
			sp, ok := wit()
			if !ok || sp.Key > hi {
				break
			}
			if sp.Key >= lo {
				got++
			}
		}
		if werr != nil {
			die("span %d stream: %v", w, werr)
		}
		if got == spanLen {
			zs.found++
		}
	}
	u1 = l.bucket.Usage()
	zs.gets, zs.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, zs)

	// lindex: positional prefix sums then one block.
	u0 = l.bucket.Usage()
	lc := cell{name: "lindex", ops: c.samples}
	for i := 0; i < c.samples; i++ {
		want := (i * 7919) % len(lfold)
		idx, base, ok := obs1.ListRunAtIndex(lrefs, want)
		if !ok {
			die("lindex %d: no run", want)
		}
		var werr error
		wit := obs1.ListRunIter(lrefs, idx, l.fetchRun, nil, now, &werr)
		var got []byte
		for skip := want - base; ; skip-- {
			v, ok := wit()
			if !ok {
				break
			}
			if skip == 0 {
				got = v
				break
			}
		}
		if werr != nil {
			die("lindex %d stream: %v", want, werr)
		}
		if string(got) == lValue(lbase+want) {
			lc.found++
		}
	}
	u1 = l.bucket.Usage()
	lc.gets, lc.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, lc)

	// xrange: floor by ms, stream the window, IDs and values exact.
	u0 = l.bucket.Usage()
	const window = 100
	xc := cell{name: "xrange_window", ops: 20}
	for w := 0; w < 20; w++ {
		p0 := (w * (len(sfold) - window)) / 20
		id0, id1 := sfold[p0].id, sfold[p0+window-1].id
		idx := obs1.StreamRunFloor(srefs, id0.Ms)
		var werr error
		wit := obs1.StreamRunIter(srefs[idx:], 0, l.fetchRun, nil, &werr)
		got := 0
		for {
			e, ok := wit()
			if !ok || id1.Less(e.ID) {
				break
			}
			if !e.ID.Less(id0) {
				if v := string(e.Fields[0].Value); v != sValue(int(e.ID.Ms)-1) {
					die("xrange window %d: entry %d-%d value %q", w, e.ID.Ms, e.ID.Seq, v)
				}
				got++
			}
		}
		if werr != nil {
			die("xrange window %d: %v", w, werr)
		}
		if got == window {
			xc.found++
		}
	}
	u1 = l.bucket.Usage()
	xc.gets, xc.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, xc)

	// Resident shares: the directory's per-chunk weight over the folded
	// element counts, per projection family.
	zmrefs := l.b.Dirs[zg].CollChunksKind(zloc, obs1.Fingerprint([]byte(zsetKey)), kindZsetMember)
	res.shares = []share{
		{name: "zset_dual", chunks: len(zsrefs) + len(zmrefs), elems: len(zfold),
			bPerEl: float64(dirChunkCost*(len(zsrefs)+len(zmrefs))) / float64(len(zfold))},
		{name: "list", chunks: len(lrefs), elems: len(lfold),
			bPerEl: float64(dirChunkCost*len(lrefs)) / float64(len(lfold))},
		{name: "stream", chunks: len(srefs), elems: len(sfold),
			bPerEl: float64(dirChunkCost*len(srefs)) / float64(len(sfold))},
	}

	res.cold = l.b.Cold.Stats()
	return res, nil
}

func main() {
	quick := flag.Bool("quick", false, "small corpus for a fast pass")
	flag.Parse()
	c := cfg{members: 20000, elems: 20000, entries: 20000, samples: 2000}
	if *quick {
		c = cfg{members: 4000, elems: 4000, entries: 4000, samples: 200}
	}
	res, err := run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("# folded after %d pressure rounds; prep walks billed %d GETs (disclosed)\n", res.rounds, res.prep)
	fmt.Println("cell,ops,gets,gets_per_op,kib_per_op,found_pct")
	for _, row := range res.cells {
		fmt.Println(row.row())
	}
	for _, s := range res.shares {
		fmt.Printf("share_%s,%d,%d,%.4f,,\n", s.name, s.chunks, s.elems, s.bPerEl)
	}
	fmt.Printf("# cold stats: %+v\n", res.cold)
}
