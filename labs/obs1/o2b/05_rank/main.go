// rank scores PRED-OBS1-O2B-RANK: cold ZRANK p99 stays flat from 10^3
// to 10^8 members at a fixed cache budget. The read plane is the landed
// one end to end: both zset projections as real ChunkPacker chunks in a
// real segment built by BuildSegment and AppendSegment, the member half
// resolved through a real Directory and Keymap and fetched by the real
// ColdReader, the rank half planned by ZsetRankFloor over the
// kind-restricted CollChunksKind refs and settled by one ZsetRunIter
// boundary block, all against the sim with the doc 01 S3 Standard
// latency envelope drawn per GET.
//
// A full cold ZRANK is two serial GETs on this plane: the command holds
// only the member, so the score comes off the member projection first
// (the ZSCORE plan), then the rank floor is RAM prefix sums plus the one
// boundary score-run block. Flatness is the claim the doc 09 2x byte
// weight buys, and the zsetdual lab priced it on a model; this lab
// prices the latency on the landed plane.
//
// The corpus rides the bigcollection trick reshaped for ranks: scores
// are a multiplicative permutation of 0..n-1, so the exact rank of
// member i is (i*A) mod n by construction and the score-run projection
// generates directly in rank order through the modular inverse, never
// holding a sorted copy.
//
// Fixed cache budget means the resident plan only: keymap plus
// directory, reported per decade. There is no block cache (doc 05
// section 4 is owed), so every op pays its two GETs, which is exactly
// the claim under test: the bill does not grow with cardinality.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The two projection kind bytes the zset demoter emits (engine/obs1/zset):
// 0x02 the member-hash projection, 0x06 the score runs.
const (
	kindZsetMember = 0x02
	kindZsetScore  = 0x06
)

const (
	group  = 3
	nowMs  = int64(1_700_000_000_000)
	permA  = 2654435761 // odd, not a multiple of 5: bijective mod every 10^k
	objKey = "seg/g003/rank"
)

type cfg struct {
	maxN    int  // largest decade
	fetches int  // scored ops per decade
	lat     bool // draw the S3 Standard envelope per GET
}

type rankRow struct {
	n         int
	chunks    int
	objMiB    float64
	getsPerOp float64
	kibPerOp  float64
	exactPct  float64
	floorUs   float64
	p50Ms     float64
	p99Ms     float64
	dirBytes  int
	dirBPerEl float64
}

type results struct {
	rows []rankRow
	cold obs1.ColdReadStats
}

type labErr struct{ err error }

func die(format string, args ...any) {
	panic(labErr{fmt.Errorf(format, args...)})
}

func member(i int) []byte { return fmt.Appendf(nil, "m:%09d", i) }

// modInverse of a mod n by extended Euclid; a and n coprime by choice.
func modInverse(a, n int64) int64 {
	t, newT := int64(0), int64(1)
	r, newR := n, a%n
	for newR != 0 {
		q := r / newR
		t, newT = newT, t-q*newT
		r, newR = newR, r-q*newR
	}
	if r != 1 {
		die("permutation constant not coprime with %d", n)
	}
	if t < 0 {
		t += n
	}
	return t
}

// scoreKeyOf mirrors the demoter's IEEE-754 total-order lift.
func scoreKeyOf(s float64) uint64 {
	b := math.Float64bits(s)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

type discIdx struct {
	disc uint64
	idx  int
}

// buildDecade packs both projections of an n-member zset into one
// segment on a fresh sim. One segment is the envelope the O2a labs
// documented: the plan works within the segment the keymap pins, the
// state a doc 06 rewrite pass leaves. Member i carries score
// (i*permA) mod n, so the score-run projection generates in rank order
// via the modular inverse and the exact rank is arithmetic.
func buildDecade(ctx context.Context, n int, seed uint64, lat bool) (*sim.Sim, *obs1.Directory, *obs1.Keymap, error) {
	colKey := []byte("z")
	var chunks []obs1.SegmentChunk
	var pk store.ChunkPacker

	// Score runs, generated in rank order: rank r belongs to member
	// (r*ainv) mod n. The chunk disc is the demoter's composite, score
	// key then member bytes, with the leading 8 bytes as FirstDisc.
	ainv := modInverse(permA%int64(n), int64(n))
	var sbits [8]byte
	var firstKey uint64
	var firstMember []byte
	flushScore := func() {
		if pk.Count() == 0 {
			return
		}
		payload, flags := pk.Finish()
		var d [8]byte
		binary.BigEndian.PutUint64(d[:], firstKey)
		frame := store.AppendRunChunk(nil, kindZsetScore|store.ChunkKindBit, flags, uint16(pk.Count()), colKey, append(d[:], firstMember...), payload)
		chunks = append(chunks, obs1.SegmentChunk{
			Key: colKey, Kind: kindZsetScore | store.ChunkKindBit, Flags: flags,
			FirstDisc: firstKey, Count: uint16(pk.Count()), LiveHint: uint16(pk.Count()),
			Data: frame,
		})
		pk.Reset()
	}
	for r := 0; r < n; r++ {
		i := int((int64(r) * ainv) % int64(n))
		m := member(i)
		if pk.Count() == 0 {
			firstKey = scoreKeyOf(float64(r))
			firstMember = m
		}
		binary.BigEndian.PutUint64(sbits[:], math.Float64bits(float64(r)))
		pk.Add(m, sbits[:], 0)
		if pk.Bytes() >= obs1.ChunkTargetDefault {
			flushScore()
		}
	}
	flushScore()

	// The member projection, sorted by the fold coordinate through the
	// (disc, index) table so no member blobs are held.
	tab := make([]discIdx, n)
	for i := range n {
		tab[i] = discIdx{disc: obs1.Disc(member(i)), idx: i}
	}
	sort.Slice(tab, func(i, j int) bool { return tab[i].disc < tab[j].disc })
	var first uint64
	flushMember := func() {
		if pk.Count() == 0 {
			return
		}
		payload, flags := pk.Finish()
		var d [8]byte
		binary.BigEndian.PutUint64(d[:], first)
		frame := store.AppendRunChunk(nil, kindZsetMember|store.ChunkKindBit, flags, uint16(pk.Count()), colKey, d[:], payload)
		chunks = append(chunks, obs1.SegmentChunk{
			Key: colKey, Kind: kindZsetMember | store.ChunkKindBit, Flags: flags,
			FirstDisc: first, Count: uint16(pk.Count()), LiveHint: uint16(pk.Count()),
			Data: frame,
		})
		pk.Reset()
	}
	for _, e := range tab {
		if pk.Count() == 0 {
			first = e.disc
		}
		binary.BigEndian.PutUint64(sbits[:], math.Float64bits(float64((int64(e.idx)*permA)%int64(n))))
		pk.Add(member(e.idx), sbits[:], 0)
		if pk.Bytes() >= obs1.ChunkTargetDefault {
			flushMember()
		}
	}
	flushMember()
	tab = nil
	_ = tab

	seg, err := obs1.BuildSegment(obs1.SegmentFooter{Group: group, Epoch: 1, SegSeq: 1}, chunks, [][]byte{colKey}, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	chunks = nil
	_ = chunks
	obj, err := obs1.AppendSegment(nil, 0xED, seg)
	if err != nil {
		return nil, nil, nil, err
	}
	seg.BlockData = nil

	// AppendSegment derives the block index and never writes it back
	// (#1102), so the directory's copy is read off the built object.
	foff, flen, err := obs1.ParseTail(obj[len(obj)-obs1.TailSize:])
	if err != nil {
		return nil, nil, nil, err
	}
	footer, err := obs1.ParseSegmentFooter(obj[foff : foff+uint64(flen)])
	if err != nil {
		return nil, nil, nil, err
	}

	scfg := sim.Config{Seed: seed}
	if lat {
		scfg.Latency = sim.S3Standard
	}
	s := sim.New(scfg)
	if _, err := s.Put(ctx, objKey, obj); err != nil {
		return nil, nil, nil, err
	}

	dir := obs1.NewDirectory()
	if err := dir.Add(objKey, &footer); err != nil {
		return nil, nil, nil, err
	}
	km := obs1.NewKeymap()
	if err := km.Put(obs1.Fingerprint(colKey), obs1.KeyLoc{Seg: 1}); err != nil {
		return nil, nil, nil, err
	}
	return s, dir, km, nil
}

// fetchScore runs one kind-restricted FetchField on the member
// projection and blocks for its completion, the ZSCORE half of ZRANK.
func fetchScore(cr *obs1.ColdReader, loc obs1.KeyLoc, m []byte) (obs1.ColdField, error) {
	var (
		cf   obs1.ColdField
		rerr error
		done = make(chan struct{})
	)
	cr.FetchFieldKind(group, []byte("z"), loc, m, kindZsetMember|store.ChunkKindBit, nowMs, func(f obs1.ColdField, err error) {
		cf, rerr = f, err
		close(done)
	})
	<-done
	return cf, rerr
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * q)
	return sorted[i]
}

// scoreDecade builds one decade and runs the scored serial op loop.
// Serial on purpose, the flat lab's stance: every op pays its own draws.
func scoreDecade(ctx context.Context, n int, c cfg, cold *obs1.ColdReadStats) rankRow {
	s, dir, km, err := buildDecade(ctx, n, uint64(n)*0x9E37+5, c.lat)
	if err != nil {
		die("build n=%d: %v", n, err)
	}
	fp := obs1.Fingerprint([]byte("z"))
	loc, ok := km.Lookup(fp)
	if !ok {
		die("n=%d: collection not in keymap", n)
	}
	refs := dir.CollChunksKind(loc, fp, kindZsetScore|store.ChunkKindBit)
	if len(refs) == 0 {
		die("n=%d: no score runs planned", n)
	}
	if got := obs1.ZsetCard(refs); got != n {
		die("n=%d: ZsetCard = %d off the plan", n, got)
	}

	// The rank floor walk is the one resident term that grows with
	// cardinality, linear over the score-run refs. Timed alone, no sim.
	ft0 := time.Now()
	for i := range c.fetches {
		key := scoreKeyOf(float64((int64((i*7919)%n) * permA) % int64(n)))
		obs1.ZsetRankFloor(refs, key)
	}
	floorUs := float64(time.Since(ft0).Microseconds()) / float64(c.fetches)

	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: s,
		Dir:   func(uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		die("n=%d: cold reader: %v", n, err)
	}
	defer cr.Close()

	fetchRun := func(ref obs1.DirRef) ([]byte, error) {
		off, sz := ref.Block.BlockSpan()
		raw, _, err := s.GetRange(ctx, ref.ObjKey, off, sz)
		if err != nil {
			return nil, err
		}
		return obs1.ParseSegmentBlock(raw, ref.Block)
	}

	before := s.Usage()
	lats := make([]time.Duration, 0, c.fetches)
	exact := 0
	for i := range c.fetches {
		idx := (i * 7919) % n
		m := member(idx)
		wantRank := int((int64(idx) * permA) % int64(n))

		t0 := time.Now()
		cf, err := fetchScore(cr, loc, m)
		if err != nil {
			die("n=%d score fetch %s: %v", n, m, err)
		}
		if !cf.Found || len(cf.Value) != 8 {
			die("n=%d: member %s missing from the member projection", n, m)
		}
		score := math.Float64frombits(binary.BigEndian.Uint64(cf.Value))
		key := scoreKeyOf(score)
		ridx, base := obs1.ZsetRankFloor(refs, key)
		var serr error
		it := obs1.ZsetRunIter(refs, ridx, fetchRun, nil, nowMs, &serr)
		rank := -1
		for off := 0; ; off++ {
			sp, ok := it()
			if !ok || sp.Key > key {
				break
			}
			if sp.Key == key && bytes.Equal(sp.Member, m) {
				rank = base + off
				break
			}
		}
		lats = append(lats, time.Since(t0))
		if serr != nil {
			die("n=%d rank stream %s: %v", n, m, serr)
		}
		if rank == wantRank {
			exact++
		}
	}
	after := s.Usage()
	st := cr.Stats()
	cold.Fetches += st.Fetches
	cold.BlockGETs += st.BlockGETs
	cold.Attached += st.Attached
	cold.Unresolved += st.Unresolved
	cold.Misses += st.Misses
	cold.Errs += st.Errs

	slices.Sort(lats)
	return rankRow{
		n:         n,
		chunks:    dir.Chunks(),
		objMiB:    float64(after.BytesStored) / (1 << 20),
		getsPerOp: float64(after.GetRequests-before.GetRequests) / float64(c.fetches),
		kibPerOp:  float64(after.BytesDown-before.BytesDown) / float64(c.fetches) / 1024,
		exactPct:  100 * float64(exact) / float64(c.fetches),
		floorUs:   floorUs,
		p50Ms:     float64(quantile(lats, 0.50)) / float64(time.Millisecond),
		p99Ms:     float64(quantile(lats, 0.99)) / float64(time.Millisecond),
		dirBytes:  dir.Bytes(),
		dirBPerEl: float64(dir.Bytes()) / float64(n),
	}
}

func run(c cfg) (res results, rerr error) {
	defer func() {
		if r := recover(); r != nil {
			le, ok := r.(labErr)
			if !ok {
				panic(r)
			}
			rerr = le.err
		}
	}()
	ctx := context.Background()
	for n := 1000; n <= c.maxN; n *= 10 {
		res.rows = append(res.rows, scoreDecade(ctx, n, c, &res.cold))
	}
	if res.cold.Errs != 0 || res.cold.Unresolved != 0 || res.cold.Misses != 0 {
		return res, fmt.Errorf("cold reader not clean: %+v", res.cold)
	}
	return res, nil
}

func main() {
	quick := flag.Bool("quick", false, "smoke sizes, no latency envelope")
	flag.Parse()
	c := cfg{maxN: 100_000_000, fetches: 1000, lat: true}
	if *quick {
		c = cfg{maxN: 100_000, fetches: 50, lat: false}
	}
	res, err := run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("n,chunks,obj_mib,gets_per_op,kib_per_op,rank_exact_pct,floor_us,p50_ms,p99_ms,dir_b,dir_b_per_elem")
	minP99, maxP99 := res.rows[0].p99Ms, res.rows[0].p99Ms
	for _, r := range res.rows {
		fmt.Printf("%d,%d,%.1f,%.4f,%.1f,%.1f,%.1f,%.1f,%.1f,%d,%.4f\n",
			r.n, r.chunks, r.objMiB, r.getsPerOp, r.kibPerOp, r.exactPct,
			r.floorUs, r.p50Ms, r.p99Ms, r.dirBytes, r.dirBPerEl)
		if r.p99Ms < minP99 {
			minP99 = r.p99Ms
		}
		if r.p99Ms > maxP99 {
			maxP99 = r.p99Ms
		}
	}
	if minP99 > 0 {
		fmt.Printf("p99_ratio_max_over_min,%.2f\n", maxP99/minP99)
	}
	fmt.Printf("# cold stats: %+v\n", res.cold)
}
