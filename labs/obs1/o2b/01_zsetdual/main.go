// zsetdual prices the doc 08 section 5 sorted-set model before the zset
// slices land: the dual projection's write amplification, ZSCORE and
// ZRANGEBYSCORE request bills, rank math over directory prefix sums, and
// projection consistency under churn. The counter-arm prices ZRANK
// without score runs, which is the cost the 2x cold-byte weight buys out.
//
// The chunk frames are the real store codec on the counting sim; the
// element packing, directory, and rank arithmetic are lab-local models
// disclosed in the README, the same stance as the typepoint lab. The
// zset slices replace the models with the landed planes and the O2b
// ledger prediction re-measures these cells there.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

const (
	kindZset      = 0x05
	blockBytes    = 128 << 10
	coalesceBytes = 16 << 20
	dirEntBytes   = 24 // #1269 as-built
	runCountBytes = 2  // the per-run count the rank math reads
	chunkHdr      = 16
)

// scoreKey encodes a float64 into the IEEE754 total-order u64 the score
// runs sort by: flip the sign bit on non-negatives, all bits on negatives.
func scoreKey(s float64) uint64 {
	b := math.Float64bits(s)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

type member struct {
	name  []byte
	score float64
}

func corpus(n int) []member {
	out := make([]member, n)
	for i := range n {
		// Deterministic scores with duplicates and negatives in the mix.
		s := float64((i*2654435761)%(2*n)) - float64(n)/2
		out[i] = member{name: fmt.Appendf(nil, "m:%09d", i), score: s}
	}
	return out
}

type chunkEnt struct {
	first uint64
	count int
	off   int64
	ln    int
}

// projection packs one sorted projection of the corpus into a chunked
// object: member chunks sort by Disc(member) and carry (member, score);
// score runs sort by scoreKey and carry (score, member).
type projection struct {
	obj []byte
	dir []chunkEnt
}

func pack(dst []byte, disc uint64, name []byte, score float64) []byte {
	var h [18]byte
	binary.LittleEndian.PutUint64(h[0:], disc)
	binary.LittleEndian.PutUint64(h[8:], math.Float64bits(score))
	binary.LittleEndian.PutUint16(h[16:], uint16(len(name)))
	dst = append(dst, h[:]...)
	return append(dst, name...)
}

func buildProjection(ms []member, byScore bool, chunkTarget int) projection {
	idx := make([]int, len(ms))
	for i := range idx {
		idx[i] = i
	}
	disc := func(i int) uint64 {
		if byScore {
			return scoreKey(ms[i].score)
		}
		return obs1.Disc(ms[i].name)
	}
	sort.Slice(idx, func(a, b int) bool { return disc(idx[a]) < disc(idx[b]) })
	var p projection
	var payload []byte
	var first uint64
	count := 0
	colKey := []byte("z")
	flush := func() {
		if count == 0 {
			return
		}
		var d [8]byte
		binary.BigEndian.PutUint64(d[:], first)
		frameLen := chunkHdr + len(colKey) + 8 + len(payload)
		if rem := blockBytes - int(int64(len(p.obj))%blockBytes); frameLen > rem {
			p.obj = append(p.obj, make([]byte, rem)...)
		}
		off := int64(len(p.obj))
		p.obj = store.AppendRunChunk(p.obj, kindZset|store.ChunkKindBit, 0, uint16(count), colKey, d[:], payload)
		p.dir = append(p.dir, chunkEnt{first: first, count: count, off: off, ln: int(int64(len(p.obj)) - off)})
		payload, count = payload[:0], 0
	}
	for _, i := range idx {
		if count == 0 {
			first = disc(i)
		}
		payload = pack(payload, disc(i), ms[i].name, ms[i].score)
		count++
		if len(payload) >= chunkTarget {
			flush()
		}
	}
	flush()
	return p
}

func findChunk(dir []chunkEnt, disc uint64) int {
	i := sort.Search(len(dir), func(i int) bool { return dir[i].first > disc })
	if i == 0 {
		return 0
	}
	return i - 1
}

// fetchBlock GETs the one block covering a chunk and returns the chunk's
// decoded entries as (disc, name, score).
type entry struct {
	disc  uint64
	name  []byte
	score float64
}

func fetchChunk(ctx context.Context, s *sim.Sim, key string, p projection, ci int) ([]entry, error) {
	ce := p.dir[ci]
	blk := ce.off / blockBytes * blockBytes
	nb := int64(blockBytes)
	if blk+nb > int64(len(p.obj)) {
		nb = int64(len(p.obj)) - blk
	}
	b, _, err := s.GetRange(ctx, key, blk, nb)
	if err != nil {
		return nil, err
	}
	f := b[ce.off-blk:]
	total := int(binary.LittleEndian.Uint32(f[0:]))
	klen := int(binary.LittleEndian.Uint16(f[6:]))
	payload := f[chunkHdr+klen+8 : total]
	var out []entry
	for len(payload) >= 18 {
		d := binary.LittleEndian.Uint64(payload[0:])
		sc := math.Float64frombits(binary.LittleEndian.Uint64(payload[8:]))
		nl := int(binary.LittleEndian.Uint16(payload[16:]))
		out = append(out, entry{disc: d, name: payload[18 : 18+nl], score: sc})
		payload = payload[18+nl:]
	}
	return out, nil
}

// zscore is the member-chunk point read: one block GET, scan the chunk.
func zscore(ctx context.Context, s *sim.Sim, key string, p projection, name []byte) (float64, bool, error) {
	d := obs1.Disc(name)
	ents, err := fetchChunk(ctx, s, key, p, findChunk(p.dir, d))
	if err != nil {
		return 0, false, err
	}
	for _, e := range ents {
		if e.disc == d && string(e.name) == string(name) {
			return e.score, true, nil
		}
	}
	return 0, false, nil
}

// zrank computes rank(score, member) over the score projection: prefix
// sums over the resident per-run counts pick the boundary chunk, one
// block GET resolves the exact rank inside it. Ties on score resolve by
// member bytes, redis order.
func zrank(ctx context.Context, s *sim.Sim, key string, p projection, m member) (int, bool, error) {
	sk := scoreKey(m.score)
	ci := findChunk(p.dir, sk)
	rank := 0
	for i := range ci {
		rank += p.dir[i].count
	}
	ents, err := fetchChunk(ctx, s, key, p, ci)
	if err != nil {
		return 0, false, err
	}
	for _, e := range ents {
		if e.disc < sk || (e.disc == sk && string(e.name) < string(m.name)) {
			rank++
			continue
		}
		if e.disc == sk && string(e.name) == string(m.name) {
			return rank, true, nil
		}
	}
	return 0, false, nil
}

// zrankScan is the counter-arm: no score runs, so rank means scanning the
// member projection whole in coalesced ranges and counting smaller pairs.
func zrankScan(ctx context.Context, s *sim.Sim, key string, p projection, m member) (int, error) {
	sk := scoreKey(m.score)
	rank := 0
	for off := int64(0); off < int64(len(p.obj)); off += coalesceBytes {
		nb := int64(coalesceBytes)
		if off+nb > int64(len(p.obj)) {
			nb = int64(len(p.obj)) - off
		}
		if _, _, err := s.GetRange(ctx, key, off, nb); err != nil {
			return 0, err
		}
	}
	// The count itself is local math over what the scan returned; the lab
	// bills requests, so it recounts from the corpus-order projection dir
	// by fetching nothing further.
	for ci := range p.dir {
		ents, err := decodeLocal(p, ci)
		if err != nil {
			return 0, err
		}
		for _, e := range ents {
			esk := scoreKey(e.score)
			if esk < sk || (esk == sk && string(e.name) < string(m.name)) {
				rank++
			}
		}
	}
	return rank, nil
}

// decodeLocal decodes a chunk from the already-transferred object bytes,
// request-free, standing in for the scan buffer the client just paid for.
func decodeLocal(p projection, ci int) ([]entry, error) {
	ce := p.dir[ci]
	f := p.obj[ce.off:]
	total := int(binary.LittleEndian.Uint32(f[0:]))
	klen := int(binary.LittleEndian.Uint16(f[6:]))
	payload := f[chunkHdr+klen+8 : total]
	var out []entry
	for len(payload) >= 18 {
		d := binary.LittleEndian.Uint64(payload[0:])
		sc := math.Float64frombits(binary.LittleEndian.Uint64(payload[8:]))
		nl := int(binary.LittleEndian.Uint16(payload[16:]))
		out = append(out, entry{disc: d, name: payload[18 : 18+nl], score: sc})
		payload = payload[18+nl:]
	}
	return out, nil
}

// zrangeByScore plans [lo, hi] over the score runs: prefix sums pick the
// covering chunk span, the boundary block GET starts it, and the rest of
// the span transfers in coalesced ranges, the doc 08 1 + ceil row.
func zrangeByScore(ctx context.Context, s *sim.Sim, key string, p projection, lo, hi float64) (int, error) {
	lok, hik := scoreKey(lo), scoreKey(hi)
	ci := findChunk(p.dir, lok)
	cj := findChunk(p.dir, hik)
	got := 0
	ents, err := fetchChunk(ctx, s, key, p, ci)
	if err != nil {
		return 0, err
	}
	for _, e := range ents {
		if e.disc >= lok && e.disc <= hik {
			got++
		}
	}
	if cj > ci {
		start := p.dir[ci+1].off
		end := p.dir[cj].off + int64(p.dir[cj].ln)
		for off := start; off < end; off += coalesceBytes {
			nb := int64(coalesceBytes)
			if off+nb > end {
				nb = end - off
			}
			if _, _, err := s.GetRange(ctx, key, off, nb); err != nil {
				return 0, err
			}
		}
		for k := ci + 1; k <= cj; k++ {
			ents, err := decodeLocal(p, k)
			if err != nil {
				return 0, err
			}
			for _, e := range ents {
				if e.disc >= lok && e.disc <= hik {
					got++
				}
			}
		}
	}
	return got, nil
}

// churn applies u score updates through an overlay and rebuilds both
// projections in one pass, the doc 08 fold rule; consistency means the
// two projections carry the same (member, score) multiset afterward.
func churn(ms []member, u int) []member {
	out := make([]member, len(ms))
	copy(out, ms)
	for k := range u {
		i := (k * 6364136223846793005) % len(out)
		if i < 0 {
			i += len(out)
		}
		out[i].score = out[i].score + float64((k%17)-8)
	}
	return out
}

func projectionPairs(ctx context.Context, s *sim.Sim, key string, p projection) (map[string]float64, error) {
	pairs := make(map[string]float64)
	for off := int64(0); off < int64(len(p.obj)); off += coalesceBytes {
		nb := int64(coalesceBytes)
		if off+nb > int64(len(p.obj)) {
			nb = int64(len(p.obj)) - off
		}
		if _, _, err := s.GetRange(ctx, key, off, nb); err != nil {
			return nil, err
		}
	}
	for ci := range p.dir {
		ents, err := decodeLocal(p, ci)
		if err != nil {
			return nil, err
		}
		for _, e := range ents {
			pairs[string(e.name)] = e.score
		}
	}
	return pairs, nil
}

type cell struct {
	name  string
	ops   int
	gets  float64
	bytes float64
	extra string
}

type results struct {
	n         int
	ampRatio  float64
	dirBPerEl float64
	cells     []cell
	consist   bool
}

type labErr struct{ err error }

func die(format string, args ...any) { panic(labErr{fmt.Errorf(format, args...)}) }

func run(n, ops, churnU int) (res results, rerr error) {
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
	ms := corpus(n)
	ref := make([]member, len(ms))
	copy(ref, ms)
	sort.Slice(ref, func(a, b int) bool {
		ka, kb := scoreKey(ref[a].score), scoreKey(ref[b].score)
		if ka != kb {
			return ka < kb
		}
		return string(ref[a].name) < string(ref[b].name)
	})
	rankOf := make(map[string]int, n)
	for i, m := range ref {
		rankOf[string(m.name)] = i
	}

	s := sim.New(sim.Config{})
	memberProj := buildProjection(ms, false, obs1.ChunkTargetDefault)
	scoreProj := buildProjection(ms, true, obs1.ChunkTargetDefault)
	if _, err := s.Put(ctx, "seg/zm", memberProj.obj); err != nil {
		die("put member: %v", err)
	}
	if _, err := s.Put(ctx, "seg/zs", scoreProj.obj); err != nil {
		die("put score: %v", err)
	}
	res.n = n
	res.ampRatio = float64(len(memberProj.obj)+len(scoreProj.obj)) / float64(len(memberProj.obj))
	res.dirBPerEl = float64((dirEntBytes+runCountBytes)*(len(memberProj.dir)+len(scoreProj.dir))) / float64(n)

	score := func(name string, ops int, f func(i int) (bool, error)) {
		before := s.Usage()
		found := 0
		for i := range ops {
			ok, err := f(i)
			if err != nil {
				die("%s op %d: %v", name, i, err)
			}
			if ok {
				found++
			}
		}
		after := s.Usage()
		res.cells = append(res.cells, cell{
			name: name, ops: ops,
			gets:  float64(after.GetRequests-before.GetRequests) / float64(ops),
			bytes: float64(after.BytesDown-before.BytesDown) / float64(ops) / 1024,
			extra: fmt.Sprintf("%.1f found%%", 100*float64(found)/float64(ops)),
		})
	}

	score("zscore", ops, func(i int) (bool, error) {
		m := ms[(i*7919)%n]
		sc, ok, err := zscore(ctx, s, "seg/zm", memberProj, m.name)
		if err == nil && ok && sc != m.score {
			return false, fmt.Errorf("zscore %s = %v want %v", m.name, sc, m.score)
		}
		return ok, err
	})
	score("zrank", ops, func(i int) (bool, error) {
		m := ms[(i*104729)%n]
		r, ok, err := zrank(ctx, s, "seg/zs", scoreProj, m)
		if err == nil && ok && r != rankOf[string(m.name)] {
			return false, fmt.Errorf("zrank %s = %d want %d", m.name, r, rankOf[string(m.name)])
		}
		return ok, err
	})
	scanOps := 3
	score("zrank_scan", scanOps, func(i int) (bool, error) {
		m := ms[(i*7919)%n]
		r, err := zrankScan(ctx, s, "seg/zm", memberProj, m)
		if err == nil && r != rankOf[string(m.name)] {
			return false, fmt.Errorf("zrank_scan %s = %d want %d", m.name, r, rankOf[string(m.name)])
		}
		return true, err
	})
	for _, span := range []float64{0.001, 0.05, 0.25} {
		lo := -float64(n) / 4
		hi := lo + span*float64(n)*2
		want := 0
		for _, m := range ms {
			if m.score >= lo && m.score <= hi {
				want++
			}
		}
		name := fmt.Sprintf("zrangebyscore_%g", span)
		score(name, 5, func(int) (bool, error) {
			got, err := zrangeByScore(ctx, s, "seg/zs", scoreProj, lo, hi)
			if err == nil && got != want {
				return false, fmt.Errorf("%s got %d want %d", name, got, want)
			}
			return true, err
		})
	}

	// Churn arm: fold both projections from the churned overlay in one
	// pass and cross-check them against each other, T-I3's shape.
	churned := churn(ms, churnU)
	mp2 := buildProjection(churned, false, obs1.ChunkTargetDefault)
	sp2 := buildProjection(churned, true, obs1.ChunkTargetDefault)
	if _, err := s.Put(ctx, "seg/zm2", mp2.obj); err != nil {
		die("put churned member: %v", err)
	}
	if _, err := s.Put(ctx, "seg/zs2", sp2.obj); err != nil {
		die("put churned score: %v", err)
	}
	a, err := projectionPairs(ctx, s, "seg/zm2", mp2)
	if err != nil {
		die("walk churned member: %v", err)
	}
	b, err := projectionPairs(ctx, s, "seg/zs2", sp2)
	if err != nil {
		die("walk churned score: %v", err)
	}
	res.consist = len(a) == len(b) && len(a) == n
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			res.consist = false
			break
		}
	}
	return res, nil
}

func main() {
	quick := flag.Bool("quick", false, "smoke sizes")
	flag.Parse()
	decades := []int{100_000, 1_000_000, 10_000_000}
	ops := 2000
	if *quick {
		decades, ops = []int{50_000}, 200
	}
	fmt.Println("n,cell,ops,gets_per_op,kib_per_op,extra")
	for _, n := range decades {
		res, err := run(n, ops, n/10)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, c := range res.cells {
			fmt.Printf("%d,%s,%d,%.4f,%.1f,%s\n", n, c.name, c.ops, c.gets, c.bytes, c.extra)
		}
		fmt.Printf("%d,dual_amp_ratio,,%.4f,,\n", n, res.ampRatio)
		fmt.Printf("%d,dir_b_per_elem_dual,,%.4f,,\n", n, res.dirBPerEl)
		fmt.Printf("%d,churn_consistent,,,,%v\n", n, res.consist)
	}
}
