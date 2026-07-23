// flat scores PRED-OBS1-O2A-FLAT: cold HGET latency stays flat from
// 10^3 to 10^8 fields at a fixed cache budget. The read plane is the
// landed one end to end: real ChunkPacker chunks in a real segment built
// by BuildSegment and AppendSegment, resolved through a real Directory
// and Keymap, fetched by the real ColdReader against the sim with the
// doc 01 S3 Standard latency envelope drawn per GET.
//
// The corpus rides the bigcollection lab's trick: elements generate from
// a sorted (disc, index) table, so a 10^8 collection costs the table plus
// the object, never per-element blobs. Hash decades carry 64 B values to
// 10^7; the 10^8 decade is the valueless set shape, which doc 08 defines
// as a hash with empty values and identical planning.
//
// Fixed cache budget means the resident plan only: keymap plus directory,
// reported per decade. There is no block cache yet (doc 05 section 4 is
// owed), so every fetch pays its GET, which is exactly the claim under
// test: the bill per op does not grow with cardinality.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

// kindHash pins the hash record kind byte the way the engine tests pin it.
const kindHash = 0x04

const (
	group = 3
	nowMs = int64(1_700_000_000_000)
)

type cfg struct {
	maxN    int  // largest decade
	fetches int  // scored fetches per decade
	lat     bool // draw the S3 Standard envelope per op
}

type flatRow struct {
	kind      string
	n         int
	chunks    int
	objMiB    float64
	getsPerOp float64
	kibPerOp  float64
	foundPct  float64
	resolveUs float64
	p50Ms     float64
	p99Ms     float64
	dirBytes  int
	dirBPerEl float64
}

type results struct {
	rows []flatRow
	cold obs1.ColdReadStats
}

type labErr struct{ err error }

func die(format string, args ...any) {
	panic(labErr{fmt.Errorf(format, args...)})
}

func elemName(i int) []byte { return fmt.Appendf(nil, "m:%09d", i) }

type discIdx struct {
	disc uint64
	idx  int
}

// buildDecade packs one collection of n elements into one segment on a
// fresh sim and returns everything the read plane needs. One segment is
// the lab's envelope: ResolveField plans within the segment the keymap
// pins, and BuildSegment has no size ceiling, so the whole collection
// folds into a single object the way a doc 06 rewrite pass would leave it.
func buildDecade(ctx context.Context, n int, value []byte, seed uint64, lat bool) (*sim.Sim, *obs1.Directory, *obs1.Keymap, error) {
	tab := make([]discIdx, n)
	for i := range n {
		tab[i] = discIdx{disc: obs1.Disc(elemName(i)), idx: i}
	}
	sort.Slice(tab, func(i, j int) bool { return tab[i].disc < tab[j].disc })

	colKey := []byte("h")
	var chunks []obs1.SegmentChunk
	var pk store.ChunkPacker
	var first uint64
	flush := func() {
		if pk.Count() == 0 {
			return
		}
		payload, flags := pk.Finish()
		var disc [8]byte
		binary.BigEndian.PutUint64(disc[:], first)
		frame := store.AppendRunChunk(nil, kindHash|store.ChunkKindBit, flags, uint16(pk.Count()), colKey, disc[:], payload)
		chunks = append(chunks, obs1.SegmentChunk{
			Key: colKey, Kind: kindHash | store.ChunkKindBit, Flags: flags,
			FirstDisc: first, Count: uint16(pk.Count()), LiveHint: uint16(pk.Count()),
			Data: frame,
		})
		pk.Reset()
	}
	for _, e := range tab {
		if pk.Count() == 0 {
			first = e.disc
		}
		pk.Add(elemName(e.idx), value, 0)
		if pk.Bytes() >= obs1.ChunkTargetDefault {
			flush()
		}
	}
	flush()
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

	// AppendSegment derives the block index and never writes it back into
	// the builder's footer (#1102), so the directory's copy is read off
	// the built object, the same way boot recovery reads it.
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
	if _, err := s.Put(ctx, "seg/g003/flat", obj); err != nil {
		return nil, nil, nil, err
	}

	dir := obs1.NewDirectory()
	if err := dir.Add("seg/g003/flat", &footer); err != nil {
		return nil, nil, nil, err
	}
	km := obs1.NewKeymap()
	if err := km.Put(obs1.Fingerprint(colKey), obs1.KeyLoc{Seg: 1}); err != nil {
		return nil, nil, nil, err
	}
	return s, dir, km, nil
}

// fetchField runs one FetchField and blocks for its completion.
func fetchField(cr *obs1.ColdReader, loc obs1.KeyLoc, field []byte) (obs1.ColdField, error) {
	var (
		cf   obs1.ColdField
		rerr error
		done = make(chan struct{})
	)
	cr.FetchField(group, []byte("h"), loc, field, nowMs, func(f obs1.ColdField, err error) {
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

// scoreDecade builds one decade and runs the scored serial fetch loop.
// Serial on purpose: concurrent fetches into a small collection coalesce
// on the single-flight table, which prices load shapes, not the per-op
// claim under test; every op here pays its own draw.
func scoreDecade(ctx context.Context, kind string, n int, value []byte, c cfg, cold *obs1.ColdReadStats) flatRow {
	s, dir, km, err := buildDecade(ctx, n, value, uint64(n)*0x9E37+1, c.lat)
	if err != nil {
		die("build n=%d: %v", n, err)
	}
	loc, ok := km.Lookup(obs1.Fingerprint([]byte("h")))
	if !ok {
		die("n=%d: collection not in keymap", n)
	}
	fp := obs1.Fingerprint([]byte("h"))

	// The resolver walk is the one term that grows with cardinality: a
	// linear scan over the segment's chunk rows. Timed alone, resident,
	// no sim in the path.
	rt0 := time.Now()
	for i := range c.fetches {
		name := elemName((i * 7919) % n)
		if _, ok := dir.ResolveField(loc, fp, obs1.Disc(name)); !ok {
			die("n=%d: resolve miss on %s", n, name)
		}
	}
	resolveUs := float64(time.Since(rt0).Microseconds()) / float64(c.fetches)

	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: s,
		Dir:   func(uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		die("n=%d: cold reader: %v", n, err)
	}
	defer cr.Close()

	before := s.Usage()
	lats := make([]time.Duration, 0, c.fetches)
	found := 0
	for i := range c.fetches {
		name := elemName((i * 7919) % n)
		t0 := time.Now()
		cf, err := fetchField(cr, loc, name)
		lats = append(lats, time.Since(t0))
		if err != nil {
			die("n=%d fetch %s: %v", n, name, err)
		}
		if cf.Found && len(cf.Value) == len(value) {
			found++
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
	return flatRow{
		kind:      kind,
		n:         n,
		chunks:    dir.Chunks(),
		objMiB:    float64(after.BytesStored) / (1 << 20),
		getsPerOp: float64(after.GetRequests-before.GetRequests) / float64(c.fetches),
		kibPerOp:  float64(after.BytesDown-before.BytesDown) / float64(c.fetches) / 1024,
		foundPct:  100 * float64(found) / float64(c.fetches),
		resolveUs: resolveUs,
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
	value := make([]byte, 64)
	for i := range value {
		value[i] = byte(i*37 + 11)
	}
	for n := 1000; n <= c.maxN && n <= 10_000_000; n *= 10 {
		res.rows = append(res.rows, scoreDecade(ctx, "hash", n, value, c, &res.cold))
	}
	if c.maxN >= 100_000_000 {
		res.rows = append(res.rows, scoreDecade(ctx, "set", 100_000_000, nil, c, &res.cold))
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
	fmt.Println("kind,n,chunks,obj_mib,gets_per_op,kib_per_op,found_pct,resolve_us,p50_ms,p99_ms,dir_b,dir_b_per_elem")
	minP99, maxP99 := res.rows[0].p99Ms, res.rows[0].p99Ms
	for _, r := range res.rows {
		fmt.Printf("%s,%d,%d,%.1f,%.4f,%.1f,%.1f,%.1f,%.1f,%.1f,%d,%.4f\n",
			r.kind, r.n, r.chunks, r.objMiB, r.getsPerOp, r.kibPerOp, r.foundPct,
			r.resolveUs, r.p50Ms, r.p99Ms, r.dirBytes, r.dirBPerEl)
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
