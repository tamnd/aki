// handofftime scores PRED-OBS1-O3A-HANDOFF: graceful handoff wall time
// vs group WAL-tail depth and taker warmth, and whether the warm window
// is flat in data size. The doc 02 section 4.3 seven-step handoff prices
// as four terms on the landed primitives, all against the sim with the
// doc 01 S3 Standard latency envelope drawn per request:
//
//	release   the holder's ChainAppender.Append of the release record
//	observe   the taker's Follow poll that sees it (batch GET + 404 GET)
//	grant     the taker's ChainAppender.Append of the grant record
//	replay    serial GET + parse of the D WAL-tail objects past FoldPos
//
// The design claim under test is the ordering one: the seven-step
// sequence lets the taker pre-warm its resident structures (manifest,
// directory, keymap) before the release lands, so the critical window is
// release + observe + grant + replay and never touches a segment. That
// is the only way the window can be flat from 1 GiB to 1 TiB, and the
// lab proves the flatness by op accounting, not by timing two identical
// code paths: the sim's request counters over the warm phase show
// exactly the chain and WAL-tail ops, zero segment GETs, with no term
// that grows with data size.
//
// The pre-warm term itself, a crash taker's whole bill, is the as-built
// RebuildResident: one serial whole-object GET per manifest segment.
// The lab sweeps it over segment count serial and behind a lab-side
// prefetch fan (parallel GETs into memory, then the unchanged
// RebuildResident feeding from the prefetched bodies), verifying the
// rebuilt stats are identical, to price the fan constant the handoff
// slice must bake for crash takeover to fit its bar.
//
// Chain semantics are real: both nodes fold the chain through LeaseFold
// and the lab checks the holder and epoch agree on both folds after
// every handoff, back and forth, epochs monotone.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

const (
	group      = 3
	kindString = 0x01 // doc 04 string record kind, the fold's plain run
	valLen     = 64
	walFrames  = 128
	walPayload = 256
)

type cfg struct {
	segSweep   []int // segment counts for the rebuild sweep
	recsPerSeg int
	warmReps   int
	rebReps    int
	replayReps int
	walDepth   int // WAL-tail objects built; replay sweeps up to this
}

func fullCfg() cfg {
	return cfg{
		segSweep:   []int{8, 32, 128, 256},
		recsPerSeg: 4096,
		warmReps:   40,
		rebReps:    3,
		replayReps: 10,
		walDepth:   16,
	}
}

func quickCfg() cfg {
	return cfg{
		segSweep:   []int{4, 8},
		recsPerSeg: 256,
		warmReps:   6,
		rebReps:    1,
		replayReps: 2,
		walDepth:   4,
	}
}

// segPrefix keeps each sweep size in its own namespace so every arm
// rebuilds from its own manifest.
func segPrefix(n int) string { return fmt.Sprintf("ho/n%03d", n) }

// segObjKey mirrors the unexported segKey naming (doc 03 section 1):
// <prefix>/seg/g<ggg>/<16-digit-seq>, which is where RebuildResident
// will GET.
func segObjKey(prefix string, seq uint64) string {
	return fmt.Sprintf("%s/seg/g%03d/%016d", prefix, group, seq)
}

func walKey(seq int) string { return fmt.Sprintf("ho/wal/%016d", seq) }

func valueFor(seg, i int) []byte {
	v := make([]byte, valLen)
	copy(v, fmt.Sprintf("seg%05d-rec%06d-", seg, i))
	for j := range v {
		if v[j] == 0 {
			v[j] = byte('a' + (seg+i+j)%26)
		}
	}
	return v
}

// buildSegment writes one segment of recs plain string records as run
// chunks the way the folder's cut() emits them: records sorted by
// fingerprint, frame-concatenated payload, chunk kind with the chunk bit,
// FirstDisc the first record's fingerprint.
func buildSegment(segSeq uint64, recs int) ([]byte, error) {
	type rec struct {
		fp    uint64
		key   []byte
		frame []byte
	}
	rows := make([]rec, 0, recs)
	for i := 0; i < recs; i++ {
		k := []byte(fmt.Sprintf("k%05d-%06d", segSeq, i))
		v := valueFor(int(segSeq), i)
		frame := store.AppendRecordFrame(nil, kindString, 0, uint32(len(v)), k, v, 0)
		rows = append(rows, rec{fp: obs1.Fingerprint(k), key: k, frame: frame})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].fp < rows[j].fp })

	var chunks []obs1.SegmentChunk
	var memberKeys [][]byte
	var payload []byte
	var run []rec
	cut := func() {
		if len(run) == 0 {
			return
		}
		first := run[0]
		var disc [8]byte
		for i := 0; i < 8; i++ {
			disc[i] = byte(first.fp >> (56 - 8*i))
		}
		data := store.AppendRunChunk(nil, kindString|store.ChunkKindBit, store.ChunkFlagRun,
			uint16(len(run)), first.key, disc[:], payload)
		chunks = append(chunks, obs1.SegmentChunk{
			Key: first.key, Kind: kindString | store.ChunkKindBit, Flags: store.ChunkFlagRun,
			FirstDisc: first.fp, Count: uint16(len(run)), LiveHint: uint16(len(run)),
			Data: data,
		})
		payload, run = nil, nil
	}
	for i := range rows {
		if len(run) == 0xFFFF {
			cut()
		}
		run = append(run, rows[i])
		payload = append(payload, rows[i].frame...)
		memberKeys = append(memberKeys, rows[i].key)
	}
	cut()
	seg, err := obs1.BuildSegment(obs1.SegmentFooter{Group: group, Epoch: 1, SegSeq: segSeq}, chunks, memberKeys, 0)
	if err != nil {
		return nil, err
	}
	return obs1.AppendSegment(nil, 0xB0, seg)
}

// buildWAL writes one WAL-tail object: one section for the group,
// walFrames op frames of walPayload bytes each.
func buildWAL(seq int) ([]byte, error) {
	frames := make([]obs1.WALFrame, walFrames)
	for i := range frames {
		p := make([]byte, walPayload)
		for j := range p {
			p[j] = byte('a' + (seq+i+j)%26)
		}
		frames[i] = obs1.WALFrame{
			Kind: kindString, Slot: 100, Seq: uint64(seq*walFrames + i + 1),
			Key:     []byte(fmt.Sprintf("wk%04d-%04d", seq, i)),
			Payload: p,
		}
	}
	return obs1.AppendWAL(nil, 1, []obs1.WALSection{{Group: group, Epoch: 1, Frames: frames}})
}

// prefetched serves Get from prefetched bodies and delegates everything
// else, so RebuildResident runs unchanged over a fan-fetched cache.
type prefetched struct {
	obs1.Store
	m map[string][]byte
}

func (p prefetched) Get(ctx context.Context, key string) ([]byte, obs1.ObjectInfo, error) {
	if b, ok := p.m[key]; ok {
		return b, obs1.ObjectInfo{Size: int64(len(b))}, nil
	}
	return p.Store.Get(ctx, key)
}

func quantiles(ds []time.Duration) (p50, p99 time.Duration) {
	if len(ds) == 0 {
		return 0, 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	p50 = s[len(s)/2]
	i := (len(s)*99 + 99) / 100
	if i >= len(s) {
		i = len(s) - 1
	}
	return p50, s[i]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func replayTail(ctx context.Context, s obs1.Store, depth, walDepth int) (int, error) {
	frames := 0
	for d := 0; d < depth; d++ {
		body, _, err := s.Get(ctx, walKey(walDepth-depth+d+1))
		if err != nil {
			return 0, err
		}
		sections, _, err := obs1.ParseWAL(body)
		if err != nil {
			return 0, err
		}
		for _, sec := range sections {
			if sec.Group == group {
				frames += len(sec.Frames)
			}
		}
	}
	return frames, nil
}

func run(c cfg) error {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 20260724, Latency: sim.S3Standard})

	// Build phase, not scored: segments for every sweep size plus the
	// WAL tail, fanned 16 wide since build wall is not a measurement.
	t0 := time.Now()
	type putJob struct {
		key  string
		body []byte
	}
	var jobs []putJob
	for _, n := range c.segSweep {
		for seq := 1; seq <= n; seq++ {
			body, err := buildSegment(uint64(seq), c.recsPerSeg)
			if err != nil {
				return fmt.Errorf("build segment %d/%d: %w", n, seq, err)
			}
			jobs = append(jobs, putJob{segObjKey(segPrefix(n), uint64(seq)), body})
		}
	}
	for d := 1; d <= c.walDepth; d++ {
		body, err := buildWAL(d)
		if err != nil {
			return fmt.Errorf("build wal %d: %w", d, err)
		}
		jobs = append(jobs, putJob{walKey(d), body})
	}
	var wg sync.WaitGroup
	errs := make([]error, 16)
	jobCh := make(chan putJob)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := range jobCh {
				if _, err := s.Put(ctx, j.key, j.body); err != nil {
					errs[w] = err
				}
			}
		}(w)
	}
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	segBytes := 0
	for _, j := range jobs {
		segBytes += len(j.body)
	}
	fmt.Printf("build,objects=%d,bytes=%d,wall_s=%.1f\n", len(jobs), segBytes, time.Since(t0).Seconds())

	// Warm phase: the four-term window, handoff back and forth between
	// two nodes with real LeaseFold semantics checked on both sides.
	foldA, foldB := obs1.NewLeaseFold(), obs1.NewLeaseFold()
	apA, err := obs1.NewChainAppender(s, "ho/chain", 0, 1, 1, obs1.ChainPos{}, foldA)
	if err != nil {
		return err
	}
	apB, err := obs1.NewChainAppender(s, "ho/chain", 0, 2, 1, obs1.ChainPos{}, foldB)
	if err != nil {
		return err
	}
	// Seed: node 1 holds the lease at epoch 1.
	if _, err := apA.Append(ctx, []obs1.ChainRecord{obs1.GrantRecord{Group: group, Node: 1, Epoch: 1}}); err != nil {
		return err
	}
	if err := apB.Follow(ctx); err != nil {
		return err
	}

	depths := []int{0, 1, 4}
	usageBefore := s.Usage()
	release := make([]time.Duration, 0, c.warmReps)
	observe := make([]time.Duration, 0, c.warmReps)
	grant := make([]time.Duration, 0, c.warmReps)
	replays := map[int][]time.Duration{}
	windows := map[int][]time.Duration{}
	epoch := uint32(1)
	wantFrames := 0
	for _, d := range depths {
		wantFrames += d * walFrames
	}
	for r := 0; r < c.warmReps; r++ {
		holder, taker := apA, apB
		holderFold, takerFold := foldA, foldB
		takerNode := uint64(2)
		if r%2 == 1 {
			holder, taker = apB, apA
			holderFold, takerFold = foldB, foldA
			takerNode = 1
		}
		t := time.Now()
		if _, err := holder.Append(ctx, []obs1.ChainRecord{obs1.ReleaseRecord{Group: group, Epoch: epoch}}); err != nil {
			return fmt.Errorf("rep %d release: %w", r, err)
		}
		dRel := time.Since(t)
		t = time.Now()
		if err := taker.Follow(ctx); err != nil {
			return fmt.Errorf("rep %d observe: %w", r, err)
		}
		dObs := time.Since(t)
		t = time.Now()
		if _, err := taker.Append(ctx, []obs1.ChainRecord{obs1.GrantRecord{Group: group, Node: takerNode, Epoch: epoch + 1}}); err != nil {
			return fmt.Errorf("rep %d grant: %w", r, err)
		}
		dGrant := time.Since(t)
		epoch++
		gotFrames := 0
		for _, d := range depths {
			t = time.Now()
			n, err := replayTail(ctx, s, d, c.walDepth)
			if err != nil {
				return fmt.Errorf("rep %d replay depth %d: %w", r, d, err)
			}
			dRep := time.Since(t)
			gotFrames += n
			replays[d] = append(replays[d], dRep)
			windows[d] = append(windows[d], dRel+dObs+dGrant+dRep)
		}
		if gotFrames != wantFrames {
			return fmt.Errorf("rep %d replayed %d frames, want %d", r, gotFrames, wantFrames)
		}
		release, observe, grant = append(release, dRel), append(observe, dObs), append(grant, dGrant)
		// The old holder catches up off the clock so its next append
		// starts from the true tail, the steady state a live node holds.
		if err := holder.Follow(ctx); err != nil {
			return fmt.Errorf("rep %d holder catch-up: %w", r, err)
		}
		for _, f := range []*obs1.LeaseFold{holderFold, takerFold} {
			node, e, ok := f.Holder(group)
			if !ok || node != takerNode || e != epoch {
				return fmt.Errorf("rep %d fold disagrees: holder %d epoch %d ok %v, want %d at %d", r, node, e, ok, takerNode, epoch)
			}
		}
	}
	usageAfter := s.Usage()
	warmGets := usageAfter.GetRequests - usageBefore.GetRequests
	warmPuts := usageAfter.PutRequests - usageBefore.PutRequests
	// The flatness accounting: per rep the window is 2 chain PUTs, the
	// observe pair and catch-up pair of chain GETs, and the swept WAL
	// GETs. Nothing scales with data size; the assertion makes the count
	// a checked invariant rather than a printout.
	walGets := 0
	for _, d := range depths {
		walGets += d
	}
	wantGets := int64(c.warmReps * (2 + 2 + walGets))
	wantPuts := int64(c.warmReps * 2)
	if warmGets != wantGets || warmPuts != wantPuts {
		return fmt.Errorf("warm phase billed %d GETs %d PUTs, want %d and %d: a size-dependent op leaked into the window", warmGets, warmPuts, wantGets, wantPuts)
	}
	p50, p99 := quantiles(release)
	fmt.Printf("term,release,reps=%d,p50_ms=%.1f,p99_ms=%.1f\n", len(release), ms(p50), ms(p99))
	p50, p99 = quantiles(observe)
	fmt.Printf("term,observe,reps=%d,p50_ms=%.1f,p99_ms=%.1f\n", len(observe), ms(p50), ms(p99))
	p50, p99 = quantiles(grant)
	fmt.Printf("term,grant,reps=%d,p50_ms=%.1f,p99_ms=%.1f\n", len(grant), ms(p50), ms(p99))
	for _, d := range depths {
		p50, p99 = quantiles(replays[d])
		fmt.Printf("term,replay,depth=%d,p50_ms=%.1f,p99_ms=%.1f\n", d, ms(p50), ms(p99))
	}
	for _, d := range depths {
		p50, p99 = quantiles(windows[d])
		fmt.Printf("warm_window,depth=%d,reps=%d,p50_ms=%.1f,p99_ms=%.1f,segment_gets=0\n", d, len(windows[d]), ms(p50), ms(p99))
	}
	fmt.Printf("warm_ops,gets=%d,puts=%d,per_rep_gets=%d,per_rep_puts=2,size_dependent_ops=0\n", warmGets, warmPuts, 2+2+walGets)

	// Rebuild sweep: the cold taker's pre-warm term, serial as built and
	// behind the lab prefetch fan, stats verified identical.
	fans := []int{1, 8, 32}
	rebuildP50 := map[[2]int]time.Duration{}
	for _, n := range c.segSweep {
		m := obs1.Manifest{Group: group}
		for seq := 1; seq <= n; seq++ {
			m.Segs = append(m.Segs, obs1.ManifestSeg{SegSeq: uint64(seq)})
		}
		var want obs1.ResidentStats
		for _, fan := range fans {
			durs := make([]time.Duration, 0, c.rebReps)
			var st obs1.ResidentStats
			for rep := 0; rep < c.rebReps; rep++ {
				km, dir := obs1.NewKeymap(), obs1.NewDirectory()
				t := time.Now()
				if fan == 1 {
					var err error
					st, err = obs1.RebuildResident(ctx, s, segPrefix(n), m, dir, km)
					if err != nil {
						return fmt.Errorf("serial rebuild n=%d: %w", n, err)
					}
				} else {
					bodies := make(map[string][]byte, n)
					var mu sync.Mutex
					var wg sync.WaitGroup
					ferrs := make([]error, fan)
					seqCh := make(chan int)
					for w := 0; w < fan; w++ {
						wg.Add(1)
						go func(w int) {
							defer wg.Done()
							for seq := range seqCh {
								key := segObjKey(segPrefix(n), uint64(seq))
								b, _, err := s.Get(ctx, key)
								if err != nil {
									ferrs[w] = err
									continue
								}
								mu.Lock()
								bodies[key] = b
								mu.Unlock()
							}
						}(w)
					}
					for seq := 1; seq <= n; seq++ {
						seqCh <- seq
					}
					close(seqCh)
					wg.Wait()
					for _, err := range ferrs {
						if err != nil {
							return fmt.Errorf("fan %d fetch n=%d: %w", fan, n, err)
						}
					}
					var err error
					st, err = obs1.RebuildResident(ctx, prefetched{Store: s, m: bodies}, segPrefix(n), m, dir, km)
					if err != nil {
						return fmt.Errorf("fan %d rebuild n=%d: %w", fan, n, err)
					}
				}
				durs = append(durs, time.Since(t))
			}
			if st.Segments != n || st.Records != n*c.recsPerSeg || st.Tombstones != 0 {
				return fmt.Errorf("rebuild n=%d fan=%d stats %+v, want %d segments %d records", n, fan, st, n, n*c.recsPerSeg)
			}
			if fan == 1 {
				want = st
			} else if st != want {
				return fmt.Errorf("rebuild n=%d fan=%d stats %+v diverge from serial %+v", n, fan, st, want)
			}
			p50, _ := quantiles(durs)
			rebuildP50[[2]int{n, fan}] = p50
			perSeg := ms(p50) / float64(n)
			fmt.Printf("rebuild,n=%d,fan=%d,reps=%d,p50_ms=%.0f,ms_per_seg=%.1f,records=%d\n",
				n, fan, c.rebReps, ms(p50), perSeg, want.Records)
		}
	}

	// Replay depth sweep on its own clock for the linearity row.
	sweep := []int{1, 4, c.walDepth}
	sweep = slices.Compact(sweep)
	for _, d := range sweep {
		durs := make([]time.Duration, 0, c.replayReps)
		for rep := 0; rep < c.replayReps; rep++ {
			t := time.Now()
			n, err := replayTail(ctx, s, d, c.walDepth)
			if err != nil {
				return err
			}
			if n != d*walFrames {
				return fmt.Errorf("replay depth %d walked %d frames, want %d", d, n, d*walFrames)
			}
			durs = append(durs, time.Since(t))
		}
		p50, p99 := quantiles(durs)
		fmt.Printf("replay_sweep,depth=%d,reps=%d,p50_ms=%.1f,p99_ms=%.1f\n", d, c.replayReps, ms(p50), ms(p99))
	}

	// Cold takeover composition from the measured medians: observe +
	// grant + rebuild + replay(4). Release is not in a crash takeover
	// and the taker-side TTL wait is policy time, reported beside it.
	obsP50, _ := quantiles(observe)
	grantP50, _ := quantiles(grant)
	repP50, _ := quantiles(replays[4])
	for _, n := range c.segSweep {
		for _, fan := range fans {
			cold := obsP50 + grantP50 + rebuildP50[[2]int{n, fan}] + repP50
			fmt.Printf("cold_takeover,n=%d,fan=%d,composed_p50_ms=%.0f\n", n, fan, ms(cold))
		}
	}
	return nil
}

func main() {
	quick := flag.Bool("quick", false, "small corpus smoke")
	flag.Parse()
	c := fullCfg()
	if *quick {
		c = quickCfg()
	}
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, "handofftime:", err)
		os.Exit(1)
	}
}
