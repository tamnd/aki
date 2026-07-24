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
	if err := runE2E(c); err != nil {
		return err
	}
	return runTakeoverE2E(c)
}

// e2eNode is one node's real coordination stack: fold, gate, tail
// window, appender, manager.
type e2eNode struct {
	fold *obs1.LeaseFold
	gate *obs1.LeaseGate
	win  *obs1.TailWindow
	ap   *obs1.ChainAppender
	mgr  *obs1.LeaseManager
}

func newE2ENode(s obs1.Store, self uint64) (*e2eNode, error) {
	fold := obs1.NewLeaseFold()
	gate := obs1.NewLeaseGate(0, 0)
	win, err := obs1.NewTailWindow(fold, fold)
	if err != nil {
		return nil, err
	}
	ap, err := obs1.NewChainAppender(s, "e2e", 0, self, 1, obs1.ChainPos{}, win)
	if err != nil {
		return nil, err
	}
	mgr, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{
		Self: self, Appender: ap, Fold: fold, Gate: gate,
	})
	if err != nil {
		return nil, err
	}
	return &e2eNode{fold, gate, win, ap, mgr}, nil
}

// e2eFlush is the rep's final flush: one real WAL object under the
// holder's namespace, its commit record returned for the release batch.
func e2eFlush(ctx context.Context, s obs1.Store, prefix string, node, walSeq uint64, epoch uint32, firstSeq uint64) (obs1.CommitRecord, uint64, error) {
	frames := make([]obs1.WALFrame, walFrames)
	for i := range frames {
		p := make([]byte, walPayload)
		for j := range p {
			p[j] = byte('a' + (int(walSeq)+i+j)%26)
		}
		frames[i] = obs1.WALFrame{
			Kind: kindString, Slot: 100, Seq: firstSeq + uint64(i),
			Key:     []byte(fmt.Sprintf("ek%06d-%04d", walSeq, i)),
			Payload: p,
		}
	}
	body, err := obs1.AppendWAL(nil, node, []obs1.WALSection{{Group: group, Epoch: epoch, Frames: frames}})
	if err != nil {
		return obs1.CommitRecord{}, 0, err
	}
	key := fmt.Sprintf("%s/wal/%016x/%016d", prefix, node, walSeq)
	if _, err := s.Put(ctx, key, body); err != nil {
		return obs1.CommitRecord{}, 0, err
	}
	off, flen, err := obs1.ParseTail(body[len(body)-obs1.TailSize:])
	if err != nil {
		return obs1.CommitRecord{}, 0, err
	}
	entries, err := obs1.ParseWALFooter(body[off : off+uint64(flen)])
	if err != nil {
		return obs1.CommitRecord{}, 0, err
	}
	rec := obs1.CommitRecord{WALNode: node, WALSeq: walSeq, WALSize: uint64(len(body))}
	for _, e := range entries {
		rec.Sections = append(rec.Sections, e.CommitSection())
	}
	return rec, frames[len(frames)-1].Seq, nil
}

// runE2E re-scores PRED-OBS1-O3A-HANDOFF on the slice's real sequence:
// Handoff (final flush's commit and the release in one batch) through
// Follow, Reconcile, Acquire, and TakeGroup on the taker, warm taker,
// depth 1 by construction with a checkpoint after each rep keeping the
// retained window at exactly the rep's own flush.
func runE2E(c cfg) error {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 20260725, Latency: sim.S3Standard})

	a, err := newE2ENode(s, 1)
	if err != nil {
		return err
	}
	b, err := newE2ENode(s, 2)
	if err != nil {
		return err
	}
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 1, Incarnation: 1, Weight: 100}},
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 2, Incarnation: 1, Weight: 100}},
	}); err != nil {
		return err
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		return fmt.Errorf("e2e seed acquire: %v %v", won, err)
	}
	if err := b.ap.Follow(ctx); err != nil {
		return err
	}
	b.mgr.Reconcile()

	nodes := map[uint64]*e2eNode{1: a, 2: b}
	windows := make([]time.Duration, 0, c.warmReps)
	holderID, takerID := uint64(1), uint64(2)
	walSeqs := map[uint64]uint64{1: 0, 2: 0}
	lastSeq := uint64(0)
	usageBefore := s.Usage()
	for r := 0; r < c.warmReps; r++ {
		holder, taker := nodes[holderID], nodes[takerID]
		floor := lastSeq
		t := time.Now()
		err := holder.mgr.Handoff(ctx, group, func(ctx context.Context) ([]obs1.ChainRecord, error) {
			walSeqs[holderID]++
			rec, last, err := e2eFlush(ctx, s, "e2e", holderID, walSeqs[holderID], uint32(r+1), floor+1)
			if err != nil {
				return nil, err
			}
			lastSeq = last
			return []obs1.ChainRecord{rec}, nil
		})
		if err != nil {
			return fmt.Errorf("e2e rep %d handoff: %w", r, err)
		}
		if err := taker.ap.Follow(ctx); err != nil {
			return fmt.Errorf("e2e rep %d observe: %w", r, err)
		}
		taker.mgr.Reconcile()
		if won, err := taker.mgr.Acquire(ctx, group); err != nil || !won {
			return fmt.Errorf("e2e rep %d take: %v %v", r, won, err)
		}
		st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
			Store: s, Prefix: "e2e", Group: group, Window: taker.win,
			Manifest: obs1.Manifest{Group: group, FoldSeq: floor}, HasManifest: true, Warm: true,
		})
		if err != nil {
			return fmt.Errorf("e2e rep %d replay: %w", r, err)
		}
		windows = append(windows, time.Since(t))
		if st.FramesApplied != walFrames || st.Applied != lastSeq {
			return fmt.Errorf("e2e rep %d replayed %d frames to %d, want %d to %d", r, st.FramesApplied, st.Applied, walFrames, lastSeq)
		}
		wantEpoch := uint32(r + 2)
		for id, n := range nodes {
			if id == holderID {
				continue
			}
			node, e, ok := n.fold.Holder(group)
			if !ok || node != takerID || e != wantEpoch {
				return fmt.Errorf("e2e rep %d node %d fold says holder %d epoch %d ok %v, want %d at %d", r, id, node, e, ok, takerID, wantEpoch)
			}
		}
		// Off the clock: the new holder checkpoints, which trims its
		// retained window to nothing, and the old holder catches up.
		if _, err := taker.ap.Append(ctx, []obs1.ChainRecord{obs1.CheckpointRecord{Pos: taker.fold.Applied()}}); err != nil {
			return fmt.Errorf("e2e rep %d checkpoint: %w", r, err)
		}
		if got := taker.win.Retained(); got != 0 {
			return fmt.Errorf("e2e rep %d window retains %d sections after the checkpoint, want 0", r, got)
		}
		if err := holder.ap.Follow(ctx); err != nil {
			return fmt.Errorf("e2e rep %d catch-up: %w", r, err)
		}
		holder.mgr.Reconcile()
		holderID, takerID = takerID, holderID
	}
	usageAfter := s.Usage()

	// The on-clock bill per rep: the WAL PUT, the handoff batch, and the
	// grant; the observe pair and the one ranged section GET. Off-clock
	// the checkpoint appends one PUT and the catch-up reads the taker's
	// two objects plus the 404 probe.
	gets := usageAfter.GetRequests - usageBefore.GetRequests
	puts := usageAfter.PutRequests - usageBefore.PutRequests
	wantGets := int64(c.warmReps * (2 + 1 + 3))
	wantPuts := int64(c.warmReps * (3 + 1))
	if gets != wantGets || puts != wantPuts {
		return fmt.Errorf("e2e billed %d GETs %d PUTs, want %d and %d: a size-dependent op leaked into the sequence", gets, puts, wantGets, wantPuts)
	}
	p50, p99 := quantiles(windows)
	fmt.Printf("e2e_window,reps=%d,p50_ms=%.1f,p99_ms=%.1f,on_clock_gets=3,on_clock_puts=3,segment_gets=0\n",
		len(windows), ms(p50), ms(p99))
	return nil
}

// labClock is the policy clock the takeover phase advances by hand: the
// discipline (chain-observed staleness plus the taker's full-TTL watch)
// is wall-clock waiting in production and simulated here, while the
// mechanics are timed on the real clock against the sim's latency draws.
type labClock struct{ t time.Time }

func (c *labClock) now() time.Time          { return c.t }
func (c *labClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newLabClock(base time.Time) *labClock  { return &labClock{t: base} }

// ctoNode is the crash-taker composition: liveness in the apply chain
// and a takeover judge over it.
type ctoNode struct {
	fold  *obs1.LeaseFold
	gate  *obs1.LeaseGate
	win   *obs1.TailWindow
	live  *obs1.Liveness
	ap    *obs1.ChainAppender
	mgr   *obs1.LeaseManager
	judge *obs1.TakeoverJudge
}

func newCtoNode(s obs1.Store, self uint64, clk *labClock) (*ctoNode, error) {
	fold := obs1.NewLeaseFold()
	gate := obs1.NewLeaseGate(0, 0)
	win, err := obs1.NewTailWindow(fold, fold)
	if err != nil {
		return nil, err
	}
	live, err := obs1.NewLiveness(win, obs1.DefaultLeaseTTL, obs1.DefaultSkewBound, clk.now)
	if err != nil {
		return nil, err
	}
	ap, err := obs1.NewChainAppender(s, "cto", 0, self, 1, obs1.ChainPos{}, live)
	if err != nil {
		return nil, err
	}
	mgr, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{
		Self: self, Appender: ap, Fold: fold, Gate: gate, Now: clk.now,
	})
	if err != nil {
		return nil, err
	}
	return &ctoNode{fold, gate, win, live, ap, mgr, obs1.NewTakeoverJudge(fold, live, 0)}, nil
}

// runTakeoverE2E scores PRED-OBS1-O3A-TAKEOVER on the real machinery:
// each rep the holder flushes a depth-4 WAL tail with commits and goes
// silent, the taker runs the case (b) discipline on the policy clock,
// and the mechanics are timed on the wall clock: the silence probe, the
// Takeover grant, and the cold TakeGroup, fan-8 prewarm over the full
// segment set plus the ranged replay of the retained tail.
func runTakeoverE2E(c cfg) error {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 20260726, Latency: sim.S3Standard})
	n := c.segSweep[len(c.segSweep)-1]
	const depth = 4
	reps := c.warmReps / 2

	// Build phase, not scored: the crashed holder's published segments.
	t0 := time.Now()
	type putJob struct {
		key  string
		body []byte
	}
	jobs := make([]putJob, 0, n)
	for seq := 1; seq <= n; seq++ {
		body, err := buildSegment(uint64(seq), c.recsPerSeg)
		if err != nil {
			return fmt.Errorf("cto build segment %d: %w", seq, err)
		}
		jobs = append(jobs, putJob{segObjKey("cto", uint64(seq)), body})
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
	fmt.Printf("cto_build,segments=%d,wall_s=%.1f\n", n, time.Since(t0).Seconds())

	clk := newLabClock(time.Now())
	a, err := newCtoNode(s, 1, clk)
	if err != nil {
		return err
	}
	b, err := newCtoNode(s, 2, clk)
	if err != nil {
		return err
	}
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 1, Incarnation: 1, Weight: 100}},
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 2, Incarnation: 1, Weight: 100}},
	}); err != nil {
		return err
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		return fmt.Errorf("cto seed acquire: %v %v", won, err)
	}
	if err := b.ap.Follow(ctx); err != nil {
		return err
	}

	nodes := map[uint64]*ctoNode{1: a, 2: b}
	windows := make([]time.Duration, 0, reps)
	holderID, takerID := uint64(1), uint64(2)
	walSeqs := map[uint64]uint64{1: 0, 2: 0}
	lastSeq := uint64(0)
	usageBefore := s.Usage()
	for r := 0; r < reps; r++ {
		holder, taker := nodes[holderID], nodes[takerID]
		floor := lastSeq
		epoch := uint32(r + 1)

		// Off the clock: the holder's last flushes land, the taker's
		// routine poll sees them, then the holder crashes.
		for d := 0; d < depth; d++ {
			walSeqs[holderID]++
			rec, last, err := e2eFlush(ctx, s, "cto", holderID, walSeqs[holderID], epoch, lastSeq+1)
			if err != nil {
				return fmt.Errorf("cto rep %d flush %d: %w", r, d, err)
			}
			lastSeq = last
			if _, err := holder.ap.Append(ctx, []obs1.ChainRecord{rec}); err != nil {
				return fmt.Errorf("cto rep %d commit %d: %w", r, d, err)
			}
		}
		if err := taker.ap.Follow(ctx); err != nil {
			return fmt.Errorf("cto rep %d pre-crash follow: %w", r, err)
		}

		// Policy time: chain-observed staleness, then the full-TTL watch.
		clk.advance(obs1.DefaultLeaseTTL + obs1.DefaultSkewBound + 100*time.Millisecond)
		if taker.judge.Eligible(group, clk.now()) {
			return fmt.Errorf("cto rep %d eligible at first staleness, the watch is broken", r)
		}
		clk.advance(obs1.DefaultLeaseTTL)
		if !taker.judge.Eligible(group, clk.now()) {
			return fmt.Errorf("cto rep %d not eligible after the full discipline", r)
		}

		// On the clock: silence probe, grant, cold rebuild plus replay.
		km, dir := obs1.NewKeymap(), obs1.NewDirectory()
		m := obs1.Manifest{Group: group, FoldSeq: floor}
		for seq := 1; seq <= n; seq++ {
			m.Segs = append(m.Segs, obs1.ManifestSeg{SegSeq: uint64(seq)})
		}
		t := time.Now()
		if err := taker.ap.Follow(ctx); err != nil {
			return fmt.Errorf("cto rep %d probe: %w", r, err)
		}
		won, err := taker.mgr.Takeover(ctx, group)
		if err != nil || !won {
			return fmt.Errorf("cto rep %d takeover: %v %v", r, won, err)
		}
		st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
			Store: s, Prefix: "cto", Group: group, Window: taker.win,
			Manifest: m, HasManifest: true, Dir: dir, Km: km,
		})
		if err != nil {
			return fmt.Errorf("cto rep %d take: %w", r, err)
		}
		windows = append(windows, time.Since(t))

		if st.FramesApplied != uint64(depth*walFrames) || st.Applied != lastSeq {
			return fmt.Errorf("cto rep %d replayed %d frames to %d, want %d to %d", r, st.FramesApplied, st.Applied, depth*walFrames, lastSeq)
		}
		if st.Resident.Segments != n || st.Resident.Records != n*c.recsPerSeg || st.Resident.Tombstones != 0 {
			return fmt.Errorf("cto rep %d rebuilt %+v, want %d segments %d records", r, st.Resident, n, n*c.recsPerSeg)
		}
		// Off the clock: checkpoint trims the window, the crashed node
		// wakes, catches up, and demotes.
		if _, err := taker.ap.Append(ctx, []obs1.ChainRecord{obs1.CheckpointRecord{Pos: taker.fold.Applied()}}); err != nil {
			return fmt.Errorf("cto rep %d checkpoint: %w", r, err)
		}
		if got := taker.win.Retained(); got != 0 {
			return fmt.Errorf("cto rep %d window retains %d after the checkpoint", r, got)
		}
		if err := holder.ap.Follow(ctx); err != nil {
			return fmt.Errorf("cto rep %d wake: %w", r, err)
		}
		holder.mgr.Reconcile()
		if held := holder.mgr.Held(); len(held) != 0 {
			return fmt.Errorf("cto rep %d woken holder still holds %v", r, held)
		}
		wantEpoch := uint32(r + 2)
		for id, nd := range nodes {
			node, e, ok := nd.fold.Holder(group)
			if !ok || node != takerID || e != wantEpoch {
				return fmt.Errorf("cto rep %d node %d sees holder %d epoch %d ok %v, want %d at %d", r, id, node, e, ok, takerID, wantEpoch)
			}
		}
		holderID, takerID = takerID, holderID
	}
	usageAfter := s.Usage()

	// The bill per rep, hard-asserted: on the clock 1 silence probe, n
	// segment GETs, depth ranged WAL section GETs, and the one grant PUT;
	// off the clock the depth WAL PUTs and commit appends, the taker's
	// pre-crash follow (depth batches plus the probe), the checkpoint
	// PUT, and the wake follow (grant and checkpoint batches plus the
	// probe). Nothing else, and nothing that scales past the segment set.
	gets := usageAfter.GetRequests - usageBefore.GetRequests
	puts := usageAfter.PutRequests - usageBefore.PutRequests
	wantGets := int64(reps * (1 + n + depth + (depth + 1) + 3))
	wantPuts := int64(reps * (1 + depth + depth + 1))
	if gets != wantGets || puts != wantPuts {
		return fmt.Errorf("cto billed %d GETs %d PUTs, want %d and %d: an unpriced op leaked into the sequence", gets, puts, wantGets, wantPuts)
	}
	p50, p99 := quantiles(windows)
	policy := obs1.DefaultLeaseTTL*2 + obs1.DefaultSkewBound + 100*time.Millisecond
	fmt.Printf("cto_window,n=%d,fan=%d,depth=%d,reps=%d,p50_ms=%.1f,p99_ms=%.1f,on_clock_gets=%d,on_clock_puts=1,policy_wait_ms=%.0f\n",
		n, obs1.DefaultHandoffFan, depth, len(windows), ms(p50), ms(p99), 1+n+depth, ms(policy))
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
