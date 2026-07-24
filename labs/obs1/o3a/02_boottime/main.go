// boottime scores PRED-OBS1-O3A-BOOTTIME: node cold boot to serving vs
// checkpoint distance and manifest count, on the landed boot path against
// the sim with the doc 01 S3 Standard envelope drawn per request. This is
// the lab that bakes the fleet checkpoint cadence: the doc 02 section 2.5
// cadence is stated in records and seconds, but the boot replay term is
// priced per chain OBJECT (one GET each, serial by construction), so the
// constant that matters for boot is how many objects a checkpoint may
// trail the tail by.
//
// Terms, each on the real primitive:
//
//	fixed      BootChain: root GET, checkpoint GET, appender primed
//	replay     Prime + Follow from the checkpoint's Through to the tail,
//	           one GET per chain batch object plus the terminal 404
//	discover   LoadManifests GET-next walk, one GET per manifest + 404
//	rebuild    RebuildResident per group (priced in 01_handofftime),
//	           groups fanned, segments serial within a group
//	wal        the group's WAL-tail object, GET + parse
//
// The composed cell boots a 32-group estate to serving the way the fleet
// slice will: chain boot once, then every group's discover + rebuild +
// wal behind a group fan of 8. Its cost is asserted as an exact op count,
// the accounting form of "boot pays for the estate you own, not the data
// you store": nothing in the walk depends on record count beyond the
// segment objects the manifests name.
//
// Correctness throughout: every booted fold's StateSum must equal the
// builder fold's at the same tail, rebuilt stats are exact per group,
// and WAL frame counts match what was built.
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
	prefix     = "bt"
	kindString = 0x01
	valLen     = 64
	walFrames  = 64
	walPayload = 128
	seedGroups = 8 // groups granted in the seed batch, the checkpoint's content
)

type cfg struct {
	distances  []int // chain objects past the checkpoint
	distReps   []int // reps per distance, aligned
	manifests  []int // manifest-count sweep points
	manReps    int
	bootGroups int // groups in the composed boot
	segsPerGrp int
	recsPerSeg int
	groupFan   int
	compReps   int
}

func fullCfg() cfg {
	return cfg{
		distances:  []int{0, 16, 64, 256},
		distReps:   []int{5, 5, 4, 2},
		manifests:  []int{1, 4, 16},
		manReps:    5,
		bootGroups: 32,
		segsPerGrp: 4,
		recsPerSeg: 1024,
		groupFan:   8,
		compReps:   5,
	}
}

func quickCfg() cfg {
	return cfg{
		distances:  []int{0, 4},
		distReps:   []int{2, 2},
		manifests:  []int{1, 4},
		manReps:    2,
		bootGroups: 4,
		segsPerGrp: 2,
		recsPerSeg: 64,
		groupFan:   2,
		compReps:   2,
	}
}

func segObjKey(group uint16, seq uint64) string {
	return fmt.Sprintf("%s/seg/g%03d/%016d", prefix, group, seq)
}

func groupWALKey(group uint16) string { return fmt.Sprintf("%s/gw/g%03d", prefix, group) }

// buildSegment is the 01_handofftime corpus shape: plain string records
// as run chunks cut the folder's way, keys unique per group and segment.
func buildSegment(group uint16, segSeq uint64, recs int) ([]byte, error) {
	type rec struct {
		fp    uint64
		key   []byte
		frame []byte
	}
	rows := make([]rec, 0, recs)
	for i := 0; i < recs; i++ {
		k := []byte(fmt.Sprintf("g%03dk%05d-%06d", group, segSeq, i))
		v := make([]byte, valLen)
		copy(v, fmt.Sprintf("g%03ds%05d-%06d-", group, segSeq, i))
		for j := range v {
			if v[j] == 0 {
				v[j] = byte('a' + (int(group)+i+j)%26)
			}
		}
		frame := store.AppendRecordFrame(nil, kindString, 0, uint32(len(v)), k, v, 0)
		rows = append(rows, rec{fp: obs1.Fingerprint(k), key: k, frame: frame})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].fp < rows[j].fp })
	var payload []byte
	memberKeys := make([][]byte, 0, recs)
	for i := range rows {
		payload = append(payload, rows[i].frame...)
		memberKeys = append(memberKeys, rows[i].key)
	}
	first := rows[0]
	var disc [8]byte
	for i := 0; i < 8; i++ {
		disc[i] = byte(first.fp >> (56 - 8*i))
	}
	data := store.AppendRunChunk(nil, kindString|store.ChunkKindBit, store.ChunkFlagRun,
		uint16(len(rows)), first.key, disc[:], payload)
	chunks := []obs1.SegmentChunk{{
		Key: first.key, Kind: kindString | store.ChunkKindBit, Flags: store.ChunkFlagRun,
		FirstDisc: first.fp, Count: uint16(len(rows)), LiveHint: uint16(len(rows)),
		Data: data,
	}}
	seg, err := obs1.BuildSegment(obs1.SegmentFooter{Group: group, Epoch: 1, SegSeq: segSeq}, chunks, memberKeys, 0)
	if err != nil {
		return nil, err
	}
	return obs1.AppendSegment(nil, 0xB1, seg)
}

func buildWAL(group uint16) ([]byte, error) {
	frames := make([]obs1.WALFrame, walFrames)
	for i := range frames {
		p := make([]byte, walPayload)
		for j := range p {
			p[j] = byte('a' + (int(group)+i+j)%26)
		}
		frames[i] = obs1.WALFrame{
			Kind: kindString, Slot: group, Seq: uint64(i + 1),
			Key:     []byte(fmt.Sprintf("g%03dw%04d", group, i)),
			Payload: p,
		}
	}
	return obs1.AppendWAL(nil, 1, []obs1.WALSection{{Group: group, Epoch: 1, Frames: frames}})
}

func quantiles(ds []time.Duration) (p50, worst time.Duration) {
	if len(ds) == 0 {
		return 0, 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[len(s)/2], s[len(s)-1]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// bootChainOnce is the measured chain half: BootChain, prime, Follow to
// the tail. It returns the booted fold for the state check.
func bootChainOnce(ctx context.Context, s obs1.Store, writer uint64) (*obs1.LeaseFold, error) {
	fold := obs1.NewLeaseFold()
	ap, ck, err := obs1.BootChain(ctx, s, prefix, false, 0, writer, 1, fold)
	if err != nil {
		return nil, err
	}
	if ck.Through.Seq != 0 {
		if err := fold.Prime(ck); err != nil {
			return nil, err
		}
	}
	if err := ap.Follow(ctx); err != nil {
		return nil, err
	}
	return fold, nil
}

func run(c cfg) error {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 20260725, Latency: sim.S3Standard})

	// Estate build, not scored: root, seeded chain, per-group segments,
	// manifests, and WAL tails, object puts fanned 16 wide.
	t0 := time.Now()
	if err := obs1.CreateRoot(ctx, s, prefix, false, obs1.Root{G: 128, D: 1}); err != nil {
		return err
	}
	builder := obs1.NewLeaseFold()
	ckpter, err := obs1.NewCheckpointer(builder, 1, 3*time.Second, 4096, 4096, time.Minute, time.Now)
	if err != nil {
		return err
	}
	ap, err := obs1.NewChainAppender(s, prefix, 0, 1, 1, obs1.ChainPos{}, ckpter)
	if err != nil {
		return err
	}
	seed := make([]obs1.ChainRecord, 0, seedGroups)
	for g := uint16(1); g <= seedGroups; g++ {
		seed = append(seed, obs1.GrantRecord{Group: g, Node: 1, Epoch: 1})
	}
	if _, err := ap.Append(ctx, seed); err != nil {
		return err
	}

	type putJob struct {
		key  string
		body []byte
	}
	var jobs []putJob
	for g := 0; g < c.bootGroups; g++ {
		group := uint16(200 + g)
		m := obs1.Manifest{Group: group, Epoch: 1, ManSeq: 1}
		for seq := 1; seq <= c.segsPerGrp; seq++ {
			body, err := buildSegment(group, uint64(seq), c.recsPerSeg)
			if err != nil {
				return err
			}
			jobs = append(jobs, putJob{segObjKey(group, uint64(seq)), body})
			m.Segs = append(m.Segs, obs1.ManifestSeg{SegSeq: uint64(seq)})
		}
		if err := obs1.PutManifest(ctx, s, prefix, 1, m); err != nil {
			return err
		}
		wb, err := buildWAL(group)
		if err != nil {
			return err
		}
		jobs = append(jobs, putJob{groupWALKey(group), wb})
	}
	// The manifest-count sweep group: manifests appended one seq at a
	// time between measurement points, each carrying one segment row so
	// discovery parses real payloads; nothing fetches the segments.
	sweepGroup := uint16(77)
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
	fmt.Printf("build,objects=%d,wall_s=%.1f\n", len(jobs)+2, time.Since(t0).Seconds())

	// Checkpoint-distance sweep: before each arm, checkpoint at the
	// current tail, then push the tail out by the arm's distance in
	// heartbeat batches, then boot fresh nodes and clock chain boot.
	for di, dist := range c.distances {
		if _, err := ckpter.WriteCheckpoint(ctx, s, prefix, false, ap); err != nil {
			return err
		}
		for i := 0; i < dist; i++ {
			if _, err := ap.Append(ctx, []obs1.ChainRecord{obs1.HeartbeatRecord{}}); err != nil {
				return err
			}
		}
		wantSum := builder.StateSum()
		durs := make([]time.Duration, 0, c.distReps[di])
		before := s.Usage()
		for rep := 0; rep < c.distReps[di]; rep++ {
			t := time.Now()
			fold, err := bootChainOnce(ctx, s, uint64(1000+100*di+rep))
			if err != nil {
				return fmt.Errorf("distance %d rep %d: %w", dist, rep, err)
			}
			durs = append(durs, time.Since(t))
			if got := fold.StateSum(); got != wantSum {
				return fmt.Errorf("distance %d rep %d: booted StateSum %08x, builder %08x", dist, rep, got, wantSum)
			}
		}
		gets := s.Usage().GetRequests - before.GetRequests
		// Root, checkpoint, the 0x06 record's own batch (it lands after
		// Through, so it always replays), the distance batches, the 404.
		wantGets := int64(c.distReps[di]) * int64(dist+4)
		if gets != wantGets {
			return fmt.Errorf("distance %d billed %d GETs over %d reps, want %d", dist, gets, c.distReps[di], wantGets)
		}
		p50, worst := quantiles(durs)
		fmt.Printf("chain_boot,distance=%d,reps=%d,p50_ms=%.0f,worst_ms=%.0f,gets_per_boot=%d\n",
			dist, c.distReps[di], ms(p50), ms(worst), wantGets/int64(c.distReps[di]))
	}

	// Manifest-count sweep: dense seqs, GET-next discovery, M+1 GETs.
	firstSegRef := obs1.ManifestSeg{SegSeq: 1}
	nextSeq := uint64(1)
	for _, m := range c.manifests {
		for ; nextSeq <= uint64(m); nextSeq++ {
			mm := obs1.Manifest{Group: sweepGroup, Epoch: 1, ManSeq: nextSeq, Segs: []obs1.ManifestSeg{firstSegRef}}
			if err := obs1.PutManifest(ctx, s, prefix, 1, mm); err != nil {
				return err
			}
		}
		durs := make([]time.Duration, 0, c.manReps)
		before := s.Usage()
		for rep := 0; rep < c.manReps; rep++ {
			t := time.Now()
			got, err := obs1.LoadManifests(ctx, s, prefix, sweepGroup, 1)
			if err != nil {
				return err
			}
			durs = append(durs, time.Since(t))
			if len(got) != m {
				return fmt.Errorf("manifest sweep at %d found %d", m, len(got))
			}
		}
		gets := s.Usage().GetRequests - before.GetRequests
		if want := int64(c.manReps) * int64(m+1); gets != want {
			return fmt.Errorf("manifest sweep at %d billed %d GETs, want %d", m, gets, want)
		}
		p50, worst := quantiles(durs)
		fmt.Printf("manifests,count=%d,reps=%d,p50_ms=%.0f,worst_ms=%.0f,gets=%d\n", m, c.manReps, ms(p50), ms(worst), m+1)
	}

	// Composed boot to serving: chain boot at the last sweep's tail
	// distance, then every group's discover + rebuild + WAL replay
	// behind the group fan. One exact op count per boot.
	if _, err := ckpter.WriteCheckpoint(ctx, s, prefix, false, ap); err != nil {
		return err
	}
	const compDist = 16
	for i := 0; i < compDist; i++ {
		if _, err := ap.Append(ctx, []obs1.ChainRecord{obs1.HeartbeatRecord{}}); err != nil {
			return err
		}
	}
	wantSum := builder.StateSum()
	perGroupGets := int64(2 + c.segsPerGrp + 1) // manifest + 404 + segments + wal
	wantBootGets := int64(compDist+4) + int64(c.bootGroups)*perGroupGets
	durs := make([]time.Duration, 0, c.compReps)
	before := s.Usage()
	for rep := 0; rep < c.compReps; rep++ {
		t := time.Now()
		fold, err := bootChainOnce(ctx, s, uint64(5000+rep))
		if err != nil {
			return fmt.Errorf("composed boot rep %d: %w", rep, err)
		}
		gwg := sync.WaitGroup{}
		gerrs := make([]error, c.groupFan)
		groupCh := make(chan uint16)
		for w := 0; w < c.groupFan; w++ {
			gwg.Add(1)
			go func(w int) {
				defer gwg.Done()
				for group := range groupCh {
					mans, err := obs1.LoadManifests(ctx, s, prefix, group, 1)
					if err == nil && len(mans) != 1 {
						err = fmt.Errorf("group %d found %d manifests", group, len(mans))
					}
					if err != nil {
						gerrs[w] = err
						continue
					}
					km, dir := obs1.NewKeymap(), obs1.NewDirectory()
					st, err := obs1.RebuildResident(ctx, s, prefix, mans[0], dir, km)
					if err == nil && (st.Segments != c.segsPerGrp || st.Records != c.segsPerGrp*c.recsPerSeg) {
						err = fmt.Errorf("group %d rebuilt %+v", group, st)
					}
					if err != nil {
						gerrs[w] = err
						continue
					}
					body, _, err := s.Get(ctx, groupWALKey(group))
					if err != nil {
						gerrs[w] = err
						continue
					}
					secs, _, err := obs1.ParseWAL(body)
					if err == nil && (len(secs) != 1 || len(secs[0].Frames) != walFrames) {
						err = fmt.Errorf("group %d wal parsed %d sections", group, len(secs))
					}
					if err != nil {
						gerrs[w] = err
					}
				}
			}(w)
		}
		for g := 0; g < c.bootGroups; g++ {
			groupCh <- uint16(200 + g)
		}
		close(groupCh)
		gwg.Wait()
		for _, err := range gerrs {
			if err != nil {
				return fmt.Errorf("composed boot rep %d: %w", rep, err)
			}
		}
		durs = append(durs, time.Since(t))
		if got := fold.StateSum(); got != wantSum {
			return fmt.Errorf("composed boot rep %d: StateSum %08x, want %08x", rep, got, wantSum)
		}
	}
	gets := s.Usage().GetRequests - before.GetRequests
	if want := int64(c.compReps) * wantBootGets; gets != want {
		return fmt.Errorf("composed boot billed %d GETs over %d reps, want %d", gets, c.compReps, want)
	}
	p50, worst := quantiles(durs)
	fmt.Printf("composed_boot,groups=%d,fan=%d,distance=%d,reps=%d,p50_ms=%.0f,worst_ms=%.0f,gets_per_boot=%d\n",
		c.bootGroups, c.groupFan, compDist, c.compReps, ms(p50), ms(worst), wantBootGets)
	return nil
}

func main() {
	quick := flag.Bool("quick", false, "small estate smoke")
	flag.Parse()
	c := fullCfg()
	if *quick {
		c = quickCfg()
	}
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, "boottime:", err)
		os.Exit(1)
	}
}
