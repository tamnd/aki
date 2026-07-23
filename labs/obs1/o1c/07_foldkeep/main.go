// foldkeep measures whether the fold pipeline holds the WAL replay
// floor under sustained design ingest: the real write log, folder, and
// publisher run against the sim while paced ingest streams through, and
// the lab samples the gap between each group's committed watermark and
// the FoldSeq in its latest published manifest. If the gap plateaus the
// replay floor tracks ingest and boot replay work stays bounded; if it
// grows with total ingest the fold is falling behind. Scores
// PRED-OBS1-O1C-FOLDKEEP.
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

type cfg struct {
	payloadBytes int64
	groups       int
	valBytes     int
	flushSize    int
	segTarget    int
	foldAge      time.Duration
	rateMiBs     float64 // 0 means unpaced
	sampleEvery  int64   // payload bytes between samples
}

type sample struct {
	ingested  int64
	maxLag    uint64 // frames above the replay floor, worst group
	meanLag   float64
	published uint64
	segPuts   uint64
}

type rig struct {
	bucket *sim.Sim
	fold   *obs1.LeaseFold
	wl     *obs1.WriteLog
	folder *obs1.Folder
	pub    *obs1.ManifestPublisher
	groups int
	// manifest walk cursors and accumulated manifests per group, so each
	// sample only GETs the slots published since the last one.
	manFrom []uint64
	mans    [][]obs1.Manifest
}

// mapKey mirrors the reqgib route: fnv over the key, group count from
// the run config.
func mapKey(groups int) func(key []byte) (uint16, uint16) {
	return func(key []byte) (uint16, uint16) {
		h := fnv.New32a()
		h.Write(key)
		g := uint16(h.Sum32()) % uint16(groups)
		return g, g
	}
}

func build(c cfg) (*rig, error) {
	ctx := context.Background()
	bucket := sim.New(sim.Config{})
	const node = uint64(0x2F0)
	if err := obs1.CreateRoot(ctx, bucket, "p", false, obs1.Root{
		CreatedMS: 1, G: uint16(c.groups), D: 1,
	}); err != nil {
		return nil, err
	}
	fold := obs1.NewLeaseFold()
	chain, err := obs1.NewChainAppender(bucket, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		return nil, err
	}
	if err := chain.Follow(ctx); err != nil {
		return nil, err
	}
	grants := make([]obs1.ChainRecord, 0, c.groups)
	for g := range uint16(c.groups) {
		grants = append(grants, obs1.GrantRecord{Group: g, Node: node, Epoch: 1})
	}
	if _, err := chain.Append(ctx, grants); err != nil {
		return nil, err
	}
	pub, err := obs1.NewManifestPublisher(obs1.ManPubConfig{
		Store: bucket, Prefix: "p", Node: node,
	})
	if err != nil {
		return nil, err
	}
	route := mapKey(c.groups)
	wl, err := obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: bucket, Prefix: "p", Node: node,
		Chain: chain, Fold: fold,
		Groups: c.groups, MapKey: route,
		FlushSize: c.flushSize,
		FlushAge:  10 * time.Second,
		OnVerdict: pub.OnVerdict,
	})
	if err != nil {
		pub.Close()
		return nil, err
	}
	folder, err := obs1.NewFolder(obs1.FoldConfig{
		Store: bucket, Prefix: "p", Node: node, Incarnation: 1,
		MapKey: route, Mark: wl.GroupMark, Marks: wl.Marks(),
		OnPublish: pub.OnFolded,
		FoldAge:   c.foldAge, SegTargetBytes: c.segTarget,
	})
	if err != nil {
		_ = wl.Close()
		pub.Close()
		return nil, err
	}
	for g := range uint16(c.groups) {
		wl.SetGroup(g, 1, 1)
	}
	return &rig{
		bucket: bucket, fold: fold, wl: wl, folder: folder, pub: pub,
		groups:  c.groups,
		manFrom: make([]uint64, c.groups),
		mans:    make([][]obs1.Manifest, c.groups),
	}, nil
}

// replayFloor walks each group's manifest slots forward from the last
// sample's cursor and returns the FoldSeq of the winning manifest, zero
// while nothing is published yet.
func (r *rig) replayFloor(ctx context.Context, group int) (uint64, error) {
	ms, err := obs1.LoadManifests(ctx, r.bucket, "p", uint16(group), r.manFrom[group])
	if err != nil {
		return 0, err
	}
	if len(ms) > 0 {
		r.mans[group] = append(r.mans[group], ms...)
		r.manFrom[group] = ms[len(ms)-1].ManSeq + 1
	}
	m, ok := obs1.SelectManifest(uint16(group), r.mans[group], r.fold)
	if !ok {
		return 0, nil
	}
	return m.FoldSeq, nil
}

func (r *rig) sample(ctx context.Context, ingested int64) (sample, error) {
	marks := r.wl.Marks()
	var maxLag, sum uint64
	for g := range r.groups {
		floor, err := r.replayFloor(ctx, g)
		if err != nil {
			return sample{}, err
		}
		committed := marks.Committed(uint16(g))
		var lag uint64
		if committed > floor {
			lag = committed - floor
		}
		if lag > maxLag {
			maxLag = lag
		}
		sum += lag
	}
	return sample{
		ingested: ingested, maxLag: maxLag,
		meanLag:   float64(sum) / float64(r.groups),
		published: r.pub.Stats().Published,
		segPuts:   r.folder.Stats().SegmentsPut,
	}, nil
}

func run(c cfg) ([]sample, sample, error) {
	ctx := context.Background()
	r, err := build(c)
	if err != nil {
		return nil, sample{}, err
	}
	defer r.pub.Close()
	defer r.folder.Close()

	val := make([]byte, c.valBytes)
	for i := range val {
		val[i] = byte(i*131 + 7)
	}
	var samples []sample
	var frames []byte
	var ingested, nextSample int64
	nextSample = c.sampleEvery
	start := time.Now()
	for i := 0; ingested < c.payloadBytes; i++ {
		key := fmt.Sprintf("r:%012d", i)
		if _, _, err := r.wl.StrSet([]byte(key), val, 0, false); err != nil {
			_ = r.wl.Close()
			return nil, sample{}, err
		}
		frames = store.AppendRecordFrame(frames, 0x01, 0, uint32(len(val)), []byte(key), val)
		if len(frames) >= 1<<20 {
			r.folder.Add(frames)
			frames = frames[:0]
		}
		ingested += int64(len(key) + len(val))
		if c.rateMiBs > 0 {
			ahead := time.Duration(float64(ingested)/(c.rateMiBs*float64(1<<20))*float64(time.Second)) - time.Since(start)
			if ahead > time.Millisecond {
				time.Sleep(ahead)
			}
		}
		if ingested >= nextSample {
			s, err := r.sample(ctx, ingested)
			if err != nil {
				_ = r.wl.Close()
				return nil, sample{}, err
			}
			samples = append(samples, s)
			nextSample += c.sampleEvery
		}
	}
	if len(frames) > 0 {
		r.folder.Add(frames)
	}

	// Quiesce: barrier, drain the fold, and take the settled floor.
	r.wl.Barrier()
	done := make(chan struct{})
	r.wl.NotifyAllCommitted(func() { close(done) })
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		_ = r.wl.Close()
		return nil, sample{}, fmt.Errorf("commit barrier never fired")
	}
	r.folder.Flush()
	deadline := time.Now().Add(60 * time.Second)
	for {
		fs := r.folder.Stats()
		if fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut && r.pub.Stats().Published >= fs.Published {
			break
		}
		if time.Now().After(deadline) {
			_ = r.wl.Close()
			return nil, sample{}, fmt.Errorf("fold pipeline never quiesced: %+v pub %+v", fs, r.pub.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	final, err := r.sample(ctx, ingested)
	if err != nil {
		_ = r.wl.Close()
		return nil, sample{}, err
	}
	fs := r.folder.Stats()
	ps := r.pub.Stats()
	if err := r.wl.Close(); err != nil {
		return nil, sample{}, err
	}
	if fs.BuildErrs != 0 || fs.WalkErrs != 0 || ps.BuildErrs != 0 || ps.CoverMiss != 0 || ps.RowErrs != 0 {
		return nil, sample{}, fmt.Errorf("pipeline errors: folder %+v pub %+v", fs, ps)
	}
	return samples, final, nil
}

// grew reports whether the lag trend rises across the run: the worst
// lag in the second half of the samples against the worst in the first.
func grew(samples []sample) (firstMax, secondMax uint64, ratio float64) {
	half := len(samples) / 2
	for i, s := range samples {
		if i < half {
			if s.maxLag > firstMax {
				firstMax = s.maxLag
			}
		} else if s.maxLag > secondMax {
			secondMax = s.maxLag
		}
	}
	if firstMax > 0 {
		ratio = float64(secondMax) / float64(firstMax)
	}
	return firstMax, secondMax, ratio
}

func main() {
	gib := flag.Float64("gib", 2.0, "payload GiB to ingest")
	groups := flag.Int("groups", 4, "slot groups")
	valBytes := flag.Int("val", 1000, "value size in bytes")
	rate := flag.Float64("mibs", 100, "ingest pace in MiB/s, 0 for unpaced")
	quick := flag.Bool("quick", false, "smoke run at shrunken constants")
	flag.Parse()

	c := cfg{
		payloadBytes: int64(*gib * float64(1<<30)),
		groups:       *groups,
		valBytes:     *valBytes,
		flushSize:    8 << 20,
		segTarget:    64 << 20,
		foldAge:      500 * time.Millisecond,
		rateMiBs:     *rate,
		sampleEvery:  128 << 20,
	}
	if *quick {
		c.payloadBytes = 128 << 20
		c.flushSize = 1 << 20
		c.segTarget = 4 << 20
		c.foldAge = 50 * time.Millisecond
		c.rateMiBs = 0
		c.sampleEvery = 16 << 20
	}
	samples, final, err := run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("sample,ingested_bytes,max_lag_frames,mean_lag_frames,man_published,seg_puts")
	for i, s := range samples {
		fmt.Printf("%d,%d,%d,%.0f,%d,%d\n", i+1, s.ingested, s.maxLag, s.meanLag, s.published, s.segPuts)
	}
	fmt.Printf("final,%d,%d,%.0f,%d,%d\n", final.ingested, final.maxLag, final.meanLag, final.published, final.segPuts)
	fm, sm, ratio := grew(samples)
	fmt.Printf("trend,first_half_max=%d,second_half_max=%d,ratio=%.2f\n", fm, sm, ratio)
}
