// foldload asks whether the fold pipeline perturbs the WAL flush
// cadence or the frame overhead, and what folding costs the ingest
// loop: the same rig as reqgib runs twice per configuration, once with
// the folder consuming the record stream and once without, on identical
// ingest. The WAL-side counters must not move between arms and the wall
// cost of the fold-on arm is the in-process tax the K1 kill line bounds.
// Scores PRED-OBS1-O1C-FOLDLOAD.
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strings"
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
	withFold     bool
}

type arm struct {
	ops          int64
	payload      int64
	wallNs       int64 // ingest loop only, first StrSet to last
	rateMiBs     float64
	walFlushes   uint64
	walBytes     uint64
	chainBatches uint64
	segPuts      uint64
	published    uint64
}

func (a arm) nsPerOp() float64 { return float64(a.wallNs) / float64(a.ops) }

// achievedMiBs is the ingest rate the loop realized.
func (a arm) achievedMiBs() float64 {
	return float64(a.payload) / float64(1<<20) / (float64(a.wallNs) / float64(time.Second))
}

// overheadPerOp is the WAL bytes each op adds beyond its key and value.
func (a arm) overheadPerOp() float64 {
	return float64(int64(a.walBytes)-a.payload) / float64(a.ops)
}

func mapKey(groups int) func(key []byte) (uint16, uint16) {
	return func(key []byte) (uint16, uint16) {
		h := fnv.New32a()
		h.Write(key)
		g := uint16(h.Sum32()) % uint16(groups)
		return g, g
	}
}

func run(c cfg) (arm, error) {
	ctx := context.Background()
	bucket := sim.New(sim.Config{})
	const node = uint64(0x8F1)
	if err := obs1.CreateRoot(ctx, bucket, "p", false, obs1.Root{
		CreatedMS: 1, G: uint16(c.groups), D: 1,
	}); err != nil {
		return arm{}, err
	}
	fold := obs1.NewLeaseFold()
	chain, err := obs1.NewChainAppender(bucket, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		return arm{}, err
	}
	if err := chain.Follow(ctx); err != nil {
		return arm{}, err
	}
	grants := make([]obs1.ChainRecord, 0, c.groups)
	for g := range uint16(c.groups) {
		grants = append(grants, obs1.GrantRecord{Group: g, Node: node, Epoch: 1})
	}
	if _, err := chain.Append(ctx, grants); err != nil {
		return arm{}, err
	}
	pub, err := obs1.NewManifestPublisher(obs1.ManPubConfig{
		Store: bucket, Prefix: "p", Node: node,
	})
	if err != nil {
		return arm{}, err
	}
	defer pub.Close()
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
		return arm{}, err
	}
	var folder *obs1.Folder
	if c.withFold {
		folder, err = obs1.NewFolder(obs1.FoldConfig{
			Store: bucket, Prefix: "p", Node: node, Incarnation: 1,
			MapKey: route, Mark: wl.GroupMark, Marks: wl.Marks(),
			OnPublish: pub.OnFolded,
			FoldAge:   c.foldAge, SegTargetBytes: c.segTarget,
		})
		if err != nil {
			_ = wl.Close()
			return arm{}, err
		}
		defer folder.Close()
	}
	for g := range uint16(c.groups) {
		wl.SetGroup(g, 1, 1)
	}

	val := make([]byte, c.valBytes)
	for i := range val {
		val[i] = byte(i*131 + 7)
	}
	var frames []byte
	var ingested int64
	var ops int64
	start := time.Now()
	for i := 0; ingested < c.payloadBytes; i++ {
		key := fmt.Sprintf("r:%012d", i)
		if _, _, err := wl.StrSet([]byte(key), val, 0, false); err != nil {
			_ = wl.Close()
			return arm{}, err
		}
		if c.withFold {
			frames = store.AppendRecordFrame(frames, 0x01, 0, uint32(len(val)), []byte(key), val, 0)
			if len(frames) >= 1<<20 {
				folder.Add(frames)
				frames = frames[:0]
			}
		}
		ingested += int64(len(key) + len(val))
		ops++
		if c.rateMiBs > 0 {
			ahead := time.Duration(float64(ingested)/(c.rateMiBs*float64(1<<20))*float64(time.Second)) - time.Since(start)
			if ahead > time.Millisecond {
				time.Sleep(ahead)
			}
		}
	}
	wallNs := time.Since(start).Nanoseconds()
	if c.withFold && len(frames) > 0 {
		folder.Add(frames)
	}

	wl.Barrier()
	done := make(chan struct{})
	wl.NotifyAllCommitted(func() { close(done) })
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		_ = wl.Close()
		return arm{}, fmt.Errorf("commit barrier never fired")
	}
	a := arm{ops: ops, payload: ingested, wallNs: wallNs, rateMiBs: c.rateMiBs}
	if c.withFold {
		folder.Flush()
		deadline := time.Now().Add(60 * time.Second)
		for {
			fs := folder.Stats()
			if fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut && pub.Stats().Published >= fs.Published {
				break
			}
			if time.Now().After(deadline) {
				_ = wl.Close()
				return arm{}, fmt.Errorf("fold pipeline never quiesced: %+v pub %+v", fs, pub.Stats())
			}
			time.Sleep(5 * time.Millisecond)
		}
		fs := folder.Stats()
		ps := pub.Stats()
		if fs.BuildErrs != 0 || fs.WalkErrs != 0 || ps.BuildErrs != 0 || ps.CoverMiss != 0 || ps.RowErrs != 0 {
			_ = wl.Close()
			return arm{}, fmt.Errorf("pipeline errors: folder %+v pub %+v", fs, ps)
		}
		a.segPuts = fs.SegmentsPut
		a.published = ps.Published
	}
	info := string(wl.AppendInfo(nil))
	if err := wl.Close(); err != nil {
		return arm{}, err
	}
	a.walFlushes = infoRow(info, "wal_flushes")
	a.walBytes = infoRow(info, "wal_flushed_bytes")
	a.chainBatches = infoRow(info, "chain_commit_batches")
	return a, nil
}

func infoRow(info, name string) uint64 {
	i := strings.Index(info, name+":")
	if i < 0 {
		return 0
	}
	var v uint64
	if _, err := fmt.Sscanf(info[i+len(name)+1:], "%d", &v); err != nil {
		return 0
	}
	return v
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func main() {
	mib := flag.Float64("mib", 512, "payload MiB to ingest per arm per rep")
	groups := flag.Int("groups", 4, "slot groups")
	reps := flag.Int("reps", 3, "alternating repetitions per arm")
	pace := flag.Float64("mibs", 100, "design ingest pace in MiB/s for the paced pair")
	quick := flag.Bool("quick", false, "smoke run at shrunken constants")
	flag.Parse()

	c := cfg{
		payloadBytes: int64(*mib * float64(1<<20)),
		groups:       *groups,
		flushSize:    8 << 20,
		segTarget:    64 << 20,
		foldAge:      500 * time.Millisecond,
	}
	nreps := *reps
	if *quick {
		c.payloadBytes = 32 << 20
		c.flushSize = 1 << 20
		c.segTarget = 4 << 20
		c.foldAge = 50 * time.Millisecond
		nreps = 1
	}

	do := func(valBytes int, rate float64, withFold bool, rep int) arm {
		c.valBytes = valBytes
		c.rateMiBs = rate
		c.withFold = withFold
		a, err := run(c)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		name := "off"
		if withFold {
			name = "on"
		}
		mode := "sat"
		if rate > 0 {
			mode = "paced"
		}
		fmt.Printf("%d,%s,%s,%d,%d,%.1f,%.1f,%d,%d,%.1f,%d,%d,%d\n",
			valBytes, mode, name, rep, a.ops, a.nsPerOp(), a.achievedMiBs(), a.walFlushes, a.walBytes,
			a.overheadPerOp(), a.chainBatches, a.segPuts, a.published)
		return a
	}

	fmt.Println("val_bytes,mode,arm,rep,ops,ns_per_op,achieved_mibs,wal_flushes,wal_flushed_bytes,overhead_b_per_op,chain_batches,seg_puts,man_published")
	for _, valBytes := range []int{200, 1000} {
		// Saturated pairs: the CPU-bound ceiling with and without the fold
		// consuming the stream, arms alternating within every rep so a lead
		// arm absorbs any drift alone.
		offNs := make([]float64, 0, nreps)
		onNs := make([]float64, 0, nreps)
		var off, on arm
		for rep := range nreps {
			off = do(valBytes, 0, false, rep+1)
			offNs = append(offNs, off.nsPerOp())
			on = do(valBytes, 0, true, rep+1)
			onNs = append(onNs, on.nsPerOp())
		}
		offMed, onMed := median(offNs), median(onNs)
		fmt.Printf("summary_sat,%d,off_ns=%.1f,on_ns=%.1f,added_ns=%.1f,ceiling_ratio=%.2f,flushes_off=%d,flushes_on=%d,overhead_off=%.1f,overhead_on=%.1f,mean_obj_mib_off=%.2f,mean_obj_mib_on=%.2f\n",
			valBytes, offMed, onMed, onMed-offMed, offMed/onMed,
			off.walFlushes, on.walFlushes, off.overheadPerOp(), on.overheadPerOp(),
			float64(off.walBytes)/float64(off.walFlushes)/float64(1<<20),
			float64(on.walBytes)/float64(on.walFlushes)/float64(1<<20))

		// The paced pair at design rate: the cadence claim lives here. The
		// fold-on arm must hold the pace and the WAL-side counters must
		// match the fold-off arm.
		poff := do(valBytes, *pace, false, 1)
		pon := do(valBytes, *pace, true, 1)
		fmt.Printf("summary_paced,%d,target_mibs=%.0f,off_mibs=%.1f,on_mibs=%.1f,flushes_off=%d,flushes_on=%d,overhead_off=%.1f,overhead_on=%.1f,mean_obj_mib_off=%.2f,mean_obj_mib_on=%.2f\n",
			valBytes, *pace, poff.achievedMiBs(), pon.achievedMiBs(),
			poff.walFlushes, pon.walFlushes, poff.overheadPerOp(), pon.overheadPerOp(),
			float64(poff.walBytes)/float64(poff.walFlushes)/float64(1<<20),
			float64(pon.walBytes)/float64(pon.walFlushes)/float64(1<<20))
	}
}
