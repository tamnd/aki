// reqgib measures requests per ingested GiB end to end on the simulator:
// the real write log flushes 8 MiB WAL objects and chains commits, the
// real folder cuts 64 MiB segments, and the real publisher CASes
// manifests, all against a counting sim bucket. The doc 09 ledger says
// about 300 requests per GiB at the size-triggered regime and gate CG1
// caps the measurement at 400; this lab scores PRED-OBS1-O1C-REQGIB.
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
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
}

type result struct {
	payloadBytes int64
	puts         int64
	gets         int64
	free         int64
	walFlushes   uint64
	chainBatches uint64
	segPuts      uint64
	published    uint64
	putRetries   uint64
}

func (r result) total() int64 { return r.puts + r.gets + r.free }

func (r result) perGiB() float64 {
	return float64(r.total()) / (float64(r.payloadBytes) / float64(1<<30))
}

// mapKey is the lab's group route: fnv over the key, group count from
// the run config. The ledger arithmetic is group-count-insensitive at
// the size-triggered regime, but segments only reach their size cut
// when per-group ingest does, so the scored run uses few groups.
func mapKey(groups int) func(key []byte) (uint16, uint16) {
	return func(key []byte) (uint16, uint16) {
		h := fnv.New32a()
		h.Write(key)
		g := uint16(h.Sum32()) % uint16(groups)
		return g, g
	}
}

func run(c cfg) (result, error) {
	ctx := context.Background()
	bucket := sim.New(sim.Config{})
	const node = uint64(0x1AB)
	if err := obs1.CreateRoot(ctx, bucket, "p", false, obs1.Root{
		CreatedMS: 1, G: uint16(c.groups), D: 1,
	}); err != nil {
		return result{}, err
	}
	fold := obs1.NewLeaseFold()
	chain, err := obs1.NewChainAppender(bucket, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		return result{}, err
	}
	if err := chain.Follow(ctx); err != nil {
		return result{}, err
	}
	grants := make([]obs1.ChainRecord, 0, c.groups)
	for g := range uint16(c.groups) {
		grants = append(grants, obs1.GrantRecord{Group: g, Node: node, Epoch: 1})
	}
	if _, err := chain.Append(ctx, grants); err != nil {
		return result{}, err
	}
	pub, err := obs1.NewManifestPublisher(obs1.ManPubConfig{
		Store: bucket, Prefix: "p", Node: node,
	})
	if err != nil {
		return result{}, err
	}
	defer pub.Close()
	route := mapKey(c.groups)
	wl, err := obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: bucket, Prefix: "p", Node: node,
		Chain: chain, Fold: fold,
		Groups: c.groups, MapKey: route,
		FlushSize: c.flushSize,
		// The age trigger sits far out so the run stays in the
		// size-triggered regime the ledger prices.
		FlushAge:  10 * time.Second,
		OnVerdict: pub.OnVerdict,
	})
	if err != nil {
		return result{}, err
	}
	folder, err := obs1.NewFolder(obs1.FoldConfig{
		Store: bucket, Prefix: "p", Node: node, Incarnation: 1,
		MapKey: route, Mark: wl.GroupMark, Marks: wl.Marks(),
		OnPublish: pub.OnFolded,
		FoldAge:   c.foldAge, SegTargetBytes: c.segTarget,
	})
	if err != nil {
		_ = wl.Close()
		return result{}, err
	}
	defer folder.Close()
	for g := range uint16(c.groups) {
		wl.SetGroup(g, 1, 1)
	}
	base := bucket.Usage()

	// Ingest: every set also folds, the everything-cools shape, so the
	// run pays the full WAL plus segment ledger on the same bytes.
	val := make([]byte, c.valBytes)
	for i := range val {
		val[i] = byte(i*131 + 7)
	}
	var frames []byte
	var ingested int64
	for i := 0; ingested < c.payloadBytes; i++ {
		key := fmt.Sprintf("r:%012d", i)
		if _, _, err := wl.StrSet([]byte(key), val, 0, false); err != nil {
			_ = wl.Close()
			return result{}, err
		}
		frames = store.AppendRecordFrame(frames, 0x01, 0, uint32(len(val)), []byte(key), val)
		if len(frames) >= 1<<20 {
			folder.Add(frames)
			frames = frames[:0]
		}
		ingested += int64(len(key) + len(val))
	}
	if len(frames) > 0 {
		folder.Add(frames)
	}
	wl.Barrier()
	done := make(chan struct{})
	wl.NotifyAllCommitted(func() { close(done) })
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		_ = wl.Close()
		return result{}, fmt.Errorf("commit barrier never fired")
	}
	folder.Flush()

	// Quiesce: every cut segment put and published, the publisher idle.
	deadline := time.Now().Add(60 * time.Second)
	for {
		fs := folder.Stats()
		if fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut && pub.Stats().Published >= fs.Published {
			break
		}
		if time.Now().After(deadline) {
			_ = wl.Close()
			return result{}, fmt.Errorf("fold pipeline never quiesced: %+v pub %+v", fs, pub.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	info := string(wl.AppendInfo(nil))
	if err := wl.Close(); err != nil {
		return result{}, err
	}

	u := bucket.Usage()
	fs := folder.Stats()
	ps := pub.Stats()
	return result{
		payloadBytes: ingested,
		puts:         u.PutRequests - base.PutRequests,
		gets:         u.GetRequests - base.GetRequests,
		free:         u.FreeRequests - base.FreeRequests,
		walFlushes:   infoRow(info, "wal_flushes"),
		chainBatches: infoRow(info, "chain_commit_batches"),
		segPuts:      fs.SegmentsPut,
		published:    ps.Published,
		putRetries:   fs.PutRetries + ps.PutRetries,
	}, nil
}

// infoRow pulls one counter out of the INFO section text, the write
// log's only stats surface.
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

func main() {
	gib := flag.Float64("gib", 1.0, "payload GiB to ingest")
	groups := flag.Int("groups", 4, "slot groups")
	valBytes := flag.Int("val", 1000, "value size in bytes")
	quick := flag.Bool("quick", false, "smoke run at shrunken constants")
	flag.Parse()

	c := cfg{
		payloadBytes: int64(*gib * float64(1<<30)),
		groups:       *groups,
		valBytes:     *valBytes,
		flushSize:    8 << 20,
		segTarget:    64 << 20,
		foldAge:      500 * time.Millisecond,
	}
	if *quick {
		c.payloadBytes = 32 << 20
		c.flushSize = 1 << 20
		c.segTarget = 4 << 20
	}
	r, err := run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("payload_bytes,puts,gets,free,total,req_per_gib,wal_flushes,chain_batches,seg_puts,man_published,put_retries")
	fmt.Printf("%d,%d,%d,%d,%d,%.1f,%d,%d,%d,%d,%d\n",
		r.payloadBytes, r.puts, r.gets, r.free, r.total(), r.perGiB(),
		r.walFlushes, r.chainBatches, r.segPuts, r.published, r.putRetries)
}
