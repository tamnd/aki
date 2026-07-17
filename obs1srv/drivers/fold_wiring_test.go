package drivers

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// foldedServer is the full O1c cold pipeline over the sim store, the
// composition the doc 06 server wiring names: manifest publisher on the
// verdict feed in front of the watermarks, folder on the fold tap and the
// keydel feed, publisher on the folder's publish seam, and the fold
// progress and pressure seams into the runtime.
type foldedServer struct {
	wl     *obs1.WriteLog
	folder *obs1.Folder
	pub    *obs1.ManifestPublisher
	store  *sim.Sim
	nc     net.Conn
	r      *bufio.Reader
	taps   atomic.Uint64
}

func startFoldedServer(t *testing.T, residentCap uint64) *foldedServer {
	t.Helper()
	const node = uint64(0xE1)
	fs := &foldedServer{store: sim.New(sim.Config{})}
	fold := obs1.NewLeaseFold()
	ap, err := obs1.NewChainAppender(fs.store, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		t.Fatal(err)
	}
	recs := make([]obs1.ChainRecord, 0, shard.DefaultSlotGroups)
	for g := range shard.DefaultSlotGroups {
		recs = append(recs, obs1.GrantRecord{Group: uint16(g), Node: node, Epoch: 1})
	}
	if _, err := ap.Append(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	fs.pub, err = obs1.NewManifestPublisher(obs1.ManPubConfig{
		Store: fs.store, Prefix: "p", Node: node,
	})
	if err != nil {
		t.Fatal(err)
	}
	fs.wl, err = obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: fs.store, Prefix: "p", Node: node, Chain: ap, Fold: fold,
		Groups: shard.DefaultSlotGroups, MapKey: ClusterMapKey,
		FlushAge:  5 * time.Millisecond,
		OnVerdict: fs.pub.OnVerdict,
	})
	if err != nil {
		t.Fatal(err)
	}
	fs.folder, err = obs1.NewFolder(obs1.FoldConfig{
		Store: fs.store, Prefix: "p", Node: node,
		MapKey:    ClusterMapKey,
		Mark:      fs.wl.GroupMark,
		Marks:     fs.wl.Marks(),
		OnPublish: fs.pub.OnFolded,
		FoldAge:   20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	fs.wl.SetKeyDelFeed(fs.folder.Delete)
	o := Options{
		Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 16 << 20, SegBytes: 1 << 18,
		ConnShape: testConnShape(), NetDriver: testNetDriver(),
		WriteLog: fs.wl, WALInfo: fs.wl.AppendInfo,
		FoldTap: func(frames []byte) {
			fs.taps.Add(1)
			fs.folder.Add(frames)
		},
		FoldProgress: func() uint64 { return fs.pub.Stats().Published },
		FoldKick:     fs.folder.Flush,
	}
	if residentCap > 0 {
		dir := t.TempDir()
		o.VlogDir = dir
		o.ColdDir = dir
		o.ResidentCapBytes = residentCap
	}
	srv, err := Listen(o)
	if err != nil {
		t.Fatal(err)
	}
	for g := range shard.DefaultSlotGroups {
		fs.wl.SetGroup(uint16(g), 1, 1)
	}
	go srv.Serve()
	fs.nc, err = net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	fs.r = bufio.NewReader(fs.nc)
	t.Cleanup(func() {
		fs.nc.Close()
		srv.Close()
		if err := fs.wl.Close(); err != nil {
			t.Errorf("write log close: %v", err)
		}
		fs.folder.Close()
		fs.pub.Close()
	})
	return fs
}

// pollFor is waitFor's local twin: the cold pipeline runs on flush, fold,
// and publish goroutines, so the assertions poll.
func pollFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestFoldWiringDeleteToManifest drives one DEL over the socket all the way
// to a manifest on the store: the keydel feed hands the folder a tombstone,
// the age cadence cuts it without any explicit Flush, the verdict feed has
// the covering position on file before the watermark opens the publish
// gate (CoverMiss zero is that ordering), and the publisher states the
// group's truth with the fold cursor at the delete's seq.
func TestFoldWiringDeleteToManifest(t *testing.T) {
	fs := startFoldedServer(t, 0)
	send(t, fs.nc, "SET", "k", "v")
	expect(t, fs.r, "+OK\r\n")
	send(t, fs.nc, "DEL", "k")
	expect(t, fs.r, ":1\r\n")

	_, group := ClusterMapKey([]byte("k"))
	ctx := context.Background()
	var mans []obs1.Manifest
	pollFor(t, "the delete's manifest", func() bool {
		var err error
		mans, err = obs1.LoadManifests(ctx, fs.store, "p", group, 0)
		if err != nil {
			t.Fatalf("LoadManifests: %v", err)
		}
		return len(mans) > 0
	})
	m := mans[len(mans)-1]
	if m.Epoch != 1 || m.FoldSeq != 2 {
		t.Fatalf("manifest = %+v, want epoch 1 fold seq 2 (SET then DEL)", m)
	}
	if m.FoldPos == (obs1.ChainPos{}) {
		t.Fatalf("manifest carries a zero fold position: %+v", m)
	}
	if len(m.Segs) != 1 || m.Segs[0].NRecords != 1 || m.Segs[0].FooterLen == 0 {
		t.Fatalf("manifest rows = %+v, want one tombstone-bearing segment", m.Segs)
	}
	if st := fs.folder.Stats(); st.Tombstones != 1 || st.SegmentsCut != 1 {
		t.Fatalf("folder stats = %+v, want one tombstone in one aged cut", st)
	}
	if st := fs.pub.Stats(); st.CoverMiss != 0 || st.RowErrs != 0 || st.BuildErrs != 0 {
		t.Fatalf("publisher stats = %+v, want a clean publish", st)
	}
	led := fs.folder.Ledger()
	if len(led) != 1 || led[0].CoveredSeq != 2 || led[0].SegSeq != m.Segs[0].SegSeq {
		t.Fatalf("ledger = %+v, disagreeing with the manifest row", led)
	}
}

// TestFoldWiringTapHearsDrains proves the Options fold tap is the staged
// drain feed on a serving node: a working set of embedded records past the
// resident cap makes the migrator stage cold drains, and every one of
// those buffers reaches the folder through the tap. Separated values spill
// straight to the value log at write time, which is not a drain, so the
// workload here is many small records, the foldtap_test shape driven over
// the socket, pipelined in batches to keep the round trips off the clock.
func TestFoldWiringTapHearsDrains(t *testing.T) {
	fs := startFoldedServer(t, 64<<10)
	const keys, batch = 6000, 500
	for base := 0; base < keys; base += batch {
		for i := base; i < base+batch; i++ {
			send(t, fs.nc, "SET", "d:"+strconv.Itoa(i), "v-"+strconv.Itoa(i))
		}
		for range batch {
			expect(t, fs.r, "+OK\r\n")
		}
	}
	pollFor(t, "the tap to hear a staged drain", func() bool { return fs.taps.Load() > 0 })
	pollFor(t, "the folder to accumulate records", func() bool {
		st := fs.folder.Stats()
		return st.Records > 0 && st.WalkErrs == 0 && st.NoEpoch == 0
	})
	// And the loop closes: the age cadence cuts the drained records into
	// segments and the publisher states them, record-bearing manifests on
	// the store with no coverage misses.
	pollFor(t, "a record-bearing manifest", func() bool { return fs.pub.Stats().Published > 0 })
	if st := fs.pub.Stats(); st.CoverMiss != 0 || st.RowErrs != 0 || st.BuildErrs != 0 {
		t.Fatalf("publisher stats = %+v, want a clean publish", st)
	}
	led := fs.folder.Ledger()
	if len(led) == 0 || led[0].NRecords == 0 {
		t.Fatalf("ledger = %+v, want record-bearing segments", led)
	}
}
