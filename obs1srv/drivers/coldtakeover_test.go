package drivers

// The read half of the O1c async cold read row in its real serving shape:
// a node boots against a bucket that already holds root, grants, segments,
// and manifests but no WAL objects, which is exactly what a takeover or a
// trimmed WAL leaves behind. Nothing replays, the keymap rebuilds from the
// winning manifests, and every GET must park through the cold plan and
// come back off the bucket.

import (
	"context"
	"strconv"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

// seedFoldedBucket writes a previous holder's durable estate: the root,
// an epoch-1 grant for every group, and one published fold of the given
// keys, all under the same node id the booting server will claim.
func seedFoldedBucket(t *testing.T, bucket *sim.Sim, node uint64, kv map[string]string) {
	t.Helper()
	ctx := context.Background()
	if err := obs1.CreateRoot(ctx, bucket, "p", false, obs1.Root{
		CreatedMS: 1, G: shard.DefaultSlotGroups, D: 1,
	}); err != nil {
		t.Fatal(err)
	}

	fold := obs1.NewLeaseFold()
	chain, err := obs1.NewChainAppender(bucket, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		t.Fatal(err)
	}
	if err := chain.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	grants := make([]obs1.ChainRecord, 0, shard.DefaultSlotGroups)
	for g := range uint16(shard.DefaultSlotGroups) {
		grants = append(grants, obs1.GrantRecord{Group: g, Node: node, Epoch: 1})
	}
	pos, err := chain.Append(ctx, grants)
	if err != nil {
		t.Fatal(err)
	}

	// One group, one fold, one covering verdict fed straight to the
	// publisher and the watermark; the commit record itself stays off the
	// chain because there is no WAL object behind it, which is the point.
	var group uint16
	var last uint64
	for k := range kv {
		_, group = ClusterMapKey([]byte(k))
		break
	}
	last = uint64(len(kv))
	marks := obs1.NewWatermarks()
	pub, err := obs1.NewManifestPublisher(obs1.ManPubConfig{
		Store: bucket, Prefix: "p", Node: node,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	folder, err := obs1.NewFolder(obs1.FoldConfig{
		Store: bucket, Prefix: "p", Node: node,
		MapKey:    ClusterMapKey,
		Mark:      func(uint16) (uint32, uint64) { return 1, last },
		Marks:     marks,
		OnPublish: pub.OnFolded,
		FoldAge:   -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer folder.Close()

	var buf []byte
	nframes := uint32(0)
	for k, v := range kv {
		buf = store.AppendRecordFrame(buf, 0x01, 0, uint32(len(v)), []byte(k), []byte(v))
		nframes++
	}
	folder.Add(buf)
	folder.Flush()
	v := obs1.CommitVerdict{
		Pos: pos, Writer: node,
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{
			{Group: group, Epoch: 1, NFrames: nframes, FirstSeq: 1, LastSeq: last},
		}},
		Live: []bool{true},
	}
	if err := pub.OnVerdict(v); err != nil {
		t.Fatal(err)
	}
	if err := marks.ApplyVerdict(v); err != nil {
		t.Fatal(err)
	}
	pollFor(t, "the seeded fold to publish", func() bool {
		return pub.Stats().Published >= 1
	})
}

func TestColdServeTakeover(t *testing.T) {
	// Sixty-four keys from one slot group, so a single fold and a single
	// covering section describe them all.
	kv := map[string]string{}
	_, want := ClusterMapKey([]byte("t:0"))
	for i := 0; len(kv) < 64; i++ {
		k := "t:" + strconv.Itoa(i)
		if _, g := ClusterMapKey([]byte(k)); g == want {
			kv[k] = "w-" + strconv.Itoa(i)
		}
	}

	bucket := sim.New(sim.Config{})
	seedFoldedBucket(t, bucket, 0xE9, kv)

	b, srv, nc, r := bootColdServer(t, bucket, 2)
	if b.Replay.Frames != 0 {
		t.Fatalf("replayed %d frames from a WAL-less bucket", b.Replay.Frames)
	}
	if b.Resident.Records != len(kv) {
		t.Fatalf("rebuilt %d records, want %d", b.Resident.Records, len(kv))
	}
	for k, v := range kv {
		send(t, nc, "GET", k)
		expect(t, r, "$"+strconv.Itoa(len(v))+"\r\n"+v+"\r\n")
	}
	st := b.Cold.Stats()
	if st.Fetches != uint64(len(kv)) || st.BlockGETs == 0 {
		t.Fatalf("cold stats %+v, want every GET served off the bucket", st)
	}
	if st.Errs != 0 || st.Misses != 0 || st.Unresolved != 0 {
		t.Fatalf("cold stats %+v, want a clean sweep", st)
	}
	t.Logf("takeover sweep: %+v over %d keys", st, len(kv))
	commitAndStop(t, b, srv, nc)
}
