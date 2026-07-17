package obs1_test

import (
	"context"
	"sync"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// pubFixture is a Folder wired into a ManifestPublisher over the simulator,
// the manifest slice's end-to-end shape: staged frames fold into segments,
// verdicts open the publish gate, and every published segment turns into
// the group's next manifest.
type pubFixture struct {
	sim    *sim.Sim
	marks  *obs1.Watermarks
	folder *obs1.Folder
	pub    *obs1.ManifestPublisher
	last   uint64

	mu   sync.Mutex
	mans []obs1.Manifest
}

func newPubFixture(t *testing.T, seed []obs1.Manifest) *pubFixture {
	t.Helper()
	fx := &pubFixture{sim: sim.New(sim.Config{Seed: 1}), marks: obs1.NewWatermarks()}
	pub, err := obs1.NewManifestPublisher(obs1.ManPubConfig{
		Store: fx.sim, Prefix: "db/t", Node: 0xA1,
		OnManifest: func(m obs1.Manifest) {
			fx.mu.Lock()
			fx.mans = append(fx.mans, m)
			fx.mu.Unlock()
		},
		Seed: seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.pub = pub
	f, err := obs1.NewFolder(obs1.FoldConfig{
		Store:  fx.sim,
		Prefix: "db/t",
		Node:   0xA1,
		MapKey: func(key []byte) (uint16, uint16) { return 9, 3 },
		Mark:   func(group uint16) (uint32, uint64) { return 7, fx.last },
		Marks:  fx.marks,

		OnPublish: pub.OnFolded,
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.folder = f
	t.Cleanup(pub.Close)
	t.Cleanup(f.Close)
	return fx
}

// verdict feeds one live single-section commit verdict through the slice's
// wiring order: the publisher's coverage feed first, then the watermark,
// so the covering position is on file before any publish gate opens.
func (fx *pubFixture) verdict(t *testing.T, lastSeq uint64, pos obs1.ChainPos) {
	t.Helper()
	v := obs1.CommitVerdict{
		Pos: pos, Writer: 0xA1,
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{
			{Group: 3, Epoch: 7, NFrames: 1, FirstSeq: 1, LastSeq: lastSeq},
		}},
		Live: []bool{true},
	}
	if err := fx.pub.OnVerdict(v); err != nil {
		t.Fatal(err)
	}
	if err := fx.marks.ApplyVerdict(v); err != nil {
		t.Fatal(err)
	}
}

// manifests returns the published manifests seen so far, a copy.
func (fx *pubFixture) manifests() []obs1.Manifest {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return append([]obs1.Manifest(nil), fx.mans...)
}

// TestManifestPerFold drives two folds end to end: each publish produces
// the group's next manifest in the dense sequence, complete both times,
// with the fold cursor advanced to the covering verdict.
func TestManifestPerFold(t *testing.T) {
	fx := newPubFixture(t, nil)
	ctx := context.Background()

	fx.last = 5
	fx.folder.Add(frames("k1", "v1", "k2", "v2"))
	fx.folder.Flush()
	fx.verdict(t, 5, obs1.ChainPos{DD: 1, Seq: 41})
	waitFor(t, "first manifest", func() bool { return len(fx.manifests()) == 1 })

	m := fx.manifests()[0]
	if m.Group != 3 || m.Epoch != 7 || m.ManSeq != 0 {
		t.Fatalf("first manifest identity %+v", m)
	}
	if m.FoldSeq != 5 || m.FoldPos != (obs1.ChainPos{DD: 1, Seq: 41}) {
		t.Fatalf("first fold cursor %+v", m)
	}
	led := fx.folder.Ledger()
	if len(m.Segs) != 1 || len(led) != 1 {
		t.Fatalf("first manifest rows %+v ledger %+v", m.Segs, led)
	}
	row, want := m.Segs[0], led[0]
	if row.SegSeq != want.SegSeq || row.Level != 0 || row.TTLClass != 0 ||
		row.Size != uint64(want.Size) || row.NRecords != want.NRecords || row.RawBytes != want.RawBytes ||
		row.FooterOff != want.FooterOff || row.FooterLen != want.FooterLen {
		t.Fatalf("row %+v does not restate ledger entry %+v", row, want)
	}

	fx.last = 9
	fx.folder.Add(frames("k3", "v3"))
	fx.folder.Flush()
	fx.verdict(t, 9, obs1.ChainPos{DD: 1, Seq: 55})
	waitFor(t, "second manifest", func() bool { return len(fx.manifests()) == 2 })

	m = fx.manifests()[1]
	if m.ManSeq != 1 || m.FoldSeq != 9 || m.FoldPos != (obs1.ChainPos{DD: 1, Seq: 55}) {
		t.Fatalf("second manifest %+v", m)
	}
	if len(m.Segs) != 2 || m.Segs[0].SegSeq >= m.Segs[1].SegSeq || m.Segs[0] != fx.manifests()[0].Segs[0] {
		t.Fatalf("second manifest rows %+v", m.Segs)
	}

	// The bucket sequence reads back dense and the reader rule picks the
	// newest manifest.
	ms, err := obs1.LoadManifests(ctx, fx.sim, "db/t", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("loaded %d manifests, want 2", len(ms))
	}
	hist := fakeHistX{7: {DD: 1, Seq: 100}}
	got, ok := obs1.SelectManifest(3, ms, hist)
	if !ok || got.ManSeq != 1 || len(got.Segs) != 2 {
		t.Fatalf("selected %+v ok=%v", got, ok)
	}
	if s := fx.pub.Stats(); s.Published != 2 || s.CoverMiss != 0 || s.RowErrs != 0 || s.SlotSkips != 0 {
		t.Fatalf("stats %+v", s)
	}
}

// TestManifestFoldPosCovering pins the cursor rule when verdicts run ahead
// of the fold: FoldPos is the first verdict whose section reaches the
// fold's covered seq, not the newest one, and FoldSeq keeps the exact
// frame floor when the covering section spans past it, which is the case
// that makes the floor necessary at all.
func TestManifestFoldPosCovering(t *testing.T) {
	posA, posB := obs1.ChainPos{DD: 1, Seq: 100}, obs1.ChainPos{DD: 1, Seq: 200}

	fx := newPubFixture(t, nil)
	fx.verdict(t, 10, posA)
	fx.verdict(t, 20, posB)
	fx.last = 10
	fx.folder.Add(frames("k1", "v1"))
	fx.folder.Flush()
	waitFor(t, "manifest behind two verdicts", func() bool { return len(fx.manifests()) == 1 })
	if m := fx.manifests()[0]; m.FoldSeq != 10 || m.FoldPos != posA {
		t.Fatalf("cursor picked %+v, want fold seq 10 at %+v", m, posA)
	}

	// One wide section spanning the cursor: the chain position alone would
	// re-apply frames 11..20's predecessors, the frame floor says stop at 10.
	fy := newPubFixture(t, nil)
	fy.verdict(t, 20, posB)
	fy.last = 10
	fy.folder.Add(frames("k1", "v1"))
	fy.folder.Flush()
	waitFor(t, "manifest behind a spanning verdict", func() bool { return len(fy.manifests()) == 1 })
	if m := fy.manifests()[0]; m.FoldSeq != 10 || m.FoldPos != posB {
		t.Fatalf("spanning cursor picked %+v, want fold seq 10 at %+v", m, posB)
	}
	if s := fy.pub.Stats(); s.CoverMiss != 0 {
		t.Fatalf("stats %+v", s)
	}
}

// TestManifestSlotCollision seeds the bucket with a prior incarnation's
// manifest at the next slot: the publisher must restate its truth at the
// following slot with the body's ManSeq matching, and the reader rule must
// still pick the newer statement.
func TestManifestSlotCollision(t *testing.T) {
	fx := newPubFixture(t, nil)
	ctx := context.Background()
	if err := obs1.PutManifest(ctx, fx.sim, "db/t", 0xB2, obs1.Manifest{Group: 3, Epoch: 6, ManSeq: 0}); err != nil {
		t.Fatal(err)
	}

	fx.last = 4
	fx.folder.Add(frames("k1", "v1"))
	fx.folder.Flush()
	fx.verdict(t, 4, obs1.ChainPos{DD: 1, Seq: 33})
	waitFor(t, "manifest past the held slot", func() bool { return len(fx.manifests()) == 1 })
	if m := fx.manifests()[0]; m.ManSeq != 1 || len(m.Segs) != 1 {
		t.Fatalf("collision landed %+v", m)
	}
	if s := fx.pub.Stats(); s.SlotSkips != 1 || s.Published != 1 {
		t.Fatalf("stats %+v", s)
	}

	ms, err := obs1.LoadManifests(ctx, fx.sim, "db/t", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[0].Epoch != 6 || ms[1].ManSeq != 1 {
		t.Fatalf("loaded %+v", ms)
	}
	hist := fakeHistX{6: {DD: 1, Seq: 10}, 7: {DD: 1, Seq: 100}}
	got, ok := obs1.SelectManifest(3, ms, hist)
	if !ok || got.ManSeq != 1 {
		t.Fatalf("selected %+v ok=%v", got, ok)
	}
}

// TestManifestTombstoneOnlyFold folds nothing but a delete: the tombstone
// segment still earns a manifest row, since a segment whose only claim is
// absence must still shadow older copies for cold readers.
func TestManifestTombstoneOnlyFold(t *testing.T) {
	fx := newPubFixture(t, nil)
	fx.last = 3
	fx.folder.Delete([]byte("gone"))
	fx.folder.Flush()
	fx.verdict(t, 3, obs1.ChainPos{DD: 1, Seq: 21})
	waitFor(t, "tombstone-only manifest", func() bool { return len(fx.manifests()) == 1 })
	m := fx.manifests()[0]
	if len(m.Segs) != 1 || m.Segs[0].NRecords != 1 || m.FoldSeq != 3 {
		t.Fatalf("tombstone-only manifest %+v", m)
	}
}

// TestManifestSeedContinues boots the publisher from a winning manifest:
// the next statement continues the dense sequence, keeps the seeded row,
// never regresses the cursor, and ignores dead sections in the verdict
// feed. The folder is bypassed because seeding its SegSeq counter is the
// boot recovery slice's work; the publisher hears the fold result directly.
func TestManifestSeedContinues(t *testing.T) {
	seedRow := obs1.ManifestSeg{SegSeq: 5, Size: 4096, NRecords: 40, RawBytes: 900, FooterOff: 3900, FooterLen: 180}
	seed := obs1.Manifest{Group: 3, Epoch: 7, ManSeq: 4, FoldPos: obs1.ChainPos{DD: 1, Seq: 30}, FoldSeq: 7, Segs: []obs1.ManifestSeg{seedRow}}
	fx := newPubFixture(t, []obs1.Manifest{seed})

	dead := obs1.CommitVerdict{
		Pos: obs1.ChainPos{DD: 1, Seq: 90}, Writer: 0xB2,
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{
			{Group: 3, Epoch: 6, NFrames: 1, FirstSeq: 1, LastSeq: 100},
		}},
		Live: []bool{false},
	}
	if err := fx.pub.OnVerdict(dead); err != nil {
		t.Fatal(err)
	}
	fx.verdict(t, 9, obs1.ChainPos{DD: 1, Seq: 60})
	fx.pub.OnFolded(obs1.FoldedSegment{
		Group: 3, Epoch: 7, SegSeq: 6, Key: "db/t/seg/g003/0000000000000006",
		Size: 2048, FooterOff: 1900, FooterLen: 148, NRecords: 12, RawBytes: 300, CoveredSeq: 9,
	})
	waitFor(t, "seeded manifest", func() bool { return len(fx.manifests()) == 1 })

	m := fx.manifests()[0]
	if m.ManSeq != 5 || m.FoldSeq != 9 || m.FoldPos != (obs1.ChainPos{DD: 1, Seq: 60}) {
		t.Fatalf("seeded manifest %+v", m)
	}
	if len(m.Segs) != 2 || m.Segs[0] != seedRow || m.Segs[1].SegSeq != 6 {
		t.Fatalf("seeded rows %+v", m.Segs)
	}
	if _, _, err := fx.sim.Get(context.Background(), "db/t/man/g003/0000000000000005"); err != nil {
		t.Fatalf("manifest not at the continued slot: %v", err)
	}

	// A stale ledger row regressing SegSeq is a data error, dropped loudly.
	fx.pub.OnFolded(obs1.FoldedSegment{Group: 3, Epoch: 7, SegSeq: 6, CoveredSeq: 9})
	waitFor(t, "row error", func() bool { return fx.pub.Stats().RowErrs == 1 })
	if s := fx.pub.Stats(); s.Published != 1 || s.CoverMiss != 0 {
		t.Fatalf("stats %+v", s)
	}
}

// fakeHistX mirrors manifest_test.go's fakeHist for the external test
// package: each epoch maps to the last chain position where it held the
// group's lease.
type fakeHistX map[uint32]obs1.ChainPos

func (h fakeHistX) EpochCurrentAtOrAfter(_ uint16, epoch uint32, from obs1.ChainPos) bool {
	last, ok := h[epoch]
	return ok && !last.Before(from)
}
