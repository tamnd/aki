package obs1_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

// kindString is the store's string record kind byte, pinned here the way
// the format tests pin wire bytes: a fold segment's chunk kinds are format,
// so a silent renumber must fail a test.
const kindString = 0x01

// foldFixture is a Folder over the simulator with a fixed group route:
// every key maps to group 3, the mark returns epoch 7 and the seq the test
// sets, and the watermark surface is a fresh one the test advances.
type foldFixture struct {
	sim    *sim.Sim
	marks  *obs1.Watermarks
	folder *obs1.Folder
	last   uint64
}

func newFoldFixture(t *testing.T, chunkTarget int) *foldFixture {
	t.Helper()
	fx := &foldFixture{sim: sim.New(sim.Config{Seed: 1}), marks: obs1.NewWatermarks()}
	f, err := obs1.NewFolder(obs1.FoldConfig{
		Store:  fx.sim,
		Prefix: "db/t",
		Node:   0xA1,
		MapKey: func(key []byte) (uint16, uint16) { return 9, 3 },
		Mark:   func(group uint16) (uint32, uint64) { return 7, fx.last },
		Marks:  fx.marks,

		ChunkTargetBytes: chunkTarget,
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.folder = f
	t.Cleanup(f.Close)
	return fx
}

// frames builds a staged buffer of embedded string records.
func frames(kv ...string) []byte {
	if len(kv)%2 != 0 {
		panic("frames wants key value pairs")
	}
	var buf []byte
	for i := 0; i < len(kv); i += 2 {
		buf = store.AppendRecordFrame(buf, kindString, 0, uint32(len(kv[i+1])), []byte(kv[i]), []byte(kv[i+1]))
	}
	return buf
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// segRecords parses a segment object and returns every whole record in it
// by unpacking each run chunk's payload, the tombstone keys from tombstone
// run chunks, plus the raw collection chunks.
func segRecords(t *testing.T, obj []byte) (map[string]string, map[string]bool, []store.FoldFrame) {
	t.Helper()
	seg, _, err := obs1.ParseSegment(obj)
	if err != nil {
		t.Fatalf("parse segment: %v", err)
	}
	recs := make(map[string]string)
	tombs := make(map[string]bool)
	var colls []store.FoldFrame
	for _, e := range seg.Footer.Chunks {
		data := seg.BlockData[e.Block][e.OffInBlock:]
		total := binary.LittleEndian.Uint32(data[0:4])
		var outer store.FoldFrame
		if err := store.WalkStagedFrames(data[:total], func(f store.FoldFrame) error {
			outer = f
			return nil
		}); err != nil {
			t.Fatalf("chunk at block %d off %d did not parse: %v", e.Block, e.OffInBlock, err)
		}
		if outer.Kind != e.Kind || outer.Count != e.Count {
			t.Fatalf("chunk entry disagrees with its frame: entry %+v frame kind 0x%02x count %d", e, outer.Kind, outer.Count)
		}
		switch outer.Kind {
		case kindString | store.ChunkKindBit:
			if werr := store.WalkStagedFrames(outer.Payload, func(r store.FoldFrame) error {
				recs[string(r.Key)] = string(r.Payload)
				return nil
			}); werr != nil {
				t.Fatalf("run payload walk: %v", werr)
			}
		case store.KindTombstone | store.ChunkKindBit:
			if !outer.Tombstone {
				t.Fatalf("tombstone run chunk not classified: %+v", outer)
			}
			if werr := store.WalkStagedFrames(outer.Payload, func(r store.FoldFrame) error {
				if !r.Tombstone || len(r.Payload) != 0 {
					t.Fatalf("tombstone frame misread: %+v", r)
				}
				tombs[string(r.Key)] = true
				return nil
			}); werr != nil {
				t.Fatalf("tombstone payload walk: %v", werr)
			}
		default:
			colls = append(colls, outer)
		}
	}
	return recs, tombs, colls
}

// TestFolderSegmentRoundTrip drives the whole slice on the simulator: a
// staged buffer of records plus one collection chunk folds into a segment,
// the segment lands under the doc 03 key, publishes at once (the mark is
// already covered), and the object round-trips: whole-object parse, every
// record at its staged value, the collection chunk verbatim, and the
// ledger's footer fields opening the footer by ranged read (#1102).
func TestFolderSegmentRoundTrip(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 64) // small run target so the ten records span chunks

	buf := frames(
		"k0", "v0", "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4",
		"k5", "v5", "k6", "v6", "k7", "v7", "k8", "v8", "k9", "v9",
	)
	buf = store.AppendRunChunk(buf, 0x03|store.ChunkKindBit, 0, 7, []byte("coll"), []byte("disc8bYt"), []byte("packed-blob"))
	fx.folder.Add(buf)
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	led := fx.folder.Ledger()[0]
	wantKey := "db/t/seg/g003/0000000000000001"
	if led.Key != wantKey || led.Group != 3 || led.Epoch != 7 || led.SegSeq != 1 || led.NRecords != 17 {
		t.Fatalf("ledger row %+v", led)
	}
	obj, _, err := fx.sim.Get(ctx, wantKey)
	if err != nil {
		t.Fatalf("get segment: %v", err)
	}
	if int64(len(obj)) != led.Size {
		t.Fatalf("object is %d bytes, ledger says %d", len(obj), led.Size)
	}

	recs, _, colls := segRecords(t, obj)
	if len(recs) != 10 {
		t.Fatalf("segment holds %d records, want 10", len(recs))
	}
	for i := range 10 {
		k := fmt.Sprintf("k%d", i)
		if recs[k] != fmt.Sprintf("v%d", i) {
			t.Fatalf("record %s = %q", k, recs[k])
		}
	}
	if len(colls) != 1 || colls[0].Count != 7 || string(colls[0].Key) != "coll" || string(colls[0].Payload) != "packed-blob" {
		t.Fatalf("collection chunk did not pass through: %+v", colls)
	}

	// The #1102 cold open: tail, then one ranged GET of the footer.
	tail, _, err := fx.sim.GetTail(ctx, wantKey, obs1.TailSize)
	if err != nil {
		t.Fatalf("get tail: %v", err)
	}
	footerOff, footerLen, err := obs1.ParseTail(tail)
	if err != nil {
		t.Fatalf("parse tail: %v", err)
	}
	if footerOff != led.FooterOff || footerLen != led.FooterLen {
		t.Fatalf("ledger footer (%d,%d) vs tail (%d,%d)", led.FooterOff, led.FooterLen, footerOff, footerLen)
	}
	fslice, _, err := fx.sim.GetRange(ctx, wantKey, int64(footerOff), int64(footerLen))
	if err != nil {
		t.Fatalf("ranged footer get: %v", err)
	}
	footer, err := obs1.ParseSegmentFooter(fslice)
	if err != nil {
		t.Fatalf("parse ranged footer: %v", err)
	}
	if footer.Group != 3 || footer.Epoch != 7 || footer.SegSeq != 1 || footer.Level != 0 || footer.TTLClass != 0 {
		t.Fatalf("ranged footer %+v", footer)
	}
	st := fx.folder.Stats()
	if st.Records != 10 || st.Chunks != 1 || st.SegmentsCut != 1 || st.SegmentsPut != 1 || st.Published != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// TestFolderWatermarkGate pins eligibility: a segment whose mark is ahead
// of the committed watermark PUTs eagerly but stays out of the ledger
// until a commit verdict covers the mark.
func TestFolderWatermarkGate(t *testing.T) {
	fx := newFoldFixture(t, 0)
	fx.last = 42

	fx.folder.Add(frames("k", "v"))
	fx.folder.Flush()
	waitFor(t, "put", func() bool { return fx.folder.Stats().SegmentsPut == 1 })
	if got := fx.folder.Ledger(); len(got) != 0 {
		t.Fatalf("published %d segments before the watermark covered seq 42", len(got))
	}

	if err := fx.marks.ApplyVerdict(obs1.CommitVerdict{
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{{Group: 3, Epoch: 7, FirstSeq: 1, LastSeq: 42}}},
		Live:   []bool{true},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "gated publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	if led := fx.folder.Ledger()[0]; led.CoveredSeq != 42 {
		t.Fatalf("ledger covered seq %d, want 42", led.CoveredSeq)
	}
}

// TestFolderSegSeqCollision pins the CAS-create policy: a seq slot held by
// another writer (a prior incarnation's segment) advances the folder past
// it rather than fencing it out, and the next cut keeps counting from
// there.
func TestFolderSegSeqCollision(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 0)
	if _, err := fx.sim.PutIfAbsent(ctx, "db/t/seg/g003/0000000000000001", []byte("held"),
		obs1.WriteTag{Writer: "another-node", Batch: "b1"}); err != nil {
		t.Fatal(err)
	}

	fx.folder.Add(frames("k1", "v1"))
	fx.folder.Flush()
	waitFor(t, "first publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	if led := fx.folder.Ledger()[0]; led.SegSeq != 2 || led.Key != "db/t/seg/g003/0000000000000002" {
		t.Fatalf("collision landed at %+v", led)
	}

	fx.folder.Add(frames("k2", "v2"))
	fx.folder.Flush()
	waitFor(t, "second publish", func() bool { return len(fx.folder.Ledger()) == 2 })
	if led := fx.folder.Ledger()[1]; led.SegSeq != 3 {
		t.Fatalf("post-collision cut landed at seq %d, want 3", led.SegSeq)
	}
}

// TestFolderDedupeRestaged pins the accumulator's identity rule: a record
// staged twice before the cut (a failed pwrite re-stages it) folds once,
// at its newest state.
func TestFolderDedupeRestaged(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 0)

	fx.folder.Add(frames("k", "old", "other", "x"))
	fx.folder.Add(frames("k", "new"))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	obj, _, err := fx.sim.Get(ctx, fx.folder.Ledger()[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	recs, _, _ := segRecords(t, obj)
	if len(recs) != 2 || recs["k"] != "new" || recs["other"] != "x" {
		t.Fatalf("deduped records %v", recs)
	}
	if st := fx.folder.Stats(); st.Records != 2 || st.Replaced != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// TestFolderTombstoneInterleave pins the within-segment story of doc 06
// section 1.3: a delete displaces the pending copy of its key, a re-set
// displaces a pending tombstone, an unseen key's delete still emits (no
// keymap wired here, so emission stays unfiltered), the tombstones pack into
// their own run chunk, and the segment publishes only once the watermark
// covers the marks the deletes were taken under.
func TestFolderTombstoneInterleave(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 0)
	fx.last = 5

	fx.folder.Add(frames("k1", "v1", "k2", "v2"))
	fx.folder.Delete([]byte("k1"))
	fx.folder.Delete([]byte("k3"))
	fx.folder.Add(frames("k3", "v3"))
	fx.folder.Flush()

	waitFor(t, "put", func() bool { return fx.folder.Stats().SegmentsPut == 1 })
	if got := fx.folder.Ledger(); len(got) != 0 {
		t.Fatalf("published %d segments before the watermark covered the delete marks", len(got))
	}
	if err := fx.marks.ApplyVerdict(obs1.CommitVerdict{
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{{Group: 3, Epoch: 7, FirstSeq: 1, LastSeq: 5}}},
		Live:   []bool{true},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	led := fx.folder.Ledger()[0]
	if led.CoveredSeq != 5 || led.NRecords != 3 {
		t.Fatalf("ledger row %+v", led)
	}
	obj, _, err := fx.sim.Get(ctx, led.Key)
	if err != nil {
		t.Fatal(err)
	}
	recs, tombs, _ := segRecords(t, obj)
	if len(recs) != 2 || recs["k2"] != "v2" || recs["k3"] != "v3" {
		t.Fatalf("records %v", recs)
	}
	if len(tombs) != 1 || !tombs["k1"] {
		t.Fatalf("tombstones %v", tombs)
	}
	st := fx.folder.Stats()
	if st.Records != 2 || st.Tombstones != 2 || st.Replaced != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestFolderCrossSegmentShadow drives the shadowing rule across three
// published segments: value, tombstone, re-set. The claims resolve by
// SegSeq alone, order-independent: through the second segment the key is
// absent, through the third it holds the newest value.
func TestFolderCrossSegmentShadow(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 0)

	fx.folder.Add(frames("k", "v1"))
	fx.folder.Flush()
	waitFor(t, "first publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	fx.folder.Delete([]byte("k"))
	fx.folder.Flush()
	waitFor(t, "second publish", func() bool { return len(fx.folder.Ledger()) == 2 })
	fx.folder.Add(frames("k", "v2"))
	fx.folder.Flush()
	waitFor(t, "third publish", func() bool { return len(fx.folder.Ledger()) == 3 })

	var claims []obs1.ShadowEntry
	for _, led := range fx.folder.Ledger() {
		obj, _, err := fx.sim.Get(ctx, led.Key)
		if err != nil {
			t.Fatalf("get %s: %v", led.Key, err)
		}
		recs, tombs, _ := segRecords(t, obj)
		switch {
		case tombs["k"]:
			claims = append(claims, obs1.ShadowEntry{SegSeq: led.SegSeq, Tombstone: true})
		case recs["k"] != "":
			claims = append(claims, obs1.ShadowEntry{SegSeq: led.SegSeq, Frame: []byte(recs["k"])})
		default:
			t.Fatalf("segment %d holds no claim about k", led.SegSeq)
		}
	}
	if len(claims) != 3 || claims[0].SegSeq != 1 || claims[1].SegSeq != 2 || claims[2].SegSeq != 3 {
		t.Fatalf("claims %+v", claims)
	}

	if got, ok := obs1.ResolveShadow(claims[:2]); ok {
		t.Fatalf("k resolved to %q through the tombstone", got)
	}
	// Order independence: the newest claim wins from either end.
	rev := []obs1.ShadowEntry{claims[2], claims[0], claims[1]}
	got, ok := obs1.ResolveShadow(rev)
	if !ok || string(got) != "v2" {
		t.Fatalf("k resolved to %q %v, want v2 true", got, ok)
	}
	if _, ok := obs1.ResolveShadow(nil); ok {
		t.Fatal("no claims resolved to present")
	}
}

// TestFolderRealDrainRoundTrip is the integration half: a real store under
// resident pressure feeds the folder through the fold tap while its drains
// complete normally, and every record the segments hold reads back at the
// value the store was given. This is the slice's proof that the fold
// consumes the migrator's actual bytes, not a synthetic imitation.
func TestFolderRealDrainRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := store.Open(store.Options{
		ArenaBytes:       16 << 20,
		SegBytes:         256 << 10,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	fx := newFoldFixture(t, 0)
	s.SetFoldTap(fx.folder.Add)

	const n = 30000
	for i := range n {
		k := fmt.Appendf(nil, "k:%07d", i)
		v := fmt.Appendf(nil, "v-%d", i)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if !s.NeedsColdDrain() {
		t.Fatal("fixture did not cross the cap")
	}
	buf := make([]byte, 0, 256<<10)
	for pass := 0; pass < 64 && s.NeedsColdDrain(); pass++ {
		d := s.StageColdDrain(buf)
		if d == nil || len(d.Buf()) == 0 {
			break
		}
		if _, werr := s.ColdWriteAt(d.Off(), d.Buf()); werr != nil {
			t.Fatalf("cold write: %v", werr)
		}
		s.CompleteColdDrain(d, true)
	}
	fx.folder.Flush()
	waitFor(t, "publish", func() bool {
		st := fx.folder.Stats()
		return st.SegmentsCut > 0 && uint64(len(fx.folder.Ledger())) == st.SegmentsCut
	})

	folded := 0
	for _, led := range fx.folder.Ledger() {
		obj, _, gerr := fx.sim.Get(ctx, led.Key)
		if gerr != nil {
			t.Fatalf("get %s: %v", led.Key, gerr)
		}
		recs, _, _ := segRecords(t, obj)
		for k, v := range recs {
			var i int
			if _, serr := fmt.Sscanf(k, "k:%d", &i); serr != nil {
				t.Fatalf("unexpected folded key %q", k)
			}
			if want := fmt.Sprintf("v-%d", i); v != want {
				t.Fatalf("folded %s = %q, want %q", k, v, want)
			}
			folded++
		}
	}
	st := fx.folder.Stats()
	if uint64(folded) != st.Records {
		t.Fatalf("segments hold %d records, folder accumulated %d", folded, st.Records)
	}
	if folded == 0 {
		t.Fatal("the drain folded nothing")
	}
	if st.WalkErrs != 0 || st.BuildErrs != 0 || st.NoEpoch != 0 {
		t.Fatalf("fold errors: %+v", st)
	}
}

// TestFoldAgeCadence pins the doc 06 section 1.4 age trigger: bytes far
// below the segment target cut on the folder's own cadence, no explicit
// Flush, and publish once the watermark covers them.
func TestFoldAgeCadence(t *testing.T) {
	fx := &foldFixture{sim: sim.New(sim.Config{Seed: 1}), marks: obs1.NewWatermarks()}
	f, err := obs1.NewFolder(obs1.FoldConfig{
		Store:  fx.sim,
		Prefix: "db/t",
		Node:   0xA1,
		MapKey: func(key []byte) (uint16, uint16) { return 9, 3 },
		Mark:   func(group uint16) (uint32, uint64) { return 7, fx.last },
		Marks:  fx.marks,

		FoldAge: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.folder = f
	t.Cleanup(f.Close)

	fx.last = 5
	if err := fx.marks.ApplyVerdict(obs1.CommitVerdict{
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{{Group: 3, Epoch: 7, FirstSeq: 1, LastSeq: 5}}},
		Live:   []bool{true},
	}); err != nil {
		t.Fatal(err)
	}
	f.Add(frames("aged", "v"))
	waitFor(t, "aged cut to publish", func() bool { return len(f.Ledger()) == 1 })
	led := f.Ledger()[0]
	if led.CoveredSeq != 5 || led.NRecords != 1 {
		t.Fatalf("aged segment = %+v", led)
	}
	if st := f.Stats(); st.SegmentsCut != 1 {
		t.Fatalf("cuts = %d, want 1 (the age trigger, not the size one)", st.SegmentsCut)
	}
}

func TestFolderSeedContinuesSegSeq(t *testing.T) {
	fx := &foldFixture{sim: sim.New(sim.Config{Seed: 1}), marks: obs1.NewWatermarks()}
	f, err := obs1.NewFolder(obs1.FoldConfig{
		Store:  fx.sim,
		Prefix: "db/t",
		Node:   0xA1,
		MapKey: func(key []byte) (uint16, uint16) { return 9, 3 },
		Mark:   func(group uint16) (uint32, uint64) { return 7, fx.last },
		Marks:  fx.marks,

		FoldAge: -1,
		Seed: []obs1.Manifest{{
			Group: 3, Epoch: 7, ManSeq: 2, FoldSeq: 4,
			Segs: []obs1.ManifestSeg{{SegSeq: 2}, {SegSeq: 5}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.folder = f
	t.Cleanup(f.Close)

	fx.last = 6
	if err := fx.marks.ApplyVerdict(obs1.CommitVerdict{
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{{Group: 3, Epoch: 7, FirstSeq: 1, LastSeq: 6}}},
		Live:   []bool{true},
	}); err != nil {
		t.Fatal(err)
	}
	f.Add(frames("seeded", "v"))
	f.Flush()
	waitFor(t, "seeded cut to publish", func() bool { return len(f.Ledger()) == 1 })
	if led := f.Ledger()[0]; led.SegSeq != 6 {
		t.Fatalf("seeded segment took SegSeq %d, want 6, one past the manifest's highest row", led.SegSeq)
	}
}

// newFoldKeymapFixture is the fold fixture with a single group 3 keymap
// wired in, the maintenance slice's shape.
func newFoldKeymapFixture(t *testing.T) (*foldFixture, *obs1.Keymap) {
	t.Helper()
	fx := &foldFixture{sim: sim.New(sim.Config{Seed: 1}), marks: obs1.NewWatermarks()}
	km := obs1.NewKeymap()
	f, err := obs1.NewFolder(obs1.FoldConfig{
		Store:  fx.sim,
		Prefix: "db/t",
		Node:   0xA1,
		MapKey: func(key []byte) (uint16, uint16) { return 9, 3 },
		Mark:   func(group uint16) (uint32, uint64) { return 7, fx.last },
		Marks:  fx.marks,
		Keymap: func(group uint16) *obs1.Keymap { return km },
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.folder = f
	t.Cleanup(f.Close)
	return fx, km
}

// TestFolderKeymapMaintains drives the fold, delete, re-fold cycle and
// checks the keymap tracks it: a publish inserts each record's locator,
// an apply-time delete drops the key, the tombstone segment leaves it
// absent, and a re-fold points it at the newer segment.
func TestFolderKeymapMaintains(t *testing.T) {
	fx, km := newFoldKeymapFixture(t)
	fp1, fp2 := obs1.Fingerprint([]byte("k1")), obs1.Fingerprint([]byte("k2"))

	fx.folder.Add(frames("k1", "v1", "k2", "v2"))
	fx.folder.Flush()
	waitFor(t, "first publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	led := fx.folder.Ledger()[0]
	if len(led.Places) != 2 {
		t.Fatalf("places %+v", led.Places)
	}
	for _, fp := range []uint64{fp1, fp2} {
		got, ok := km.Lookup(fp)
		if !ok || got.Seg != uint32(led.SegSeq) {
			t.Fatalf("fp %d got %+v ok=%v want seg %d", fp, got, ok, led.SegSeq)
		}
	}

	fx.folder.Delete([]byte("k1"))
	if _, ok := km.Lookup(fp1); ok {
		t.Fatal("k1 still in the keymap after its apply-time delete")
	}
	if _, ok := km.Lookup(fp2); !ok {
		t.Fatal("k2 vanished with k1's delete")
	}
	fx.folder.Flush()
	waitFor(t, "tombstone publish", func() bool { return len(fx.folder.Ledger()) == 2 })
	if _, ok := km.Lookup(fp1); ok {
		t.Fatal("the tombstone's own publish resurrected k1")
	}
	tled := fx.folder.Ledger()[1]
	if len(tled.Places) != 1 || !tled.Places[0].Tombstone {
		t.Fatalf("tombstone segment places %+v", tled.Places)
	}

	fx.folder.Add(frames("k1", "v2"))
	fx.folder.Flush()
	waitFor(t, "re-fold publish", func() bool { return len(fx.folder.Ledger()) == 3 })
	got, ok := km.Lookup(fp1)
	if !ok || got.Seg != uint32(fx.folder.Ledger()[2].SegSeq) {
		t.Fatalf("re-folded k1 got %+v ok=%v", got, ok)
	}
	st := fx.folder.Stats()
	if st.PlacesApplied != 3 || st.PlacesKilled != 0 || st.PlaceErrs != 0 || st.Tombstones != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// TestFolderTombstoneFilter pins the never-folded filter: a delete of a
// key with no cold copy anywhere emits nothing, a delete of a key whose
// only copy sits in the live accumulator just removes it, and a delete
// of a folded key still emits its tombstone.
func TestFolderTombstoneFilter(t *testing.T) {
	fx, km := newFoldKeymapFixture(t)

	fx.folder.Delete([]byte("never-seen"))
	fx.folder.Add(frames("staged", "v"))
	fx.folder.Delete([]byte("staged"))
	fx.folder.Flush()
	st := fx.folder.Stats()
	if st.SegmentsCut != 0 {
		t.Fatalf("empty accumulator cut a segment: %+v", st)
	}
	if st.Tombstones != 0 || st.TombstonesSkipped != 2 {
		t.Fatalf("filter stats %+v", st)
	}

	fx.folder.Add(frames("folded", "v"))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	fx.folder.Delete([]byte("folded"))
	fx.folder.Flush()
	waitFor(t, "tombstone publish", func() bool { return len(fx.folder.Ledger()) == 2 })
	if _, ok := km.Lookup(obs1.Fingerprint([]byte("folded"))); ok {
		t.Fatal("deleted folded key still resolves")
	}
	if st := fx.folder.Stats(); st.Tombstones != 1 || st.TombstonesSkipped != 2 {
		t.Fatalf("post-fold stats %+v", st)
	}
}

// TestFolderDeleteKillsInFlightPlace closes the delete-versus-publish
// race: a key's segment is cut and PUT but the watermark holds its
// publish; the delete lands in between; the publish must not resurrect
// the key in the keymap, and the tombstone still reaches the bucket.
func TestFolderDeleteKillsInFlightPlace(t *testing.T) {
	fx, km := newFoldKeymapFixture(t)
	fx.last = 7
	fp := obs1.Fingerprint([]byte("k"))

	fx.folder.Add(frames("k", "v"))
	fx.folder.Flush()
	waitFor(t, "put", func() bool { return fx.folder.Stats().SegmentsPut == 1 })
	fx.folder.Delete([]byte("k"))

	if err := fx.marks.ApplyVerdict(obs1.CommitVerdict{
		Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{{Group: 3, Epoch: 7, FirstSeq: 1, LastSeq: 7}}},
		Live:   []bool{true},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "gated publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	if _, ok := km.Lookup(fp); ok {
		t.Fatal("the gated publish resurrected a deleted key")
	}
	if led := fx.folder.Ledger()[0]; len(led.Places) != 0 {
		t.Fatalf("killed place survived into the ledger row: %+v", led.Places)
	}

	fx.folder.Flush()
	waitFor(t, "tombstone publish", func() bool { return len(fx.folder.Ledger()) == 2 })
	if _, ok := km.Lookup(fp); ok {
		t.Fatal("keymap holds k after the tombstone publish")
	}
	st := fx.folder.Stats()
	if st.PlacesKilled != 1 || st.PlacesApplied != 0 || st.Tombstones != 1 {
		t.Fatalf("stats %+v", st)
	}
}
