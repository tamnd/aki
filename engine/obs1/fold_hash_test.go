package obs1_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The hashes slice end to end (spec 2064/obs1 doc 08 section 3): a demoter's
// packed hash chunks fold into a bucket segment, the collection placement
// lands in the keymap, and the field planner serves HGET, HMGET, and HGETALL
// out of the published object. The chunk frames here are built with the same
// codec and coordinate the hash demoter uses (store.ChunkPacker, obs1.Disc,
// big-endian disc bytes), so these tests hold the exact byte stream the
// demote tap emits to the read contract the planner promises.

// kindHash pins the hash record kind byte the way kindString is pinned
// above: chunk kinds are format.
const kindHash = 0x04

type hfield struct {
	name, val string
	exp       uint64
}

// hashChunkFrames packs fields into demoter-shaped chunk frames under key:
// sorted by the fold coordinate, perChunk entries per chunk, each frame's
// disc the big-endian Disc of its first field.
func hashChunkFrames(key string, fields []hfield, perChunk int) []byte {
	sort.Slice(fields, func(i, j int) bool {
		return obs1.Disc([]byte(fields[i].name)) < obs1.Disc([]byte(fields[j].name))
	})
	var buf []byte
	var pk store.ChunkPacker
	for i := 0; i < len(fields); i += perChunk {
		end := i + perChunk
		if end > len(fields) {
			end = len(fields)
		}
		pk.Reset()
		for _, f := range fields[i:end] {
			pk.Add([]byte(f.name), []byte(f.val), f.exp)
		}
		payload, flags := pk.Finish()
		var disc [8]byte
		binary.BigEndian.PutUint64(disc[:], obs1.Disc([]byte(fields[i].name)))
		buf = store.AppendRunChunk(buf, kindHash|store.ChunkKindBit, flags, uint16(pk.Count()), []byte(key), disc[:], payload)
	}
	return buf
}

// fetchFieldWait runs one FetchField and blocks for its completion.
func fetchFieldWait(t *testing.T, cr *obs1.ColdReader, key string, loc obs1.KeyLoc, field string, nowMs int64) (obs1.ColdField, error) {
	t.Helper()
	var (
		cf   obs1.ColdField
		rerr error
		done = make(chan struct{})
	)
	cr.FetchField(3, []byte(key), loc, []byte(field), nowMs, func(f obs1.ColdField, err error) {
		cf, rerr = f, err
		close(done)
	})
	<-done
	return cf, rerr
}

// hashFields builds n distinct fields; every seventh carries an expiry a
// little past now, and one fixed field carries an already-fired one.
func hashFields(n int, nowMs int64) []hfield {
	var out []hfield
	for i := 0; i < n; i++ {
		f := hfield{name: fmt.Sprintf("field%03d", i), val: fmt.Sprintf("value-%03d", i)}
		if i%7 == 0 {
			f.exp = uint64(nowMs + 60_000 + int64(i))
		}
		out = append(out, f)
	}
	out[3].exp = uint64(nowMs - 1) // fired before the read clock
	return out
}

// TestFolderHashFieldReads drives the whole loop: fold the chunks, look the
// collection up in the keymap, and read fields back through the cold reader.
func TestFolderHashFieldReads(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	fields := hashFields(40, nowMs)

	fx.folder.Add(hashChunkFrames("h1", fields, 14))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	led := fx.folder.Ledger()[0]

	// The fold placed the collection: one keymap entry pinning the segment.
	loc, ok := km.Lookup(obs1.Fingerprint([]byte("h1")))
	if !ok || loc.Seg != uint32(led.SegSeq) {
		t.Fatalf("h1 locator %+v ok=%v, want seg %d", loc, ok, led.SegSeq)
	}
	if len(led.Places) != 1 || string(led.Places[0].Key) != "h1" {
		t.Fatalf("ledger places %+v, want one h1 placement", led.Places)
	}

	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: fx.sim,
		Dir:   func(group uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cr.Close)

	for _, f := range fields {
		cf, err := fetchFieldWait(t, cr, "h1", loc, f.name, nowMs)
		if err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		if f.exp != 0 && f.exp <= uint64(nowMs) {
			if cf.Found {
				t.Fatalf("%s found after its inline expiry fired", f.name)
			}
			continue
		}
		if !cf.Found || string(cf.Value) != f.val || cf.Exp != f.exp {
			t.Fatalf("%s = (%q, %d, %v), want (%q, %d)", f.name, cf.Value, cf.Exp, cf.Found, f.val, f.exp)
		}
	}

	// An absent field is a definitive miss, never an error.
	cf, err := fetchFieldWait(t, cr, "h1", loc, "stranger", nowMs)
	if err != nil || cf.Found {
		t.Fatalf("stranger = %+v err %v, want a clean miss", cf, err)
	}
	if st := cr.Stats(); st.Errs != 0 || st.Unresolved != 0 || st.Misses != 2 {
		t.Fatalf("stats %+v, want 2 misses (the fired expiry and the stranger) and no errors", st)
	}
}

// TestDirectoryFieldPlanning holds the request arithmetic without the
// reader: two fields packed into the same chunk resolve to the same block
// ref, which is what lets an HMGET spanning one chunk share a single GET,
// and CollChunks returns every chunk of the collection in disc order for
// the HGETALL walk.
func TestDirectoryFieldPlanning(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	fields := hashFields(40, nowMs)

	fx.folder.Add(hashChunkFrames("h1", fields, 14))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	loc, _ := km.Lookup(obs1.Fingerprint([]byte("h1")))
	fp := obs1.Fingerprint([]byte("h1"))

	// hashChunkFrames sorted the slice, so fields[0] and fields[1] share
	// the first chunk (14 per chunk).
	sort.Slice(fields, func(i, j int) bool {
		return obs1.Disc([]byte(fields[i].name)) < obs1.Disc([]byte(fields[j].name))
	})
	r0, ok0 := dir.ResolveField(loc, fp, obs1.Disc([]byte(fields[0].name)))
	r1, ok1 := dir.ResolveField(loc, fp, obs1.Disc([]byte(fields[1].name)))
	if !ok0 || !ok1 {
		t.Fatal("same-chunk fields did not resolve")
	}
	if r0.ObjKey != r1.ObjKey || r0.Block.Offset != r1.Block.Offset || r0.OffInBlock != r1.OffInBlock {
		t.Fatalf("same-chunk fields plan different refs: %+v vs %+v", r0, r1)
	}
	last, okLast := dir.ResolveField(loc, fp, obs1.Disc([]byte(fields[len(fields)-1].name)))
	if !okLast || last.OffInBlock == r0.OffInBlock && last.Block.Offset == r0.Block.Offset && last.ObjKey == r0.ObjKey {
		t.Fatalf("the last field floored to the first chunk: %+v", last)
	}

	// 40 fields at 14 per chunk is 3 chunks, in disc order.
	refs := dir.CollChunks(loc, fp)
	if len(refs) != 3 {
		t.Fatalf("CollChunks returned %d refs, want 3", len(refs))
	}
	ctx := context.Background()
	live := map[string]string{}
	blocks := map[uint64]bool{}
	for _, ref := range refs {
		blocks[ref.Block.Offset] = true
		off, n := ref.Block.BlockSpan()
		raw, _, err := fx.sim.GetRange(ctx, ref.ObjKey, off, n)
		if err != nil {
			t.Fatal(err)
		}
		data, err := obs1.ParseSegmentBlock(raw, ref.Block)
		if err != nil {
			t.Fatal(err)
		}
		if werr := obs1.WalkColdFields(data, ref.OffInBlock, nowMs, func(p store.PackedPair) error {
			live[string(p.Field)] = string(p.Value)
			return nil
		}); werr != nil {
			t.Fatal(werr)
		}
	}
	wantLive := 0
	for _, f := range fields {
		if f.exp != 0 && f.exp <= uint64(nowMs) {
			continue
		}
		wantLive++
		if live[f.name] != f.val {
			t.Fatalf("HGETALL walk %q = %q, want %q", f.name, live[f.name], f.val)
		}
	}
	if len(live) != wantLive {
		t.Fatalf("HGETALL walk yielded %d fields, want %d live", len(live), wantLive)
	}
	// The GET bill is per distinct block, never per chunk past it: the doc
	// 08 ceil identity has the block count as its ceiling.
	if len(blocks) > len(refs) {
		t.Fatalf("%d distinct blocks exceed %d chunks", len(blocks), len(refs))
	}
}

// TestFolderHashRefoldShadows re-folds the collection and holds the
// newest-segment rule: the keymap placement moves to the new segment, and a
// field read serves the re-folded value.
func TestFolderHashRefoldShadows(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	fields := hashFields(40, nowMs)

	fx.folder.Add(hashChunkFrames("h1", fields, 14))
	fx.folder.Flush()
	waitFor(t, "first publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	firstSeg := uint32(fx.folder.Ledger()[0].SegSeq)

	// Re-demote with one field rewritten, the promote-then-demote cycle's
	// fold view.
	fields[5].val = "rewritten"
	fx.folder.Add(hashChunkFrames("h1", fields, 14))
	fx.folder.Flush()
	waitFor(t, "second publish", func() bool { return len(fx.folder.Ledger()) == 2 })
	secondSeg := uint32(fx.folder.Ledger()[1].SegSeq)

	loc, ok := km.Lookup(obs1.Fingerprint([]byte("h1")))
	if !ok || loc.Seg != secondSeg || loc.Seg == firstSeg {
		t.Fatalf("locator %+v ok=%v, want the re-folded segment %d", loc, ok, secondSeg)
	}

	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: fx.sim,
		Dir:   func(group uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cr.Close)
	cf, err := fetchFieldWait(t, cr, "h1", loc, fields[5].name, nowMs)
	if err != nil || !cf.Found || string(cf.Value) != "rewritten" {
		t.Fatalf("re-folded field = %+v err %v, want rewritten", cf, err)
	}
}

// TestHashMergeShadowSame exercises the doc 08 merge rule with the hash
// identity callback: an overlay write shadows exactly the cold copy of its
// field, a dead overlay claim suppresses its field, and everything else
// passes through in disc order.
func TestHashMergeShadowSame(t *testing.T) {
	mk := func(field, val string, exp uint64, dead bool) obs1.Elem {
		return obs1.Elem{
			Disc: obs1.Disc([]byte(field)),
			Data: obs1.HashElem(nil, []byte(field), []byte(val), exp),
			Dead: dead,
		}
	}
	byDisc := func(es []obs1.Elem) []obs1.Elem {
		sort.Slice(es, func(i, j int) bool { return es[i].Disc < es[j].Disc })
		return es
	}
	cold := byDisc([]obs1.Elem{
		mk("alpha", "a-cold", 0, false),
		mk("beta", "b-cold", 77, false),
		mk("gamma", "g-cold", 0, false),
	})
	overlay := byDisc([]obs1.Elem{
		mk("beta", "b-hot", 0, false),
		mk("gamma", "", 0, true),
	})

	got := map[string]string{}
	if err := obs1.MergeShadow(obs1.SliceIter(cold), obs1.SliceIter(overlay), obs1.HashSame, func(e obs1.Elem) error {
		p, ok := obs1.HashElemPair(e.Data)
		if !ok {
			return fmt.Errorf("merged element does not decode")
		}
		got[string(p.Field)] = string(p.Value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"alpha": "a-cold", "beta": "b-hot"}
	if len(got) != len(want) {
		t.Fatalf("merged view %v, want %v", got, want)
	}
	for f, v := range want {
		if got[f] != v {
			t.Fatalf("merged %q = %q, want %q", f, got[f], v)
		}
	}
}
