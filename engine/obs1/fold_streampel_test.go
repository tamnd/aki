package obs1_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The stream PEL projection and the trim manifest drop over a folded
// segment (spec 2064/obs1 doc 08 section 7): a group's pending entries
// fold as kindStreamPel chunks under the stream key, one per (group,
// demoted block), discs leading with the group's tag so the plan filters
// by group in RAM; an approximate XTRIM's whole-block drop folds as a
// zero-count chunk under the dropped run's own disc, which the folder
// replaces by identity and the planners skip without a GET.

// kindStreamPelChunk mirrors the stream package's kindStreamPel.
const kindStreamPelChunk = 0x07

type pelFixEnt struct {
	id         obs1.StreamRunID
	consumer   string
	deliveries uint16
	delivered  uint64
}

// pelChunkFrame packs one (group, block) PEL chunk the way the demote
// emission does: packed pairs of 16-byte big-endian IDs against the
// delivery facts, under the 24-byte (tag, ms, seq) disc.
func pelChunkFrame(key string, tag uint64, first obs1.StreamRunID, ents []pelFixEnt) []byte {
	var pk store.ChunkPacker
	var idb [16]byte
	for _, e := range ents {
		binary.BigEndian.PutUint64(idb[0:], e.id.Ms)
		binary.BigEndian.PutUint64(idb[8:], e.id.Seq)
		val := make([]byte, 10, 10+len(e.consumer))
		binary.BigEndian.PutUint64(val[0:], e.delivered)
		binary.BigEndian.PutUint16(val[8:], e.deliveries)
		val = append(val, e.consumer...)
		pk.Add(idb[:], val, 0)
	}
	payload, flags := pk.Finish()
	var disc [24]byte
	binary.BigEndian.PutUint64(disc[0:], tag)
	binary.BigEndian.PutUint64(disc[8:], first.Ms)
	binary.BigEndian.PutUint64(disc[16:], first.Seq)
	return store.AppendRunChunk(nil, kindStreamPelChunk|store.ChunkKindBit, flags, uint16(len(ents)), []byte(key), disc[:], payload)
}

func TestFolderStreamPelPlan(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	entries, _ := streamFixture()

	// The entry runs and two groups' PEL chunks fold under the same key:
	// group g pends across two blocks (one entry ownerless, the claim
	// moment), group h across one.
	gTag := obs1.StreamPelTag([]byte("g"))
	hTag := obs1.StreamPelTag([]byte("h"))
	gWant := [][]pelFixEnt{
		{
			{id: entries[2].id, consumer: "c1", deliveries: 1, delivered: 111},
			{id: entries[7].id, consumer: "c2", deliveries: 3, delivered: 222},
		},
		{
			{id: entries[20].id, deliveries: 0, delivered: 0},
			{id: entries[30].id, consumer: "c1", deliveries: 2, delivered: 333},
		},
	}
	hWant := [][]pelFixEnt{
		{{id: entries[5].id, consumer: "hc", deliveries: 1, delivered: 444}},
	}
	buf := streamRunFrames("sp", entries, 16)
	buf = append(buf, pelChunkFrame("sp", gTag, entries[0].id, gWant[0])...)
	buf = append(buf, pelChunkFrame("sp", gTag, entries[16].id, gWant[1])...)
	buf = append(buf, pelChunkFrame("sp", hTag, entries[0].id, hWant[0])...)
	fx.folder.Add(buf)
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("sp"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("sp has no locator")
	}

	// The kind-restricted plans keep the planes apart: 4 entry runs, 3 PEL
	// chunks, no crossover.
	if refs := dir.CollChunksKind(loc, fp, uint8(kindStreamChunk|store.ChunkKindBit)); len(refs) != 4 {
		t.Fatalf("entry plan holds %d runs, want 4", len(refs))
	}
	pelKind := uint8(kindStreamPelChunk | store.ChunkKindBit)
	refs := dir.CollChunksKind(loc, fp, pelKind)
	if len(refs) != 3 {
		t.Fatalf("pel plan holds %d chunks, want 3", len(refs))
	}
	for i, r := range refs {
		if r.ChunkKind != pelKind {
			t.Fatalf("pel ref %d kind 0x%02x crossed the kind-restricted plan", i, r.ChunkKind)
		}
	}

	fetch, _ := zsetRunFetcher(t, fx)
	for _, tc := range []struct {
		group string
		tag   uint64
		want  [][]pelFixEnt
	}{
		{group: "g", tag: gTag, want: gWant},
		{group: "h", tag: hTag, want: hWant},
	} {
		gr := obs1.StreamPelRefs(refs, tc.tag)
		if len(gr) != len(tc.want) {
			t.Fatalf("group %s filtered to %d chunks, want %d", tc.group, len(gr), len(tc.want))
		}
		var flat []pelFixEnt
		for _, chunk := range tc.want {
			flat = append(flat, chunk...)
		}
		var serr error
		it := obs1.StreamPelIter(gr, fetch, nil, &serr)
		var got []obs1.StreamPelEntry
		for {
			e, ok := it()
			if !ok {
				break
			}
			got = append(got, e)
		}
		if serr != nil {
			t.Fatalf("group %s stream: %v", tc.group, serr)
		}
		if len(got) != len(flat) {
			t.Fatalf("group %s yielded %d entries, want %d", tc.group, len(got), len(flat))
		}
		for i, e := range got {
			w := flat[i]
			if e.ID != w.id || !bytes.Equal(e.Consumer, []byte(w.consumer)) || e.Deliveries != w.deliveries || e.DeliveredMs != w.delivered {
				t.Fatalf("group %s entry %d = %+v, want %+v", tc.group, i, e, w)
			}
		}
	}
}

// TestFolderStreamPelGuards holds the PEL iterator to its misfile and
// interleave guards: a 16-byte disc is the entry-run shape not the PEL's,
// a pair whose field is not the 16-byte ID pair is a misfile, and a
// backward ID across a group's chunks is the partition interleave error.
func TestFolderStreamPelGuards(t *testing.T) {
	tag := obs1.StreamPelTag([]byte("g"))

	fold := func(t *testing.T, key string, frames []byte) ([]obs1.DirRef, func(obs1.DirRef) ([]byte, error)) {
		t.Helper()
		fx, km, dir := newFoldDirFixture(t)
		fx.folder.Add(frames)
		fx.folder.Flush()
		waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
		fp := obs1.Fingerprint([]byte(key))
		loc, ok := km.Lookup(fp)
		if !ok {
			t.Fatalf("%s has no locator", key)
		}
		refs := dir.CollChunksKind(loc, fp, uint8(kindStreamPelChunk|store.ChunkKindBit))
		fetch, _ := zsetRunFetcher(t, fx)
		return refs, fetch
	}
	drain := func(refs []obs1.DirRef, fetch func(obs1.DirRef) ([]byte, error)) error {
		var serr error
		it := obs1.StreamPelIter(refs, fetch, nil, &serr)
		for {
			if _, ok := it(); !ok {
				break
			}
		}
		return serr
	}

	t.Run("short disc", func(t *testing.T) {
		var pk store.ChunkPacker
		var idb [16]byte
		binary.BigEndian.PutUint64(idb[0:], 9)
		pk.Add(idb[:], make([]byte, 10), 0)
		payload, flags := pk.Finish()
		var disc [16]byte
		binary.BigEndian.PutUint64(disc[0:], tag)
		binary.BigEndian.PutUint64(disc[8:], 9)
		frames := store.AppendRunChunk(nil, kindStreamPelChunk|store.ChunkKindBit, flags, 1, []byte("pa"), disc[:], payload)
		refs, fetch := fold(t, "pa", frames)
		if err := drain(refs, fetch); err == nil {
			t.Fatal("short-disc pel chunk streamed, want the misfile guard")
		}
	})

	t.Run("short field", func(t *testing.T) {
		var pk store.ChunkPacker
		var idb [8]byte
		binary.BigEndian.PutUint64(idb[0:], 9)
		pk.Add(idb[:], make([]byte, 10), 0)
		payload, flags := pk.Finish()
		var disc [24]byte
		binary.BigEndian.PutUint64(disc[0:], tag)
		binary.BigEndian.PutUint64(disc[8:], 9)
		frames := store.AppendRunChunk(nil, kindStreamPelChunk|store.ChunkKindBit, flags, 1, []byte("pb"), disc[:], payload)
		refs, fetch := fold(t, "pb", frames)
		if err := drain(refs, fetch); err == nil {
			t.Fatal("short-field pel pair streamed, want the misfile guard")
		}
	})

	t.Run("backward IDs", func(t *testing.T) {
		frames := pelChunkFrame("pc", tag, obs1.StreamRunID{Ms: 100}, []pelFixEnt{{id: obs1.StreamRunID{Ms: 100}, consumer: "c"}})
		frames = append(frames, pelChunkFrame("pc", tag, obs1.StreamRunID{Ms: 50}, []pelFixEnt{{id: obs1.StreamRunID{Ms: 50}, consumer: "c"}})...)
		refs, fetch := fold(t, "pc", frames)
		if len(refs) != 2 {
			t.Fatalf("planned %d pel chunks, want 2", len(refs))
		}
		if err := drain(refs, fetch); !errors.Is(err, obs1.ErrDiscOrder) {
			t.Fatalf("backward IDs drained with %v, want ErrDiscOrder", err)
		}
	})
}

// TestFolderStreamTrimDrop folds four entry runs, then a zero-count chunk
// under the second run's disc, the trim's manifest drop. The folder
// replaces the run by its (kind, key, disc) identity, and the iterator
// skips the emptied range without a GET.
func TestFolderStreamTrimDrop(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	entries, live := streamFixture()

	buf := streamRunFrames("st", entries, 16)
	var disc [16]byte
	binary.BigEndian.PutUint64(disc[0:], entries[16].id.Ms)
	binary.BigEndian.PutUint64(disc[8:], entries[16].id.Seq)
	buf = store.AppendRunChunk(buf, kindStreamChunk|store.ChunkKindBit, 0, 0, []byte("st"), disc[:], nil)
	fx.folder.Add(buf)
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("st"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("st has no locator")
	}
	refs := dir.CollChunksKind(loc, fp, uint8(kindStreamChunk|store.ChunkKindBit))
	if len(refs) != 4 {
		t.Fatalf("planned %d runs, want the 4 discs (the drop replaces, never adds)", len(refs))
	}
	dropped := 0
	for _, r := range refs {
		if r.Count == 0 {
			dropped++
		}
	}
	if dropped != 1 {
		t.Fatalf("plan holds %d zero-count runs, want the one dropped block", dropped)
	}

	// The dropped range's entries are gone and everything else streams in
	// order; entries 16..31 lived in the dropped block.
	var want []streamEnt
	for _, e := range live {
		if e.id.Ms >= entries[16].id.Ms && e.id.Ms < entries[32].id.Ms {
			continue
		}
		want = append(want, e)
	}
	fetch, _ := zsetRunFetcher(t, fx)
	var serr error
	it := obs1.StreamRunIter(refs, 0, fetch, nil, &serr)
	var got []obs1.StreamEntry
	for {
		e, ok := it()
		if !ok {
			break
		}
		got = append(got, e)
	}
	if serr != nil {
		t.Fatalf("catch-up stream: %v", serr)
	}
	if len(got) != len(want) {
		t.Fatalf("catch-up yielded %d entries, want %d survivors", len(got), len(want))
	}
	for i, e := range got {
		checkStreamEntry(t, e, want[i])
	}

	// Skip-without-GET, held directly: a plan of only the dropped run must
	// never call fetch.
	var only []obs1.DirRef
	for _, r := range refs {
		if r.Count == 0 {
			only = append(only, r)
		}
	}
	serr = nil
	it = obs1.StreamRunIter(only, 0, func(obs1.DirRef) ([]byte, error) {
		t.Fatal("a zero-count run was fetched")
		return nil, nil
	}, nil, &serr)
	if _, ok := it(); ok || serr != nil {
		t.Fatalf("dropped-only plan streamed (err %v), want a silent empty walk", serr)
	}
}
