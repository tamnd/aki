package obs1_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The stream ID-range run plane over a folded segment (spec 2064/obs1
// doc 08 section 7): runs are whole demoted blocks in the master-delta
// wire form under 16-byte (ms, seq) discs, XRANGE floors the plan by ms
// and streams from there, and a catch-up read walks every run in ID
// order with the shared window and prefetch discipline. The encoder
// below mirrors stream/block.go's frame layout (master general frame,
// same-schema value-only frames, XDEL tombstones the walk skips), the
// emission the stream demote pass ships blob-whole.

// kindStreamChunk is the stream collection kind, format like kindListChunk.
const kindStreamChunk = 0x05

type streamEnt struct {
	id      obs1.StreamRunID
	fields  [][2]string
	deleted bool
}

func sameStreamNames(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i][0] != b[i][0] {
			return false
		}
	}
	return true
}

// encodeStreamRun packs one run in the block wire form: per frame a
// flags byte, the ID delta against the run's first ID (unsigned ms,
// signed seq), then either value-only bodies against the master's names
// or a general field list.
func encodeStreamRun(entries []streamEnt) []byte {
	first := entries[0].id
	master := entries[0]
	var b []byte
	for i, e := range entries {
		flags := byte(0)
		same := i > 0 && sameStreamNames(e.fields, master.fields)
		if same {
			flags |= 1
		}
		if e.deleted {
			flags |= 2
		}
		b = append(b, flags)
		b = binary.AppendUvarint(b, e.id.Ms-first.Ms)
		b = binary.AppendVarint(b, int64(e.id.Seq)-int64(first.Seq))
		if same {
			for _, f := range e.fields {
				b = binary.AppendUvarint(b, uint64(len(f[1])))
				b = append(b, f[1]...)
			}
		} else {
			b = binary.AppendUvarint(b, uint64(len(e.fields)))
			for _, f := range e.fields {
				b = binary.AppendUvarint(b, uint64(len(f[0])))
				b = append(b, f[0]...)
				b = binary.AppendUvarint(b, uint64(len(f[1])))
				b = append(b, f[1]...)
			}
		}
	}
	return b
}

// streamRunFrames splits entries into runs of perChunk frames, each a
// chunk frame under the 16-byte disc of its first entry, tombstones
// counted like the demote pass counts them.
func streamRunFrames(key string, entries []streamEnt, perChunk int) []byte {
	var buf []byte
	for i := 0; i < len(entries); i += perChunk {
		end := min(i+perChunk, len(entries))
		run := entries[i:end]
		payload := encodeStreamRun(run)
		var disc [16]byte
		binary.BigEndian.PutUint64(disc[0:], run[0].id.Ms)
		binary.BigEndian.PutUint64(disc[8:], run[0].id.Seq)
		buf = store.AppendRunChunk(buf, kindStreamChunk|store.ChunkKindBit, 0, uint16(len(run)), []byte(key), disc[:], payload)
	}
	return buf
}

// streamFixture builds 60 entries with climbing (ms, seq) IDs and a
// mostly uniform two-field schema: entry 25 breaks schema (a general
// frame past the master) and entry 40 is an XDEL tombstone.
func streamFixture() (entries []streamEnt, live []streamEnt) {
	for i := 0; i < 60; i++ {
		e := streamEnt{
			id: obs1.StreamRunID{Ms: 1000 + uint64(i)*10, Seq: uint64(i % 3)},
			fields: [][2]string{
				{"f", fmt.Sprintf("v%03d", i)},
				{"g", fmt.Sprintf("w%03d", i)},
			},
		}
		if i == 25 {
			e.fields = [][2]string{{"h", "solo"}}
		}
		if i == 40 {
			e.deleted = true
		}
		entries = append(entries, e)
		if !e.deleted {
			live = append(live, e)
		}
	}
	return entries, live
}

func checkStreamEntry(t *testing.T, got obs1.StreamEntry, want streamEnt) {
	t.Helper()
	if got.ID != want.id {
		t.Fatalf("entry ID %d-%d, want %d-%d", got.ID.Ms, got.ID.Seq, want.id.Ms, want.id.Seq)
	}
	if len(got.Fields) != len(want.fields) {
		t.Fatalf("entry %d-%d has %d fields, want %d", got.ID.Ms, got.ID.Seq, len(got.Fields), len(want.fields))
	}
	for i, f := range got.Fields {
		if !bytes.Equal(f.Name, []byte(want.fields[i][0])) || !bytes.Equal(f.Value, []byte(want.fields[i][1])) {
			t.Fatalf("entry %d-%d field %d = %q=%q, want %q=%q", got.ID.Ms, got.ID.Seq, i, f.Name, f.Value, want.fields[i][0], want.fields[i][1])
		}
	}
}

func TestFolderStreamIDRangeRuns(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	entries, live := streamFixture()

	fx.folder.Add(streamRunFrames("sx", entries, 16))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("sx"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("sx has no locator")
	}
	streamKind := uint8(kindStreamChunk | store.ChunkKindBit)
	refs := dir.CollChunksKind(loc, fp, streamKind)
	if len(refs) != 4 {
		t.Fatalf("planned %d ID-range runs, want 4", len(refs))
	}
	for i, r := range refs {
		if r.ChunkKind != streamKind {
			t.Fatalf("run %d kind 0x%02x crossed the kind-restricted plan", i, r.ChunkKind)
		}
		if r.FirstDisc != entries[i*16].id.Ms {
			t.Fatalf("run %d coordinate %d, want the first entry's ms %d", i, r.FirstDisc, entries[i*16].id.Ms)
		}
	}

	fetch, fetches := zsetRunFetcher(t, fx)

	// An XRANGE point read for every live entry: floor the plan by ms,
	// stream the floored run, one new block at most.
	for _, want := range live {
		idx := obs1.StreamRunFloor(refs, want.id.Ms)
		before := *fetches
		var serr error
		it := obs1.StreamRunIter(refs, idx, fetch, nil, &serr)
		found := false
		for {
			e, ok := it()
			if !ok {
				break
			}
			if e.ID == want.id {
				checkStreamEntry(t, e, want)
				found = true
				break
			}
			if want.id.Less(e.ID) {
				break
			}
		}
		if serr != nil {
			t.Fatalf("point read %d-%d: %v", want.id.Ms, want.id.Seq, serr)
		}
		if !found {
			t.Fatalf("point read %d-%d found nothing", want.id.Ms, want.id.Seq)
		}
		if d := *fetches - before; d > 1 {
			t.Fatalf("point read %d-%d billed %d new blocks, want at most one", want.id.Ms, want.id.Seq, d)
		}
	}

	// The tombstone reads as absent: streaming its run never yields its ID.
	dead := entries[40]
	var serr error
	it := obs1.StreamRunIter(refs, obs1.StreamRunFloor(refs, dead.id.Ms), fetch, nil, &serr)
	for {
		e, ok := it()
		if !ok {
			break
		}
		if e.ID == dead.id {
			t.Fatalf("tombstoned entry %d-%d streamed", e.ID.Ms, e.ID.Seq)
		}
		if dead.id.Less(e.ID) {
			break
		}
	}
	if serr != nil {
		t.Fatalf("tombstone probe: %v", serr)
	}

	// An XRANGE window across a run boundary: floor by the start ms,
	// stream, keep IDs inside the bounds.
	lo, hi := live[13].id, live[19].id
	serr = nil
	it = obs1.StreamRunIter(refs, obs1.StreamRunFloor(refs, lo.Ms), fetch, nil, &serr)
	var window []obs1.StreamEntry
	for {
		e, ok := it()
		if !ok {
			break
		}
		if e.ID.Less(lo) {
			continue
		}
		if hi.Less(e.ID) {
			break
		}
		window = append(window, e)
	}
	if serr != nil {
		t.Fatalf("window stream: %v", serr)
	}
	if len(window) != 7 {
		t.Fatalf("window yielded %d entries, want 7", len(window))
	}
	for i, e := range window {
		checkStreamEntry(t, e, live[13+i])
	}

	// The full catch-up streams every live entry in ID order, and the
	// prefetch seam announces every distinct block past the first.
	var announced []obs1.DirRef
	serr = nil
	it = obs1.StreamRunIter(refs, 0, fetch, func(r obs1.DirRef) { announced = append(announced, r) }, &serr)
	var all []obs1.StreamEntry
	for {
		e, ok := it()
		if !ok {
			break
		}
		all = append(all, e)
	}
	if serr != nil {
		t.Fatalf("catch-up stream: %v", serr)
	}
	if len(all) != len(live) {
		t.Fatalf("catch-up yielded %d entries, want %d live", len(all), len(live))
	}
	for i, e := range all {
		checkStreamEntry(t, e, live[i])
	}
	distinct := map[string]bool{}
	for _, r := range refs {
		distinct[fmt.Sprintf("%s@%d", r.ObjKey, r.Block.Offset)] = true
	}
	if want := len(distinct) - 1; len(announced) < want {
		t.Fatalf("prefetch announced %d blocks, want at least %d", len(announced), want)
	}
}

// TestFolderStreamDiscLengthGuard folds a stream-kind run under an
// 8-byte disc and holds the iterator to the misfile guard: the exact
// first ID rides the 16-byte (ms, seq) disc, so anything else is an
// error, not a truncated coordinate.
func TestFolderStreamDiscLengthGuard(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)

	payload := encodeStreamRun([]streamEnt{{
		id:     obs1.StreamRunID{Ms: 5, Seq: 0},
		fields: [][2]string{{"f", "v"}},
	}})
	var disc [8]byte
	binary.BigEndian.PutUint64(disc[:], 5)
	buf := store.AppendRunChunk(nil, kindStreamChunk|store.ChunkKindBit, 0, 1, []byte("sb"), disc[:], payload)
	fx.folder.Add(buf)
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("sb"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("sb has no locator")
	}
	refs := dir.CollChunksKind(loc, fp, uint8(kindStreamChunk|store.ChunkKindBit))
	if len(refs) != 1 {
		t.Fatalf("planned %d runs, want 1", len(refs))
	}
	fetch, _ := zsetRunFetcher(t, fx)
	var serr error
	it := obs1.StreamRunIter(refs, 0, fetch, nil, &serr)
	if _, ok := it(); ok || serr == nil {
		t.Fatalf("short-disc run streamed (err %v), want the misfile guard", serr)
	}
}

// TestWalkStreamRunGuards drives the standalone walker into its torn
// and misfiled shapes directly.
func TestWalkStreamRunGuards(t *testing.T) {
	first := obs1.StreamRunID{Ms: 10, Seq: 0}
	nop := func(obs1.StreamRunID, []obs1.StreamField) error { return nil }

	// A master frame claiming same-schema has no names to borrow.
	bad := []byte{1}
	bad = binary.AppendUvarint(bad, 0)
	bad = binary.AppendVarint(bad, 0)
	if err := obs1.WalkStreamRun(bad, first, 1, nop); err == nil {
		t.Fatal("same-schema master walked clean")
	}

	// A count past the payload is torn, not short.
	good := encodeStreamRun([]streamEnt{{id: first, fields: [][2]string{{"f", "v"}}}})
	if err := obs1.WalkStreamRun(good, first, 2, nop); err == nil {
		t.Fatal("overcounted payload walked clean")
	}
	if err := obs1.WalkStreamRun(good[:len(good)-1], first, 1, nop); err == nil {
		t.Fatal("truncated payload walked clean")
	}
}
