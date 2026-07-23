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

// The sets slice end to end (spec 2064/obs1 doc 08 section 4): a set
// demoter's valueless chunks fold into a bucket segment, the collection
// placement lands in the keymap, SISMEMBER answers through the field
// reader, SMEMBERS walks CollChunks, and the algebra streams two folded
// sets one block per operand. The frames are built with the demoter's own
// codec and coordinate, so the byte stream under test is the demote tap's.

// kindSetChunk pins the set collection kind byte, format like kindString
// and kindHash above.
const kindSetChunk = 0x01

// setChunkFrames packs members into demoter-shaped valueless chunk frames
// under key: sorted by the fold coordinate, perChunk entries per chunk.
func setChunkFrames(key string, members []string, perChunk int) []byte {
	sort.Slice(members, func(i, j int) bool {
		return obs1.Disc([]byte(members[i])) < obs1.Disc([]byte(members[j]))
	})
	var buf []byte
	var pk store.ChunkPacker
	for i := 0; i < len(members); i += perChunk {
		end := i + perChunk
		if end > len(members) {
			end = len(members)
		}
		pk.Reset()
		for _, m := range members[i:end] {
			pk.Add([]byte(m), nil, 0)
		}
		payload, flags := pk.Finish()
		var disc [8]byte
		binary.BigEndian.PutUint64(disc[:], obs1.Disc([]byte(members[i])))
		buf = store.AppendRunChunk(buf, kindSetChunk|store.ChunkKindBit, flags, uint16(pk.Count()), []byte(key), disc[:], payload)
	}
	return buf
}

func setMembers(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-member%03d", prefix, i)
	}
	return out
}

// TestFolderSetMemberReads folds one set and drives SISMEMBER through the
// cold reader: every member answers found with an empty value, a stranger
// misses cleanly.
func TestFolderSetMemberReads(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	members := setMembers("s1", 40)

	fx.folder.Add(setChunkFrames("s1", members, 14))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	led := fx.folder.Ledger()[0]

	loc, ok := km.Lookup(obs1.Fingerprint([]byte("s1")))
	if !ok || loc.Seg != uint32(led.SegSeq) {
		t.Fatalf("s1 locator %+v ok=%v, want seg %d", loc, ok, led.SegSeq)
	}

	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: fx.sim,
		Dir:   func(group uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cr.Close)

	for _, m := range members {
		cf, err := fetchFieldWait(t, cr, "s1", loc, m, nowMs)
		if err != nil {
			t.Fatalf("%s: %v", m, err)
		}
		if !cf.Found || len(cf.Value) != 0 || cf.Exp != 0 {
			t.Fatalf("%s = %+v, want found with an empty value", m, cf)
		}
	}
	cf, err := fetchFieldWait(t, cr, "s1", loc, "stranger", nowMs)
	if err != nil || cf.Found {
		t.Fatalf("stranger = %+v err %v, want a clean miss", cf, err)
	}
	if st := cr.Stats(); st.Errs != 0 || st.Unresolved != 0 || st.Misses != 1 {
		t.Fatalf("stats %+v, want the stranger's one miss and no errors", st)
	}
}

// TestSetAlgebraStreamsCold folds two sets and runs the SUNION, SINTER,
// and SDIFF merges over ColdCollIter streams, holding the request
// arithmetic: each operand pages its chunks in plan order and fetches one
// GET per distinct block, never one per chunk past it.
func TestSetAlgebraStreamsCold(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	shared := setMembers("both", 20)
	onlyA := setMembers("a", 15)
	onlyB := setMembers("b", 15)
	sa := append(append([]string(nil), shared...), onlyA...)
	sb := append(append([]string(nil), shared...), onlyB...)

	fx.folder.Add(setChunkFrames("sa", sa, 12))
	fx.folder.Flush()
	waitFor(t, "first publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	fx.folder.Add(setChunkFrames("sb", sb, 12))
	fx.folder.Flush()
	waitFor(t, "second publish", func() bool { return len(fx.folder.Ledger()) == 2 })

	ctx := context.Background()
	fetches := 0
	fetch := func(ref obs1.DirRef) ([]byte, error) {
		fetches++
		off, n := ref.Block.BlockSpan()
		raw, _, err := fx.sim.GetRange(ctx, ref.ObjKey, off, n)
		if err != nil {
			return nil, err
		}
		return obs1.ParseSegmentBlock(raw, ref.Block)
	}
	plan := func(key string) []obs1.DirRef {
		fp := obs1.Fingerprint([]byte(key))
		loc, ok := km.Lookup(fp)
		if !ok {
			t.Fatalf("%s has no locator", key)
		}
		refs := dir.CollChunks(loc, fp)
		if len(refs) < 2 {
			t.Fatalf("%s planned %d chunks, want the pack split", key, len(refs))
		}
		return refs
	}
	refsA, refsB := plan("sa"), plan("sb")
	distinctBlocks := func(refs []obs1.DirRef) int {
		seen := map[string]bool{}
		for _, r := range refs {
			seen[fmt.Sprintf("%s@%d", r.ObjKey, r.Block.Offset)] = true
		}
		return len(seen)
	}

	run := func(op func([]obs1.ElemIter, func([]byte) error) error) map[string]bool {
		var errA, errB error
		its := []obs1.ElemIter{
			obs1.ColdCollIter(refsA, fetch, nowMs, &errA),
			obs1.ColdCollIter(refsB, fetch, nowMs, &errB),
		}
		got := map[string]bool{}
		var prev uint64
		if err := op(its, func(m []byte) error {
			if d := obs1.Disc(m); d < prev {
				return fmt.Errorf("member %q yielded out of disc order", m)
			} else {
				prev = d
			}
			got[string(m)] = true
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if errA != nil || errB != nil {
			t.Fatalf("stream errors %v / %v", errA, errB)
		}
		return got
	}
	wantSet := func(name string, got map[string]bool, want []string) {
		if len(got) != len(want) {
			t.Fatalf("%s yielded %d members, want %d", name, len(got), len(want))
		}
		for _, m := range want {
			if !got[m] {
				t.Fatalf("%s missing %q", name, m)
			}
		}
	}

	fetches = 0
	union := run(obs1.SetUnion)
	wantSet("SUNION", union, append(append(append([]string(nil), shared...), onlyA...), onlyB...))
	if maxGETs := distinctBlocks(refsA) + distinctBlocks(refsB); fetches > maxGETs {
		t.Fatalf("SUNION fetched %d blocks, want at most one per operand block (%d)", fetches, maxGETs)
	}

	wantSet("SINTER", run(obs1.SetInter), shared)
	wantSet("SDIFF", run(obs1.SetDiff), onlyA)

	// A plan whose chunk ranges run backward (the partition-interleave state
	// before the doc 06 rewrite re-sorts it) fails loudly instead of merging
	// the wrong range.
	rev := append([]obs1.DirRef(nil), refsA...)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	var revErr error
	it := obs1.ColdCollIter(rev, fetch, nowMs, &revErr)
	for {
		if _, ok := it(); !ok {
			break
		}
	}
	if revErr != obs1.ErrDiscOrder {
		t.Fatalf("backward plan ended with %v, want ErrDiscOrder", revErr)
	}
}
