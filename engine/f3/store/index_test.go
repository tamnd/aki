package store

import (
	"fmt"
	"testing"
)

// segSnapshot captures a segment's entire observable state so a test can
// prove a split elsewhere never read or wrote it.
type segSnapshot struct {
	localDepth uint8
	used       uint16
	chained    uint16
	buckets    [homeBuckets]bucket
	overflow   []bucket
}

func snapshot(seg *indexSegment) segSnapshot {
	return segSnapshot{
		localDepth: seg.localDepth,
		used:       seg.used,
		chained:    seg.chained,
		buckets:    seg.buckets,
		overflow:   append([]bucket(nil), seg.overflow...),
	}
}

func snapshotEqual(a, b segSnapshot) bool {
	if a.localDepth != b.localDepth || a.used != b.used || a.chained != b.chained {
		return false
	}
	if a.buckets != b.buckets {
		return false
	}
	if len(a.overflow) != len(b.overflow) {
		return false
	}
	for i := range a.overflow {
		if a.overflow[i] != b.overflow[i] {
			return false
		}
	}
	return true
}

// TestSplitDirectoryConsistency grows the index through several generations
// of splits and checks the extendible-hashing invariants: every directory
// slot points at a live segment, a segment at localDepth d owns exactly
// 2^(gd-d) contiguous aligned slots, and every key routes to a segment that
// actually holds it.
func TestSplitDirectoryConsistency(t *testing.T) {
	s := testStore(t, 8)
	const n = 30000
	for i := 0; i < n; i++ {
		if err := s.Set([]byte(fmt.Sprintf("key-%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	ix := &s.idx
	if ix.gd == 0 {
		t.Fatal("30000 keys never doubled the directory")
	}
	if len(ix.dir) != 1<<ix.gd {
		t.Fatalf("directory has %d slots, want %d", len(ix.dir), 1<<ix.gd)
	}
	covered := make(map[uint32]int)
	for slot, ord := range ix.dir {
		seg := ix.segs[ord]
		if seg == nil {
			t.Fatalf("dir slot %d points at freed ordinal %d", slot, ord)
		}
		if seg.localDepth > ix.gd {
			t.Fatalf("segment %d localDepth %d exceeds gd %d", ord, seg.localDepth, ix.gd)
		}
		covered[ord]++
	}
	for ord, got := range covered {
		want := 1 << (ix.gd - ix.segs[ord].localDepth)
		if got != want {
			t.Fatalf("segment %d covers %d dir slots, want %d", ord, got, want)
		}
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		if _, ok := get(t, s, k); !ok {
			t.Fatalf("%s unroutable after splits", k)
		}
	}
}

// TestSplitDoesNotStallOtherBuckets is the growth-pause gate: a split of one
// segment must not read, write, or move any other segment. The test grows a
// multi-segment index, snapshots every segment except the victim, drives keys
// at the victim until it splits, and then requires every other segment to be
// bit-identical and every previously written key to still resolve. Inline
// growth is only pause-free if its blast radius is one segment; this pins
// that radius.
func TestSplitDoesNotStallOtherBuckets(t *testing.T) {
	s := testStore(t, 8)

	// Grow to several segments so there is an "elsewhere" to protect.
	written := make(map[string]string)
	for i := 0; s.idx.gd < 2; i++ {
		k := fmt.Sprintf("seed-%d", i)
		v := fmt.Sprintf("val-%d", i)
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		written[k] = v
	}

	victimOrd := s.idx.dir[0]
	victim := s.idx.segs[victimOrd]
	before := make(map[*indexSegment]segSnapshot)
	for _, ord := range s.idx.dir {
		seg := s.idx.segs[ord]
		if seg != victim {
			before[seg] = snapshot(seg)
		}
	}

	// Drive inserts that route only to the victim segment until it splits.
	splitsBefore := s.idx.splits
	inserted := 0
	for i := 0; s.idx.splits == splitsBefore; i++ {
		if i >= 1<<22 {
			t.Fatal("victim segment never split")
		}
		k := fmt.Sprintf("aim-%d", i)
		h := Hash([]byte(k))
		if s.idx.segs[s.idx.dir[dirIndex(h, s.idx.gd)]] != victim {
			continue
		}
		v := fmt.Sprintf("vv-%d", i)
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		written[k] = v
		inserted++
	}
	if inserted == 0 {
		t.Fatal("no insert reached the victim segment")
	}

	// Every unrelated segment is untouched, bit for bit.
	for seg, snap := range before {
		if !snapshotEqual(snapshot(seg), snap) {
			t.Fatal("a split of one segment modified an unrelated segment")
		}
	}
	// And each is still reachable through the directory.
	reachable := make(map[*indexSegment]bool)
	for _, ord := range s.idx.dir {
		reachable[s.idx.segs[ord]] = true
	}
	for seg := range before {
		if !reachable[seg] {
			t.Fatal("a split unmapped an unrelated segment from the directory")
		}
	}
	// Every key written before and during the split still resolves.
	for k, want := range written {
		if v, ok := get(t, s, k); !ok || v != want {
			t.Fatalf("%s = %q,%v want %q after split", k, v, ok, want)
		}
	}
}

// TestChainCapSplits fills one home bucket's chain to its cap and checks the
// next colliding insert splits on chain pressure instead of growing the probe
// tail, keeping every colliding key resolvable.
func TestChainCapSplits(t *testing.T) {
	s := testStore(t, 8)
	target := uint64(37) // arbitrary home bucket
	full := slotsPerBucket * (1 + chainCap)
	keys := make([]string, 0, full+4)
	for i := 0; len(keys) < full+4; i++ {
		k := fmt.Sprintf("c-%d", i)
		h := Hash([]byte(k))
		if dirIndex(h, s.idx.gd) != 0 || bucketIndex(h) != target {
			continue
		}
		if err := s.Set([]byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	if s.Splits() == 0 {
		t.Fatalf("%d colliding keys never split on chain pressure", len(keys))
	}
	for _, k := range keys {
		if _, ok := get(t, s, k); !ok {
			t.Fatalf("%s lost across the chain-pressure split", k)
		}
	}
}
