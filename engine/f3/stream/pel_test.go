package stream

import (
	"testing"
)

// Unit tests for the pending-entries list below the command harness (spec
// 2064/f3/14 section 7.4): the tree-plus-hash over slabs agree on membership, an
// ack frees a slot the next insert reuses, and the end peeks report the range.

func TestPELInsertAckRoundTrip(t *testing.T) {
	p := newPEL()
	ids := []streamID{{1, 0}, {1, 5}, {2, 0}, {9, 0}}
	for i, id := range ids {
		p.insert(id, int64(100+i), uint32(i%2))
	}
	if p.tree.Len() != len(ids) {
		t.Fatalf("tree holds %d, want %d", p.tree.Len(), len(ids))
	}
	// The hash and the tree agree: every inserted ID acks exactly once, then misses.
	for i, id := range ids {
		owner, ok := p.ack(id)
		if !ok {
			t.Fatalf("ack %v missed, want hit", id)
		}
		if want := uint32(i % 2); owner != want {
			t.Fatalf("ack %v owner = %d, want %d", id, owner, want)
		}
		if _, ok := p.ack(id); ok {
			t.Fatalf("second ack %v hit, want miss", id)
		}
	}
	if p.tree.Len() != 0 {
		t.Fatalf("tree holds %d after draining, want 0", p.tree.Len())
	}
}

func TestPELAckReusesSlab(t *testing.T) {
	p := newPEL()
	p.insert(streamID{1, 0}, 100, 0)
	if _, ok := p.ack(streamID{1, 0}); !ok {
		t.Fatal("ack missed")
	}
	if len(p.free) != 1 {
		t.Fatalf("free list = %d, want 1 after an ack", len(p.free))
	}
	// The next insert reuses the freed ordinal instead of growing the arena.
	p.insert(streamID{2, 0}, 200, 0)
	if len(p.slabs) != 1 {
		t.Fatalf("arena grew to %d slabs, want 1 (freed slot reused)", len(p.slabs))
	}
}

func TestPELMinMaxEmpty(t *testing.T) {
	p := newPEL()
	if p.minEntry() != nil || p.maxEntry() != nil {
		t.Fatal("empty PEL reported a min or max entry")
	}
	p.insert(streamID{5, 0}, 1, 0)
	p.insert(streamID{2, 0}, 1, 0)
	p.insert(streamID{9, 0}, 1, 0)
	if got := p.minEntry().id; got != (streamID{2, 0}) {
		t.Fatalf("min = %v, want 2-0", got)
	}
	if got := p.maxEntry().id; got != (streamID{9, 0}) {
		t.Fatalf("max = %v, want 9-0", got)
	}
}

func TestPELWalkFromOrdered(t *testing.T) {
	p := newPEL()
	for _, id := range []streamID{{3, 0}, {1, 0}, {2, 7}, {2, 1}} {
		p.insert(id, 1, 0)
	}
	var got []streamID
	p.walkFrom(streamID{2, 0}, func(e *pelEntry) bool {
		got = append(got, e.id)
		return true
	})
	want := []streamID{{2, 1}, {2, 7}, {3, 0}}
	if len(got) != len(want) {
		t.Fatalf("walk yielded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
