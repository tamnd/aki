package sqlo1

import (
	"bytes"
	"context"
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"runtime/debug"
	"testing"
)

func TestComputeBudgetTable(t *testing.T) {
	cap := int64(512 << 20)
	b := ComputeBudget(cap, 1)

	if b.Arenas != cap*55/100 {
		t.Fatalf("arenas %d, want 55%% of cap", b.Arenas)
	}
	if b.Entries != int(b.Arenas/avgRecordEstimate) {
		t.Fatalf("entries %d, want arenas over %d", b.Entries, avgRecordEstimate)
	}
	if b.Headers != int64(b.Entries)*hotEntryOverhead {
		t.Fatalf("headers %d, want %d per entry", b.Headers, hotEntryOverhead)
	}
	if b.Ghosts != int64(b.Entries/16)*16 {
		t.Fatalf("ghosts %d, want 16 B per entries/16", b.Ghosts)
	}
	if b.ChunkCache != cap/10 || b.Directory != cap*15/100 || b.Slack != cap*15/100 {
		t.Fatal("percentage rows drifted from the doc 04 table")
	}
	if b.GroupBufs != int64(2*drainMaxOps)*4096 {
		t.Fatalf("group buffers %d, want 2 x batch x 4 KiB", b.GroupBufs)
	}
	if b.Compaction != 2<<20 || b.WALBufs != 8<<20 || b.Dicts != 4*(112<<10) {
		t.Fatal("sized rows drifted from the doc 04 table")
	}

	// Shards scale only the per-shard rows.
	b4 := ComputeBudget(cap, 4)
	if b4.GroupBufs != 4*b.GroupBufs || b4.Compaction != 4*b.Compaction || b4.WALBufs != 4*b.WALBufs {
		t.Fatal("per-shard rows must scale with shards")
	}
	if b4.Arenas != b.Arenas || b4.Entries != b.Entries {
		t.Fatal("hot tier rows must not scale with shards")
	}
}

func TestBudgetApplySetsGOMEMLIMIT(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	b := ComputeBudget(512<<20, 1)
	b.Apply()
	if got := debug.SetMemoryLimit(-1); got != b.Cap {
		t.Fatalf("GOMEMLIMIT %d after Apply, want %d", got, b.Cap)
	}
}

// reservedCensus checks the budget's book against the chunks that
// actually exist: reserved never exceeds the limit and always equals the
// bytes held across both arenas (released oversize chunks are nil and
// count zero, matching their unreserve).
func reservedCensus(t *testing.T, ht *HotTable) {
	t.Helper()
	b := ht.keys.budget
	if b.reserved > b.limit {
		t.Fatalf("reserved %d over limit %d", b.reserved, b.limit)
	}
	var sum int64
	for _, a := range []*arena{&ht.keys, &ht.vals} {
		for _, c := range a.chunks {
			sum += int64(len(c))
		}
	}
	if sum != b.reserved {
		t.Fatalf("reserved %d but chunks hold %d", b.reserved, sum)
	}
}

func TestHardCapProperty(t *testing.T) {
	// Four standard chunks of room: both arenas' chunk 0 take two, and
	// the traffic below fights over the rest with sizes from tiny to
	// oversize. No operation sequence may push reserved past the limit.
	b := Budget{Arenas: 4 * arenaChunkSize, Entries: 128}
	ht := NewBudgetedHotTable(b)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	rng := rand.New(rand.NewPCG(42, 43))

	// The standard classes here peak under one chunk per arena, so the
	// oversize entries are what fight over the remaining budget and a
	// full free provably makes room for a refused oversize retry.
	sizes := []int{16, 200, 4 << 10, arenaChunkSize} // last is oversize
	refused, recovered := 0, 0
	for i := range 4000 {
		key := fmt.Appendf(nil, "cap-%02d", rng.IntN(24))
		switch rng.IntN(10) {
		case 0:
			ht.Del(key)
		case 1:
			if _, err := d.drain(context.Background()); err != nil {
				t.Fatal(err)
			}
		default:
			val := bytes.Repeat([]byte("x"), sizes[rng.IntN(len(sizes))])
			if !ht.Put(key, val, TagString) {
				refused++
				// Free something and show the refused bytes fit again.
				for j := range 24 {
					ht.Del(fmt.Appendf(nil, "cap-%02d", j))
				}
				drainAll(t, d)
				if ht.Put(key, val, TagString) {
					recovered++
				}
			}
		}
		if i%50 == 0 {
			reservedCensus(t, ht)
		}
	}
	reservedCensus(t, ht)
	if refused == 0 {
		t.Fatal("cap never refused a put; the property was not exercised")
	}
	if recovered == 0 {
		t.Fatal("no refused put ever succeeded after frees; the cap does not release")
	}
}

func TestPutFailureIsNoOp(t *testing.T) {
	// One standard chunk per arena and nothing more: any alloc that
	// needs a second chunk is refused, and the refusal must leave no
	// trace anywhere.
	b := Budget{Arenas: 2 * arenaChunkSize, Entries: 16}
	ht := NewBudgetedHotTable(b)
	ht.SetTick(3)
	d := newDrainer(ht, NewMemStore())
	huge := bytes.Repeat([]byte("h"), arenaChunkSize) // oversize, always refused

	// Refused insert: no slot, no index entry, no queue entry.
	if ht.Put([]byte("nope"), huge, TagString) {
		t.Fatal("oversize insert fit inside one chunk of budget")
	}
	if ht.Len() != 0 || len(ht.hdrs) != 0 || ht.dirtyN != 0 || ht.dirtyBytes != 0 {
		t.Fatal("refused insert left state behind")
	}
	if _, ok := ht.lookup(maphash.Bytes(ht.seed, []byte("nope")), []byte("nope")); ok {
		t.Fatal("refused insert reached the index")
	}

	// Refused overwrite of a resident: value, state, stamps all stand.
	key := []byte("keep")
	if !ht.Put(key, []byte("small"), TagString) {
		t.Fatal("seed put refused")
	}
	drainAll(t, d)
	s := slotOf(t, ht, key)
	before := ht.hdrs[s]
	ht.SetTick(9)
	if ht.Put(key, huge, TagString) {
		t.Fatal("oversize overwrite fit inside one chunk of budget")
	}
	if ht.hdrs[s] != before {
		t.Fatal("refused overwrite mutated the header")
	}
	if v, ok := ht.Get(key); !ok || string(v) != "small" {
		t.Fatal("refused overwrite disturbed the value")
	}
	if ht.dirtyBytes != 0 || ht.dirtyN != 0 {
		t.Fatal("refused overwrite dirtied the record")
	}

	// Refused tombstone revive: the tombstone stays a tombstone.
	if !ht.Del(key) {
		t.Fatal("del refused")
	}
	dirtyBefore := ht.dirtyBytes
	if ht.Put(key, huge, TagString) {
		t.Fatal("oversize revive fit inside one chunk of budget")
	}
	ts := slotOf(t, ht, key)
	if ht.hdrs[ts].valRef != 0 || ht.Len() != 0 || ht.dirtyBytes != dirtyBefore {
		t.Fatal("refused revive disturbed the tombstone")
	}
	if _, ok := ht.Get(key); ok {
		t.Fatal("refused revive made the key visible")
	}
}
