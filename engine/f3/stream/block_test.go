package stream

import (
	"fmt"
	"math/rand"
	"testing"
)

// f builds a field from two strings, the common test shape.
func f(name, value string) field { return field{name: []byte(name), value: []byte(value)} }

// collect walks the block and copies every live entry out, since the walk
// yields blob views valid only until the next mutation.
type gotEntry struct {
	id     streamID
	fields []field
}

func collect(b *block) []gotEntry {
	var out []gotEntry
	scratch := make([]field, 0, 8)
	b.walk(scratch, func(id streamID, fields []field) bool {
		e := gotEntry{id: id}
		for _, fl := range fields {
			e.fields = append(e.fields, f(string(fl.name), string(fl.value)))
		}
		out = append(out, e)
		return true
	})
	return out
}

func fieldsEqual(a, b []field) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if string(a[i].name) != string(b[i].name) || string(a[i].value) != string(b[i].value) {
			return false
		}
	}
	return true
}

func TestBlockMasterThenSameSchemaRoundTrip(t *testing.T) {
	b := newBlock()
	want := []gotEntry{
		{streamID{1000, 0}, []field{f("temp", "21"), f("hum", "40")}},
		{streamID{1000, 1}, []field{f("temp", "22"), f("hum", "41")}},
		{streamID{1001, 0}, []field{f("temp", "23"), f("hum", "39")}},
	}
	for _, e := range want {
		if !b.appendEntry(e.id, e.fields) {
			t.Fatalf("appendEntry(%s) rejected on an unfilled block", e.id)
		}
	}
	if b.entries() != 3 || b.live() != 3 {
		t.Fatalf("entries=%d live=%d want 3/3", b.entries(), b.live())
	}
	if b.firstID() != (streamID{1000, 0}) || b.lastID() != (streamID{1001, 0}) {
		t.Fatalf("first=%s last=%s want 1000-0 / 1001-0", b.firstID(), b.lastID())
	}
	got := collect(b)
	if len(got) != len(want) {
		t.Fatalf("walked %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].id != want[i].id || !fieldsEqual(got[i].fields, want[i].fields) {
			t.Fatalf("entry %d: got %s %v, want %s %v", i, got[i].id, got[i].fields, want[i].id, want[i].fields)
		}
	}
}

func TestBlockGeneralEntryCarriesItsOwnNames(t *testing.T) {
	// An entry whose field set differs from the master stores its names in full
	// and must decode back exactly, alongside same-schema neighbours.
	b := newBlock()
	want := []gotEntry{
		{streamID{5, 0}, []field{f("a", "1"), f("b", "2")}},              // master
		{streamID{5, 1}, []field{f("a", "3"), f("b", "4")}},              // same schema
		{streamID{5, 2}, []field{f("x", "9"), f("y", "8"), f("z", "7")}}, // general: different names
		{streamID{5, 3}, []field{f("a", "5"), f("b", "6")}},              // back to the master schema
	}
	for _, e := range want {
		if !b.appendEntry(e.id, e.fields) {
			t.Fatalf("appendEntry(%s) rejected", e.id)
		}
	}
	got := collect(b)
	for i := range want {
		if got[i].id != want[i].id || !fieldsEqual(got[i].fields, want[i].fields) {
			t.Fatalf("entry %d: got %s %v, want %s %v", i, got[i].id, got[i].fields, want[i].id, want[i].fields)
		}
	}
}

func TestSameSchemaEntryDropsNameBytes(t *testing.T) {
	// The master carries the names, a same-schema entry drops them: the second
	// entry must be smaller than the first by at least the name bytes, which is
	// the ~100x field-name collapse of section 3.3.
	b := newBlock()
	fields := []field{f("temperature", "21"), f("humidity", "40")}
	b.appendEntry(streamID{1, 0}, fields)
	masterSize := b.size()
	b.appendEntry(streamID{1, 1}, fields)
	sameSize := b.size() - masterSize
	nameBytes := len("temperature") + len("humidity")
	if masterSize-sameSize < nameBytes {
		t.Fatalf("master %dB, same-schema %dB, saved %dB, want >= %dB of dropped names",
			masterSize, sameSize, masterSize-sameSize, nameBytes)
	}
}

func TestFrameLenMatchesEncodedGrowth(t *testing.T) {
	// frameLen must stay in lockstep with appendEntry, since the budget check
	// prices a frame before committing it. Assert the blob grows by exactly
	// frameLen for both a same-schema and a general entry.
	b := newBlock()
	master := []field{f("a", "11"), f("b", "22")}
	b.appendEntry(streamID{7, 0}, master)

	same := []field{f("a", "333"), f("b", "444")}
	before := b.size()
	want := b.frameLen(streamID{7, 1}, same, b.sameSchema(same))
	b.appendEntry(streamID{7, 1}, same)
	if got := b.size() - before; got != want {
		t.Fatalf("same-schema frame grew blob by %d, frameLen said %d", got, want)
	}

	gen := []field{f("c", "5"), f("d", "6"), f("e", "7")}
	before = b.size()
	want = b.frameLen(streamID{7, 2}, gen, b.sameSchema(gen))
	b.appendEntry(streamID{7, 2}, gen)
	if got := b.size() - before; got != want {
		t.Fatalf("general frame grew blob by %d, frameLen said %d", got, want)
	}
}

func TestBlockSealsOnEntryCap(t *testing.T) {
	// Tiny entries never reach the byte budget, so the 128 entry cap binds first.
	b := newBlock()
	fields := []field{f("k", "v")}
	added := 0
	for i := 0; i < blockCap+50; i++ {
		if !b.appendEntry(streamID{1, uint64(i)}, fields) {
			break
		}
		added++
	}
	if added != blockCap {
		t.Fatalf("entry cap sealed at %d entries, want %d", added, blockCap)
	}
	if !b.full() {
		t.Fatalf("block not full after the entry cap bound")
	}
	if b.appendEntry(streamID{1, 9999}, fields) {
		t.Fatalf("a full block accepted another entry")
	}
}

func TestBlockSealsOnByteBudget(t *testing.T) {
	// Fat entries reach the 4096 byte budget before the 128 entry cap.
	b := newBlock()
	big := make([]byte, 400)
	for i := range big {
		big[i] = 'x'
	}
	fields := []field{{name: []byte("blob"), value: big}}
	added := 0
	for i := 0; i < blockCap; i++ {
		if !b.appendEntry(streamID{1, uint64(i)}, fields) {
			break
		}
		added++
	}
	if added >= blockCap {
		t.Fatalf("byte budget did not bind before the entry cap (added %d)", added)
	}
	if b.size() > blockBudget {
		t.Fatalf("block grew to %d bytes, past the %d budget", b.size(), blockBudget)
	}
	if b.size()+b.frameLen(streamID{1, uint64(added)}, fields, true) <= blockBudget {
		t.Fatalf("block sealed early: another fat entry would still have fit")
	}
}

func TestMasterAlwaysLandsEvenWhenFat(t *testing.T) {
	// An empty block must accept its first entry even if it exceeds the budget;
	// the fat-entry solo-block decision belongs to the caller (section 3.7).
	b := newBlock()
	big := make([]byte, blockBudget*2)
	if !b.appendEntry(streamID{1, 0}, []field{{name: []byte("v"), value: big}}) {
		t.Fatalf("empty block rejected its master entry")
	}
	if b.entries() != 1 {
		t.Fatalf("entries=%d after the fat master, want 1", b.entries())
	}
	got := collect(b)
	if len(got) != 1 || len(got[0].fields[0].value) != len(big) {
		t.Fatalf("fat master did not round-trip")
	}
}

func TestBlockDenseIDFuzzRoundTrip(t *testing.T) {
	// Fill blocks with dense monotone IDs at a settable per-millisecond rate,
	// crossing millisecond boundaries, and assert every block round-trips.
	rng := rand.New(rand.NewSource(7))
	for _, rate := range []uint64{1000, 100, 10, 1} {
		var ms, seq uint64 = 1_700_000_000_000, 0
		want := make([]gotEntry, 0, 4096)
		b := newBlock()
		blocks := []*block{b}
		var all [][]gotEntry
		flush := func() { all = append(all, want); want = nil }
		for i := 0; i < 4096; i++ {
			if seq >= rate {
				ms++
				seq = 0
			}
			id := streamID{ms, seq}
			seq++
			fields := []field{
				f("sensor", fmt.Sprintf("s%d", rng.Intn(4))),
				f("value", fmt.Sprintf("%d", rng.Intn(100000))),
				f("flag", "1"),
			}
			if !b.appendEntry(id, fields) {
				flush()
				b = newBlock()
				blocks = append(blocks, b)
				b.appendEntry(id, fields)
			}
			cp := gotEntry{id: id}
			for _, fl := range fields {
				cp.fields = append(cp.fields, f(string(fl.name), string(fl.value)))
			}
			want = append(want, cp)
		}
		flush()
		for bi, blk := range blocks {
			got := collect(blk)
			exp := all[bi]
			if len(got) != len(exp) {
				t.Fatalf("rate %d block %d: walked %d entries, want %d", rate, bi, len(got), len(exp))
			}
			for i := range exp {
				if got[i].id != exp[i].id || !fieldsEqual(got[i].fields, exp[i].fields) {
					t.Fatalf("rate %d block %d entry %d: got %s %v want %s %v",
						rate, bi, i, got[i].id, got[i].fields, exp[i].id, exp[i].fields)
				}
			}
		}
	}
}
