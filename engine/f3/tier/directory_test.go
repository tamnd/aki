package tier

import (
	"bytes"
	"testing"
)

// d8 encodes n as 8 big-endian bytes, an order-preserving discriminator stand-in
// (a set's member hash rides the directory this way): byte order equals numeric
// order, which is the contract the directory compares under.
func d8(n uint64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(n)
		n >>= 8
	}
	return b
}

// TestDirectoryOrderAndTotal inserts chunks out of order and asserts the array
// comes back in discriminator order with an exact running total.
func TestDirectoryOrderAndTotal(t *testing.T) {
	var dir Directory
	dir.Insert(d8(30), 3, 300)
	dir.Insert(d8(10), 1, 100)
	dir.Insert(d8(20), 2, 200)

	if dir.Len() != 3 {
		t.Fatalf("len %d, want 3", dir.Len())
	}
	if dir.Total() != 6 {
		t.Fatalf("total %d, want 6", dir.Total())
	}
	var last []byte
	for i := 0; i < dir.Len(); i++ {
		disc := dir.DiscAt(i)
		if last != nil && bytes.Compare(last, disc) >= 0 {
			t.Fatalf("chunk %d disc %x not after %x", i, disc, last)
		}
		last = disc
	}
	// The offsets tracked their discriminators through the ordered inserts.
	if off, count, _ := dir.At(0); off != 100 || count != 1 {
		t.Fatalf("chunk 0 = off %d count %d, want 100/1", off, count)
	}
	if off, count, _ := dir.At(2); off != 300 || count != 3 {
		t.Fatalf("chunk 2 = off %d count %d, want 300/3", off, count)
	}
}

// TestDirectoryInsertOverwrite covers a repeated discriminator: the descriptor is
// replaced, not duplicated, and the total swaps the old count for the new.
func TestDirectoryInsertOverwrite(t *testing.T) {
	var dir Directory
	dir.Insert(d8(10), 5, 100)
	dir.Insert(d8(10), 8, 400)
	if dir.Len() != 1 {
		t.Fatalf("len %d, want 1", dir.Len())
	}
	if dir.Total() != 8 {
		t.Fatalf("total %d, want 8", dir.Total())
	}
	if off, count, _ := dir.At(0); off != 400 || count != 8 {
		t.Fatalf("chunk 0 = off %d count %d, want 400/8", off, count)
	}
}

// TestDirectoryFloor walks the owning-chunk search across its boundaries: below
// every chunk is a resident miss, an exact first hits its own chunk, a value
// between firsts lands on the lower chunk, and a value past the last stays on the
// last.
func TestDirectoryFloor(t *testing.T) {
	var dir Directory
	dir.Insert(d8(10), 1, 100)
	dir.Insert(d8(20), 1, 200)
	dir.Insert(d8(30), 1, 300)

	if _, ok := dir.Floor(d8(5)); ok {
		t.Fatal("disc below every chunk should be a resident miss")
	}
	if idx, ok := dir.Floor(d8(10)); !ok || idx != 0 {
		t.Fatalf("exact first: idx %d ok %v, want 0/true", idx, ok)
	}
	if idx, ok := dir.Floor(d8(25)); !ok || idx != 1 {
		t.Fatalf("between firsts: idx %d ok %v, want 1/true", idx, ok)
	}
	if idx, ok := dir.Floor(d8(999)); !ok || idx != 2 {
		t.Fatalf("past last: idx %d ok %v, want 2/true", idx, ok)
	}
}

// TestDirectoryRankBefore checks the prefix sum a rank descent accumulates before
// it reads the owning chunk.
func TestDirectoryRankBefore(t *testing.T) {
	var dir Directory
	dir.Insert(d8(10), 4, 100)
	dir.Insert(d8(20), 7, 200)
	dir.Insert(d8(30), 2, 300)

	if r := dir.RankBefore(0); r != 0 {
		t.Fatalf("rank before 0 = %d, want 0", r)
	}
	if r := dir.RankBefore(2); r != 11 {
		t.Fatalf("rank before 2 = %d, want 11", r)
	}
	if r := dir.RankBefore(3); r != 13 {
		t.Fatalf("rank before end = %d, want 13", r)
	}
}

// TestDirectoryRemove drops a middle chunk and asserts the order holds and the
// total loses exactly that chunk's count.
func TestDirectoryRemove(t *testing.T) {
	var dir Directory
	dir.Insert(d8(10), 1, 100)
	dir.Insert(d8(20), 2, 200)
	dir.Insert(d8(30), 3, 300)

	dir.Remove(1)
	if dir.Len() != 2 || dir.Total() != 4 {
		t.Fatalf("after remove: len %d total %d, want 2/4", dir.Len(), dir.Total())
	}
	if !bytes.Equal(dir.DiscAt(0), d8(10)) || !bytes.Equal(dir.DiscAt(1), d8(30)) {
		t.Fatalf("remove broke order: %x %x", dir.DiscAt(0), dir.DiscAt(1))
	}
}

// TestDirectoryStatus round-trips the status bits through the descriptor.
func TestDirectoryStatus(t *testing.T) {
	var dir Directory
	dir.Insert(d8(10), 1, 100)
	dir.SetStatus(0, DescPromoting|DescDirty)
	if _, _, st := dir.At(0); st != DescPromoting|DescDirty {
		t.Fatalf("status %#x, want %#x", st, DescPromoting|DescDirty)
	}
	dir.SetStatus(0, DescDead)
	if _, _, st := dir.At(0); st != DescDead {
		t.Fatalf("status %#x, want %#x", st, DescDead)
	}
}

// TestDirectoryDiscTruncated covers a discriminator longer than the inline width:
// it is truncated to maxDisc, and the truncated prefix still orders.
func TestDirectoryDiscTruncated(t *testing.T) {
	long := bytes.Repeat([]byte("a"), maxDisc+8)
	var dir Directory
	dir.Insert(long, 1, 100)
	if got := dir.DiscAt(0); len(got) != maxDisc {
		t.Fatalf("stored disc len %d, want %d", len(got), maxDisc)
	}
}
