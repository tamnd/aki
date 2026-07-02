package f1srv

import (
	"encoding/binary"
	"testing"

	"github.com/tamnd/aki/engine/f1raw"
)

// These two benchmarks quantify the one number that decides the large-list value-scan model:
// the per-element cost of a forward value scan in a contiguous packed layout (what Redis walks
// for LPOS, LINSERT-pivot, and LREM) versus an element-per-row layout (what the f1raw list stores
// today and what the order-statistic model would keep storing). The scan-bound list commands on a
// large single list are dominated by this walk, so the ratio of these two costs, combined with the
// parallelism aki already has at pipeline depth 16, is what says whether an element-per-row model
// can reach the 2x bar on those commands or whether the list needs packed nodes.
//
// The comparison is deliberately apples to apples: both scan the same number of elements of the
// same size looking for a value that is not present (the worst case LPOS/LREM pay when the target
// is near the tail), and both do the same length-prefixed compare per element. The only difference
// is the memory layout: one blob walked front to back, versus one f1raw record per element reached
// through the ordered index's forward pointers and the arena.

const scanBenchN = 200000 // elements in the probe list; large enough to amortize seek, small enough to run fast
const scanBenchVal = 16   // element value width, the small-element case the value scans hit

// buildContiguousList packs scanBenchN elements, each a 4-byte length prefix then scanBenchVal
// value bytes, into one blob, the shape a listpack/quicklist node stores.
func buildContiguousList() []byte {
	buf := make([]byte, 0, scanBenchN*(4+scanBenchVal))
	var elem [scanBenchVal]byte
	for i := 0; i < scanBenchN; i++ {
		binary.LittleEndian.PutUint32(elem[:4], uint32(i))
		var lp [4]byte
		binary.LittleEndian.PutUint32(lp[:], uint32(scanBenchVal))
		buf = append(buf, lp[:]...)
		buf = append(buf, elem[:]...)
	}
	return buf
}

// scanContiguous walks the packed blob comparing each element to target, returning the index or -1.
func scanContiguous(blob, target []byte) int {
	off := 0
	idx := 0
	for off < len(blob) {
		n := int(binary.LittleEndian.Uint32(blob[off:]))
		off += 4
		if n == len(target) && string(blob[off:off+n]) == string(target) {
			return idx
		}
		off += n
		idx++
	}
	return -1
}

// BenchmarkScanContiguous is the Redis-side per-element scan cost: one contiguous walk.
func BenchmarkScanContiguous(b *testing.B) {
	blob := buildContiguousList()
	target := make([]byte, scanBenchVal)
	target[8] = 0xff // byte 8 is always zero in a real element (only bytes 0..3 carry the index), so never matches
	b.SetBytes(int64(scanBenchN))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if scanContiguous(blob, target) != -1 {
			b.Fatal("unexpected match")
		}
	}
	b.StopTimer()
	perElem := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / float64(scanBenchN)
	b.ReportMetric(perElem, "ns/elem")
}

// elemCompositeKey builds the element-per-row composite key uvarint(len(lkey)) | lkey | orderKey,
// the same shape the list stores each element under.
func elemCompositeKey(lkey []byte, seq int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(lkey)))
	k := make([]byte, 0, n+len(lkey)+8)
	k = append(k, tmp[:n]...)
	k = append(k, lkey...)
	k = append(k, orderKeyEnd(seq)...)
	return k
}

// BenchmarkScanElementPerRow is the aki-side per-element scan cost today: walk the ordered index's
// forward pointers, and for each element read its value from the arena and compare. This is the
// best case an element-per-row model can do, better than the current per-position GetKind because
// it follows the ordered forward pointer instead of recomputing a hash and probing a bucket, yet it
// still pays a pointer chase to a heap-scattered index node and a scattered arena read per element.
func BenchmarkScanElementPerRow(b *testing.B) {
	store := f1raw.New(1<<20, 1<<30)
	defer store.Close()
	lkey := []byte("list:probe")
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(lkey)))
	prefix := make([]byte, 0, n+len(lkey))
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, lkey...)

	var elem [scanBenchVal]byte
	for i := 0; i < scanBenchN; i++ {
		binary.LittleEndian.PutUint32(elem[:4], uint32(i))
		key := elemCompositeKey(lkey, int64(i))
		if _, err := store.PutKind(key, elem[:], kindListElem); err != nil {
			b.Fatal(err)
		}
		store.CollInsert(key, kindListElem)
	}

	target := make([]byte, scanBenchVal)
	target[8] = 0xff // byte 8 is always zero in a real element, so never matches
	b.SetBytes(int64(scanBenchN))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		found := scanElementPerRow(store, prefix, target)
		if found {
			b.Fatal("unexpected match")
		}
	}
	b.StopTimer()
	perElem := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / float64(scanBenchN)
	b.ReportMetric(perElem, "ns/elem")
}

// scanElementPerRow walks the whole prefix through the ordered index in batches, reading each
// element's value from its record offset and comparing, the value search an element-per-row LPOS
// runs. It returns whether target was found.
func scanElementPerRow(store *f1raw.Store, prefix, target []byte) bool {
	const batch = 512
	var after []byte
	keys := make([][]byte, 0, batch)
	offs := make([]uint64, 0, batch)
	vbuf := make([]byte, 0, 64)
	for {
		keys = keys[:0]
		offs = offs[:0]
		var last []byte
		keys, offs, last = store.CollScanKV(prefix, after, batch, keys, offs)
		if len(offs) == 0 {
			return false
		}
		for _, off := range offs {
			v := store.ReadValueAt(off, vbuf[:0])
			if len(v) == len(target) && string(v) == string(target) {
				return true
			}
		}
		if last == nil || len(offs) < batch {
			return false
		}
		after = append(after[:0], last...)
	}
}
