package setalgebra

import (
	"strconv"
	"testing"
)

// This file reproduces the three lessons in doc.go with plain-Go models. None of
// it imports aki: the point is to isolate the mechanism, not to re-measure the
// real command. Every fixture is built before b.ResetTimer so the timed region
// is only the operation under test, which is lesson 3 in practice.

const (
	// labN is the per-set member count. The probe table is sized to ~0.5 load
	// factor, so the table is labN*16 bytes of uint64 slots; at labN=1<<20 that
	// is a 16 MiB index, comfortably past L2 on the machines these labs target,
	// which is what makes the probe memory-bound the way the real composite index
	// is. Shrink it for a quick run; the direction holds well below cache size,
	// it just narrows.
	labN = 1 << 20
)

// buildMembers returns count distinct member byte slices, m0..m{count-1}. Reused
// as the source sets and the probe keys so a lab never hashes a fresh string in
// the timed loop.
func buildMembers(count int) [][]byte {
	ms := make([][]byte, count)
	for i := range ms {
		ms[i] = []byte("member:" + strconv.Itoa(i))
	}
	return ms
}

// fnv1a is a small, fast byte hash. It stands in for the real engine's word-at-a-
// time composite-key hash; the lab only needs a hash that spreads members across
// the table, not the exact one.
func fnv1a(b []byte) uint64 {
	h := uint64(1469598103934665603)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// probeTable is an open-addressed set of member hashes, the lab's model of the
// shared composite index the real SINTER probes. Slot 0 means empty; a real
// index stores a tombstone/occupancy bit, but for a build-once, probe-many lab a
// sentinel is enough and keeps the probe a plain (non-atomic) load so the lab
// measures cache behavior, not the acquire barrier.
type probeTable struct {
	slots []uint64
	mask  uint64
}

func newProbeTable(members [][]byte) *probeTable {
	// Round up to a power of two at ~0.5 load factor.
	n := 1
	for n < len(members)*2 {
		n <<= 1
	}
	t := &probeTable{slots: make([]uint64, n), mask: uint64(n - 1)}
	for _, m := range members {
		h := fnv1a(m)
		if h == 0 {
			h = 1 // keep 0 as the empty sentinel
		}
		for i := h & t.mask; ; i = (i + 1) & t.mask {
			if t.slots[i] == 0 {
				t.slots[i] = h
				break
			}
			if t.slots[i] == h {
				break
			}
		}
	}
	return t
}

func (t *probeTable) has(h uint64) bool {
	if h == 0 {
		h = 1
	}
	for i := h & t.mask; ; i = (i + 1) & t.mask {
		s := t.slots[i]
		if s == 0 {
			return false
		}
		if s == h {
			return true
		}
	}
}

// encodeBulk appends one RESP bulk string for m, the lab's model of writeBulk:
// a length prefix (with the strconv the real one runs) plus the payload. It is
// the work that, interleaved into the probe loop, evicts the index cache lines.
func encodeBulk(dst, m []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(m)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, m...)
	dst = append(dst, '\r', '\n')
	return dst
}

// writeArrayHeader appends "*<n>\r\n" up front, the model of the buffered form
// that knows its count before it writes any element and pays no shift.
func writeArrayHeader(dst []byte, n int) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

// spliceHeader inserts "*<n>\r\n" at pos ahead of the elements already written,
// shifting the whole payload right by the header width. It is the model of the
// real spliceArrayHeader: the price the streaming/deferred-length form pays for
// not knowing its count until the walk is done. On a large result that shift is a
// multi-megabyte memmove, the cost the interleaved probe bench was missing.
func spliceHeader(out []byte, pos, n int) []byte {
	var hb [24]byte
	h := hb[:0]
	h = append(h, '*')
	h = strconv.AppendInt(h, int64(n), 10)
	h = append(h, '\r', '\n')
	hl := len(h)
	oldLen := len(out)
	out = append(out, hb[:hl]...)
	copy(out[pos+hl:], out[pos:oldLen])
	copy(out[pos:], hb[:hl])
	return out
}

// intersectFixture builds two sets that overlap by half, plus the probe table for
// the second, and returns the driver (first set) to walk. Intersection ~= labN/2.
func intersectFixture() (driver [][]byte, table *probeTable) {
	all := buildMembers(labN + labN/2)
	setA := all[:labN]
	setB := all[labN/2 : labN/2+labN]
	return setA, newProbeTable(setB)
}

// BenchmarkProbeInterleaved streams each qualifying member into the reply as the
// probe finds it: probe and encode share one loop, and the array header is spliced
// in at the end because the count is not known until the walk finishes. This is
// the deferred-length shape that regressed the real SINTER. Two costs ride along
// that two-phase does not pay: the encode work interleaved into the probe loop,
// and the whole-payload memmove of the splice.
func BenchmarkProbeInterleaved(b *testing.B) {
	driver, table := intersectFixture()
	out := make([]byte, 0, labN*16)
	for b.Loop() {
		out = out[:0]
		hdrPos := len(out)
		count := 0
		for _, m := range driver {
			if table.has(fnv1a(m)) {
				out = encodeBulk(out, m)
				count++
			}
		}
		out = spliceHeader(out, hdrPos, count)
		sinkBytes = len(out)
	}
}

// BenchmarkProbeTwoPhase buffers the qualifying members in a tight append loop,
// then writes the header (count now known, no shift) and encodes in a separate
// pass. The probe loop's working set stays small and the index stays hot, which
// is why this wins on a memory-bound probe.
func BenchmarkProbeTwoPhase(b *testing.B) {
	driver, table := intersectFixture()
	out := make([]byte, 0, labN*16)
	buf := make([][]byte, 0, labN)
	for b.Loop() {
		buf = buf[:0]
		for _, m := range driver {
			if table.has(fnv1a(m)) {
				buf = append(buf, m)
			}
		}
		out = out[:0]
		out = writeArrayHeader(out, len(buf))
		for _, m := range buf {
			out = encodeBulk(out, m)
		}
		sinkBytes = len(out)
	}
}

// unionFixture builds two sets overlapping by half; distinct union ~= 3*labN/2.
func unionFixture() (a, b [][]byte) {
	all := buildMembers(labN + labN/2)
	return all[:labN], all[labN/2 : labN/2+labN]
}

// BenchmarkUnionTwoPass models the old SUNION: build the whole dedup set once to
// count for the array header, then rebuild it again to emit. The dedup build is
// the dominant cost, so doing it twice roughly doubles the command.
func BenchmarkUnionTwoPass(b *testing.B) {
	setA, setB := unionFixture()
	hint := len(setA) + len(setB)
	out := make([]byte, 0, hint*16)
	for b.Loop() {
		count := 0
		seen := make(map[string]struct{}, hint)
		for _, src := range [][][]byte{setA, setB} {
			for _, m := range src {
				if _, ok := seen[string(m)]; !ok {
					seen[string(m)] = struct{}{}
					count++
				}
			}
		}
		out = out[:0]
		seen2 := make(map[string]struct{}, hint)
		for _, src := range [][][]byte{setA, setB} {
			for _, m := range src {
				if _, ok := seen2[string(m)]; !ok {
					seen2[string(m)] = struct{}{}
					out = encodeBulk(out, m)
				}
			}
		}
		sinkBytes = len(out) + count
	}
}

// BenchmarkUnionSinglePass models the new SUNION: walk the sources once, buffer
// the distinct members, frame from the buffer length. One dedup build instead of
// two.
func BenchmarkUnionSinglePass(b *testing.B) {
	setA, setB := unionFixture()
	hint := len(setA) + len(setB)
	out := make([]byte, 0, hint*16)
	buf := make([][]byte, 0, hint)
	for b.Loop() {
		buf = buf[:0]
		seen := make(map[string]struct{}, hint)
		for _, src := range [][][]byte{setA, setB} {
			for _, m := range src {
				if _, ok := seen[string(m)]; !ok {
					seen[string(m)] = struct{}{}
					buf = append(buf, m)
				}
			}
		}
		out = out[:0]
		for _, m := range buf {
			out = encodeBulk(out, m)
		}
		sinkBytes = len(out)
	}
}

// sinkBytes keeps the compiler from optimizing the encoded output away.
var sinkBytes int
