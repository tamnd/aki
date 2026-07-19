// Lab 08: fused XREAD reply build vs the two-phase gather-then-encode, for the
// nested [key, [entries]] shape (doc 14 section 6.4, M5 lab 08).
//
// XREAD is XRANGE over an open lower bound, but its reply nests: the outer array
// holds one [key, entries] pair per stream that produced entries, streams with no
// new entries are omitted, and the inner entries array is the same forward walk
// XRANGE frames. Lab 07 fused the flat XRANGE reply; XREAD never got that fix, so
// its immediate read still gathered every stream's entries into a []rangeEntry
// (cloning the field headers to survive the block walk's scratch reuse) and then
// re-encoded them in frameReadResults. On a card-10k single-stream XREAD that is
// the same allocation profile lab 07 measured for XRANGE, and it read 0.87x/0.98x
// on the box gate against XRANGE's post-fix 1.05x/1.23x.
//
// The fix mirrors lab 07 through the nesting: each stream frames its [key, entries]
// pair straight into the reply during one forward walk, the inner entries-array
// header shifted in with prependArrayHeader once the entry count is known, an empty
// stream's pair rolled back by truncating to its start offset, and the outer array
// header shifted in once the non-empty-stream count is known. No []rangeEntry, no
// per-entry clone, no second encode pass.
//
// This lab prices the nested trade directly. It models the engine's walk
// faithfully: a block whose walk yields field-header views out of a scratch slice
// it overwrites per entry, so the two-phase arm MUST clone to stay correct and the
// fused arm must consume each entry before the next decode. Both arms reuse the
// reply buffer across calls, as the shard reuses cx.Aux. The sweep varies the
// stream count, entries-per-stream, and the fraction of empty streams (the omitted
// pairs the fused arm rolls back), so the empty-stream roll-back is exercised, not
// just the dense read.
//
// Two arms over the identical multi-stream source and reused reply buffer:
//
//	two-phase  gather [][]rangeEntry with cloneFields per stream, then RESP-encode
//	fused      one walk per stream framing pairs into the reply, prepend headers
//
// Read: allocs/op, bytes/op, and ns/op per reply across the sweep. The fused arm
// should show a flat handful of allocs/op (the reply buffer's own growth) against
// the two-phase arm's per-entry clones, and build the nested reply faster, with
// byte-identical replies (TestArmsAgree in the test file is the CI guard).
package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

// field mirrors engine/f3/stream.field: a name and value view. The block walk
// hands these out of a scratch slice it reuses per entry, so a view is valid only
// until the next entry decodes.
type field struct {
	name  []byte
	value []byte
}

// entry is one live stream entry: an ID and its field views.
type entry struct {
	ms, seq uint64
	fields  []field
}

// streamSrc models one stream's block: its live entries and a scratch slice the
// walk reuses per entry, exactly as engine/f3/stream.block.walk does. A read that
// keeps an entry past the next walk step must clone the field headers, the reason
// cloneFields exists in range.go.
type streamSrc struct {
	key     []byte
	entries []entry
	scratch []field
}

// walk yields each entry with its fields copied into the reused scratch, so the
// views the visitor sees are clobbered by the next call. visit returns false to
// stop early (the COUNT cap).
func (s *streamSrc) walk(visit func(ms, seq uint64, fields []field) bool) {
	for _, e := range s.entries {
		if cap(s.scratch) < len(e.fields) {
			s.scratch = make([]field, len(e.fields))
		}
		s.scratch = s.scratch[:len(e.fields)]
		copy(s.scratch, e.fields)
		if !visit(e.ms, e.seq, s.scratch) {
			return
		}
	}
}

// makeStreams builds n streams of nf-field entries each, with about emptyEvery-th
// stream empty (an omitted pair the fused arm rolls back). entriesPer is the live
// entry count in a non-empty stream, vb the value byte width.
func makeStreams(n, entriesPer, nf, vb, emptyEvery int) []*streamSrc {
	streams := make([]*streamSrc, n)
	for i := 0; i < n; i++ {
		s := &streamSrc{key: []byte("stream:" + strconv.Itoa(i))}
		empty := emptyEvery > 0 && i%emptyEvery == emptyEvery-1
		if !empty {
			for e := 0; e < entriesPer; e++ {
				fields := make([]field, nf)
				for f := 0; f < nf; f++ {
					fields[f] = field{
						name:  []byte("field" + strconv.Itoa(f)),
						value: make([]byte, vb),
					}
				}
				s.entries = append(s.entries, entry{ms: uint64(e + 1), seq: 0, fields: fields})
			}
		}
		streams[i] = s
	}
	return streams
}

// cloneFields copies the field headers the walk reuses per entry, the header-only
// clone range.go's resident path uses. Name and value bytes are not copied.
func cloneFields(fields []field) []field {
	return append([]field(nil), fields...)
}

// gathered pairs a key with the entries a stream produced, held for the second
// encode pass.
type gathered struct {
	key     []byte
	entries []entry
}

// twoPhase is the pre-fix build: gather each stream's entries into a slice with a
// per-entry field clone, drop the empty streams, then RESP-encode the survivors.
func twoPhase(streams []*streamSrc, limit int, dst []byte) []byte {
	dst = dst[:0]
	var results []gathered
	for _, s := range streams {
		var ents []entry
		n := 0
		s.walk(func(ms, seq uint64, fields []field) bool {
			ents = append(ents, entry{ms: ms, seq: seq, fields: cloneFields(fields)})
			n++
			return limit <= 0 || n < limit
		})
		if len(ents) > 0 {
			results = append(results, gathered{key: s.key, entries: ents})
		}
	}
	if len(results) == 0 {
		return appendNullArray(dst)
	}
	dst = appendArrayHeader(dst, len(results))
	for _, rr := range results {
		dst = appendArrayHeader(dst, 2)
		dst = appendBulk(dst, rr.key)
		dst = appendArrayHeader(dst, len(rr.entries))
		for _, e := range rr.entries {
			dst = appendEntry(dst, e.ms, e.seq, e.fields)
		}
	}
	return dst
}

// fused is the post-fix build: each stream frames its pair straight into the reply
// during one walk, the inner header shifted in once the entry count is known, an
// empty stream's pair rolled back, the outer header shifted in once the non-empty
// count is known. No gather slice, no per-entry clone, no second pass.
func fused(streams []*streamSrc, limit int, dst []byte) []byte {
	dst = dst[:0]
	outerStart := len(dst)
	nStreams := 0
	for _, s := range streams {
		pairStart := len(dst)
		dst = appendArrayHeader(dst, 2)
		dst = appendBulk(dst, s.key)
		bodyStart := len(dst)
		n := 0
		s.walk(func(ms, seq uint64, fields []field) bool {
			dst = appendEntry(dst, ms, seq, fields)
			n++
			return limit <= 0 || n < limit
		})
		if n == 0 {
			dst = dst[:pairStart]
			continue
		}
		dst = prependArrayHeader(dst, bodyStart, n)
		nStreams++
	}
	if nStreams == 0 {
		return appendNullArray(dst[:outerStart])
	}
	return prependArrayHeader(dst, outerStart, nStreams)
}

// appendEntry writes one [id, [field value ...]] pair, mirroring range.go's
// appendEntryReply, copying the field bytes onto the wire so the source views are
// consumed before the next decode.
func appendEntry(dst []byte, ms, seq uint64, fields []field) []byte {
	dst = appendArrayHeader(dst, 2)
	var idbuf [40]byte
	id := strconv.AppendUint(idbuf[:0], ms, 10)
	id = append(id, '-')
	id = strconv.AppendUint(id, seq, 10)
	dst = appendBulk(dst, id)
	dst = appendArrayHeader(dst, 2*len(fields))
	for i := range fields {
		dst = appendBulk(dst, fields[i].name)
		dst = appendBulk(dst, fields[i].value)
	}
	return dst
}

// prependArrayHeader inserts a RESP array header of count n before the body at
// buf[at:], shifting the body right one memmove, identical to range.go's helper.
func prependArrayHeader(buf []byte, at, n int) []byte {
	var hb [24]byte
	hdr := appendArrayHeader(hb[:0], n)
	w := len(hdr)
	buf = append(buf, hdr...)
	copy(buf[at+w:], buf[at:len(buf)-w])
	copy(buf[at:], hdr)
	return buf
}

func appendArrayHeader(dst []byte, n int) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

func appendBulk(dst, b []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}

func appendNullArray(dst []byte) []byte {
	return append(dst, '*', '-', '1', '\r', '\n')
}

type row struct {
	streams, entriesPer, empty int
	twoAllocs, fusedAllocs     float64
	twoNs, fusedNs             int64
	speedup                    float64
}

func main() {
	nf, vb, limit := 2, 64, -1
	sweep := []struct{ streams, entriesPer, empty int }{
		{1, 1, 0},
		{1, 100, 0},
		{1, 10000, 0},
		{8, 100, 0},
		{8, 1000, 0},
		{16, 500, 3}, // every 3rd stream empty: exercises the roll-back
		{64, 100, 2},
	}
	var rows []row
	for _, w := range sweep {
		streams := makeStreams(w.streams, w.entriesPer, nf, vb, w.empty)
		buf2 := make([]byte, 0, 1<<20)
		bufF := make([]byte, 0, 1<<20)
		a2 := testing.AllocsPerRun(50, func() { buf2 = twoPhase(streams, limit, buf2) })
		af := testing.AllocsPerRun(50, func() { bufF = fused(streams, limit, bufF) })
		t2 := timeBuild(func() { buf2 = twoPhase(streams, limit, buf2) })
		tf := timeBuild(func() { bufF = fused(streams, limit, bufF) })
		rows = append(rows, row{
			streams: w.streams, entriesPer: w.entriesPer, empty: w.empty,
			twoAllocs: a2, fusedAllocs: af, twoNs: t2, fusedNs: tf,
			speedup: float64(t2) / float64(tf),
		})
	}
	fmt.Println("streams entries empty | two-alloc fused-alloc | two-ns  fused-ns  speedup")
	for _, r := range rows {
		fmt.Printf("%7d %7d %5d | %9.0f %10.0f | %6d %8d  %.2fx\n",
			r.streams, r.entriesPer, r.empty, r.twoAllocs, r.fusedAllocs,
			r.twoNs, r.fusedNs, r.speedup)
	}
}

// timeBuild runs fn enough times to get a stable per-call nanosecond figure.
func timeBuild(fn func()) int64 {
	const iters = 2000
	for i := 0; i < 100; i++ { // warm
		fn()
	}
	start := time.Now()
	for i := 0; i < iters; i++ {
		fn()
	}
	return time.Since(start).Nanoseconds() / iters
}
