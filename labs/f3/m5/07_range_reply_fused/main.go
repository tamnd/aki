// Lab 07: fused XRANGE reply build vs the two-phase gather-then-encode (doc 14
// section 6.3, M5 lab 07).
//
// The question: XRANGE and XREVRANGE resolve their bounds to a [lo, hi] window
// and walk the block band's entries in order. The block walk yields each entry's
// field headers as views into a scratch slice it reuses per entry, so the views
// are only valid until the walk decodes the next entry. The original reply build
// took two passes over that: a forward walk that gathered every in-window entry
// into a []rangeEntry, cloning the field headers so a gathered entry survived the
// scratch reuse, then a second pass that RESP-encoded the gathered slice into the
// reply buffer. That is three allocation sources on the resident hot path: the
// []rangeEntry slice grows, cloneFields allocates one header slice per entry, and
// the reply buffer grows, and a box CPU profile of a card-10k XRANGE read them
// back as ~43% of the on-CPU time (growslice 36% cum, mallocgc 17% cum, cloneFields
// 7% cum, memmove 21%), against a 0.76x/0.88x gate loss vs redis/valkey.
//
// The fix fuses the two passes: a single forward walk frames each entry straight
// into the reply buffer as the walk yields it, before the next entry's decode
// reuses the scratch, so no []rangeEntry and no per-entry clone are needed (the
// same reply-copy elision the whole-collection reads use, set/smembers.go and
// hash/hgetall.go). The RESP array header needs the entry count first, so the body
// is built at a remembered offset and the header shifted in with one memmove once
// the count is known. That trades the per-entry clone + gather-slice growth + a
// second encode pass for one body-sized memmove per reply.
//
// This lab prices the trade directly. It models the engine's walk faithfully: a
// block whose walk yields field-header views out of a scratch slice it overwrites
// per entry, so the two-phase arm MUST clone to stay correct (the same reason
// cloneFields exists in range.go) and the fused arm must consume each entry before
// the next decode. Both arms reuse the reply buffer across calls, exactly as the
// shard reuses cx.Aux, so the measured delta is the gather slice + the per-entry
// clones + the second pass, minus the one header-shift memmove the fused arm adds.
//
// Two arms over the identical entry source and reused reply buffer:
//
//	two-phase  gather []rangeEntry with cloneFields, then RESP-encode the slice
//	fused      one walk framing each entry into the reply, prepend the header
//
// Read: allocs/op, bytes/op, and ns/op per reply across a window sweep. The fused
// arm should show a flat two allocs/op (the reply buffer's own growth, amortized
// to near zero once the reused buffer is warm) against the two-phase arm's
// per-entry clone allocs, and a lower ns/op once the window is more than a handful
// of entries. See README.md for the tables and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// field is one entry field header, a name and value view. In the engine these
// alias the block blob; here they alias the source arena, and the walk hands out
// views into a reused scratch so the two arms face the same aliasing the real
// walk imposes.
type field struct {
	name  []byte
	value []byte
}

// streamID mirrors the engine ID: a millisecond and a sequence.
type streamID struct {
	ms  uint64
	seq uint64
}

// source is a synthetic entry log: per-entry field headers over one backing
// arena, the shape a block's blob decodes to. The walk reads it and yields views
// out of a per-entry scratch it reuses, modeling walkIn's scratch reuse.
type source struct {
	ids    []streamID
	fields [][]field
}

// walk yields each entry's ID and field headers in order, reusing scratch across
// entries the way the engine's walkIn reuses its decode scratch. fn returns false
// to stop. The views handed to fn are only valid until the next call, so a caller
// that keeps them past one call must copy, exactly the constraint range.go's
// cloneFields answers.
func (s *source) walk(scratch []field, fn func(id streamID, fields []field) bool) {
	for i := range s.ids {
		scratch = scratch[:0]
		scratch = append(scratch, s.fields[i]...) // reuse: overwrite last entry's views
		if !fn(s.ids[i], scratch) {
			return
		}
	}
}

// rangeEntry is the gathered pair the two-phase arm builds, ID plus a cloned
// field-header slice, mirroring range.go's rangeEntry.
type rangeEntry struct {
	id     streamID
	fields []field
}

func cloneFields(fields []field) []field {
	return append([]field(nil), fields...)
}

// twoPhase builds the reply the original way: gather every entry into a
// []rangeEntry, cloning the field headers so each survives the scratch reuse,
// then RESP-encode the gathered slice. out is the reused reply buffer.
func twoPhase(s *source, out []byte) []byte {
	var gathered []rangeEntry
	var scratch []field
	s.walk(scratch, func(id streamID, fields []field) bool {
		gathered = append(gathered, rangeEntry{id: id, fields: cloneFields(fields)})
		return true
	})
	out = appendArrayHeader(out[:0], len(gathered))
	for i := range gathered {
		out = appendEntry(out, gathered[i].id, gathered[i].fields)
	}
	return out
}

// fused builds the reply the new way: one walk frames each entry into the reply
// buffer as the walk yields it, then the array header is shifted in once the
// count is known. No gather slice, no per-entry clone.
func fused(s *source, out []byte) []byte {
	out = out[:0]
	bodyStart := len(out)
	n := 0
	var scratch []field
	s.walk(scratch, func(id streamID, fields []field) bool {
		out = appendEntry(out, id, fields)
		n++
		return true
	})
	return prependArrayHeader(out, bodyStart, n)
}

// prependArrayHeader inserts a RESP array header of count n before the body bytes
// at buf[at:], shifting the body right by the header width. One memmove, the exact
// helper range.go ships.
func prependArrayHeader(buf []byte, at, n int) []byte {
	var hb [24]byte
	hdr := appendArrayHeader(hb[:0], n)
	w := len(hdr)
	buf = append(buf, hdr...)
	copy(buf[at+w:], buf[at:len(buf)-w])
	copy(buf[at:], hdr)
	return buf
}

// appendArrayHeader and appendBulk are the minimal RESP writers the lab needs,
// byte-identical in shape to resp.AppendArrayHeader / resp.AppendBulk so the
// reply-buffer growth the two arms pay is the same growth the engine pays.
func appendArrayHeader(dst []byte, n int) []byte {
	dst = append(dst, '*')
	dst = appendUint(dst, n)
	return append(dst, '\r', '\n')
}

func appendBulk(dst, b []byte) []byte {
	dst = append(dst, '$')
	dst = appendUint(dst, len(b))
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}

func appendUint(dst []byte, n int) []byte {
	return append(dst, strconv.Itoa(n)...)
}

// appendEntry writes one [id, [name value ...]] pair, the shape appendEntryReply
// writes in range.go.
func appendEntry(dst []byte, id streamID, fields []field) []byte {
	dst = appendArrayHeader(dst, 2)
	var idbuf [40]byte
	dst = appendBulk(dst, formatID(idbuf[:0], id))
	dst = appendArrayHeader(dst, 2*len(fields))
	for i := range fields {
		dst = appendBulk(dst, fields[i].name)
		dst = appendBulk(dst, fields[i].value)
	}
	return dst
}

func formatID(dst []byte, id streamID) []byte {
	dst = strconv.AppendUint(dst, id.ms, 10)
	dst = append(dst, '-')
	return strconv.AppendUint(dst, id.seq, 10)
}

// makeSource builds a source of n entries, each with nf fields of nb-byte names
// and vb-byte values, over one arena. Dense auto-IDs at 1000 per ms, the log
// shape XRANGE reads.
func makeSource(n, nf, nb, vb int) *source {
	s := &source{ids: make([]streamID, n), fields: make([][]field, n)}
	arena := make([]byte, 0, n*nf*(nb+vb))
	for i := 0; i < n; i++ {
		s.ids[i] = streamID{ms: uint64(i / 1000), seq: uint64(i % 1000)}
		fs := make([]field, nf)
		for j := 0; j < nf; j++ {
			ns := len(arena)
			for k := 0; k < nb; k++ {
				arena = append(arena, byte('a'+(i+j+k)%26))
			}
			vs := len(arena)
			for k := 0; k < vb; k++ {
				arena = append(arena, byte('0'+(i+j+k)%10))
			}
			fs[j] = field{name: arena[ns:vs], value: arena[vs:len(arena)]}
		}
		s.fields[i] = fs
	}
	return s
}

func main() {
	quick := flag.Bool("quick", false, "smaller windows for the shared runner")
	flag.Parse()

	windows := []int{1, 10, 100, 1000, 10000}
	if *quick {
		windows = []int{10, 100, 1000}
	}
	const nf, nb, vb = 2, 8, 16 // two 8B-named 16B-valued fields per entry

	fmt.Println("Lab 07: fused XRANGE reply build vs two-phase gather-then-encode")
	fmt.Printf("%s\n\n", runtime.Version())
	fmt.Printf("Fields/entry %d, name %dB, value %dB. Reply buffer reused across ops (cx.Aux shape).\n\n", nf, nb, vb)

	fmt.Println("allocs/op (lower is better):")
	fmt.Printf("%-8s %-14s %-14s\n", "window", "two-phase", "fused")
	for _, w := range windows {
		s := makeSource(w, nf, nb, vb)
		buf := make([]byte, 0, 1<<20)
		a2 := int(testing.AllocsPerRun(200, func() { buf = twoPhase(s, buf) }))
		bufF := make([]byte, 0, 1<<20)
		af := int(testing.AllocsPerRun(200, func() { bufF = fused(s, bufF) }))
		fmt.Printf("%-8d %-14d %-14d\n", w, a2, af)
	}

	fmt.Println("\nns/op (lower is better):")
	fmt.Printf("%-8s %-14s %-14s %-8s\n", "window", "two-phase", "fused", "speedup")
	for _, w := range windows {
		s := makeSource(w, nf, nb, vb)
		reps := timeReps(w)
		buf := make([]byte, 0, 1<<20)
		t2 := timeArm(reps, func() { buf = twoPhase(s, buf) })
		bufF := make([]byte, 0, 1<<20)
		tf := timeArm(reps, func() { bufF = fused(s, bufF) })
		fmt.Printf("%-8d %-14.0f %-14.0f %.2fx\n", w, t2, tf, t2/tf)
	}

	if !checkEqual(windows, nf, nb, vb) {
		fmt.Fprintln(os.Stderr, "\nFAIL: arms disagree on reply bytes")
		os.Exit(1)
	}
	fmt.Println("\nOK: both arms produce byte-identical replies across the sweep.")
}

// timeReps scales the repetition count so a small window still runs long enough
// to time and a large window does not run for minutes.
func timeReps(w int) int {
	switch {
	case w >= 10000:
		return 2000
	case w >= 1000:
		return 20000
	default:
		return 200000
	}
}

func timeArm(reps int, fn func()) float64 {
	// warm the reused buffer, then time.
	for i := 0; i < 50; i++ {
		fn()
	}
	start := time.Now()
	for i := 0; i < reps; i++ {
		fn()
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps)
}

// checkEqual confirms the two arms emit byte-identical replies, the invariant
// that makes the fused arm a drop-in for the two-phase one.
func checkEqual(windows []int, nf, nb, vb int) bool {
	for _, w := range windows {
		s := makeSource(w, nf, nb, vb)
		a := twoPhase(s, nil)
		b := fused(s, nil)
		if string(a) != string(b) {
			return false
		}
	}
	return true
}
