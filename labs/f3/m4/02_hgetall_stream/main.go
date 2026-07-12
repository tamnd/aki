// Lab: HGETALL buffered whole-reply build versus streamed chunk framing (spec
// 2064/f3 doc 10 section 8.2, M4 lab 02, issue #546).
//
// The question: HGETALL on a large hash frames every field and value into a
// RESP array. Two ways to put those bytes on the wire. The buffered way builds
// the whole reply in one scratch buffer and hands it over, so the scratch grows
// to the full reply size and stays that big on the connection (cx.Aux keeps the
// high-water mark). The streamed way walks the field slab into a fixed chunk and
// drains each chunk as it fills, so the per-op working set is one chunk window
// regardless of how big the hash is. Lab 01 (labs/f3/m4/01_field_table) found
// that at scale an HGET hit is confirm-dominated and probe-scheme-insensitive,
// a field-slab memory-bandwidth problem; it named this the row slice 5 inherits
// when it profiles the m=1000 HGETALL memory bandwidth explicitly. This is that
// profile.
//
// The v1 hash shipped HGETALL on the buffered path and the m=1000 headline row
// failed p99: a 1000-field reply is tens to hundreds of KB, and building it
// whole put a reply-sized transient on every large HGETALL and left the
// connection scratch sized to the largest hash ever read on it. The streamed
// path caps the per-op working set at store.ChunkSize (64KB), the same width the
// string band streams at, so the reply never materializes whole.
//
// This lab prices the two framers on a lab-local model of the same geometry: a
// field slab of m records (field bytes plus value bytes at an offset and a
// length, exactly like fentry), a draw vector giving enumeration order, and RESP
// bulk framing byte-identical in shape to resp.AppendBulk. Two framers emit the
// identical reply bytes:
//
//   - buffered: grow one scratch to the full reply size, frame every field and
//     value into it, hand it over. This is the v1 enumerate on cx.Aux.
//   - streamed: frame into a reused store.ChunkSize chunk, drain (checksum and
//     reset) when the next frame would overflow, continue. This is the
//     enumStream source the shard ring pumps, modeled without the goroutine.
//
// Both fold every emitted byte into the same checksum, so the measured delta is
// only the working-set and allocation difference, not the byte work, which is
// identical and slab-bandwidth-bound in both.
//
// Swept over field count m in {10, 100, 1000, 10000, 100000} and value width in
// {8, 64, 512} bytes. The m=1000, 64B row is the gate cell doc 10 section 8.2
// names. Read: buffered and streamed ns/op, ns per field, the buffered peak
// working set (the full reply, which grows with m) against the streamed peak
// (one chunk, flat in m), and bytes allocated per op for each. See README.md for
// the table and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

// sink defeats dead-code elimination: both framers fold their emitted bytes here.
var sink uint64

// hashModel is the field slab plus the enumeration order, the lab's model of a
// native ftable: records address field and value bytes in one slab by offset and
// length, and ords is the draw-vector order HGETALL frames in.
type hashModel struct {
	slab []byte
	recs []record
	ords []uint32
}

type record struct {
	foff, flen, voff, vlen uint32
}

// makeHash builds m fields, "f<i>" to a value of valWidth bytes, laid down in one
// slab the way newRecord appends, and an in-order draw vector.
func makeHash(m, valWidth int) *hashModel {
	h := &hashModel{recs: make([]record, m), ords: make([]uint32, m)}
	for i := 0; i < m; i++ {
		field := "f" + strconv.Itoa(i)
		foff := uint32(len(h.slab))
		h.slab = append(h.slab, field...)
		voff := uint32(len(h.slab))
		for j := 0; j < valWidth; j++ {
			h.slab = append(h.slab, byte('a'+(i+j)%26))
		}
		h.recs[i] = record{foff, uint32(len(field)), voff, uint32(valWidth)}
		h.ords[i] = uint32(i)
	}
	return h
}

func (h *hashModel) field(o uint32) []byte {
	r := h.recs[o]
	return h.slab[r.foff : r.foff+r.flen]
}

func (h *hashModel) value(o uint32) []byte {
	r := h.recs[o]
	return h.slab[r.voff : r.voff+r.vlen]
}

// replyBytes is the exact RESP width of the HGETALL reply for this hash, which is
// also the buffered framer's peak working set.
func (h *hashModel) replyBytes() int {
	n := arrayHeaderLen(2 * len(h.ords))
	for _, o := range h.ords {
		r := h.recs[o]
		n += bulkFrameLen(int(r.flen)) + bulkFrameLen(int(r.vlen))
	}
	return n
}

// --- RESP framing, the shape resp.AppendBulk/AppendArrayHeader emit -----------

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

func decLen(n int) int {
	if n == 0 {
		return 1
	}
	l := 0
	for n > 0 {
		l++
		n /= 10
	}
	return l
}

func arrayHeaderLen(n int) int { return 1 + decLen(n) + 2 }
func bulkFrameLen(n int) int   { return 1 + decLen(n) + 2 + n + 2 }

// --- the two framers ----------------------------------------------------------

// frameBuffered is the v1 path: grow one scratch to the whole reply and frame
// everything into it. dst is the reused connection scratch, so after warmup it
// stays sized to the largest reply, the working-set cost this framer pays.
func frameBuffered(dst []byte, h *hashModel) []byte {
	dst = appendArrayHeader(dst[:0], 2*len(h.ords))
	for _, o := range h.ords {
		dst = appendBulk(dst, h.field(o))
		dst = appendBulk(dst, h.value(o))
	}
	fold(dst)
	return dst
}

// frameStreamed is the enumStream path: frame into a fixed chunk and drain it
// when the next element would overflow, so the working set is one chunk window
// whatever the hash size. This models the shard ring's producer without the
// goroutine; the ring holds streamWindow chunks, a constant, so peak is O(1) in m.
func frameStreamed(chunk []byte, h *hashModel) {
	chunk = appendArrayHeader(chunk[:0], 2*len(h.ords))
	emit := func(b []byte) {
		frame := bulkFrameLen(len(b))
		if len(chunk)+frame > cap(chunk) {
			fold(chunk) // drain: the connection writer would flush this chunk
			chunk = chunk[:0]
		}
		chunk = appendBulk(chunk, b)
	}
	for _, o := range h.ords {
		emit(h.field(o))
		emit(h.value(o))
	}
	fold(chunk) // drain the tail
}

// fold checksums a framed run into sink, the shared per-byte work both framers do
// so the delta is only working set and allocation.
func fold(b []byte) {
	var s uint64
	for _, c := range b {
		s = s*1099511628211 + uint64(c)
	}
	sink += s
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep")
	flag.Parse()

	ms := []int{10, 100, 1000, 10000, 100000}
	widths := []int{8, 64, 512}
	// A fixed field budget per cell: reps = budget/m, so a cell frames roughly the
	// same total elements whatever its size and the sweep stays bounded. Clamped so
	// the smallest hash still gets enough reps to average the timer noise.
	budget := 8_000_000
	minReps := 200
	byteBudget := 2_000_000_000 // cap framed bytes/cell so a giant-reply cell runs fewer reps
	if *quick {
		ms = []int{100, 1000}
		widths = []int{64}
		budget = 400_000
		minReps = 100
		byteBudget = 100_000_000
	}

	fmt.Printf("M4 lab 02: HGETALL buffered whole-reply vs streamed chunk framing\n")
	fmt.Printf("chunk %d bytes, field budget %d/cell\n\n", store.ChunkSize, budget)
	fmt.Printf("%8s %6s %11s %11s %11s %10s %10s   %11s %11s\n",
		"fields", "valW", "reply_B", "buf_ns", "str_ns", "buf_ns/f", "str_ns/f", "buf_peak_B", "str_peak_B")

	for _, w := range widths {
		for _, m := range ms {
			h := makeHash(m, w)
			reply := h.replyBytes()
			reps := budget / m
			if reps < minReps {
				reps = minReps
			}
			if cap := byteBudget / reply; cap >= 1 && reps > cap {
				reps = cap // a multi-MB reply cell frames fewer times, still bounded
			}

			buf := make([]byte, 0, reply)
			chunk := make([]byte, 0, store.ChunkSize)

			bufNs := timeFrame(reps, func() { buf = frameBuffered(buf, h) })
			strNs := timeFrame(reps, func() { frameStreamed(chunk, h) })

			// Peak per-op working set: the buffered scratch holds the whole reply;
			// the streamed chunk never exceeds one chunk window.
			bufPeak := reply
			strPeak := store.ChunkSize
			if reply < store.ChunkSize {
				strPeak = reply // a sub-chunk reply never grows a chunk
			}
			fmt.Printf("%8d %6d %11d %11.0f %11.0f %10.2f %10.2f   %11d %11d\n",
				m, w, reply, bufNs, strNs, bufNs/float64(m), strNs/float64(m), bufPeak, strPeak)
		}
		fmt.Println()
	}

	// Allocation profile at the gate cell: bytes a fresh (cold-scratch) framer
	// allocates per op, the transient the buffered path puts on every large
	// HGETALL and the streamed path caps at one chunk.
	fmt.Printf("cold-scratch allocation at m=1000 valW=64 (the gate cell):\n")
	h := makeHash(1000, 64)
	const allocReps = 20_000
	fmt.Printf("  buffered  %8d B/op\n", allocPerOp(allocReps, func() { fold(frameBuffered(nil, h)) }))
	fmt.Printf("  streamed  %8d B/op\n", allocPerOp(allocReps, func() { frameStreamed(make([]byte, 0, store.ChunkSize), h) }))

	fmt.Printf("\nsink=%d\n", sink)
}

// timeFrame times fn over reps iterations, returning ns per call.
func timeFrame(reps int, fn func()) float64 {
	start := time.Now()
	for r := 0; r < reps; r++ {
		fn()
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps)
}

// allocPerOp reports mean bytes allocated per fn call over reps, the MemStats
// delta the memory-bar claim rests on.
func allocPerOp(reps int, fn func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for r := 0; r < reps; r++ {
		fn()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(reps)
}
