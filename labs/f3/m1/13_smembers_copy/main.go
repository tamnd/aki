// Lab: SMEMBERS reply-framing copy elision (spec 2064/f3 doc 08 section 3.5,
// M1 lab 13).
//
// The question: the SMEMBERS streaming encoder (engine/f3/set/smembers.go)
// frames one member at a time and pumps the reply through the shard's bounded
// chunk ring, so a million-member set never builds one giant buffer. The
// original encoder framed each member's bulk into a small scratch buffer and
// then copied that scratch into the wire chunk, a second copy of every member's
// bytes on a reply whose whole cost is byte movement. On the 10k-member gate
// cell SMEMBERS lands at 1.97x redis, a bandwidth-bound near-miss, and a
// redundant per-member copy is exactly the kind of waste that keeps it under 2x.
//
// The elision: when the whole bulk frame fits in the space left in the current
// chunk, the common case for a 64B member against a kilobyte-plus chunk, frame
// it straight onto the wire and skip the scratch copy. Only a member that
// crosses the chunk boundary falls to the scratch-and-resume path, which is what
// the scratch buffer is actually for.
//
// This lab prices the two strategies against each other: it drains a full
// SMEMBERS reply for a synthetic set through fixed-size chunks with each encoder
// and reports ns per member and bytes copied per member, so the copy the elision
// removes is visible as both time and traffic. It also asserts the two encoders
// emit byte-identical replies, since a framing change on the wire path is only
// safe if the bytes are unchanged.
//
// Method: in-process, no server, no wire, no engine import. The two Next shapes
// are reproduced verbatim from smembers.go (the drain-scratch-first loop, the
// AppendArrayHeader / AppendBulk framing), over a synthetic member slab. The
// resp framing is inlined so the lab has no engine dependency.
//
// Axes: member size {8, 16, 64, 256} bytes (int-class through the listpack-value
// cap and past it), cardinality 10000 (the gate cell), chunk size {512, 4096,
// 16384} bytes (the bounded ring window). Read: ns/member and copied-bytes/member
// for both strategies and the direct-path win. See README.md for the sweep and
// the frozen verdict.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"strconv"
	"time"
)

// --- resp framing, inlined from f3srv/resp/write.go so the lab is standalone ---

func declen(u uint64) int {
	n := 1
	for u >= 10 {
		u /= 10
		n++
	}
	return n
}

func appendArrayHeader(dst []byte, n int) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

func appendBulk(dst, v []byte) []byte {
	hl := declen(uint64(len(v)))
	total := 1 + hl + 2 + len(v) + 2
	n := len(dst)
	for cap(dst) < n+total {
		dst = append(dst[:cap(dst)], 0)
	}
	dst = dst[:n+total]
	dst[n] = '$'
	putDecimal(dst[n+1:n+1+hl], uint64(len(v)))
	dst[n+1+hl] = '\r'
	dst[n+2+hl] = '\n'
	copy(dst[n+3+hl:], v)
	dst[n+total-2] = '\r'
	dst[n+total-1] = '\n'
	return dst
}

func putDecimal(dst []byte, u uint64) {
	for i := len(dst) - 1; i >= 0; i-- {
		dst[i] = byte('0' + u%10)
		u /= 10
	}
}

func bulkFrameLen(l int) int64 { return int64(1 + declen(uint64(l)) + 2 + l + 2) }

// --- the two encoders, reproduced from smembers.go ---

// stream is the shared source: the member slab and the per-drain scratch state.
type stream struct {
	members [][]byte
	idx     int
	buf     []byte
	off     int
	started bool
}

// scratchNext is the original encoder: frame every element into buf, then copy
// buf into dst. Every member's bytes move twice.
func (s *stream) scratchNext(dst []byte, copied *int64) int {
	n := 0
	for n < len(dst) {
		if s.off >= len(s.buf) {
			switch {
			case !s.started:
				s.buf = appendArrayHeader(s.buf[:0], len(s.members))
				s.started = true
				s.off = 0
			case s.idx < len(s.members):
				s.buf = appendBulk(s.buf[:0], s.members[s.idx])
				s.idx++
				s.off = 0
			default:
				return n
			}
		}
		c := copy(dst[n:], s.buf[s.off:])
		*copied += int64(c)
		s.off += c
		n += c
	}
	return n
}

// directNext is the elided encoder: when a member's whole frame fits in the space
// left in dst, frame it straight onto the wire; only a straddling member uses the
// scratch buffer.
func (s *stream) directNext(dst []byte, copied *int64) int {
	n := 0
	for n < len(dst) {
		if s.off < len(s.buf) {
			c := copy(dst[n:], s.buf[s.off:])
			*copied += int64(c)
			s.off += c
			n += c
			continue
		}
		switch {
		case !s.started:
			s.buf = appendArrayHeader(s.buf[:0], len(s.members))
			s.started = true
			s.off = 0
		case s.idx < len(s.members):
			mb := s.members[s.idx]
			s.idx++
			if bulkFrameLen(len(mb)) <= int64(len(dst)-n) {
				n = len(appendBulk(dst[:n], mb))
				continue
			}
			s.buf = appendBulk(s.buf[:0], mb)
			s.off = 0
		default:
			return n
		}
	}
	return n
}

// drainScratch and drainDirect pump a whole reply through fixed chunks and return
// the bytes copied through scratch across the reply.
func drainScratch(members [][]byte, chunk int) (out []byte, copied int64) {
	s := &stream{members: members}
	dst := make([]byte, chunk)
	for {
		n := s.scratchNext(dst, &copied)
		if n == 0 {
			break
		}
		out = append(out, dst[:n]...)
	}
	return out, copied
}

func drainDirect(members [][]byte, chunk int) (out []byte, copied int64) {
	s := &stream{members: members}
	dst := make([]byte, chunk)
	for {
		n := s.directNext(dst, &copied)
		if n == 0 {
			break
		}
		out = append(out, dst[:n]...)
	}
	return out, copied
}

// drainScratchWire and drainDirectWire pump a whole reply through a single
// reused chunk and discard each filled chunk, the way the real shard pump does:
// the wire chunk is sent to the socket and reused, never reassembled into one
// buffer. This isolates the framing cost the elision actually changes, without
// the growing-reassembly allocation that would swamp it in a naive drain.
func drainScratchWire(members [][]byte, chunk int) (total int, copied int64) {
	s := &stream{members: members}
	dst := make([]byte, chunk)
	for {
		n := s.scratchNext(dst, &copied)
		if n == 0 {
			break
		}
		total += n
	}
	return total, copied
}

func drainDirectWire(members [][]byte, chunk int) (total int, copied int64) {
	s := &stream{members: members}
	dst := make([]byte, chunk)
	for {
		n := s.directNext(dst, &copied)
		if n == 0 {
			break
		}
		total += n
	}
	return total, copied
}

func makeMembers(card, size int) [][]byte {
	m := make([][]byte, card)
	for i := range m {
		b := make([]byte, size)
		putDecimal8(b, uint64(i))
		m[i] = b
	}
	return m
}

func putDecimal8(b []byte, u uint64) {
	for i := range b {
		b[i] = byte('a' + (u+uint64(i))%26)
	}
}

func timeDrain(fn func([][]byte, int) ([]byte, int64), members [][]byte, chunk, iters int) (nsPerMember float64, copiedPerMember float64) {
	var copied int64
	start := time.Now()
	for i := 0; i < iters; i++ {
		_, copied = fn(members, chunk)
	}
	el := time.Since(start)
	nsPerMember = float64(el.Nanoseconds()) / float64(iters) / float64(len(members))
	copiedPerMember = float64(copied) / float64(len(members))
	return
}

func main() {
	iters := flag.Int("iters", 2000, "drain repetitions per cell")
	flag.Parse()

	card := 10000
	sizes := []int{8, 16, 64, 256}
	chunks := []int{512, 4096, 16384}

	fmt.Printf("SMEMBERS reply-framing copy elision, card=%d, iters=%d\n", card, *iters)
	fmt.Printf("%-6s %-6s | %-18s | %-18s | %-8s\n", "size", "chunk", "scratch ns/cp-B", "direct ns/cp-B", "ns win")
	for _, size := range sizes {
		members := makeMembers(card, size)
		// correctness: identical bytes for every chunk size.
		for _, chunk := range chunks {
			a, _ := drainScratch(members, chunk)
			b, _ := drainDirect(members, chunk)
			if !bytes.Equal(a, b) {
				fmt.Printf("MISMATCH size=%d chunk=%d: %d vs %d bytes\n", size, chunk, len(a), len(b))
				return
			}
		}
		for _, chunk := range chunks {
			sNs, sCp := timeDrain(drainScratch, members, chunk, *iters)
			dNs, dCp := timeDrain(drainDirect, members, chunk, *iters)
			fmt.Printf("%-6d %-6d | %6.1f / %6.1f  | %6.1f / %6.1f  | %5.1f%%\n",
				size, chunk, sNs, sCp, dNs, dCp, (sNs-dNs)/sNs*100)
		}
	}
}
