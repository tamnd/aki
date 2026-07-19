package set

import (
	"github.com/tamnd/aki/f3srv/resp"
)

// SMEMBERS over the native band streams (spec 2064/f3/08 section 3.5 lineage):
// a million-member set must not build one giant reply buffer, so the reply is
// framed on the fly and pumped through the shard's streaming ring, the same
// bounded window a chunked GET rides. The stream carries a snapshot of the
// draw-vector ordinals taken at command time plus a pin on the table, so the
// member bytes it reads from the live slab stay put while it drains (member.go,
// the streams field): record reuse and slab compaction both stand down until
// the last stream closes. The reply is the set as of the command, which is what
// SMEMBERS promises; a member removed after the snapshot is still returned, a
// member added after it is not.

// membersStream is the StreamRaw source for one SMEMBERS: it yields the whole
// multi-bulk reply, the array header and every element, already framed. The
// encoder is resumable across Next calls: buf holds the element being emitted
// and off is how much of it has already gone out, so an element wider than a
// chunk straddles the boundary without materializing more than itself.
type membersStream struct {
	ht   *htable
	ords []uint32

	idx     int    // next snapshot ordinal to frame
	buf     []byte // the element currently being emitted (header, then bulks)
	off     int    // bytes of buf already copied to the wire
	started bool   // array header framed into buf yet
}

// Next fills dst with the next run of reply bytes and returns the count, zero
// once the reply is exhausted. It frames one element at a time into buf and
// copies from there, so its working set is one member plus the chunk, never the
// whole set.
func (m *membersStream) Next(dst []byte) (int, error) {
	n := 0
	for n < len(dst) {
		// Drain the straddle buffer first: an element wider than the space left
		// in the previous chunk is carried here and resumed a chunk at a time.
		if m.off < len(m.buf) {
			c := copy(dst[n:], m.buf[m.off:])
			m.off += c
			n += c
			continue
		}
		switch {
		case !m.started:
			m.buf = resp.AppendArrayHeader(m.buf[:0], len(m.ords))
			m.started = true
			m.off = 0
		case m.idx < len(m.ords):
			mb := m.ht.memberByOrd(m.ords[m.idx])
			m.idx++
			// Fast path: when the whole bulk frame fits in the space left in dst,
			// frame it straight onto the wire, skipping the copy through buf. The
			// fit check guarantees dst's capacity, so slices.Grow inside AppendBulk
			// reuses dst's backing and writes in place, and the common member never
			// pays a second copy. Only a member that crosses the chunk boundary
			// falls to the straddle path below.
			if bulkFrameLen(len(mb)) <= int64(len(dst)-n) {
				n = len(resp.AppendBulk(dst[:n], mb))
				continue
			}
			m.buf = resp.AppendBulk(m.buf[:0], mb)
			m.off = 0
		default:
			return n, nil // every element framed and copied
		}
	}
	return n, nil
}

// Release unpins the table when the stream drains, on the owner goroutine off
// the pump (stream.finish). It is the match to the pinStream the handler took,
// and it lets record reuse and slab compaction resume.
func (m *membersStream) Release() { m.ht.unpinStream() }

// membersTotal is the exact byte width of the SMEMBERS reply over the native
// band: the array header plus every member's bulk frame. The handler needs it
// to decide whether the reply is worth streaming and to commit the wire to a
// length before the first chunk goes out.
func (h *htable) membersTotal() int64 {
	tot := int64(arrayHeaderLen(h.vlen()))
	for i := 0; i < h.vlen(); i++ {
		tot += bulkFrameLen(h.mlenByOrd(h.ordAt(i)))
	}
	return tot
}

// pinMembersStream snapshots the live draw-vector ordinals, pins the table, and
// returns the stream source. The snapshot is 4 bytes per member, the only copy
// the enumeration makes; the member bytes themselves are never duplicated.
func (h *htable) pinMembersStream() *membersStream {
	n := h.vlen()
	ords := make([]uint32, n)
	for i := 0; i < n; i++ {
		ords[i] = h.ordAt(i)
	}
	h.pinStream()
	return &membersStream{ht: h, ords: ords}
}

// decLen is the decimal digit count of a non-negative n, for sizing a RESP
// frame without formatting it.
func decLen(n int) int {
	d := 1
	for n >= 10 {
		n /= 10
		d++
	}
	return d
}

// arrayHeaderLen is the width of a *N\r\n header.
func arrayHeaderLen(n int) int { return 1 + decLen(n) + 2 }

// bulkFrameLen is the width of a $L\r\n<L bytes>\r\n bulk.
func bulkFrameLen(l int) int64 { return int64(1 + decLen(l) + 2 + l + 2) }
