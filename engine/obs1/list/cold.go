package list

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/engine/obs1/tier"
)

// The list cold chunk form (spec 2064/f3/06 sections 6 and 7, plan
// M7-slice-cold-chunk-list). A list's native band is a ring of chunk slabs the
// store's arena budget cannot see, so its cold tier is a demotion pass that pushes
// whole interior chunks out of resident RAM: the chunk's live frames are packed
// into a cold-region frame (store.AppendChunk), the resident blob and directory
// are released, and the ring keeps the chunk handle with its live window so the
// count and the Fenwick directory over counts stay untouched. A demoted chunk
// carries only a cold-region offset; a read preads the frame and walks it.
//
// Unlike the set (member hash) and the zset (score), a list needs no discriminator
// search to place a cold chunk for a read. The ring walk that resolves a dense
// index already lands on the chunk handle, and the handle carries the offset
// directly, so a read never touches a directory. The shared directory keyed by a
// per-list demote sequence serves dirty tracking and recovery, not reads: the
// demote pass records one descriptor per shed chunk so an M8 recovery walk and the
// promote path can find the cold frames again. LPOS and the interior mutators reach
// cold data through their own slice; the demote pass here sheds only true interior
// chunks and never runs until the trigger wires it live.

// kindList is the collection kind byte a list chunk carries, a plain kind below
// frameChunk (store.AppendChunk sets the recovery bit itself). An M8 recovery walk
// reads it to dispatch a cold list chunk back into the list registry.
const kindList byte = 0x03

// listCold is a list's cold-tier state, built on the first demote and held on the
// native band. st is the store the cold frames live in and scratch is the pread
// buffer every cold read reuses, so a steady cold read allocates nothing. dir is
// the demote-sequence directory: one descriptor per shed chunk keyed by the demote
// order (seq), the record recovery and the promote path read, since a read itself
// rides the handle offset and never touches it. seq is the per-list monotonic
// demote counter that keys those descriptors in insertion order. Owner-local, so
// nothing locks.
type listCold struct {
	st      *store.Store
	dir     tier.Directory
	seq     uint64
	scratch []byte
	frames  [][]byte // reused decode buffer for the scan cold branches, so a scan
	// crossing several cold chunks allocates nothing; excluded from residentBytes
	// like scratch, since it grows on read not mutation.
}

// residentBytes is the cold state's own resident footprint: the demote-sequence
// directory. The demoted chunk blobs are already gone from the ring's count
// (native.residentBytes drops a demoted chunk's footprint), so this adds only the
// directory the cold state itself keeps resident, matching the set and zset cold
// forms. The pread scratch is left out on purpose, the same bounded per-read buffer
// those forms exclude to keep the running total from drifting between commands.
func (lc *listCold) residentBytes() uint64 {
	return uint64(lc.dir.Bytes())
}

// payload preads the cold chunk at off into the shared scratch and returns its
// packed-frame payload. The bytes alias scratch and are valid only until the next
// cold read on this list, the single-call lifetime a resident blob alias already
// carries. It returns nil on a torn frame, which a caller reads as an empty chunk.
func (lc *listCold) payload(off uint64) []byte {
	ck, buf, ok := lc.st.ReadChunk(off, lc.scratch)
	lc.scratch = buf
	if !ok {
		return nil
	}
	return ck.Payload
}

// appendFrame packs one value into a cold payload: an unsigned-varint length then
// the raw bytes, byte-identical to the frame the resident blob stores
// (chunk.writeFrame). A cold read walks the payload exactly as a resident read
// walks the blob, so the demote pass packs with this and the read side needs no
// separate decode.
func appendFrame(payload, v []byte) []byte {
	payload = binary.AppendUvarint(payload, uint64(len(v)))
	return append(payload, v...)
}

// coldFrameAt returns the value of the ord-th frame in a cold chunk's packed
// payload, walking the length-prefixed frames from the front. A list chunk holds
// at most chunkElemCap frames, so the walk is bounded; a sequential reader
// (rangeInto) walks the payload once instead of calling this per element. The
// returned bytes alias payload.
func coldFrameAt(payload []byte, ord int) []byte {
	off := 0
	for i := 0; i < ord; i++ {
		vlen, w := binary.Uvarint(payload[off:])
		off += w + int(vlen)
	}
	vlen, w := binary.Uvarint(payload[off:])
	return payload[off+w : off+w+int(vlen)]
}

const (
	// demoteMargin is how many chunks the demote pass keeps resident at each end of
	// the ring, the head and tail chunks every LPUSH, RPUSH, LPOP, and RPOP reaches,
	// so an end operation never preads a cold chunk. One chunk each suffices (a push
	// past a full end chunk spills into a fresh resident one, a pop drains one), and
	// a second absorbs push/pop oscillation at the boundary; it is the list analogue
	// of the zset demote quantum, an F9 lab knob the trigger slice tunes.
	demoteMargin = 1
	// demoteQuantum bounds the interior chunks one demote pass sheds, so the trigger
	// drains a long ring a bounded run at a time rather than in one unbounded sweep
	// that could stall the owner. The trigger calls the pass again while the list
	// still overshoots the resident cap.
	demoteQuantum = 8
)

// demote packs a bounded run of the ring's interior chunks into the cold region,
// releases their resident blobs and directories, and returns how many chunks it
// shed. It keeps demoteMargin chunks resident at each end and sweeps the interior in
// ring order, up to demoteQuantum chunks per call. Unlike the set (member hash) and
// the zset (score), a list chunk demotes whole: its live frames are already
// contiguous and in order, so the pass copies them straight into one cold frame with
// no discriminator sort, keyed only by the per-list demote sequence.
//
// It appends every chunk first and commits nothing until the whole run lands: only
// on a clean append does it record each handle's cold offset, insert the demote
// descriptors, and release the blobs. A refused append abandons the run with every
// chunk still resident (the orphan frames the append-only region holds are dead
// space the compactor reclaims), so a broken cold region degrades demotion to a
// no-op rather than a torn list. Owner goroutine only.
func (nt *native) demote(st *store.Store, key []byte) int {
	if nt.ring.n <= 2*demoteMargin {
		return 0 // no interior once both ends keep their margin
	}
	if nt.cold == nil {
		nt.cold = &listCold{st: st}
	}
	type placed struct {
		ci    int
		off   uint64
		disc  [8]byte
		count int
	}
	var runs []placed
	var payload []byte
	for i := demoteMargin; i < nt.ring.n-demoteMargin && len(runs) < demoteQuantum; i++ {
		c := nt.ring.at(i)
		if c.cold() {
			continue // already cold from an earlier quantum
		}
		payload = payload[:0]
		for p := c.lo; p < c.hi; p++ {
			v, _ := c.frameAt(int(c.dir[p]))
			payload = appendFrame(payload, v)
		}
		count := c.count()
		var disc [8]byte
		binary.BigEndian.PutUint64(disc[:], nt.cold.seq+uint64(len(runs)))
		off, ok := st.AppendChunk(kindList, 0, uint16(count), key, disc[:], payload)
		if !ok {
			return 0 // broken region: abandon, every chunk stays resident
		}
		runs = append(runs, placed{ci: i, off: off, disc: disc, count: count})
	}
	if len(runs) == 0 {
		return 0 // interior already cold from earlier quanta
	}

	// Commit: one descriptor per shed chunk keyed by demote order, then flip each
	// handle to the cold form. Releasing blob and dir hands their backing arrays to
	// the allocator (not the freelist, which stays all-resident for reuse); the
	// window is canonicalized to lo == 0 so count and the Fenwick directory read the
	// cold chunk unchanged.
	for _, r := range runs {
		nt.cold.dir.Insert(r.disc[:], uint32(r.count), r.off)
		c := nt.ring.at(r.ci)
		c.blob = nil
		c.dir = nil
		c.lo, c.hi = 0, r.count
		c.coldOff = r.off
		nt.coldN++
	}
	nt.cold.seq += uint64(len(runs))
	return len(runs)
}

// hasResidentInterior reports whether the ring holds a resident interior chunk the
// demote pass could shed: a chunk between the two end margins that still carries its
// blob. It is the list analogue of the zset's hasResident, the demotable predicate
// the trigger's victim pick reads. It early-returns on the first resident interior
// chunk, so a list with a hot interior costs one probe; only a fully-cold interior
// walks the whole span, and that is off the steady no-pressure path. A ring with no
// interior (at or below both margins) reads not demotable, the empty-loop result.
// Owner goroutine only.
func (nt *native) hasResidentInterior() bool {
	for i := demoteMargin; i < nt.ring.n-demoteMargin; i++ {
		if !nt.ring.at(i).cold() {
			return true
		}
	}
	return false
}

// promote brings the cold chunk at ring index ci back resident: it preads the
// packed frames, re-materializes a resident blob and offset directory on the same
// handle so the ring position and the Fenwick counts over the chunk's live window
// are untouched, clears the cold marker, and drops the demote descriptor. It is the
// unconditional bring-up of section 7.3, the response to a write or a drain that
// reaches a cold chunk; a resident chunk is a no-op. A torn cold frame leaves the
// chunk cold (its read path still preads it), so promote never publishes a partial
// chunk. Owner goroutine only.
func (nt *native) promote(ci int) {
	c := nt.ring.at(ci)
	if !c.cold() {
		return
	}
	off := c.coldOff
	ck, buf, ok := nt.cold.st.ReadChunk(off, nt.cold.scratch)
	nt.cold.scratch = buf
	if !ok {
		return
	}
	// Decode the packed frames as value slices aliasing the pread scratch. A demoted
	// chunk holds at most chunkElemCap frames (an oversized lone frame is one), so the
	// stack array never overflows and the decode allocates nothing.
	var fs [chunkElemCap][]byte
	vals := fs[:0]
	off2 := 0
	for i := 0; i < ck.Count; i++ {
		vlen, w := binary.Uvarint(ck.Payload[off2:])
		off2 += w
		vals = append(vals, ck.Payload[off2:off2+int(vlen)])
		off2 += int(vlen)
	}
	// Re-materialize a resident slab on the handle. A lone oversized chunk keeps its
	// wider blob. loadChunk stages through the separate repack scratch, so packing
	// vals (which alias the pread scratch) into the fresh blob is a safe copy.
	blobCap := chunkBlobCap
	if len(ck.Payload) > blobCap {
		blobCap = len(ck.Payload)
	}
	c.blob = make([]byte, blobCap)
	c.dir = make([]uint16, chunkElemCap)
	c.coldOff = 0
	nt.loadChunk(c, vals)
	nt.coldN--
	// Drop the descriptor: chunks partition the demote sequence with no overlap, so a
	// Floor on this chunk's own first discriminator lands on it exactly. Guard on the
	// offset matching so a drifted directory aborts the drop rather than removing the
	// wrong descriptor.
	if idx, found := nt.cold.dir.Floor(ck.Disc); found {
		if dOff, _, _ := nt.cold.dir.At(idx); dOff == off {
			nt.cold.dir.Remove(idx)
		}
	}
}

// promoteIfCold brings the chunk at ring index ci resident when it is cold, the
// guard the interior mutators and the end drains use before they touch a chunk that
// a demote pass may have shed. It is a plain no-op on a resident chunk.
func (nt *native) promoteIfCold(ci int) {
	if nt.ring.at(ci).cold() {
		nt.promote(ci)
	}
}

// coldValues decodes the live frames of cold chunk c into the reused decode buffer
// as value slices aliasing the shared cold scratch, valid until the next cold read
// on this list. It is the cold-safe frame accessor the ring scans (each, lpos,
// findPivot) fall to when a chunk is demoted; a resident chunk never reaches here.
// The buffer is reused across chunks so a scan crossing several cold chunks
// allocates nothing.
func (nt *native) coldValues(c *chunk) [][]byte {
	p := nt.cold.payload(c.coldOff)
	fs := nt.cold.frames[:0]
	off := 0
	for i := c.count(); i > 0; i-- {
		vlen, w := binary.Uvarint(p[off:])
		off += w
		fs = append(fs, p[off:off+int(vlen)])
		off += int(vlen)
	}
	nt.cold.frames = fs
	return fs
}

// coldHasMatch reports whether cold chunk c holds a frame equal to v, the check
// LREM makes before it promotes: a scan that crosses a cold chunk without a hit
// leaves it cold, and only a chunk a removal actually lands in is brought resident.
// It streams the payload without materializing the frames.
func (nt *native) coldHasMatch(c *chunk, v []byte) bool {
	p := nt.cold.payload(c.coldOff)
	off := 0
	for i := c.count(); i > 0; i-- {
		vlen, w := binary.Uvarint(p[off:])
		off += w
		if bytesEqual(p[off:off+int(vlen)], v) {
			return true
		}
		off += int(vlen)
	}
	return false
}
