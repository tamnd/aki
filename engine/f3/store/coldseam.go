package store

// The cold-region read and write seam, the cold-tier counterpart of logseam.go.
// The synchronous demote and cold-read path (cold.go) reaches its region through
// these helpers rather than s.cold directly, so the cold re-home flips one seam
// onto the .aki cold-chunk adapter (akiCold) instead of rewriting each site.
// When s.akicold is live a demote cuts a cold_chunk segment in the shared .aki
// and a cold read serves from it; otherwise the store owns a per-shard scratch
// cold log and the offsets and reads address that file.
//
// The eager single-frame form here mirrors the value-log flip: a demote cuts one
// cold_chunk segment per record. A cold demote is already a batch at the migrator
// level (a whole quantum leaves the arena at once), so the batched form, one
// segment per quantum through akiCold.appendBatch, is the migrator-side follow-up
// once the async drain (coldstage.go) is flipped too. The async two-phase drain
// stays on the scratch path this slice: it reserves and pwrites off the owner, a
// mechanism the segment writer does not expose yet.

// hasCold reports whether a cold region exists in either form: the scratch cold
// log or the shared .aki cold-chunk region. The demote drivers gate on this so an
// aki store with no scratch cold file still demotes.
func (s *Store) hasCold() bool { return s.cold != nil || s.akicold != nil }

// coldBroken reports whether the cold region has taken a sticky write failure.
// Only the scratch log carries a sticky werr; the .aki adapter surfaces a write
// error per append instead, so an aki-backed cold region is never pre-broken.
func (s *Store) coldBroken() bool { return s.cold != nil && s.cold.werr != nil }

// coldAppend writes one whole cold frame and returns its region offset, the offset
// a cold-tier slot keeps. On an .aki store it cuts a single-frame cold_chunk
// segment through the adapter and returns the frame's absolute file offset;
// otherwise it appends to the scratch cold log.
func (s *Store) coldAppend(frame []byte) (uint64, error) {
	if s.akicold != nil {
		offs, err := s.akicold.appendBatch([][]byte{frame})
		if err != nil {
			return 0, err
		}
		return offs[0], nil
	}
	return s.cold.append(frame)
}

// coldReadInto reads n bytes of a cold frame at off into dst, reusing dst's
// capacity when it fits, the positioned cold sub-read the header, key, and value
// reads take. It routes to the .aki cold region when the store opened over one.
func (s *Store) coldReadInto(off uint64, n int, dst []byte) ([]byte, error) {
	if s.akicold != nil {
		return s.akicold.readInto(off, n, dst)
	}
	return s.cold.readInto(off, n, dst)
}

// coldRegionSize reports the cold region's appended bytes, the ColdStats figure.
func (s *Store) coldRegionSize() uint64 {
	if s.akicold != nil {
		total, _ := s.akicold.logBytes()
		return total
	}
	if s.cold == nil {
		return 0
	}
	return s.cold.tail
}
