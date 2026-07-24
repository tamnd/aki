package stream

// XTRIM and the XADD trim clause (spec 2064/f3/14 section 6.6). A trim removes
// entries from the front, the oldest, by one of two thresholds:
//
//   - MAXLEN n: keep at most n live entries, drop the oldest length-n.
//   - MINID id: drop every live entry with an ID below id.
//
// and in one of two modes:
//
//   - approximate (~): drop whole front blocks while doing so stays at or above
//     the threshold, then stop; the reply may leave slightly more than the
//     threshold asks, the price of never re-encoding a block. Each drop is one
//     directory delete plus the freeing of one block, so removing 10k entries
//     that live in ~80 blocks is ~80 deletes, not 10k (section 6.6).
//   - exact (=, the default): the approximate whole-block drop, then tombstoning
//     the overshoot inside the boundary block, its oldest live entries, which
//     pays the flag writes now and reclaims the bytes when that block later
//     empties or is gc-rewritten.
//
// LIMIT n caps the entries a single call removes, so a giant trim cannot
// monopolize the owner's batch; it is valid only with ~, matching Redis. The
// inline band has one block and no directory, so it always front-splices the
// blob (rebuilds it from the survivors), which also compacts any tombstones.

// maxInt is the unbounded LIMIT sentinel for the internal drivers.
const maxInt = int(^uint(0) >> 1)

// trimKind selects the threshold a trim measures against.
type trimKind uint8

const (
	trimMaxlen trimKind = iota
	trimMinid
)

// trimSpec is a parsed trim clause, shared by XTRIM and the XADD trim clause.
type trimSpec struct {
	kind     trimKind
	approx   bool // ~ given (else exact, the default)
	maxlen   uint64
	minid    streamID
	limit    int
	hasLimit bool
}

// parseTrim reads a trim clause beginning at args[0] (the MAXLEN or MINID
// keyword), shared by XTRIM and the XADD trim clause. It returns the spec, the
// index of the first argument past the clause, and an error message ("" on
// success). LIMIT is rejected without the ~ option, exactly as Redis does.
func parseTrim(args [][]byte) (sp trimSpec, next int, errMsg string) {
	i := 0
	switch {
	case eqFold(args[i], "MAXLEN"):
		sp.kind = trimMaxlen
	case eqFold(args[i], "MINID"):
		sp.kind = trimMinid
	default:
		return sp, 0, "ERR syntax error"
	}
	i++
	// Optional exact/approximate marker; exact is the default.
	if i < len(args) && len(args[i]) == 1 {
		switch args[i][0] {
		case '~':
			sp.approx = true
			i++
		case '=':
			i++
		}
	}
	if i >= len(args) {
		return sp, 0, "ERR syntax error"
	}
	switch sp.kind {
	case trimMaxlen:
		n, ok := parseUint(args[i])
		if !ok {
			return sp, 0, "ERR value is not an integer or out of range"
		}
		sp.maxlen = n
	case trimMinid:
		id, ok := parseStreamID(args[i])
		if !ok {
			return sp, 0, errInvalidID
		}
		sp.minid = id
	}
	i++
	// Optional LIMIT clause, valid only alongside ~.
	if i < len(args) && eqFold(args[i], "LIMIT") {
		if !sp.approx {
			return sp, 0, "ERR syntax error, LIMIT cannot be used without the special ~ option"
		}
		if i+1 >= len(args) {
			return sp, 0, "ERR syntax error"
		}
		n, ok := parseUint(args[i+1])
		if !ok {
			return sp, 0, "ERR value is not an integer or out of range"
		}
		sp.limit = int(n)
		sp.hasLimit = true
		i += 2
	}
	return sp, i, ""
}

// trim applies the spec and returns the number of entries removed, dispatching on
// the band. key names the stream for the whole-block drops' fold-plane manifest
// notices; the inline band never has a cold form and ignores it. A nil or empty
// stream is handled by the caller.
func (s *stream) trim(key []byte, sp trimSpec) int {
	if s.kind == bandInline {
		return s.trimInline(sp)
	}
	return s.trimNative(key, sp)
}

// wants reports whether id sits below the trim threshold, the predicate the
// exact boundary tombstone and the inline splice share for the MINID case.
func (sp trimSpec) below(id streamID) bool { return id.cmp(sp.minid) < 0 }

// trimNative drops whole front blocks, then, in exact mode, tombstones the
// overshoot in the boundary block. It never drops the tail block (a stream keeps
// at least one block, empty or not), so a walk always has a block to land in.
func (s *stream) trimNative(key []byte, sp trimSpec) int {
	removed := 0

	// Whole-block front drops. A block is droppable when removing it entirely
	// keeps the result within the threshold (MAXLEN) or lies wholly below the
	// MINID, and when LIMIT still has room for its live entries.
	dropCount, droppedLive := 0, 0
	for dropCount < len(s.blocks)-1 {
		b := s.blocks[dropCount]
		bl := b.live()
		if sp.hasLimit && droppedLive+bl > sp.limit {
			break
		}
		droppable := false
		switch sp.kind {
		case trimMaxlen:
			droppable = s.length-uint64(droppedLive)-uint64(bl) >= sp.maxlen
		case trimMinid:
			droppable = b.last.cmp(sp.minid) < 0
		}
		if !droppable {
			break
		}
		droppedLive += bl
		dropCount++
	}
	if dropCount > 0 {
		for i := 0; i < dropCount; i++ {
			db := s.blocks[i]
			s.dir.Delete(db.first.ms, seqKey(db.first), s)
			// A demoted front block drops by handle with no pread: forget its demote
			// descriptor (a no-op when resident), leaving the cold frame an orphan the
			// compactor reclaims. resBlob already lost its bytes at demote, so the
			// subtraction below is zero for a cold block and its length otherwise.
			s.forgetCold(key, db)
			s.resBlob -= uint64(len(db.blob))
		}
		// A fresh slice so the abandoned front slots do not pin their blocks or
		// leak the backing array's head capacity across repeated trims.
		ns := make([]*block, len(s.blocks)-dropCount)
		copy(ns, s.blocks[dropCount:])
		s.blocks = ns
		s.base += uint32(dropCount)
		s.length -= uint64(droppedLive)
		removed += droppedLive
	}

	// Exact mode reaches the threshold precisely by tombstoning the boundary
	// block's oldest live entries. Approximate mode stops at the whole-block drops.
	if !sp.approx {
		b := s.blocks[0]
		switch sp.kind {
		case trimMaxlen:
			if s.length > sp.maxlen {
				over := int(s.length - sp.maxlen)
				// tombstoneWhile rewrites flag bytes in place; bring a demoted boundary block
				// resident first (section 7.3), the same bring-up XDEL does. promote keeps the
				// same handle, so b now walks its own resident blob.
				s.promoteIfCold(0)
				t := b.tombstoneWhile(over, func(streamID) bool { return true })
				s.length -= uint64(t)
				removed += t
			}
		case trimMinid:
			// Only the front block can hold entries below the threshold. When its firstID is
			// already at or above it nothing tombstones, so skip the walk (and the pread a
			// cold boundary block would cost); otherwise bring it resident and tombstone.
			if b.first.cmp(sp.minid) < 0 {
				s.promoteIfCold(0)
				t := b.tombstoneWhile(maxInt, sp.below)
				s.length -= uint64(t)
				removed += t
			}
		}
	}
	return removed
}

// trimInline front-splices the single inline block: it rebuilds it from the live
// entries it keeps, dropping the oldest that the threshold and LIMIT select. The
// rebuild compacts any XDEL tombstones as a side effect. The old block's field
// views stay valid because the new block is fully built before it is swapped in.
func (s *stream) trimInline(sp trimSpec) int {
	if len(s.blocks) == 0 {
		return 0
	}
	b := s.blocks[0]

	type liveEntry struct {
		id     streamID
		fields []field
	}
	live := make([]liveEntry, 0, b.live())
	scratch := make([]field, 0, 8)
	b.walk(scratch, func(id streamID, f []field) bool {
		live = append(live, liveEntry{id: id, fields: cloneFields(f)})
		return true
	})

	drop := 0
	switch sp.kind {
	case trimMaxlen:
		if uint64(len(live)) > sp.maxlen {
			drop = len(live) - int(sp.maxlen)
		}
	case trimMinid:
		for drop < len(live) && sp.below(live[drop].id) {
			drop++
		}
	}
	if sp.hasLimit && drop > sp.limit {
		drop = sp.limit
	}
	if drop == 0 {
		return 0
	}

	nb := newBlock()
	for _, e := range live[drop:] {
		nb.appendEntry(e.id, e.fields)
	}
	// The rebuilt block replaces the old one: add its bytes before subtracting the
	// old's so the unsigned running total never dips below zero mid-swap.
	s.resBlob += uint64(len(nb.blob))
	s.resBlob -= uint64(len(s.blocks[0].blob))
	s.blocks[0] = nb
	s.length -= uint64(drop)
	return drop
}
