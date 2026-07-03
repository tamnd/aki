package f1raw

import (
	"errors"
	"strconv"
)

// ErrNotInt is returned by Incr when the existing value is not a base-10 integer,
// matching Redis's "value is not an integer or out of range".
var ErrNotInt = errors.New("f1raw: value is not an integer or out of range")

// Incr adds delta to the integer value at key and returns the new value. A missing
// key is created at delta, matching Redis INCR/DECR/INCRBY semantics. The whole
// read-modify-write is atomic against other Incr calls on the same key: an existing
// key is updated under its record latch, and a created key resolves a concurrent
// create by retrying into the update path, so two racing Incrs on a fresh key sum
// rather than clobber. It shares the relaxed cross-command consistency the package
// documents: a blind Set racing an Incr on the very same key has no defined order,
// the same caveat that already applies to two readers and a writer of one value.
func (s *Store) Incr(key []byte, delta int64) (int64, error) {
	if len(key) == 0 {
		return 0, errors.New("f1raw: empty key")
	}
	h := hash(key)
	var buf [20]byte
	for {
		off, _, _, _, found := s.find(key, h, stringKind)
		if found {
			n, ok := s.incrInPlace(off, delta)
			if !ok {
				return 0, ErrNotInt
			}
			// incrInPlace returns ok=false only on a parse error; a value that no
			// longer fits the record's reserved width falls through to a grow below.
			if n != incrNeedsGrow {
				return n, nil
			}
			// The formatted result outgrew the record. Read the current value,
			// compute, and republish a wider record under last-writer-wins; a lost
			// race just restarts the loop and re-reads.
			cur, perr := s.readInt(off)
			if perr != nil {
				return 0, ErrNotInt
			}
			n2, oerr := addChecked(cur, delta)
			if oerr != nil {
				return 0, oerr
			}
			b := strconv.AppendInt(buf[:0], n2, 10)
			if err := s.publish(key, b, h, stringKind, 0); err != nil {
				return 0, err
			}
			return n2, nil
		}
		// Absent: try to install delta as a new record, but lose gracefully to a
		// concurrent creator so the next loop iteration finds it and adds onto it.
		b := strconv.AppendInt(buf[:0], delta, 10)
		installed, existed := s.insertAbsent(key, b, h, stringKind)
		if installed {
			return delta, nil
		}
		if existed {
			continue // someone created it; loop back into the update path
		}
		return 0, ErrFull
	}
}

// incrNeedsGrow is a sentinel new-value that signals the formatted result no longer
// fits the record's reserved capacity, so the caller must republish a wider record.
// It is an out-of-band value chosen far from any real counter; the grow path
// recomputes the real result, so the sentinel never escapes.
const incrNeedsGrow = int64(-1) << 62

// incrInPlace latches the record, parses its value as an integer, adds delta, and
// writes the result back in place when it fits the reserved capacity. It returns
// ok=false on a non-integer value, and the incrNeedsGrow sentinel when the result is
// valid but too wide to fit, leaving the value unchanged for the caller to grow.
func (s *Store) incrInPlace(off uint64, delta int64) (int64, bool) {
	verp := s.verAt(off)
	vbase := off + hdrSize + align8(s.klen(off))
	for {
		v := verp.Load()
		if v&verLockBit != 0 {
			continue
		}
		if !verp.CompareAndSwap(v, v+1) {
			continue
		}
		n := uint64(s.vlenAt(off).Load())
		cur, perr := parseInt(s.arena[vbase : vbase+n])
		if perr != nil {
			verp.Store(v + 2)
			return 0, false
		}
		res, oerr := addChecked(cur, delta)
		if oerr != nil {
			verp.Store(v + 2)
			return 0, false // overflow reported to caller as not-an-integer-or-range
		}
		var buf [20]byte
		b := strconv.AppendInt(buf[:0], res, 10)
		if uint64(len(b)) > s.vcapBytes(off) {
			verp.Store(v + 2)
			return incrNeedsGrow, true
		}
		copy(s.arena[vbase:vbase+uint64(len(b))], b)
		s.vlenAt(off).Store(uint32(len(b)))
		verp.Store(v + 2)
		return res, true
	}
}

// readInt reads a record's value as an integer outside the latch. It is used only on
// the rare grow path, where the value is about to be republished anyway, so a benign
// read against a concurrent writer just loses the race and reloops.
func (s *Store) readInt(off uint64) (int64, error) {
	vbase := off + hdrSize + align8(s.klen(off))
	n := uint64(s.vlenAt(off).Load())
	return parseInt(s.arena[vbase : vbase+n])
}

// insertAbsent publishes val under key only if the key is absent. It returns
// installed=true when this call created the record, existed=true when a concurrent
// writer already holds the key (so the caller should switch to an update), and both
// false only when the arena is full. It mirrors publish's bucket scan but never
// overwrites an existing key's value.
func (s *Store) insertAbsent(key, val []byte, h uint64, kind byte) (installed, existed bool) {
	off, ok := s.alloc(recSize(len(key), len(val)))
	if !ok {
		return false, false
	}
	s.initRecord(off, key, val, kind, 0)
	tag := tagOf(h)
	newWord := tag<<tagShift | off
	for {
		b := &s.buckets[h&s.mask]
		var emptyB *bucket
		emptySlot := -1
		var last *bucket
		for b != nil {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i].Load()
				if w == 0 {
					if emptySlot < 0 {
						emptyB, emptySlot = b, i
					}
					continue
				}
				if w>>tagShift != tag {
					continue
				}
				if s.recordMatches(w&addrMask, key, kind) {
					return false, true // key already present
				}
			}
			last = b
			b = s.nextBucket(b, false)
		}
		if emptySlot >= 0 {
			if emptyB.slots[emptySlot].CompareAndSwap(0, newWord) {
				s.count.Add(1)
				s.addTop(kind, 1)
				return true, false
			}
			continue // slot filled under us; rescan (may now find the key)
		}
		if s.nextBucket(last, true) == nil {
			return false, false
		}
	}
}

// Reset empties the store: every index entry is cleared, the arena tail rewinds, and
// the live count drops to zero. It is the FLUSHALL/FLUSHDB primitive. It is NOT safe
// against concurrent foreground readers or writers; the caller must quiesce traffic
// first, which is how Redis treats a flush in practice. The arena bytes are not scrubbed;
// rewinding the tail is enough because a later publish overwrites the header before
// exposing the record through an index entry.
//
// The one background actor a flush must coordinate with is the tombstone folder: it drains
// under folderMu and touches s.oidx, so Reset takes folderMu across the whole flush. That
// makes the flush and any in-flight folder drain mutually exclusive, so replacing the oidx
// pointer never races the folder's read of it. Every queued tombstone points at a record
// this flush is about to unlink, so the queue is dropped wholesale under the same lock; a
// folder that wakes after Reset returns swaps an empty stack and does nothing.
func (s *Store) Reset() {
	s.folderMu.Lock()
	defer s.folderMu.Unlock()
	s.tombHead.Store(nil)
	s.tombPend.Store(0)
	for bi := range s.buckets {
		b := &s.buckets[bi]
		for i := 0; i < slotsPerBucket; i++ {
			b.slots[i].Store(0)
		}
		b.link.Store(0)
	}
	s.tail.Store(8)
	s.count.Store(0)
	s.topCount.Store(0)
	s.oidx = newOIndex(s)
	// A reset unlinks every record, so every byte the cold log holds is now dead space no
	// live record points at. The log itself is left in place (M1 does no reclamation; a
	// later compaction milestone truncates it), so the dead counter is set to the current
	// tail to keep it honest: without this a flushed dataset's cold bytes would read as
	// live and the compaction trigger would undercount the waste.
	if s.cold != nil {
		s.cold.dead.Store(s.cold.tail.Load())
	}
}

// parseInt parses a base-10 signed integer with no leading or trailing slack, the
// same strictness Redis applies before it will INCR a value.
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, ErrNotInt
	}
	return strconv.ParseInt(string(b), 10, 64)
}

// addChecked adds delta to n and reports overflow, so Incr can return Redis's
// out-of-range error instead of wrapping.
func addChecked(n, delta int64) (int64, error) {
	r := n + delta
	if (delta > 0 && r < n) || (delta < 0 && r > n) {
		return 0, ErrNotInt
	}
	return r, nil
}
