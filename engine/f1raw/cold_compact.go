package f1raw

import (
	"errors"
	"os"
)

// errCompactRead is returned by Compact when a live separated record's cold value
// cannot be read back from the old log during the rewrite. It means the on-disk log
// is corrupt or truncated, which is a hard failure: the compaction aborts and leaves
// the store on its original log so no live value is lost.
var errCompactRead = errors.New("f1raw: cold value read failed during compaction")

// Compact reclaims the dead bytes a same-key overwrite or a delete left behind in the
// cold value log. The log is append-only, so a superseded or deleted separated value's
// bytes stay on disk as dead space that no live record points at, and the dead-byte
// accounting (ColdBytes) measures exactly that waste. Compact acts on it: it walks every
// live index entry, copies each live separated record's cold value forward into a fresh
// log, repoints that record's in-arena 12-byte pointer at the new offset, then swaps the
// fresh log in for the old one and renames it over the old file. Only live values are
// copied, so the dead bytes are dropped, the new log's tail is the live size, and its
// dead counter starts at zero.
//
// It is the reclamation the dead-byte accounting was built to trigger: a caller reads
// ColdBytes, and when dead/total crosses a policy threshold it calls Compact to rewrite
// the log down to its live size. This is the mechanism; the threshold policy is the
// caller's.
//
// Like Reset, Compact is NOT safe against concurrent readers or writers. A cold read
// resolves a value through the live pointer against s.cold, and Compact rewrites both the
// pointers and s.cold, so the caller must quiesce traffic first, the same contract a flush
// carries. A store with no cold log has nothing to reclaim and Compact is a no-op.
//
// On any read or append error Compact aborts with the store still on its original log and
// every pointer unchanged, so a failed compaction never loses a live value; the dead bytes
// it meant to reclaim simply remain until the next attempt.
func (s *Store) Compact() error {
	if s.cold == nil {
		return nil
	}
	old := s.cold
	// Rewrite live values into a sibling log next to the current one. openColdLog truncates,
	// so a stale ".compact" file from an aborted prior run is discarded rather than appended to.
	newPath := old.path + ".compact"
	nl, err := openColdLog(newPath)
	if err != nil {
		return err
	}
	// buf is reused across reads: readSeparated copies each value into it, then append copies
	// those bytes to the new log before the next read overwrites buf, so one buffer serves the
	// whole walk. It grows to the largest live value and no larger.
	var buf []byte
	for bi := range s.buckets {
		for b := &s.buckets[bi]; b != nil; b = s.nextBucket(b, false) {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i].Load()
				if w == 0 {
					continue
				}
				off := w & addrMask
				if !s.isSep(off) {
					continue // inline record, its value lives in the arena, nothing to move
				}
				// Read the value through the OLD log (s.cold is still old), append it to the
				// new log, then repoint the record. Each slot is visited once, so a record is
				// never re-read after its pointer moves.
				v, ok := s.readSeparated(off, buf)
				if !ok {
					abortCompact(nl, newPath)
					return errCompactRead
				}
				buf = v
				newOff, err := nl.append(v)
				if err != nil {
					abortCompact(nl, newPath)
					return err
				}
				vbase := off + hdrSize + align8(s.klen(off))
				encPtr(s.arena[vbase:], newOff, len(v))
			}
		}
	}
	// Every live pointer now addresses the new log. Swap it in before dropping the old one so
	// a resolve after this point reads the compacted log, then rename the fresh file over the
	// old path so the store keeps one stable file name. The open fd survives the rename.
	s.cold = nl
	// The old log is superseded; close and drop its fd. A close error here does not affect the
	// live data (every pointer already addresses the new log), so it is best-effort.
	_ = old.close()
	if err := os.Rename(newPath, old.path); err != nil {
		return err
	}
	nl.path = old.path
	return nil
}

// abortCompact tears down a half-built compaction log after a read or append failure: it
// closes the fresh log and removes the file, both best-effort, so the store is left on its
// original log with no partial file lingering. The caller returns the triggering error.
func abortCompact(nl *coldLog, path string) {
	_ = nl.close()
	_ = os.Remove(path)
}
