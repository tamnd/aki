package store

import (
	"errors"
	"os"
)

// errCompactRead is returned by CompactLog when a live record's logged value
// cannot be read back from the old log during the rewrite. It means the
// on-disk log is corrupt or truncated, which is a hard failure: the
// compaction aborts and leaves the store on its original log so no live value
// is lost.
var errCompactRead = errors.New("store: value log read failed during compaction")

// repoint is one deferred pointer rewrite a compaction has staged: the record
// whose run pointer sits at vs moves to off in the new log. Rewrites stage
// rather than apply so an abort anywhere in the copy phase leaves every
// pointer naming the original log.
type repoint struct {
	vs   uint64
	off  uint64
	vlen uint32
}

// CompactLog reclaims the dead bytes a same-key overwrite, a delete, or an
// expiry reap left behind in the value log. The log is append-only, so a
// superseded value's bytes stay on disk as dead space no live pointer names,
// and the dead counter (LogBytes) measures exactly that waste. CompactLog
// acts on it: it walks every live index entry, copies each log-resident run
// forward into a fresh log, then repoints those records and renames the fresh
// log over the old file. Only live runs are copied, so the new log's tail is
// the live size and its dead counter starts at zero. Arena-resident runs and
// inline records are not touched.
//
// This is the mechanism; when to run it is the caller's policy, read off the
// dead/total ratio. Like Reset it runs quiesced by construction: the shard
// owner calls it between commands, and nothing else ever touches the store.
//
// The copy phase completes before the first pointer moves, so on any read or
// append error CompactLog aborts with the store still on its original log and
// every pointer unchanged: a failed compaction never loses a live value, and
// the dead bytes it meant to reclaim simply remain until the next attempt. A
// store with no log has nothing to reclaim and the call is a no-op.
func (s *Store) CompactLog() error {
	if s.vlog == nil {
		return nil
	}
	old := s.vlog
	// Rewrite live runs into a sibling log next to the current one. openVlog
	// truncates, so a stale ".compact" file from an aborted prior run is
	// discarded rather than appended to.
	newPath := old.path + ".compact"
	nl, err := openVlog(newPath)
	if err != nil {
		return err
	}
	// Copy phase. buf is reused across reads: readInto fills it from the old
	// log, append copies those bytes into the new log before the next read
	// overwrites them, so one buffer serves the whole walk. It grows to the
	// largest live value and no larger. The directory walk visits each
	// segment once: multiple directory slots alias one segment while its
	// localDepth trails the global depth, and the seen marks skip the
	// aliases. Every chain bucket lives in its segment's overflow slab, so
	// sweeping the slab covers the chains without following links.
	var buf []byte
	var moves []repoint
	seen := make([]bool, len(s.idx.segs))
	for _, ord := range s.idx.dir {
		if seen[ord] {
			continue
		}
		seen[ord] = true
		seg := s.idx.segs[ord]
		for bi := range seg.buckets {
			if moves, err = s.compactBucket(&seg.buckets[bi], old, nl, &buf, moves); err != nil {
				abortCompact(nl, newPath)
				return err
			}
		}
		for bi := range seg.overflow {
			if moves, err = s.compactBucket(&seg.overflow[bi], old, nl, &buf, moves); err != nil {
				abortCompact(nl, newPath)
				return err
			}
		}
	}
	// Apply phase, no failure possible from here. Repoint every staged
	// record, swap the new log in before dropping the old one so a read after
	// this point resolves through the compacted log, then rename the fresh
	// file over the old path so the store keeps one stable file name. The
	// open fd survives the rename.
	for _, m := range moves {
		// A log run is immutable, so its capacity is exactly its length.
		s.writePtr(m.vs, inLogBit|m.off, m.vlen, m.vlen)
	}
	s.vlog = nl
	// The old log is superseded; a close error cannot affect the live data,
	// so it is best-effort.
	_ = old.close()
	if err := os.Rename(newPath, old.path); err != nil {
		return err
	}
	nl.path = old.path
	return nil
}

// compactBucket copies each live log-resident run in one bucket forward into
// the new log and stages the pointer rewrite. Each slot is visited once, so a
// run is never re-read after it copies.
func (s *Store) compactBucket(b *bucket, old, nl *vlog, buf *[]byte, moves []repoint) ([]repoint, error) {
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 {
			continue
		}
		addr := w & addrMask
		if s.recFlags(addr)&flagSep == 0 {
			continue // inline record, its value lives in the record, nothing to move
		}
		vs := s.valueStart(addr)
		word, vlen, _ := s.readPtr(vs)
		if word&inLogBit == 0 {
			continue // arena run, resident bytes stay where they are
		}
		v, err := old.readInto(word&runAddrMask, int(vlen), *buf)
		if err != nil {
			return moves, errCompactRead
		}
		*buf = v
		newOff, err := nl.append(v)
		if err != nil {
			return moves, err
		}
		moves = append(moves, repoint{vs: vs, off: newOff, vlen: vlen})
	}
	return moves, nil
}

// abortCompact tears down a half-built compaction log after a read or append
// failure: it closes the fresh log and removes the file, both best-effort, so
// the store is left on its original log with no partial file lingering. The
// caller returns the triggering error.
func abortCompact(nl *vlog, path string) {
	_ = nl.close()
	_ = os.Remove(path)
}
