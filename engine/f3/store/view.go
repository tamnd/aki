package store

import (
	"encoding/binary"
	"strconv"
)

// The view read path: GET-shaped reads that hand resident value bytes to the
// reply builder without the scratch copy GetString pays. Profiling put that
// copy (readSep plus the reply-arena append) near 10 percent of CPU at 4KiB
// values; the reply builder copies into the reply arena anyway, so the first
// copy buys nothing.

// GetView returns the value for key with the resident bands' copy elided: an
// embedded value or an arena-resident separated run comes back as a view into
// the arena itself. An int cell renders into the store scratch, and a
// log-resident run or a chunked value materializes into it, so those bands
// still pay one copy (the log read stays on the copying path on purpose; it
// is a pread and cannot alias the arena).
//
// Lifetime rule: the view is valid until the owner returns from the current
// command execution, and dies at the next store write on this shard. The
// handler runs on the shard's single owner goroutine inside the batch's epoch
// bracket (shard worker executeOne), and nothing moves or releases arena
// bytes a live view can name mid-command: value-log compaction and arena
// compaction run only at drain boundaries (shard worker maybeCompact and the
// tight-arena check, never inside a batch), and the one command-path
// reclaimer, the full-arena backstop (reclaimOnFull), frees only fully dead
// segments, which cannot back a view because a viewed record's charge is
// live until the write that kills the view drops it. A later write may
// reuse or overwrite the viewed bytes in place, so the caller must consume
// the view (Reply.Bulk and AppendFanValue copy immediately) before its next
// store call.
func (s *Store) GetView(key []byte, now int64) ([]byte, bool) {
	h := Hash(key)
	slot, addr, _ := s.findLive(h, key, now)
	if addr == 0 {
		return nil, false
	}
	if slotCold(*slot) {
		return s.coldViewRef(h, slot, addr)
	}
	s.touchSlot(slot)
	return s.readValueRef(addr)
}

// coldViewRef serves a cold value as a view and runs the read doorkeeper. On a
// promoting second sighting the record is resident, so it returns an arena view
// of the brought-up record; otherwise it returns the frame bytes in the cold
// scratch, valid under the same GetView lifetime rule as the log-resident view
// readValueRef returns. A read no longer promotes a cold key unconditionally the
// way a bring-up would; the doorkeeper decides, so a one-hit cold GET leaves the
// working set in place.
func (s *Store) coldViewRef(h uint64, slot *uint64, off uint64) ([]byte, bool) {
	if s.ltmOn && s.promoteOnColdRead(h, slot, off) {
		return s.readValueRef(*slot & addrMask)
	}
	return s.coldValue(off)
}

// GetViewStream is GetView with the chunked band split out for streaming,
// the way GetStream splits it out of GetString: a chunked value comes back as
// a ChunkStream and no bytes, everything else through readValueRef under the
// GetView lifetime rule.
func (s *Store) GetViewStream(key []byte, now int64) ([]byte, *ChunkStream, bool) {
	h := Hash(key)
	slot, addr, _ := s.findLive(h, key, now)
	if addr == 0 {
		return nil, nil, false
	}
	if slotCold(*slot) {
		// Only the self-contained bands demote, so a cold hit is never chunked:
		// serve it through the same doorkeeper-gated view as GetView.
		v, ok := s.coldViewRef(h, slot, addr)
		return v, nil, ok
	}
	s.touchSlot(slot)
	if s.recFlags(addr)&flagChunked != 0 {
		return nil, s.chunkStreamAt(addr), true
	}
	v, ok := s.readValueRef(addr)
	return v, nil, ok
}

// readValueRef is readValue minus the copy where the band allows it. The
// embedded bytes and an arena-resident separated run return as direct views;
// an int cell, a log-resident run, and a chunked value go through the store
// scratch, whose grown capacity carries across calls like the shard scratch
// does. The result is subject to the GetView lifetime rule either way: the
// scratch is store state and the next view read reuses it.
func (s *Store) readValueRef(addr uint64) ([]byte, bool) {
	vs := s.valueStart(addr)
	f := s.recFlags(addr)
	if f&flagInt != 0 {
		n := int64(binary.LittleEndian.Uint64(s.arena.buf[vs:]))
		v := strconv.AppendInt(s.vbuf[:0], n, 10)
		s.vbuf = v[:0]
		return v, true
	}
	if f&flagChunked != 0 {
		v, ok := s.readChunked(addr, s.vbuf)
		s.vbuf = v[:cap(v)][:0]
		return v, ok
	}
	if f&flagSep != 0 {
		word, vlen, _ := s.readPtr(vs)
		if word&inLogBit != 0 {
			v, err := s.logReadInto(word&runAddrMask, int(vlen), s.vbuf)
			s.vbuf = v[:cap(v)][:0]
			if err != nil {
				return nil, false
			}
			// The residency hooks (resid.go): promotion only allocates, so a
			// view an earlier read of this batch returned stays valid. The
			// read counter is unconditional; only the policy is gated.
			s.logReads++
			if s.ltmOn {
				s.maybePromote(addr, vs, vlen, v)
			}
			return v, true
		}
		if s.ltmOn {
			s.touchResident(addr)
		}
		run := word & runAddrMask
		return s.arena.buf[run : run+uint64(vlen)], true
	}
	return s.arena.buf[vs : vs+s.vlen(addr)], true
}
