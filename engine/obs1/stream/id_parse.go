package stream

import (
	"strconv"
)

// ID allocation and parsing at the command layer (spec 2064/f3/14 section 3.6).
// XADD accepts three ID forms against the stream's lastID:
//
//   - "*"      auto: ms = max(nowMs, lastID.ms); same ms increments seq, a newer
//              ms resets seq to 0. The max with lastID.ms is the backward-clock
//              defense: a clock that steps back still yields strictly increasing
//              IDs off lastID.
//   - "ms-*"   partial: ms is fixed, seq auto-generated within it.
//   - "ms-seq" explicit: taken as given, and must strictly exceed lastID; 0-0 is
//              always rejected. A bare "ms" means "ms-*" in XADD.
//
// Every form must produce an ID strictly greater than lastID; otherwise the
// append would break the monotonicity a block run relies on.

const (
	errInvalidID = "ERR Invalid stream ID specified as stream command argument"
	errZeroID    = "ERR The ID specified in XADD must be greater than 0-0"
	errSmallerID = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
)

// allocID resolves arg into the ID this XADD will assign, given the shard's
// coarse clock nowMs. ok is false with a client error message on a malformed or
// non-increasing ID.
func (s *stream) allocID(arg []byte, nowMs uint64) (id streamID, ok bool, errMsg string) {
	if len(arg) == 1 && arg[0] == '*' {
		return s.autoID(nowMs), true, ""
	}
	msPart, seqPart, hasSeq := splitID(arg)
	ms, mok := parseUint(msPart)
	if !mok {
		return streamID{}, false, errInvalidID
	}
	// A bare "ms", or "ms-*", auto-generates the seq within ms.
	if !hasSeq || (len(seqPart) == 1 && seqPart[0] == '*') {
		return s.partialID(ms)
	}
	seq, sok := parseUint(seqPart)
	if !sok {
		return streamID{}, false, errInvalidID
	}
	return s.explicitID(streamID{ms: ms, seq: seq})
}

// autoID generates the next fully automatic ID. A seq at its ceiling within the
// current ms rolls forward into the next ms, so the ID always advances.
func (s *stream) autoID(nowMs uint64) streamID {
	ms := nowMs
	if s.lastID.ms > ms {
		ms = s.lastID.ms
	}
	if ms == s.lastID.ms {
		if s.lastID.seq == ^uint64(0) {
			return streamID{ms: ms + 1, seq: 0}
		}
		return streamID{ms: ms, seq: s.lastID.seq + 1}
	}
	return streamID{ms: ms, seq: 0}
}

// partialID generates an ID with the caller's ms and an auto seq, keeping it
// strictly above lastID.
func (s *stream) partialID(ms uint64) (streamID, bool, string) {
	switch {
	case ms < s.lastID.ms:
		return streamID{}, false, errSmallerID
	case ms == s.lastID.ms:
		if s.lastID.seq == ^uint64(0) {
			return streamID{}, false, errSmallerID
		}
		return streamID{ms: ms, seq: s.lastID.seq + 1}, true, ""
	default:
		return streamID{ms: ms, seq: 0}, true, ""
	}
}

// explicitID validates a fully specified ID against 0-0 and lastID.
func (s *stream) explicitID(id streamID) (streamID, bool, string) {
	if id.ms == 0 && id.seq == 0 {
		return streamID{}, false, errZeroID
	}
	if id.cmp(s.lastID) <= 0 {
		return streamID{}, false, errSmallerID
	}
	return id, true, ""
}

// parseStreamID parses a full "ms-seq" (or bare "ms", completed to "ms-0") for
// the commands that name an existing ID rather than allocate one (XDEL, XSETID).
// A "*" seq is not valid here.
func parseStreamID(arg []byte) (streamID, bool) {
	msPart, seqPart, hasSeq := splitID(arg)
	ms, ok := parseUint(msPart)
	if !ok {
		return streamID{}, false
	}
	if !hasSeq {
		return streamID{ms: ms, seq: 0}, true
	}
	seq, ok := parseUint(seqPart)
	if !ok {
		return streamID{}, false
	}
	return streamID{ms: ms, seq: seq}, true
}

// splitID splits an ID argument on its single '-'. hasSeq reports whether a
// separator was present, distinguishing "ms" from "ms-seq".
func splitID(arg []byte) (ms, seq []byte, hasSeq bool) {
	for i := 0; i < len(arg); i++ {
		if arg[i] == '-' {
			return arg[:i], arg[i+1:], true
		}
	}
	return arg, nil, false
}

// parseUint parses a base-10 unsigned integer without allocating, rejecting an
// empty string or any non-digit.
func parseUint(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var v uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + uint64(c-'0')
	}
	return v, true
}

// formatID writes id's text form "ms-seq" into dst (reset to dst[:0] by the
// caller), the value XADD replies as a bulk and every stream reply prints IDs in.
func formatID(dst []byte, id streamID) []byte {
	dst = strconv.AppendUint(dst, id.ms, 10)
	dst = append(dst, '-')
	dst = strconv.AppendUint(dst, id.seq, 10)
	return dst
}
