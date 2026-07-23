package stream

// Boot replay's entry points into the stream registry (spec 2064/obs1
// doc 04 section 2), under the contract set/replay.go states: plain
// arguments, the worker's real Ctx under the BootCtx contract, literal
// application with loud divergence, and no clock. Stream frames carry
// owner-decided ids: xadd's id was allocated and validated at serve
// time, xdel names the ids that actually removed, xtrim carries the
// decided count of oldest live entries, and xsetid carries the three
// resulting values unconditionally, so nothing here re-runs the clock
// or the trim threshold math. Appends ride appendEntry, the serve
// loop's own band-dispatching primitive, so the inline-to-native
// upgrade happens at the same entry it did at serve time and the
// stream's band agrees across a restart.

import (
	"fmt"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// ReplayXAdd appends one entry at the framed id over the flat
// field-value alternation the emission seam carried. create is true
// when a collnew led the frame: the stream is rebuilt empty after
// dropping whatever the key held, the reset-to-empty rule. Without
// create the stream must exist. The owner's allocID proved the id
// strictly exceeds the stream's last id before framing, so an id at or
// below it here means the store and the frame stream diverged.
func ReplayXAdd(cx *shard.Ctx, key []byte, idMs, idSeq uint64, fieldsValues [][]byte, create bool) error {
	if len(fieldsValues) == 0 || len(fieldsValues)%2 != 0 {
		return fmt.Errorf("stream replay: xadd on %q carries %d field-value items", key, len(fieldsValues))
	}
	g := registry(cx)
	var s *stream
	if create {
		g.dropStream(key)
		s = newStream()
		g.m[string(key)] = s
	} else if s = g.m[string(key)]; s == nil {
		return fmt.Errorf("stream replay: xadd names key %q but no stream exists", key)
	}
	id := streamID{ms: idMs, seq: idSeq}
	if id.cmp(s.lastID) <= 0 {
		return fmt.Errorf("stream replay: xadd id %d-%d on %q does not exceed last id %d-%d", idMs, idSeq, key, s.lastID.ms, s.lastID.seq)
	}
	s.appendEntry(id, parseFields(fieldsValues))
	g.note(s)
	return nil
}

// ReplayXDel tombstones the framed ids, ms paralleling seqs in the
// argument order the owner recorded. The frame carries only ids that
// actually removed, so a miss is divergence. A tombstone in a native
// sealed block accrues dead bytes exactly as at serve time, so the
// stream is marked for the gc maintainer the same way.
func ReplayXDel(cx *shard.Ctx, key []byte, ms, seqs []uint64) error {
	if len(ms) == 0 || len(ms) != len(seqs) {
		return fmt.Errorf("stream replay: xdel on %q carries %d ms against %d seqs", key, len(ms), len(seqs))
	}
	g := registry(cx)
	s := g.m[string(key)]
	if s == nil {
		return fmt.Errorf("stream replay: xdel names key %q but no stream exists", key)
	}
	for i := range ms {
		id := streamID{ms: ms[i], seq: seqs[i]}
		if !s.delete(id) {
			return fmt.Errorf("stream replay: xdel id %d-%d framed as removed is not live in stream %q", ms[i], seqs[i], key)
		}
	}
	if s.kind == bandNative {
		g.markDirty(s)
	}
	g.note(s)
	return nil
}

// ReplayXTrim removes the framed count of oldest live entries in id
// order. Both trim bands only ever drop a prefix of the live sequence,
// so the count fully determines the effect; replay renders it as an
// exact MAXLEN trim to the resulting length, which removes precisely
// that prefix whichever band the serve-time trim ran. A count the
// stream cannot cover is divergence.
func ReplayXTrim(cx *shard.Ctx, key []byte, count uint64) error {
	g := registry(cx)
	s := g.m[string(key)]
	if s == nil {
		return fmt.Errorf("stream replay: xtrim names key %q but no stream exists", key)
	}
	if count == 0 || count > s.length {
		return fmt.Errorf("stream replay: xtrim of %d from stream %q of %d live entries", count, key, s.length)
	}
	removed := s.trim(trimSpec{kind: trimMaxlen, maxlen: s.length - count})
	if uint64(removed) != count {
		return fmt.Errorf("stream replay: xtrim on %q removed %d of the %d framed", key, removed, count)
	}
	if s.kind == bandNative {
		g.markDirty(s)
	}
	g.note(s)
	return nil
}

// ReplayXSetID assigns the three values the frame carries, the
// optional-argument merge already done by the owner, so there are no
// flags and no ordering check: XSETID validated against the serve-time
// stream, and replay reproduces its result literally. The command
// requires the key to exist, so a missing stream is divergence.
func ReplayXSetID(cx *shard.Ctx, key []byte, lastMs, lastSeq, entriesAdded, maxDelMs, maxDelSeq uint64) error {
	g := registry(cx)
	s := g.m[string(key)]
	if s == nil {
		return fmt.Errorf("stream replay: xsetid names key %q but no stream exists", key)
	}
	s.lastID = streamID{ms: lastMs, seq: lastSeq}
	s.entriesAdded = entriesAdded
	s.maxDeletedID = streamID{ms: maxDelMs, seq: maxDelSeq}
	g.note(s)
	return nil
}

// ReplayDrop removes key's stream and reports whether one existed, the
// keydel probe. No emitter frames a stream colldrop (an emptied stream
// persists), so only a keydel or a collnew-led reset reaches this path.
func ReplayDrop(cx *shard.Ctx, key []byte) bool {
	g := registry(cx)
	if g.m[string(key)] == nil {
		return false
	}
	g.dropStream(key)
	return true
}

// dropStream removes key's stream from the registry, settles its
// accounting, and scrubs it from the gc worklist so a later maintain
// pass cannot resurrect a dropped stream's footprint.
func (g *reg) dropStream(key []byte) {
	s := g.m[string(key)]
	if s == nil {
		return
	}
	delete(g.m, string(key))
	if g.acctOn {
		g.resident -= s.acct
	}
	if s.gcDirty {
		for i, d := range g.dirty {
			if d == s {
				g.dirty = append(g.dirty[:i], g.dirty[i+1:]...)
				break
			}
		}
	}
}
