package set

// Boot replay's entry points into the set registry (spec 2064/obs1 doc
// 04 section 2): the replay package decodes collection frames and calls
// these with plain arguments, so it never touches the registry's shape
// and this package never imports the obs1 root. Every function runs on
// the boot goroutine before any worker starts, under the BootCtx
// contract, with the worker's real Ctx so the registry it builds is the
// one the owner serves from after Start.
//
// Frames carry post-decision effects, so application is literal: a
// member framed as added must add, one framed as removed must remove,
// and a miss means the store and the frame stream diverged, which the
// caller turns into a loud stop. No serve-time decision is re-run here,
// and nothing consults the clock or the string store: an expired string
// corpse the owner never saw framed must not fail a replay that the
// owner's own execution allowed.

import (
	"fmt"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// ReplayAdd applies one sadd frame's members. create is true when a
// collnew led the frame: the set is built fresh from the first member's
// shape after dropping whatever set the key held, the doc 04
// reset-to-empty rule for a STORE form landing on a live destination.
// Without create the set must exist, since a colldelta's missing-key
// case is a corruption signal.
func ReplayAdd(cx *shard.Ctx, key []byte, members [][]byte, create bool) error {
	g := registry(cx)
	var s *set
	if create {
		g.drop(key)
		s = newSet(members[0])
		g.m[string(key)] = s
	} else if s = g.m[string(key)]; s == nil {
		return fmt.Errorf("set replay: sadd names key %q but no set exists", key)
	}
	for _, m := range members {
		if !s.add(m) {
			return fmt.Errorf("set replay: member %q framed as added is already in set %q", m, key)
		}
	}
	g.note(s)
	return nil
}

// ReplayRem applies one srem frame's members, literally: the frame's
// members leave and the set stays, even at zero members, because the
// drop decision travels as its own colldrop frame and replay applies
// frames, not conclusions.
func ReplayRem(cx *shard.Ctx, key []byte, members [][]byte) error {
	g := registry(cx)
	s := g.m[string(key)]
	if s == nil {
		return fmt.Errorf("set replay: srem names key %q but no set exists", key)
	}
	for _, m := range members {
		if !s.rem(m) {
			return fmt.Errorf("set replay: member %q framed as removed is not in set %q", m, key)
		}
	}
	g.note(s)
	return nil
}

// ReplayDrop removes key's set and reports whether one existed. The
// caller decides what a miss means: a typed colldrop landing on nothing
// is corruption, while a keydel spanning both keyspaces treats this as
// one probe among several.
func ReplayDrop(cx *shard.Ctx, key []byte) bool {
	g := registry(cx)
	if g.m[string(key)] == nil {
		return false
	}
	g.drop(key)
	return true
}
