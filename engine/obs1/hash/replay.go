package hash

// Boot replay's entry points into the hash registry (spec 2064/obs1 doc
// 04 section 2), the hash half of the contract set/replay.go states:
// plain arguments, the worker's real Ctx under the BootCtx contract,
// literal application with loud divergence, and no clock. The one
// asymmetry against the set plane is that an hset frame may overwrite:
// HSET writes every pair verbatim and its frame carries the pair tail
// verbatim, so an overwrite is legal here and replays the serve-time
// TTL-clear rule instead of signalling divergence.

import (
	"fmt"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// ReplayHSet applies one hset frame's pairs, flat field-value
// alternation exactly as the emission seam carries them. create is true
// when a collnew led the frame: the hash is rebuilt empty after
// dropping whatever the key held, the reset-to-empty rule. Without
// create the hash must exist. An overwritten field loses its TTL, the
// HSET behavior the frame relies on; a TTL a preserving verb kept rides
// an hexpire frame behind this one.
func ReplayHSet(cx *shard.Ctx, key []byte, fieldsValues [][]byte, create bool) error {
	if len(fieldsValues) == 0 || len(fieldsValues)%2 != 0 {
		return fmt.Errorf("hash replay: hset on %q carries %d items, want field-value pairs", key, len(fieldsValues))
	}
	g := registry(cx)
	var h *hash
	if create {
		g.drop(key)
		h = newHash()
		g.m[string(key)] = h
	} else if h = g.m[string(key)]; h == nil {
		return fmt.Errorf("hash replay: hset names key %q but no hash exists", key)
	}
	for i := 0; i < len(fieldsValues); i += 2 {
		if !h.set(fieldsValues[i], fieldsValues[i+1]) {
			h.clearFieldExp(fieldsValues[i])
		}
	}
	g.note(h)
	return nil
}

// ReplayHDel applies one hdel frame's fields, literally: the frame
// lists only actual removals, so a field that is not there to remove is
// divergence, and an emptied hash stays until its colldrop frame lands.
func ReplayHDel(cx *shard.Ctx, key []byte, fields [][]byte) error {
	g := registry(cx)
	h := g.m[string(key)]
	if h == nil {
		return fmt.Errorf("hash replay: hdel names key %q but no hash exists", key)
	}
	for _, f := range fields {
		if !h.del(f) {
			return fmt.Errorf("hash replay: field %q framed as removed is not in hash %q", f, key)
		}
	}
	g.note(h)
	return nil
}

// ReplayHExpire applies one hexpire frame: the named fields take the
// absolute deadline atMs, 0 clearing it (the HPERSIST form and the
// HSET-overwrite restore). The frame is post-decision over live fields,
// so a missing field is divergence. The deadline lands untouched even
// when it is already past at boot: serve-time reaping owns firing it,
// the now-zero rule.
func ReplayHExpire(cx *shard.Ctx, key []byte, atMs uint64, fields [][]byte) error {
	g := registry(cx)
	h := g.m[string(key)]
	if h == nil {
		return fmt.Errorf("hash replay: hexpire names key %q but no hash exists", key)
	}
	for _, f := range fields {
		if !h.setFieldExp(f, atMs) {
			return fmt.Errorf("hash replay: field %q framed with a deadline is not in hash %q", f, key)
		}
	}
	g.note(h)
	return nil
}

// ReplayDrop removes key's hash and reports whether one existed, the
// colldrop and keydel probe.
func ReplayDrop(cx *shard.Ctx, key []byte) bool {
	g := registry(cx)
	if g.m[string(key)] == nil {
		return false
	}
	g.drop(key)
	return true
}
