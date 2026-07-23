package zset

// Boot replay's entry points into the zset registry (spec 2064/obs1 doc
// 04 section 2), under the contract set/replay.go states: plain
// arguments, the worker's real Ctx under the BootCtx contract, literal
// application with loud divergence, and no clock. A zadd frame's pairs
// are post-decision, the score each member now holds, so replay applies
// them through the same update the serve loop ran with an empty flag
// matrix: every pair must land as an add or a rescore, and a pair that
// changes nothing means the store and the frame stream diverged. GEOADD
// rides this plane unchanged, since geo frames the encoded cell scores
// it computed at serve time.

import (
	"fmt"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// ReplayZAdd applies one zadd frame's pairs, scores parallel to members
// exactly as the emission seam carries them. create is true when a
// collnew led the frame: the zset is rebuilt empty after dropping
// whatever the key held, the reset-to-empty rule, and opens as a
// listpack for encoding parity with serve-time ZADD. Without create the
// zset must exist. Each pair must add its member or move its score.
func ReplayZAdd(cx *shard.Ctx, key []byte, scores []float64, members [][]byte, create bool) error {
	if len(members) == 0 || len(scores) != len(members) {
		return fmt.Errorf("zset replay: zadd on %q carries %d scores for %d members", key, len(scores), len(members))
	}
	g := registry(cx)
	var z *zset
	if create {
		g.drop(key)
		z = newZset()
		g.m[string(key)] = z
	} else if z = g.m[string(key)]; z == nil {
		return fmt.Errorf("zset replay: zadd names key %q but no sorted set exists", key)
	}
	for i, m := range members {
		added, changed, _, _, _ := z.update(m, scores[i], flags{})
		if !added && !changed {
			return fmt.Errorf("zset replay: member %q framed as upserted already holds its score in %q", m, key)
		}
	}
	g.note(z)
	return nil
}

// ReplayZRem applies one zrem frame's members, literally: the frame
// lists only actual removals, the pop family and the ZREMRANGEBY* verbs
// included, so a member that is not there to remove is divergence, and
// an emptied sorted set stays until its colldrop frame lands.
func ReplayZRem(cx *shard.Ctx, key []byte, members [][]byte) error {
	g := registry(cx)
	z := g.m[string(key)]
	if z == nil {
		return fmt.Errorf("zset replay: zrem names key %q but no sorted set exists", key)
	}
	for _, m := range members {
		if !z.rem(m) {
			return fmt.Errorf("zset replay: member %q framed as removed is not in sorted set %q", m, key)
		}
	}
	g.note(z)
	return nil
}

// ReplayDrop removes key's sorted set and reports whether one existed,
// the colldrop and keydel probe.
func ReplayDrop(cx *shard.Ctx, key []byte) bool {
	g := registry(cx)
	if g.m[string(key)] == nil {
		return false
	}
	g.drop(key)
	return true
}
