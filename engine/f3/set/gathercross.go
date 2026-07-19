package set

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The cross-shard operand gather (spec 2064/f3/03 section 6.7, spec 2064/f3/11
// section 6): SINTER, SUNION, SDIFF, and SINTERCARD over operands that span
// shards, the read half of the F17 intent path. Dispatch sends a command here
// only when its keys really do span shards; the co-located case never comes
// near this file and keeps the owner-local handlers above.
//
// The plan: the transaction holds an intent on every operand, so all of them
// are frozen at one point between every other command's critical sections.
// One hop per remote shard clones that shard's operands into plain heap sets
// (cloneSet), then one hop on the first key's owner resolves the operands it
// owns directly from its registry, runs the same drivers the co-located
// handlers run, and builds the reply. The clones are ordinary unindexed sets,
// so a pair involving one takes the probe path, which is the always-correct
// baseline the driver documents; the members are identical to a co-located
// arrangement of the same data, which is the contract the differential suite
// pins. Clone cost is per-member and proportional to the remote operands
// only; the tier-two route is priced in labs/f3/m1/11_gather_cross.

// cloneSet copies s into a fresh set that owns its storage, safe to carry off
// the owner goroutine once the hop that built it returns. The band ladder is
// rebuilt by insertion in iteration order, the same walk a co-located SADD
// replay of the members would take. A nil or empty source stays nil (the
// registry never holds an empty set).
func cloneSet(s *set) *set {
	if s == nil || s.card() == 0 {
		return nil
	}
	var c *set
	s.each(func(m []byte) {
		if c == nil {
			c = newSet(m)
		}
		c.add(m)
	})
	return c
}

// algebraCross gathers the operands under the transaction's barrier and runs
// compute on the first key's owner. Operands owned by other shards are cloned
// with one hop per shard; operands owned by the first key's shard are read
// straight from its registry inside the compute hop, zero-copy. Any operand
// holding a string answers WRONGTYPE, the same precheck the co-located
// handlers make, and no later hop runs after it is seen (the gather mutates
// nothing, so stopping early is free).
func algebraCross(t *shard.Txn, keys [][]byte, compute func(cx *shard.Ctx, sets []*set) []byte) []byte {
	primary := t.Shard(keys[0])
	sets := make([]*set, len(keys))
	done := make([]bool, len(keys))
	wrong := false
	for i := range keys {
		sh := t.Shard(keys[i])
		if sh == primary || done[i] || wrong {
			continue
		}
		var idxs []int
		for j := i; j < len(keys); j++ {
			if !done[j] && t.Shard(keys[j]) == sh {
				idxs = append(idxs, j)
				done[j] = true
			}
		}
		t.Do(keys[i], func(cx *shard.Ctx) {
			g := registry(cx)
			for _, j := range idxs {
				s, w := g.lookup(cx, keys[j])
				if w {
					wrong = true
					return
				}
				sets[j] = cloneSet(s)
			}
		})
	}
	var out []byte
	if !wrong {
		t.Do(keys[0], func(cx *shard.Ctx) {
			g := registry(cx)
			for j := range keys {
				if t.Shard(keys[j]) != primary {
					continue
				}
				s, w := g.lookup(cx, keys[j])
				if w {
					wrong = true
					return
				}
				sets[j] = s
			}
			out = compute(cx, sets)
		})
	}
	if wrong {
		return resp.AppendError(nil, wrongType)
	}
	return out
}

// crossArray renders a driver's emitted members as a flat multi-bulk reply in
// a fresh buffer: the reply outlives the hop that builds it, so the shard
// scratch the co-located emitArray uses is not available here.
func crossArray(resp3 bool, drive func(emit func(m []byte))) []byte {
	var page []byte
	n := 0
	drive(func(m []byte) {
		page = resp.AppendBulk(page, m)
		n++
	})
	var out []byte
	if resp3 {
		out = resp.AppendSetHeader(nil, n)
	} else {
		out = resp.AppendArrayHeader(nil, n)
	}
	return append(out, page...)
}

// SinterCross answers SINTER over cross-shard operands, keys being the whole
// argument tail.
func SinterCross(t *shard.Txn, keys [][]byte) []byte {
	return algebraCross(t, keys, func(cx *shard.Ctx, sets []*set) []byte {
		return crossArray(t.Resp3(), func(emit func(m []byte)) { sinter(cx, sets, emit) })
	})
}

// SunionCross answers SUNION over cross-shard operands.
func SunionCross(t *shard.Txn, keys [][]byte) []byte {
	return algebraCross(t, keys, func(cx *shard.Ctx, sets []*set) []byte {
		return crossArray(t.Resp3(), func(emit func(m []byte)) { sunion(cx, sets, emit) })
	})
}

// SdiffCross answers SDIFF over cross-shard operands.
func SdiffCross(t *shard.Txn, keys [][]byte) []byte {
	return algebraCross(t, keys, func(cx *shard.Ctx, sets []*set) []byte {
		return crossArray(t.Resp3(), func(emit func(m []byte)) { sdiff(cx, sets, emit) })
	})
}

// SintercardCross answers SINTERCARD over cross-shard operands; args is the
// whole tail (numkeys first), reparsed here because the transaction body owns
// its own copies.
func SintercardCross(t *shard.Txn, args [][]byte) []byte {
	keys, limit, msg := sintercardArgs(args)
	if msg != "" {
		return resp.AppendError(nil, msg)
	}
	return algebraCross(t, keys, func(cx *shard.Ctx, sets []*set) []byte {
		return resp.AppendInt(nil, int64(sintercard(cx, sets, limit)))
	})
}

// SintercardKeys returns SINTERCARD's operand keys for dispatch's co-location
// check, nil when the tail is malformed (the point path then answers the
// parse error in place).
func SintercardKeys(args [][]byte) [][]byte {
	keys, _, msg := sintercardArgs(args)
	if msg != "" {
		return nil
	}
	return keys
}
