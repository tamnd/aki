package set

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The cross-shard STORE forms (spec 2064/f3/03 section 6.7, spec 2064/f3/11
// section 7): SINTERSTORE, SUNIONSTORE, and SDIFFSTORE over a destination and
// sources that do not all share one shard, the write half of the F17 intent
// path. Dispatch sends a command here only when the keys really do span shards;
// the co-located case never comes near this file and keeps the owner-local
// handlers in setstore.go, which stay byte-identical to what they shipped.
//
// The plan mirrors the read-side gather (gathercross.go), with the write landing
// on the destination's owner rather than the first source's. The transaction
// holds an intent on every key, so destination and sources are frozen at one
// point between every other command's critical sections. One hop per remote
// shard clones that shard's sources into plain heap sets (cloneSet), then one
// hop on the destination's owner reads the sources it holds directly, runs the
// same driver the co-located handler runs, builds the result off every source,
// and installs it with place. The clones are ordinary unindexed sets, so every
// operand pair takes the probe path the driver documents as the always-correct
// baseline; the stored members are identical to a co-located arrangement of the
// same data, the contract the differential suite pins. The destination is
// written only after the result is fully built off the sources, so an aliasing
// STORE (destination is also a source) needs no clone, exactly as the point
// path's place already documents.

// storeCross runs a STORE form over cross-shard sources under the transaction's
// barrier and returns the integer-cardinality reply. dest is args[0]; the
// sources are args[1:]. Sources owned by a shard other than the destination's
// are cloned with one hop per shard; sources co-located with the destination are
// read straight from its registry inside the write hop, zero-copy. Any source
// holding a string answers WRONGTYPE and leaves the destination untouched, the
// same precheck the point handler makes. hint sizes the result table from the
// gathered sources, drive emits the result members, and event is the keyspace
// event place fires on the destination.
func storeCross(t *shard.Txn, args [][]byte, event string, hint func([]*set) int, drive func(cx *shard.Ctx, sets []*set, emit func(m []byte))) []byte {
	dest := args[0]
	srcKeys := args[1:]
	primary := t.Shard(dest)
	sets := make([]*set, len(srcKeys))
	done := make([]bool, len(srcKeys))
	wrong := false
	// Clone every source owned by a shard other than the destination's, one hop
	// per such shard. Sources co-located with the destination are left for the
	// write hop, which reads them without a copy.
	for i := range srcKeys {
		sh := t.Shard(srcKeys[i])
		if sh == primary || done[i] || wrong {
			continue
		}
		var idxs []int
		for j := i; j < len(srcKeys); j++ {
			if !done[j] && t.Shard(srcKeys[j]) == sh {
				idxs = append(idxs, j)
				done[j] = true
			}
		}
		t.Do(srcKeys[i], func(cx *shard.Ctx) {
			g := registry(cx)
			for _, j := range idxs {
				s, w := g.operand(cx, srcKeys[j])
				if w {
					wrong = true
					return
				}
				sets[j] = cloneSet(s)
			}
		})
	}
	if wrong {
		return resp.AppendError(nil, wrongType)
	}
	var card int
	t.Do(dest, func(cx *shard.Ctx) {
		g := registry(cx)
		// Read the sources the destination's shard owns, type-checking each before
		// any write, so a wrong-typed local source leaves the destination untouched
		// (the point handler's up-front WRONGTYPE, preserved across shards).
		for j := range srcKeys {
			if t.Shard(srcKeys[j]) != primary {
				continue
			}
			s, w := g.operand(cx, srcKeys[j])
			if w {
				wrong = true
				return
			}
			sets[j] = s
		}
		result := storeResult(hint(sets), func(emit func(m []byte)) { drive(cx, sets, emit) })
		card = place(cx, g, dest, result, event)
	})
	if wrong {
		return resp.AppendError(nil, wrongType)
	}
	return resp.AppendInt(nil, int64(card))
}

// SinterstoreCross answers SINTERSTORE over cross-shard sources.
func SinterstoreCross(t *shard.Txn, args [][]byte) []byte {
	return storeCross(t, args, "sinterstore", minCard, sinter)
}

// SunionstoreCross answers SUNIONSTORE over cross-shard sources.
func SunionstoreCross(t *shard.Txn, args [][]byte) []byte {
	return storeCross(t, args, "sunionstore", totalCard,
		func(cx *shard.Ctx, sets []*set, emit func(m []byte)) { unionInto(sets, emit) })
}

// SdiffstoreCross answers SDIFFSTORE over cross-shard sources.
func SdiffstoreCross(t *shard.Txn, args [][]byte) []byte {
	return storeCross(t, args, "sdiffstore", firstCard, sdiff)
}
