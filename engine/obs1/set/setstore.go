package set

import (
	"slices"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The STORE forms (spec 2064/f3/11 section 7): SINTERSTORE, SUNIONSTORE, and
// SDIFFSTORE compute a result set and write it into a destination key, replacing
// whatever that key held and returning the result cardinality. An empty result
// deletes the destination and replies 0. L11 named these the largest untouched
// deficit in the f1 scoreboard (0.30x-0.55x), and section 7 decomposes the loss
// into three measured causes, each with a lab and each answered here:
//
//   - Cause 1, the fold-per-member destination build (setstorebuild): f1 built
//     the destination by calling the ordinary SADD path once per result member,
//     which re-probed and re-maintained the sorted arrays per element, an
//     O(k^2)-shaped build. f3 bulk-builds: the result streams element-per-row
//     into a fresh table through addRaw (member.go), which carries no algebra
//     maintenance tax, so the build is one linear pass with no per-member fold.
//
//   - Cause 2, the seen-set (setunionstore): f1's SUNIONSTORE kept a separate
//     seen-set to dedup members before inserting them into the destination, which
//     double-dedups, since the destination's own table already rejects
//     duplicates. f3 keeps no seen-set anywhere in the STORE paths; the fresh
//     destination table is the one and only dedup (unionInto below feeds every
//     source member straight through addRaw).
//
//   - Cause 3, arena amplification: f1's grow-only arena and a loadSets clone
//     amplified every STORE into copies that outlived the command. f3 never
//     clones a source. The destination is streamed into fresh storage and swapped
//     in at the end (place below), so an aliasing STORE (destination is also a
//     source) needs no defensive copy: the result is fully built off the sources
//     before the destination pointer moves.
//
// The band is chosen from the result, not inherited (section 7): a freshly built
// destination picks intset, listpack, or hashtable by its final cardinality and
// shape rather than growing through the conversion ladder, so a small result
// lands in the compact inline band directly (bandFor below).

// storeResult builds the destination set from the driver's emitted members. The
// members flow element-per-row into a fresh native table through addRaw (the
// bulk build, no fold-per-member and no algebra tax), and the table's own probe
// is the only dedup (no seen-set). It then picks the band from the final result
// shape and returns the destination set, or nil for an empty result, which the
// caller turns into a destination delete.
//
// storeResult knows nothing about the destination key: it consumes already
// gathered source sets through drive and produces an independent result, so the
// aliasing case (destination is also a source) is safe by construction here, and
// place swaps the result in only after this returns. That is the no-aliased-clone
// rule made structural: there is no clone step anywhere.
func storeResult(hint int, drive func(emit func(m []byte))) *set {
	ht := newHashtable(hint)
	allInt := true
	maxLen := 0
	drive(func(m []byte) {
		if !ht.addRaw(m) {
			return // a duplicate the destination table already holds, the only dedup
		}
		if len(m) > maxLen {
			maxLen = len(m)
		}
		if allInt {
			if _, ok := store.ParseInt(m); !ok {
				allInt = false
			}
		}
	})
	if ht.card() == 0 {
		return nil
	}
	if ht.card() >= partitionThreshold {
		// A destination that crosses the engagement threshold is born partitioned,
		// not left as one oversized native table (doc 11 section 7): the result is
		// already bulk-built, so pre-partitioning it here is one more redistribution
		// pass, and it saves the set from paying a first rehash pause on its next
		// write. Picking the band at build time is not a downward conversion (F4).
		return partitionedSet(ht)
	}
	return bandFor(ht, allInt, maxLen, ht.card())
}

// bandFor picks the freshly built destination's encoding from the result's final
// cardinality and shape (doc 11 section 7): an integer-only result at or under
// the intset cap lands intset-class, a small short-membered result lands
// listpack-class, and everything else keeps the native table it was built in.
// Choosing the band at build time is not a downward conversion (F4): the result
// is born in its final band rather than growing through the ladder, so no
// one-way rule is crossed. The two inline branches walk the built table once to
// re-seat the members compactly and drop the table.
func bandFor(ht *htable, allInt bool, maxLen, n int) *set {
	switch {
	case allInt && n <= maxIntsetEntries:
		s := &set{enc: encIntset, ints: make([]int64, 0, n)}
		ht.each(func(m []byte) {
			v, _ := store.ParseInt(m) // allInt guarantees ok
			s.ints = append(s.ints, v)
		})
		slices.Sort(s.ints) // intset is sorted ascending; the members are already distinct
		return s
	case n <= maxListpackEntries && maxLen <= maxListpackValue:
		s := &set{enc: encListpack}
		ht.each(func(m []byte) { s.appendListpack(m) })
		return s
	default:
		return &set{enc: encHashtable, ht: ht}
	}
}

// place installs the freshly built result as the destination key, replacing
// whatever the key held. Redis STORE overwrites any prior type and discards any
// TTL: the destination is a brand-new object, so an old string value and its
// expiry both go (deleting the string record drops the expiry it carried), and
// an old set is replaced wholesale. An empty result (result == nil) deletes the
// destination and leaves it absent. It returns the result cardinality, the STORE
// reply.
//
// place runs after storeResult has fully built the result off the sources, so
// when the destination is also a source the swap below is the first and only
// time the destination pointer moves, and the source was read in full before it
// did. Nothing is cloned to guard the aliasing.
func place(cx *shard.Ctx, g *reg, key []byte, result *set) int {
	// Drop whatever the key holds in the string store first, so a new set never
	// shadows a stale string and no old TTL survives (the destination is a new
	// object, Redis discards the old expiry). A set-typed destination is replaced
	// through the registry write below; the string Del is a no-op for it.
	cx.St.Del(key, cx.NowMs)
	if result == nil {
		g.drop(key)
		return 0
	}
	// A destination that already held a set is replaced wholesale: take its bytes
	// out of the running total before the swap, then post the freshly built
	// result's own footprint (drop unaccounts the old set, note accounts the new).
	// Guarded on acctOn so the swap stays a plain overwrite when the cold tier is
	// off, the L9 zero-delta path.
	if g.acctOn && g.m[string(key)] != nil {
		g.drop(key)
	}
	g.m[string(key)] = result
	g.note(result)
	return result.card()
}

// Sinterstore answers SINTERSTORE destination key [key ...]: intersect the
// sources, store the result in destination, reply its cardinality. A wrong-typed
// source is WRONGTYPE and leaves the destination untouched; an empty result
// deletes the destination. The destination may be one of the sources, which the
// bulk build handles without a clone.
func Sinterstore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	sets, wrong := gather(g, cx, args[1:])
	if wrong {
		r.Err(wrongType)
		return
	}
	// Presize from the smallest source: the intersection cannot exceed it
	// (setmergecollect, doc 11 section 6.4).
	result := storeResult(minCard(sets), func(emit func(m []byte)) { sinter(cx, sets, emit) })
	r.Int(int64(place(cx, g, args[0], result)))
}

// Sunionstore answers SUNIONSTORE destination key [key ...]: union the sources
// into destination, reply its cardinality. The destination table is the only
// dedup (the setunionstore lesson, no seen-set): every source member is fed
// straight through addRaw.
func Sunionstore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	sets, wrong := gather(g, cx, args[1:])
	if wrong {
		r.Err(wrongType)
		return
	}
	result := storeResult(totalCard(sets), func(emit func(m []byte)) { unionInto(sets, emit) })
	r.Int(int64(place(cx, g, args[0], result)))
}

// Sdiffstore answers SDIFFSTORE destination key [key ...]: the members of the
// first source not in any later source, stored in destination, reply the
// cardinality. A missing first source is an empty result and deletes the
// destination.
func Sdiffstore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	sets, wrong := gather(g, cx, args[1:])
	if wrong {
		r.Err(wrongType)
		return
	}
	// The diff cannot exceed the first source, which drives the walk.
	result := storeResult(firstCard(sets), func(emit func(m []byte)) { sdiff(cx, sets, emit) })
	r.Int(int64(place(cx, g, args[0], result)))
}

// unionInto streams every member of every source into emit, duplicates and all:
// the destination table dedups, so there is no seen-set here (doc 11 section 7,
// the setunionstore lesson). Missing sources contribute nothing. This differs
// from the read-side sunion, which builds a transient dedup table for the reply;
// a STORE's destination table is that table, so a second one would be waste.
func unionInto(sets []*set, emit func(m []byte)) {
	for _, s := range sets {
		if s != nil {
			s.each(emit)
		}
	}
}

// minCard is the smallest source cardinality, skipping missing sources, or 0 if
// any source is missing (an intersection with a missing set is empty). It sizes
// the intersection destination table.
func minCard(sets []*set) int {
	m := -1
	for _, s := range sets {
		if s == nil {
			return 0
		}
		if m < 0 || s.card() < m {
			m = s.card()
		}
	}
	if m < 0 {
		return 0
	}
	return m
}

// totalCard sums the source cardinalities, the upper bound on a union, so the
// destination table presizes once and never rehashes mid-build.
func totalCard(sets []*set) int {
	n := 0
	for _, s := range sets {
		if s != nil {
			n += s.card()
		}
	}
	return n
}

// firstCard is the first source's cardinality, the upper bound on a diff that
// the first source drives.
func firstCard(sets []*set) int {
	if len(sets) == 0 || sets[0] == nil {
		return 0
	}
	return sets[0].card()
}
