package list

// Boot replay's entry points into the list registry (spec 2064/obs1 doc
// 04 section 2), under the contract set/replay.go states: plain
// arguments, the worker's real Ctx under the BootCtx contract, literal
// application with loud divergence, and no clock. List frames are
// positional: the serve loop resolved every signed index, pivot search,
// and LREM value scan before framing, so replay is pure position
// surgery over the resolved indices and never re-compares element
// bytes. Pushes and pops ride the serve loop's own band-dispatching
// primitives, so budget promotion happens at the same element it did at
// serve time and OBJECT ENCODING agrees across a restart.

import (
	"fmt"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// ReplayPush applies one sided push frame's values, in push order
// exactly as the emission seam carries them. create is true when a
// collnew led the frame: the list is rebuilt empty after dropping
// whatever the key held, the reset-to-empty rule. Without create the
// list must exist.
func ReplayPush(cx *shard.Ctx, key []byte, values [][]byte, front, create bool) error {
	if len(values) == 0 {
		return fmt.Errorf("list replay: push on %q carries no values", key)
	}
	g := registry(cx)
	var l *list
	if create {
		g.drop(key)
		l = newList()
		g.m[string(key)] = l
	} else if l = g.m[string(key)]; l == nil {
		return fmt.Errorf("list replay: push names key %q but no list exists", key)
	}
	for _, v := range values {
		if front {
			l.pushFront(v)
		} else {
			l.pushBack(v)
		}
	}
	g.note(l)
	return nil
}

// ReplayPop removes count elements from one end. The frame carries only
// the decided count because pops are deterministic given prior state, so
// a count the list cannot cover is divergence, and a pop that empties
// the list leaves it at zero elements until its colldrop frame lands.
func ReplayPop(cx *shard.Ctx, key []byte, front bool, count uint32) error {
	g := registry(cx)
	l := g.m[string(key)]
	if l == nil {
		return fmt.Errorf("list replay: pop names key %q but no list exists", key)
	}
	if int(count) > l.length() || count == 0 {
		return fmt.Errorf("list replay: pop of %d from list %q of %d elements", count, key, l.length())
	}
	for range count {
		if front {
			l.popFront()
		} else {
			l.popBack()
		}
	}
	g.note(l)
	return nil
}

// ReplaySet overwrites the element at the resolved non-negative index
// the frame carries; an index outside the list is divergence.
func ReplaySet(cx *shard.Ctx, key []byte, index int64, value []byte) error {
	g := registry(cx)
	l := g.m[string(key)]
	if l == nil {
		return fmt.Errorf("list replay: lset names key %q but no list exists", key)
	}
	if index < 0 || index >= int64(l.length()) {
		return fmt.Errorf("list replay: lset index %d is outside list %q of %d elements", index, key, l.length())
	}
	l.setAt(int(index), value)
	g.note(l)
	return nil
}

// ReplayRem removes the elements at the framed positions, indices in
// the pre-removal list, strictly ascending: LREM's value scan already
// resolved which occurrences leave, so this is a positional filter with
// no byte compare. An index outside the list or out of order is
// divergence, and an emptied list stays until its colldrop frame lands.
func ReplayRem(cx *shard.Ctx, key []byte, indices []uint32) error {
	g := registry(cx)
	l := g.m[string(key)]
	if l == nil {
		return fmt.Errorf("list replay: lrem names key %q but no list exists", key)
	}
	if len(indices) == 0 {
		return fmt.Errorf("list replay: lrem on %q carries no positions", key)
	}
	n := l.length()
	for i, idx := range indices {
		if int(idx) >= n || (i > 0 && idx <= indices[i-1]) {
			return fmt.Errorf("list replay: lrem position %d is invalid in list %q of %d elements", idx, key, n)
		}
	}
	elems := l.decodeAll()
	kept := elems[:0:0]
	next := 0
	for i, e := range elems {
		if next < len(indices) && uint32(i) == indices[next] {
			next++
			continue
		}
		kept = append(kept, e)
	}
	l.rebuildFrom(kept)
	g.note(l)
	return nil
}

// ReplayIns splices the value in so it lands at the framed index of the
// resulting list; the pivot search happened at serve time and never hit
// the WAL. The index is at most the current length, since LINSERT AFTER
// the tail lands one past it.
func ReplayIns(cx *shard.Ctx, key []byte, index int64, value []byte) error {
	g := registry(cx)
	l := g.m[string(key)]
	if l == nil {
		return fmt.Errorf("list replay: lins names key %q but no list exists", key)
	}
	if index < 0 || index > int64(l.length()) {
		return fmt.Errorf("list replay: lins index %d is outside list %q of %d elements", index, key, l.length())
	}
	elems := l.decodeAll()
	at := int(index)
	elems = append(elems, nil)
	copy(elems[at+1:], elems[at:])
	elems[at] = value
	l.rebuildFrom(elems)
	g.note(l)
	return nil
}

// ReplayDrop removes key's list and reports whether one existed, the
// colldrop and keydel probe.
func ReplayDrop(cx *shard.Ctx, key []byte) bool {
	g := registry(cx)
	if g.m[string(key)] == nil {
		return false
	}
	g.drop(key)
	return true
}

// decodeAll materializes every element for an interior splice: the
// inline band's frames alias its blob, which reencodeInline tolerates,
// and the native band's copy is what rebuild requires.
func (l *list) decodeAll() [][]byte {
	if l.nat != nil {
		return l.nat.toSlice()
	}
	return l.inlineDecode()
}

// rebuildFrom repacks the list from the spliced elements through the
// band's own rebuild path, so the inline band re-checks its budget (and
// promotes at the same boundary serve time would) and a native band
// stays native, the sticky quicklist rule.
func (l *list) rebuildFrom(elems [][]byte) {
	if l.nat != nil {
		l.nat.rebuild(elems)
		return
	}
	l.reencodeInline(elems)
}
