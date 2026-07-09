package setvecbuild

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// vecSlots mirrors the engine's read snapshot: an immutable header a lock-free draw loads through an
// atomic pointer so it reads a consistent (backing, len) pair rather than a torn slice header.
type vecSlots struct {
	s []uint64
}

// memberVec mirrors engine/f1raw's memberVec: the published read snapshot (view), the writer's working
// slots slice, and the offset-to-slot back index. The field shapes and the add/publish discipline match
// the engine so the two build shapes are measured on the real structure, not a proxy.
type memberVec struct {
	view  atomic.Pointer[vecSlots]
	slots []uint64
	back  map[uint64]int
}

func (v *memberVec) publish() { v.view.Store(&vecSlots{s: v.slots}) }

// ensureBack mirrors the engine's lazy back-index build: the map is nil after a bulk build and the first
// mutation materializes it once from the slots snapshot, so a build that is only ever drawn or re-cleared
// never pays for it.
func (v *memberVec) ensureBack() {
	if v.back != nil {
		return
	}
	v.back = make(map[uint64]int, len(v.slots))
	for i, off := range v.slots {
		v.back[off] = i
	}
}

// add is the engine's add: materialize back if needed, skip a duplicate offset, record the slot, append
// (reusing freed capacity with an atomic store, else reallocating), and republish the snapshot. The
// per-member publish and the map insert are the cost the bulk build removes.
func (v *memberVec) add(off uint64) {
	v.ensureBack()
	if _, ok := v.back[off]; ok {
		return
	}
	i := len(v.slots)
	v.back[off] = i
	if i < cap(v.slots) {
		v.slots = v.slots[:i+1]
		atomic.StoreUint64(&v.slots[i], off)
	} else {
		v.slots = append(v.slots, off)
	}
	v.publish()
}

func newVec(capHint int) *memberVec {
	v := &memberVec{
		slots: make([]uint64, 0, capHint),
		back:  make(map[uint64]int, capHint),
	}
	v.publish()
	return v
}

// perMemberBuild is the pre-fix STORE path: start from the 64-capacity vector deriveOnFirstDraw builds
// after clearSetRows dropped the old one, then add each stored member's offset, paying a map insert, a
// snapshot allocation, and an occasional slots doubling per member.
func perMemberBuild(offs []uint64) *memberVec {
	v := newVec(64)
	for _, off := range offs {
		v.add(off)
	}
	return v
}

// eagerBulkBuild is the first bulk fix: size slots and back to the exact cardinality, fill both in one
// pass, publish once. It drops the per-member publishes and doubling but still builds the whole back map
// at STORE time, which at large cardinalities dominates: a big map roots a wide GC scan and fills with
// poor locality, so it can be slower than the per-member adds it replaced.
func eagerBulkBuild(offs []uint64) *memberVec {
	v := &memberVec{
		slots: make([]uint64, 0, len(offs)),
		back:  make(map[uint64]int, len(offs)),
	}
	for _, off := range offs {
		if _, ok := v.back[off]; ok {
			continue
		}
		v.back[off] = len(v.slots)
		v.slots = append(v.slots, off)
	}
	v.publish()
	return v
}

// lazyBulkBuild is the post-fix path: copy the offsets into slots in one append, publish once, and leave
// back nil. The back index is only read by the mutation paths (add/remove/retierSlot), never by any draw
// or membership read, so a STORE destination that is only drawn or re-cleared never builds it; the first
// mutation materializes it through ensureBack. This drops both the per-member work and the whole-map
// construction the eager bulk still paid.
func lazyBulkBuild(offs []uint64) *memberVec {
	v := &memberVec{slots: make([]uint64, 0, len(offs))}
	v.slots = append(v.slots, offs...)
	v.publish()
	return v
}

// storeResultOffsets models the offsets a STORE hands the vector build: k distinct arena offsets in the
// order the result members are inserted. Offsets are record-region addresses, so they are spread rather
// than dense; the exact spacing does not change the build cost (the map hashes the full uint64), so a
// fixed stride stands in for the arena layout.
func storeResultOffsets(k int) []uint64 {
	offs := make([]uint64, k)
	for i := range offs {
		offs[i] = uint64(i)*48 + 1024
	}
	return offs
}

func BenchmarkVecBuild(b *testing.B) {
	for _, k := range []int{500, 5000, 50000} {
		offs := storeResultOffsets(k)
		b.Run("k="+strconv.Itoa(k)+"/perMemberBuild", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				v := perMemberBuild(offs)
				if len(v.view.Load().s) != k {
					b.Fatalf("card = %d, want %d", len(v.view.Load().s), k)
				}
			}
		})
		b.Run("k="+strconv.Itoa(k)+"/eagerBulkBuild", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				v := eagerBulkBuild(offs)
				if len(v.view.Load().s) != k {
					b.Fatalf("card = %d, want %d", len(v.view.Load().s), k)
				}
			}
		})
		b.Run("k="+strconv.Itoa(k)+"/lazyBulkBuild", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				v := lazyBulkBuild(offs)
				if len(v.view.Load().s) != k {
					b.Fatalf("card = %d, want %d", len(v.view.Load().s), k)
				}
			}
		})
	}
}

// TestBuildsAgree pins the correctness the fix rests on: all three builds produce the same slots, and the
// lazy build's back index, once materialized through ensureBack, matches the eager one slot for slot. If
// this fails the lazy build is not a drop-in replacement and the store path cannot defer the map.
func TestBuildsAgree(t *testing.T) {
	for _, k := range []int{0, 1, 2, 64, 65, 500, 5000} {
		offs := storeResultOffsets(k)
		a, e, l := perMemberBuild(offs), eagerBulkBuild(offs), lazyBulkBuild(offs)
		as, es, ls := a.view.Load().s, e.view.Load().s, l.view.Load().s
		if len(as) != len(es) || len(as) != len(ls) {
			t.Fatalf("k=%d: len(slots) perMember=%d eager=%d lazy=%d", k, len(as), len(es), len(ls))
		}
		for i := range as {
			if as[i] != es[i] || as[i] != ls[i] {
				t.Fatalf("k=%d: slot %d perMember=%d eager=%d lazy=%d", k, i, as[i], es[i], ls[i])
			}
		}
		if l.back != nil {
			t.Fatalf("k=%d: lazy build must leave back nil until first mutation", k)
		}
		l.ensureBack()
		if len(l.back) != len(e.back) {
			t.Fatalf("k=%d: len(back) lazy=%d eager=%d", k, len(l.back), len(e.back))
		}
		for off, slot := range e.back {
			if l.back[off] != slot {
				t.Fatalf("k=%d: back[%d] lazy=%d eager=%d", k, off, l.back[off], slot)
			}
		}
	}
}
