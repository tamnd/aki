package sqlo1

import "unsafe"

// Hierarchical timer wheel, doc 11 section 3.1: three levels of 256
// buckets (1s ticks, then 256s, then 65536s spans) over resident
// volatile keys, 8 bytes per entry, keyed by the header's expireLo
// projection. Invalidation is lazy per the wheel lab verdict
// (labs/sqlo1/s1/02_wheel): an EXPIRE rewrite, PERSIST, DEL, or
// eviction never touches the old entry; the entry carries the expiry it
// filed and is dropped the moment any pass sees it no longer matches
// the header, so stale bloat is churn-proportional and self-draining
// and stale entries never ride a cascade. Filings past the level-2
// horizon park in the overflow list, rescanned daily.
//
// The wheel finds work; Del does the dying. Reaping goes through the
// ordinary tombstone path, so a reaped key drains as a Del op exactly
// like an explicit deletion (E-I5) and the reap batch is bounded so a
// due storm never stalls the command path.

const (
	wheelBuckets = 256
	wheelL1Shift = 8
	wheelL2Shift = 16
	// wheelHorizon is the level-2 reach in ticks (about 6 months at 1s
	// ticks, the doc's "about 8 months" figured loosely); beyond it an
	// entry parks in overflow.
	wheelHorizon = 1 << 24
	// wheelRescan is the overflow rescan cadence: daily at 1s ticks.
	wheelRescan = 86400
	// wheelReapBatch bounds keys reaped per pass between commands.
	wheelReapBatch = 64
)

// wheelEntry is the lab's 8-byte entry: the header slot and the expiry
// it was filed under. A slot reused by another key is still safe to
// reap on a match: equal expireLo means whoever holds the slot is due
// at this very tick, and that key's own entry becomes the stale one.
type wheelEntry struct {
	slot     uint32
	expireLo uint32
}

// The 8 bytes per volatile resident key claim is structural.
var (
	_ [8 - unsafe.Sizeof(wheelEntry{})]byte
	_ [unsafe.Sizeof(wheelEntry{}) - 8]byte
)

type wheel struct {
	ht       *HotTable
	now      uint32
	levels   [3][wheelBuckets][]wheelEntry
	overflow []wheelEntry
	// due holds validated entries waiting for bounded reap passes;
	// dueHead is the cursor so a partial pass never reshuffles.
	due     []wheelEntry
	dueHead int
}

func newWheel(ht *HotTable) *wheel {
	return &wheel{ht: ht, now: ht.tick}
}

// live reports whether entry e still names the expiry it filed: the
// slot must hold a live value carrying the same expireLo. Everything
// lazy invalidation covers (rewrite, persist, delete, eviction, slot
// reuse under a different expiry) fails this check and the entry drops.
func (w *wheel) live(e wheelEntry) bool {
	if int(e.slot) >= len(w.ht.hdrs) {
		return false
	}
	hd := &w.ht.hdrs[e.slot]
	return hd.state != 0 && hd.valRef != 0 && hd.expireLo == e.expireLo
}

// file places e by its distance from now: due entries go straight to
// the reap queue, each level covers 256 buckets of its span, and past
// the horizon is the overflow list.
func (w *wheel) file(e wheelEntry) {
	if e.expireLo <= w.now {
		w.due = append(w.due, e)
		return
	}
	switch d := e.expireLo - w.now; {
	case d < wheelBuckets:
		i := e.expireLo & (wheelBuckets - 1)
		w.levels[0][i] = append(w.levels[0][i], e)
	case d < 1<<wheelL2Shift:
		i := (e.expireLo >> wheelL1Shift) & (wheelBuckets - 1)
		w.levels[1][i] = append(w.levels[1][i], e)
	case d < wheelHorizon:
		i := (e.expireLo >> wheelL2Shift) & (wheelBuckets - 1)
		w.levels[2][i] = append(w.levels[2][i], e)
	default:
		w.overflow = append(w.overflow, e)
	}
}

// expire stamps key with an absolute coarse-tick expiry and files the
// wheel entry; at 0 is PERSIST and files nothing (the old entry, if
// any, drains lazily). It reports whether the key was there to stamp.
func (w *wheel) expire(key []byte, at uint32) bool {
	s, changed, ok := w.ht.setExpire(key, at)
	if ok && changed && at != 0 {
		w.file(wheelEntry{slot: s, expireLo: at})
	}
	return ok
}

// advance walks the wheel up to the table's tick, one tick at a time:
// higher levels cascade down at their boundaries (validating first, so
// stale entries never ride a cascade), the overflow rescans on its
// daily boundary, and the due bucket's survivors join the reap queue.
func (w *wheel) advance() {
	for w.now < w.ht.tick {
		w.now++
		t := w.now
		if t&(1<<wheelL2Shift-1) == 0 {
			w.cascade(&w.levels[2][(t>>wheelL2Shift)&(wheelBuckets-1)])
		}
		if t&(wheelBuckets-1) == 0 {
			w.cascade(&w.levels[1][(t>>wheelL1Shift)&(wheelBuckets-1)])
		}
		if t%wheelRescan == 0 {
			w.rescanOverflow()
		}
		b := &w.levels[0][t&(wheelBuckets-1)]
		for _, e := range *b {
			if w.live(e) {
				w.due = append(w.due, e)
			}
		}
		*b = (*b)[:0]
	}
}

func (w *wheel) cascade(b *[]wheelEntry) {
	for _, e := range *b {
		if w.live(e) {
			w.file(e)
		}
	}
	*b = (*b)[:0]
}

func (w *wheel) rescanOverflow() {
	kept := w.overflow[:0]
	for _, e := range w.overflow {
		if !w.live(e) {
			continue
		}
		if e.expireLo-w.now < wheelHorizon {
			w.file(e)
		} else {
			kept = append(kept, e)
		}
	}
	w.overflow = kept
}

// reap deletes up to max due keys through the ordinary tombstone path
// and returns how many actually died; entries gone stale since advance
// validated them are skips. Call repeatedly between commands until the
// queue drains.
func (w *wheel) reap(max int) int {
	n := 0
	for n < max && w.dueHead < len(w.due) {
		e := w.due[w.dueHead]
		w.dueHead++
		if !w.live(e) {
			continue
		}
		hd := &w.ht.hdrs[e.slot]
		if w.ht.Del(w.ht.keys.data(hd.keyRef)) {
			n++
		}
	}
	if w.dueHead == len(w.due) {
		w.due = w.due[:0]
		w.dueHead = 0
	}
	return n
}
