package zset

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/tamnd/aki/f3srv/resp"
)

// The inline zset band (spec 2064/f3/12 section 4): a small zset is one packed
// score-member blob and carries no tree and no member hash. Entries live in
// score-then-member order (score ascending, ties broken by raw member bytes),
// the zset total order, so ZRANGE by index is a direct slice and ZRANK counts
// while scanning. A write that breaches a cap converts one way to the native
// band (F4, never backward), matching Redis's listpack-to-skiplist latch so
// OBJECT ENCODING answers listpack then skiplist exactly.
//
// The caps mirror Redis's own zset-max-listpack-entries (128) and
// zset-max-listpack-value (64), frozen here as constants so the differential
// encoding test checks parity against a same-version Redis.
const (
	maxListpackEntries = 128
	maxListpackValue   = 64
)

// encoding is the zset's storage shape, and the string OBJECT ENCODING reports.
type encoding uint8

const (
	encListpack encoding = iota
	encSkiplist          // the native band (skiplist.go: member hash plus counted tree)
)

func (e encoding) String() string {
	if e == encListpack {
		return "listpack"
	}
	return "skiplist"
}

// zset is one key's sorted set. Exactly one representation is live at a time,
// named by enc. It is owner-local: only the shard goroutine touches it, so
// nothing here locks.
type zset struct {
	enc encoding

	// listpack-class: packed entries in score-then-member order, each
	// [len:uint8][tag:uint8][member bytes][score:8 big-endian float64 bits]. len
	// is at most maxListpackValue so it fits one byte; tag is the member's first
	// byte (0 when empty) for the scan's fast reject. n counts entries so ZCARD
	// never rescans.
	//
	// The score is stored as raw IEEE-754 bits, not the order-preserving
	// sortable form of doc 12 section 3.1. The band has no separate member hash,
	// so the blob is the only place ZSCORE can read a score from, and section
	// 3.1's rule is that the raw-bits copy is what ZSCORE formats without a
	// decode. One exception: -0.0 is stored as +0.0, because Redis's listpack
	// integer-encodes a zero score and loses the sign, so an inline member added
	// at -0.0 answers ZSCORE "0" on both engines; only the native band keeps the
	// sign, exactly like Redis's skiplist. Ordering treats -0.0 and +0.0 as
	// equal either way because a plain float compare does.
	blob []byte
	n    int

	// skiplist-class: the native band (skiplist.go). Built by listpackToNative
	// and never converted back (F4).
	nat *nativeStore
}

// newZset builds an empty listpack-class zset. Redis has no intset analogue for
// the zset, so every new zset opens as a listpack.
func newZset() *zset { return &zset{enc: encListpack} }

// card is the member count.
func (z *zset) card() int {
	if z.enc == encListpack {
		return z.n
	}
	return z.nat.card()
}

// score returns the member's score and whether it is present. Zero allocation
// on both branches: the listpack scan compares in place, and the native band
// is one member-hash probe that formats from the record's raw score bits.
func (z *zset) score(m []byte) (float64, bool) {
	if z.enc == encListpack {
		if off := z.listpackIndex(m); off >= 0 {
			_, s, _ := decodeEntry(z.blob, off)
			return s, true
		}
		return 0, false
	}
	return z.nat.score(m)
}

// listpackIndex returns the byte offset of m's entry, or -1 when absent. The
// tag and length are checked before the byte compare so most misses cost two
// byte loads.
func (z *zset) listpackIndex(m []byte) int {
	tag := tagOf(m)
	b := z.blob
	for i := 0; i < len(b); {
		n := int(b[i])
		start := i + 2
		if b[i+1] == tag && n == len(m) && bytes.Equal(b[start:start+n], m) {
			return i
		}
		i = start + n + 8
	}
	return -1
}

// flags carries the ZADD option matrix (spec 2064/f3/12 section 6.1).
type flags struct {
	nx, xx, gt, lt, ch, incr bool
}

// update applies one (member, score) pair under the flag matrix, executed
// serially on the owner exactly as Redis's zsetAdd does. The returns are:
// added (a new member landed), changed (an existing score moved), newScore
// (the resulting score, meaningful when applied), applied (a value was written
// or left in place rather than suppressed by a flag; drives the INCR nil
// reply), and nan (an INCR produced NaN, the caller rejects the command).
func (z *zset) update(m []byte, score float64, fl flags) (added, changed bool, newScore float64, applied, nan bool) {
	old, present := z.score(m)
	if present {
		if fl.nx {
			return false, false, old, false, false
		}
		if fl.incr {
			score = old + score
			if math.IsNaN(score) {
				return false, false, 0, false, true
			}
		}
		if (fl.gt && !(score > old)) || (fl.lt && !(score < old)) {
			return false, false, old, false, false
		}
		if score != old {
			z.rescore(m, score)
			return false, true, score, true, false
		}
		// Idempotent re-add: nothing written, but INCR still reports the score.
		return false, false, score, true, false
	}
	// Absent. XX suppresses the insert; GT and LT still add absent members. For
	// INCR the score is the increment itself (0 + delta), already in score.
	if fl.xx {
		return false, false, 0, false, false
	}
	z.insert(m, score)
	return true, false, score, true, false
}

// insert adds a new member (the caller has checked it is absent), converting
// the band one way when the write breaches a cap.
func (z *zset) insert(m []byte, score float64) {
	if z.enc == encListpack {
		if z.n+1 > maxListpackEntries || len(m) > maxListpackValue {
			z.listpackToNative()
			z.nat.insert(m, score)
			return
		}
		z.listpackInsert(m, score)
		return
	}
	z.nat.insert(m, score)
}

// rescore moves an existing member to a new score. In the listpack band the
// entry's ordinal position changes, so it is a remove-then-insert; neither the
// count nor the member length changes, so no conversion can trigger.
func (z *zset) rescore(m []byte, score float64) {
	if z.enc == encListpack {
		z.listpackRemove(m)
		z.listpackInsert(m, score)
		return
	}
	z.nat.rescore(m, score)
}

// rem deletes m and reports whether it was present. Removal never changes the
// encoding: a zset only ever converts upward (F4), so a shrinking native band
// stays native, matching Redis.
func (z *zset) rem(m []byte) bool {
	if z.enc == encListpack {
		return z.listpackRemove(m)
	}
	return z.nat.rem(m)
}

// entryView is one member and its score in a read snapshot. member aliases the
// blob (listpack) or the native slab; a read command holds it only until the
// reply is built, and no write runs during a read on the owner.
type entryView struct {
	member []byte
	score  float64
}

// entries returns every member in ascending zset order. It backs ZRANGE and
// ZRANK, which are linear over the inline band per section 4.
func (z *zset) entries() []entryView {
	out := make([]entryView, 0, z.card())
	if z.enc == encListpack {
		b := z.blob
		for i := 0; i < len(b); {
			m, s, next := decodeEntry(b, i)
			out = append(out, entryView{member: m, score: s})
			i = next
		}
		return out
	}
	z.nat.each(func(m []byte, s float64) { out = append(out, entryView{member: m, score: s}) })
	return out
}

// rank returns the number of members sorting before m, its score, and whether
// it is present. Linear over the inline band (count while scanning, section
// 6.3); the native band is one hash probe plus a counted descent (nat.rank).
func (z *zset) rank(m []byte) (int, float64, bool) {
	if z.enc == encSkiplist {
		return z.nat.rank(m)
	}
	sc, ok := z.score(m)
	if !ok {
		return 0, 0, false
	}
	idx := 0
	b := z.blob
	for i := 0; i < len(b); {
		em, _, next := decodeEntry(b, i)
		if bytes.Equal(em, m) {
			return idx, sc, true
		}
		idx++
		i = next
	}
	return idx, sc, true
}

// rangeByIndex streams the members at ranks lo..hi inclusive (already clamped)
// into out as RESP bulk strings, with each score appended when withScores, and
// returns the grown buffer. When rev the window is emitted high-to-low, the
// ZRANGE REV and ZREVRANGE order. The inline band slices its already-ordered
// blob; the native band seeks with a counted select and walks the leaf chain
// over just the window (section 6.4), formatting each score straight from the
// record's raw bits so a native -0.0 prints "-0" while the inline band prints
// "0". out is the shard scratch, reused across commands, so a warm buffer grows
// for none of the window's elements.
func (z *zset) rangeByIndex(out []byte, lo, hi int, rev, withScores bool) []byte {
	var sc [40]byte
	emit := func(m []byte, bits uint64) {
		out = resp.AppendBulk(out, m)
		if withScores {
			out = resp.AppendBulk(out, resp.FormatScore(sc[:0], math.Float64frombits(bits)))
		}
	}
	if z.enc == encSkiplist {
		if rev {
			// The window indexes the reversed sequence; reversed index i is
			// forward rank card-1-i, so [lo,hi] maps to the forward-rank window
			// [card-1-hi, card-1-lo], walked high-to-low.
			card := z.nat.card()
			z.nat.walkRangeRev(card-1-hi, card-1-lo, emit)
		} else {
			z.nat.walkRange(lo, hi, emit)
		}
		return out
	}
	ev := z.entries()
	if rev {
		reverse(ev)
	}
	for j := lo; j <= hi; j++ {
		emit(ev[j].member, math.Float64bits(ev[j].score))
	}
	return out
}

// listpackInsert writes one entry at its sorted position with a single memmove,
// copying the member bytes so the argument view is never retained. A -0.0
// score lands as +0.0, the collapse Redis's listpack integer encoding applies
// (see the blob comment); the native band has no such step.
func (z *zset) listpackInsert(m []byte, score float64) {
	if score == 0 {
		score = 0 // -0.0 collapses, matching Redis's listpack int encoding
	}
	off := z.listpackSeek(m, score)
	entryLen := 2 + len(m) + 8
	z.blob = append(z.blob, make([]byte, entryLen)...)
	copy(z.blob[off+entryLen:], z.blob[off:])
	z.blob[off] = byte(len(m))
	z.blob[off+1] = tagOf(m)
	copy(z.blob[off+2:], m)
	binary.BigEndian.PutUint64(z.blob[off+2+len(m):], math.Float64bits(score))
	z.n++
}

// listpackSeek returns the byte offset of the first entry that sorts at or
// after (score, m), which is where a new member is spliced in.
func (z *zset) listpackSeek(m []byte, score float64) int {
	b := z.blob
	for i := 0; i < len(b); {
		em, es, next := decodeEntry(b, i)
		if lessPair(score, m, es, em) {
			return i
		}
		i = next
	}
	return len(b)
}

// listpackRemove deletes m's entry with one memmove and reports whether it was
// present.
func (z *zset) listpackRemove(m []byte) bool {
	i := z.listpackIndex(m)
	if i < 0 {
		return false
	}
	end := i + 2 + int(z.blob[i]) + 8
	z.blob = append(z.blob[:i], z.blob[end:]...)
	z.n--
	return true
}

// listpackToNative engages the native band on the blob, the one-way transition
// of section 4. Entries are already in order, so they fill the member hash in
// one pass and bulk-load the tree at the right-edge 0.9 fill, no re-sort.
func (z *zset) listpackToNative() {
	nat := newNativeStore(z.n + 1)
	b := z.blob
	for i := 0; i < len(b); {
		m, s, next := decodeEntry(b, i)
		nat.appendSorted(m, s)
		i = next
	}
	nat.seal()
	z.nat = nat
	z.blob = nil
	z.n = 0
	z.enc = encSkiplist
}

// decodeEntry reads the entry at offset i: its member (aliasing the blob), its
// score, and the offset of the next entry.
func decodeEntry(b []byte, i int) (member []byte, score float64, next int) {
	n := int(b[i])
	start := i + 2
	member = b[start : start+n]
	score = math.Float64frombits(binary.BigEndian.Uint64(b[start+n:]))
	next = start + n + 8
	return
}

// tagOf is the entry's fast-reject byte: the member's first byte, 0 for the
// empty member.
func tagOf(m []byte) byte {
	if len(m) > 0 {
		return m[0]
	}
	return 0
}

// lessPair reports whether (sA, mA) sorts before (sB, mB) in the zset total
// order: score ascending, ties broken by raw member bytes. A plain float
// compare treats -0.0 and +0.0 as equal and orders the infinities correctly,
// and NaN never reaches here (rejected at the command door).
func lessPair(sA float64, mA []byte, sB float64, mB []byte) bool {
	if sA != sB {
		return sA < sB
	}
	return bytes.Compare(mA, mB) < 0
}
