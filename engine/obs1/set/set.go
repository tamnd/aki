package set

import (
	"sort"
	"strconv"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The inline set band (spec 2064/f3/11 section 3): a small set is one packed
// blob and carries no table, no vector, and no per-member allocation. Two
// shapes, the same ones Redis keeps so OBJECT ENCODING parity holds:
//
//   - intset-class: every member parses as a signed 64-bit integer, so the set
//     is a sorted []int64 answered by binary search. Cap 512.
//   - listpack-class: otherwise the set is a length-prefixed member blob
//     answered by a linear scan with a one-byte tag reject. Cap 128 entries,
//     64 bytes per member.
//
// A write that breaches a cap converts one way to the native member table
// (F4, never backward). The native table is the Swiss-style member table in
// member.go (spec 2064/f3/11 section 2): the three *ToHashtable functions build
// it in one bulk pass, and every hashtable-encoding command routes through it.
// The frozen caps are lab 02's verdict: intset binary search wins to 512, and
// the listpack cap is Redis's 128 for parity, its scan cost dominated by
// per-command fixed cost at that size.
const (
	maxIntsetEntries   = 512
	maxListpackEntries = 128
	maxListpackValue   = 64
)

// encoding is the set's storage shape, and the string OBJECT ENCODING reports.
type encoding uint8

const (
	encIntset encoding = iota
	encListpack
	encHashtable   // the native member table (member.go)
	encPartitioned // the partitioned band (partition.go): P native sub-tables
)

func (e encoding) String() string {
	switch e {
	case encIntset:
		return "intset"
	case encListpack:
		return "listpack"
	default:
		return "hashtable"
	}
}

// set is one key's inline set. Exactly one of the three representations is
// live at a time, named by enc. It is owner-local: only the shard goroutine
// touches it, so nothing here locks.
type set struct {
	enc encoding

	// intset-class: sorted ascending, unique.
	ints []int64

	// listpack-class: packed entries, each [len:uint8][tag:uint8][bytes]. len
	// is at most maxListpackValue so it fits one byte; tag is the member's
	// first byte (0 when empty) for the scan's fast reject. n counts entries so
	// card never rescans.
	blob []byte
	n    int

	// hashtable-class: the native member table (member.go). Built by the
	// *ToHashtable conversions and never converted back (F4).
	ht *htable

	// partitioned-class: P native sub-tables split by member hash (partition.go),
	// engaged when the native table crosses the partition threshold. Set only when
	// enc is encPartitioned; a set never converts back out of the band (F4).
	part *partitioned

	// acct is this set's resident-byte footprint as last posted to the registry
	// running total (reg.go, spec 2064/f3/06 section 6). The registry keeps
	// sum(acct) over its live sets so the shard reads a collection's resident cost
	// without walking; a mutating command reconciles this against residentBytes
	// before it returns, so the total is exact at every command boundary. It stays
	// zero, and is never touched, unless the store runs the cold tier (acctOn).
	acct uint64

	// cold is the set's cold-tier state (cold.go): the resident directory over its
	// cold chunks, the offset table its records' locators index, and the pread
	// scratch. It is nil until the demotion pass packs a member into the cold
	// region, and only the native and partitioned bands can demote (an inline set is
	// too small to be worth a chunk). A partitioned set's sub-tables all share this
	// one handle, so the directory spans the whole set.
	cold *coldChunks
}

// residentBytes estimates the set's live heap footprint from its backing
// allocations, the O(1) figure the registry sums for the shard's collection
// resident signal (spec 2064/f3/06 section 6.3). It counts the capacity that
// actually holds RAM (the intset slice, the listpack blob, or the native table's
// slab, records, draw vector, and control bytes), which is what grows with adds
// and shrinks with removes; the small fixed per-set and per-map overheads are
// left out because they do not move the demotion decision.
func (s *set) residentBytes() uint64 {
	var n uint64
	switch s.enc {
	case encIntset:
		n = uint64(cap(s.ints)) * 8
	case encListpack:
		n = uint64(cap(s.blob))
	case encPartitioned:
		n = s.part.residentBytes()
	default:
		n = s.ht.residentBytes()
	}
	if s.cold != nil {
		// A demoted set freed slab bytes into the cold region but keeps the chunk
		// directory, the offset table, and the pread scratch resident; count them so
		// the figure the demote loop reads is the true remaining footprint.
		n += s.cold.residentBytes()
	}
	return n
}

// newSet builds an empty set whose first member decides intset versus
// listpack, matching Redis: an integer first member opens an intset.
func newSet(first []byte) *set {
	if _, ok := store.ParseInt(first); ok {
		return &set{enc: encIntset}
	}
	return &set{enc: encListpack}
}

// card is the member count.
func (s *set) card() int {
	switch s.enc {
	case encIntset:
		return len(s.ints)
	case encListpack:
		return s.n
	case encPartitioned:
		return s.part.card()
	default:
		return s.ht.card()
	}
}

// has reports membership. Zero allocation on every branch: the intset parse is
// over the argument bytes, the listpack scan compares in place, and the map
// lookup takes the arg bytes as the key without a string copy (Go elides it
// for a map read).
func (s *set) has(m []byte) bool {
	switch s.enc {
	case encIntset:
		v, ok := store.ParseInt(m)
		if !ok {
			return false
		}
		return s.intsetHas(v)
	case encListpack:
		return s.listpackIndex(m) >= 0
	case encPartitioned:
		return s.part.has(m)
	default:
		return s.ht.has(m)
	}
}

func (s *set) intsetHas(v int64) bool {
	i := sort.Search(len(s.ints), func(i int) bool { return s.ints[i] >= v })
	return i < len(s.ints) && s.ints[i] == v
}

// listpackIndex returns the byte offset of m's entry, or -1 when absent. The
// tag and length are checked before the byte compare so most misses cost two
// byte loads.
func (s *set) listpackIndex(m []byte) int {
	var tag byte
	if len(m) > 0 {
		tag = m[0]
	}
	b := s.blob
	for i := 0; i < len(b); {
		n := int(b[i])
		start := i + 2
		if b[i+1] == tag && n == len(m) && string(b[start:start+n]) == string(m) {
			return i
		}
		i = start + n
	}
	return -1
}

// add inserts m, converting the band one way when the write breaches a cap. It
// returns true when the set gained a member. A no-op add (m already present)
// allocates nothing.
func (s *set) add(m []byte) bool {
	switch s.enc {
	case encIntset:
		return s.addIntset(m)
	case encListpack:
		return s.addListpack(m)
	case encPartitioned:
		return s.part.add(m)
	default:
		added := s.ht.add(m)
		if added && s.ht.card() >= partitionThreshold {
			// The write crossed the engagement threshold: split the native table
			// into the partitioned band in one bulk pass, one-way (F4). From here
			// s.part carries the set and s.ht is released (partition.go).
			s.nativeToPartitioned()
		}
		return added
	}
}

// nativeToPartitioned engages the partitioned band on the native table, the
// one-way transition of doc 11 section 4.1. It runs inline in the triggering
// SADD, redistributing every member into the derived P sub-tables; the old table
// is dropped once its members are reseated.
func (s *set) nativeToPartitioned() {
	ps := partitionedSet(s.ht)
	s.part = ps.part
	s.ht = nil
	s.enc = encPartitioned
}

func (s *set) addIntset(m []byte) bool {
	v, isInt := store.ParseInt(m)
	if !isInt {
		// A non-integer forces the intset out of its class. It goes to listpack
		// when the result still fits both listpack caps, else straight to the
		// table, exactly Redis's setTypeMaybeConvert branch.
		if len(s.ints)+1 <= maxListpackEntries && len(m) <= maxListpackValue {
			s.intsetToListpack()
			return s.addListpack(m)
		}
		s.intsetToHashtable()
		return s.ht.add(m)
	}
	i := sort.Search(len(s.ints), func(i int) bool { return s.ints[i] >= v })
	if i < len(s.ints) && s.ints[i] == v {
		return false
	}
	if len(s.ints)+1 > maxIntsetEntries {
		// The intset cap (512) is far above the listpack entry cap (128), so a
		// breach here always lands in the table; there is no listpack step.
		s.intsetToHashtable()
		return s.ht.add(m)
	}
	s.ints = append(s.ints, 0)
	copy(s.ints[i+1:], s.ints[i:])
	s.ints[i] = v
	return true
}

func (s *set) addListpack(m []byte) bool {
	if s.listpackIndex(m) >= 0 {
		return false
	}
	if s.n+1 > maxListpackEntries || len(m) > maxListpackValue {
		s.listpackToHashtable()
		return s.ht.add(m)
	}
	s.appendListpack(m)
	return true
}

// appendListpack writes one entry; the append copies the member bytes, so the
// argument view is never retained.
func (s *set) appendListpack(m []byte) {
	var tag byte
	if len(m) > 0 {
		tag = m[0]
	}
	s.blob = append(s.blob, byte(len(m)), tag)
	s.blob = append(s.blob, m...)
	s.n++
}

// rem deletes m and reports whether it was present. Removal never changes the
// encoding: a set only ever converts upward (F4), so a shrinking table stays a
// table, matching Redis.
func (s *set) rem(m []byte) bool {
	switch s.enc {
	case encIntset:
		v, ok := store.ParseInt(m)
		if !ok {
			return false
		}
		i := sort.Search(len(s.ints), func(i int) bool { return s.ints[i] >= v })
		if i >= len(s.ints) || s.ints[i] != v {
			return false
		}
		s.ints = append(s.ints[:i], s.ints[i+1:]...)
		return true
	case encListpack:
		i := s.listpackIndex(m)
		if i < 0 {
			return false
		}
		end := i + 2 + int(s.blob[i])
		s.blob = append(s.blob[:i], s.blob[end:]...)
		s.n--
		return true
	case encPartitioned:
		return s.part.rem(m)
	default:
		return s.ht.rem(m)
	}
}

// each calls fn for every member, in the encoding's natural order: intset
// ascending, listpack insertion order, table arbitrary. The []byte handed to
// fn aliases internal storage (or a scratch for integers) and is valid only
// for the call, so fn copies what it keeps.
func (s *set) each(fn func(m []byte)) {
	switch s.enc {
	case encIntset:
		var sc [20]byte
		for _, v := range s.ints {
			fn(strconv.AppendInt(sc[:0], v, 10))
		}
	case encListpack:
		b := s.blob
		for i := 0; i < len(b); {
			n := int(b[i])
			start := i + 2
			fn(b[start : start+n])
			i = start + n
		}
	case encPartitioned:
		s.part.each(fn)
	default:
		s.ht.each(fn)
	}
}

// eachUntil calls fn for each member until it returns false, the early-stop
// iteration SINTERCARD's LIMIT walk uses over any band. The []byte handed to fn
// aliases internal storage (or a scratch for integers) and is valid only for the
// call.
func (s *set) eachUntil(fn func(m []byte) bool) {
	switch s.enc {
	case encIntset:
		var sc [20]byte
		for _, v := range s.ints {
			if !fn(strconv.AppendInt(sc[:0], v, 10)) {
				return
			}
		}
	case encListpack:
		b := s.blob
		for i := 0; i < len(b); {
			n := int(b[i])
			start := i + 2
			if !fn(b[start : start+n]) {
				return
			}
			i = start + n
		}
	case encPartitioned:
		s.part.eachUntil(fn)
	default:
		s.ht.eachUntil(fn)
	}
}

// at returns the member at draw index i in [0, card), rendered into sc for the
// intset branch. Used by the uniform draw; the listpack walk is O(i), which is
// bounded by the 128 cap, and the table placeholder iterates i steps until the
// dense draw vector lands with the member-table slice.
func (s *set) at(i int, sc []byte) []byte {
	switch s.enc {
	case encIntset:
		return strconv.AppendInt(sc[:0], s.ints[i], 10)
	case encListpack:
		b := s.blob
		pos := 0
		for k := 0; k < i; k++ {
			pos += 2 + int(b[pos])
		}
		n := int(b[pos])
		return b[pos+2 : pos+2+n]
	case encPartitioned:
		return s.part.at(i)
	default:
		return s.ht.at(i)
	}
}

func (s *set) intsetToListpack() {
	var sc [20]byte
	ints := s.ints
	s.ints = nil
	s.enc = encListpack
	for _, v := range ints {
		s.appendListpack(strconv.AppendInt(sc[:0], v, 10))
	}
}

func (s *set) intsetToHashtable() {
	ints := s.ints
	s.ht = newHashtable(len(ints) + 1)
	var sc [20]byte
	for _, v := range ints {
		s.ht.add(strconv.AppendInt(sc[:0], v, 10))
	}
	s.ints = nil
	s.enc = encHashtable
}

func (s *set) listpackToHashtable() {
	// Read the blob before pointing enc at the new table: each() dispatches on
	// enc, so the walk must finish against the listpack it is draining.
	ht := newHashtable(s.n + 1)
	s.each(func(m []byte) { ht.add(m) })
	s.ht = ht
	s.blob = nil
	s.n = 0
	s.enc = encHashtable
}
