// Package collset stores a Redis set as member-ordered segments over a key-value
// store, the element-per-row collection design from spec 2064 (rewrite/03,
// implementation note 285). It is the answer to the one problem an unordered
// hash index cannot solve on its own: a set must answer point ops (SADD, SREM,
// SISMEMBER) fast and must also enumerate its members (SMEMBERS, SSCAN) in time
// proportional to the set, not to the whole keyspace.
//
// The current hybrid collection path stores a whole set as one cell that every
// SADD rewrites, O(n) per element op. This package stores a set as a handful of
// member-sorted segments plus a small routing metadata row, so an element op
// rewrites one segment (bounded by segCap), not the whole set, and a member walk
// reads the segments in order. Cold segments are cold store rows that spill to
// the single file like any other value, which is the larger-than-memory property
// neither Redis, Valkey, nor Garnet offers for a set bigger than RAM.
//
// A segment list (a B+tree with only its leaf level, routed by an in-memory
// boundary array held in the metadata row) is enough for a set: a set has no
// score dimension, so there is no need for the counted interior nodes a zset
// requires. If a set ever grows enough segments that the linear boundary array in
// metadata is itself a cost, that array is where an interior level would be
// added; for the sizes real sets reach it stays a flat, cache-friendly slice.
package collset

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// KV is the minimal store surface a set needs: point get, put, and delete of
// opaque byte rows. store.Store satisfies it, and a test map satisfies it, so the
// structure is exercised both on the real engine and in isolation.
type KV interface {
	Get(key []byte) (value []byte, found bool, err error)
	Set(key, value []byte) error
	Delete(key []byte) (existed bool, err error)
}

// segCap is the maximum members in one segment before it splits. It is the bound
// on per-write rewrite cost (a SADD rewrites at most one segment) and on the read
// amplification of a point op (a binary search within one segment). 256 keeps a
// segment near a page and a split rare.
const segCap = 256

// row-key tags. A set with id N owns the key space \x00<id:8><tag><...>, so its
// rows never collide with a user string key (which never starts \x00 here) and
// the engine's keyspace walk can skip the whole namespace by the lead byte.
const (
	tagMeta = 'M' // the routing row: count, next segment id, and the boundaries
	tagSeg  = 'S' // a member segment: <id:8>S<segno:4>
)

var errCorrupt = errors.New("collset: corrupt row")

// Set is a handle to one set's rows in the KV. It holds no member state itself;
// every operation reads the metadata row and the one segment it needs, so two
// handles to the same id stay consistent through the KV. The caller serializes
// writes to a given id (the engine's per-key or per-shard lock), exactly as it
// already does for a cell-form collection.
type Set struct {
	kv KV
	id uint64
}

// New returns a handle to the set with the given id in kv. The id is the stable
// 64-bit key the engine mints for the user key, so a RENAME is a user-key
// remap and never rewrites a member row.
func New(kv KV, id uint64) *Set { return &Set{kv: kv, id: id} }

func (s *Set) metaKey() []byte {
	k := make([]byte, 10)
	k[0] = 0
	binary.BigEndian.PutUint64(k[1:], s.id)
	k[9] = tagMeta
	return k
}

func (s *Set) segKey(segno uint32) []byte {
	k := make([]byte, 14)
	k[0] = 0
	binary.BigEndian.PutUint64(k[1:], s.id)
	k[9] = tagSeg
	binary.BigEndian.PutUint32(k[10:], segno)
	return k
}

// boundary routes a member to a segment: it is the smallest member in that
// segment, and segment segno owns every member m with boundary <= m < next
// boundary.
type boundary struct {
	first []byte
	segno uint32
}

// meta is the routing row: the cardinality (so SCARD is O(1)), the next free
// segment number (segment ids are never reused, so segment keys are stable across
// splits), and the boundary array sorted by first member.
type meta struct {
	count   uint64
	nextSeg uint32
	bounds  []boundary
}

func encodeMeta(m *meta) []byte {
	n := 8 + 4 + 4
	for _, b := range m.bounds {
		n += 4 + len(b.first) + 4
	}
	buf := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint64(buf[off:], m.count)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], m.nextSeg)
	off += 4
	binary.BigEndian.PutUint32(buf[off:], uint32(len(m.bounds)))
	off += 4
	for _, b := range m.bounds {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(b.first)))
		off += 4
		off += copy(buf[off:], b.first)
		binary.BigEndian.PutUint32(buf[off:], b.segno)
		off += 4
	}
	return buf
}

func decodeMeta(buf []byte) (*meta, error) {
	if len(buf) < 16 {
		return nil, errCorrupt
	}
	m := &meta{}
	off := 0
	m.count = binary.BigEndian.Uint64(buf[off:])
	off += 8
	m.nextSeg = binary.BigEndian.Uint32(buf[off:])
	off += 4
	nb := int(binary.BigEndian.Uint32(buf[off:]))
	off += 4
	m.bounds = make([]boundary, 0, nb)
	for range nb {
		if off+4 > len(buf) {
			return nil, errCorrupt
		}
		fl := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		if off+fl+4 > len(buf) {
			return nil, errCorrupt
		}
		first := append([]byte(nil), buf[off:off+fl]...)
		off += fl
		segno := binary.BigEndian.Uint32(buf[off:])
		off += 4
		m.bounds = append(m.bounds, boundary{first: first, segno: segno})
	}
	return m, nil
}

// decodeSeg returns the sorted members held in a segment row, each slice a copy so
// it does not alias the store page the row was read from.
func decodeSeg(buf []byte) ([][]byte, error) {
	if len(buf) < 4 {
		return nil, errCorrupt
	}
	n := int(binary.BigEndian.Uint32(buf))
	off := 4
	out := make([][]byte, 0, n)
	for range n {
		if off+4 > len(buf) {
			return nil, errCorrupt
		}
		l := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		if off+l > len(buf) {
			return nil, errCorrupt
		}
		out = append(out, append([]byte(nil), buf[off:off+l]...))
		off += l
	}
	return out, nil
}

func (s *Set) loadMeta() (*meta, error) {
	raw, found, err := s.kv.Get(s.metaKey())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return decodeMeta(raw)
}

// routeIndex returns the index into m.bounds of the segment that owns member, the
// last boundary whose first member is <= member. An empty set or a member below
// every boundary routes to index 0 (the structure keeps a segment at index 0 once
// any member exists, so index 0 is always valid for a non-empty set).
func routeIndex(m *meta, member []byte) int {
	lo, hi := 0, len(m.bounds)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(m.bounds[mid].first, member) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return 0
	}
	return lo - 1
}

// routeRaw routes member to a segment over the packed metadata bytes, with no
// decode and no allocation, the read-path twin of routeIndex. It returns the
// segment number that owns member (the last boundary whose first member is <=
// member, or the first segment if member is below every boundary). The boundary
// array is short and flat for the set sizes that matter, so a forward scan that
// stops at the first boundary past member is cheaper than decoding the array into
// a slice and binary-searching it on every read. raw must be a valid metadata row
// (len >= 16 with at least one boundary), which a non-empty set always has.
func routeRaw(raw, member []byte) uint32 {
	_, segno := routeRawIdx(raw, member)
	return segno
}

// routeRawIdx is routeRaw plus the byte offset of the owning boundary in raw, so
// a writer can splice that boundary's first member in place without decoding the
// array. boff points at the boundary's leading length field.
func routeRawIdx(raw, member []byte) (boff int, segno uint32) {
	nb := int(binary.BigEndian.Uint32(raw[12:]))
	off := 16
	// The first boundary is index 0, always a valid route (a member below every
	// boundary still belongs to the first segment).
	fl := int(binary.BigEndian.Uint32(raw[off:]))
	boff = off
	segno = binary.BigEndian.Uint32(raw[off+4+fl:])
	for range nb {
		fl = int(binary.BigEndian.Uint32(raw[off:]))
		first := raw[off+4 : off+4+fl]
		if bytes.Compare(first, member) > 0 {
			break
		}
		boff = off
		segno = binary.BigEndian.Uint32(raw[off+4+fl:])
		off += 4 + fl + 4
	}
	return boff, segno
}

// metaBumpCount adds delta to the cardinality stored in the first eight bytes of a
// packed metadata row, in place. raw is an engine-owned copy, so the mutation is
// free of allocation.
func metaBumpCount(raw []byte, delta int64) {
	binary.BigEndian.PutUint64(raw, uint64(int64(binary.BigEndian.Uint64(raw))+delta))
}

// metaSpliceFirst returns a metadata row with the first member of the boundary at
// byte offset boff replaced by member, the rest of the row unchanged. It is used
// when an insert or delete changes a segment's smallest member. One allocation,
// the same shape as segInsert.
func metaSpliceFirst(raw []byte, boff int, member []byte) []byte {
	oldFl := int(binary.BigEndian.Uint32(raw[boff:]))
	tail := boff + 4 + oldFl // start of the boundary's segno
	out := make([]byte, len(raw)-oldFl+len(member))
	copy(out, raw[:boff])
	binary.BigEndian.PutUint32(out[boff:], uint32(len(member)))
	copy(out[boff+4:], member)
	copy(out[boff+4+len(member):], raw[tail:])
	return out
}

// The segment hot path works on the packed segment bytes directly: an Add or a
// SISMEMBER scans the entries in place and an Add splices one new entry into a
// single fresh buffer. The earlier form decoded a segment into a slice of member
// copies on every op, which made an element op allocate once per member in the
// segment and erased the win over the whole-cell rewrite (measured: it was no
// faster than the cell at 1000 members and allocated 60x more). Operating on the
// packed bytes makes an op allocate O(1) buffers regardless of segment size.

// segCount reads the entry count from a packed segment header.
func segCount(buf []byte) int { return int(binary.BigEndian.Uint32(buf)) }

// segFirst returns the first (smallest) member of a non-empty packed segment,
// aliasing buf.
func segFirst(buf []byte) []byte {
	l := int(binary.BigEndian.Uint32(buf[4:]))
	return buf[8 : 8+l]
}

// segLocate scans a sorted packed segment for member. It returns the byte offset
// of the matching or insertion-point entry, the byte length of the matched entry
// (0 on a miss), and whether member is present. The scan stops at the first entry
// not less than member, so an insertion point is found without walking the rest.
func segLocate(buf, member []byte) (off, entryLen int, found bool) {
	off = 4
	for off < len(buf) {
		l := int(binary.BigEndian.Uint32(buf[off:]))
		entry := buf[off+4 : off+4+l]
		c := bytes.Compare(entry, member)
		if c == 0 {
			return off, 4 + l, true
		}
		if c > 0 {
			return off, 0, false
		}
		off += 4 + l
	}
	return off, 0, false
}

// segInsert splices member in at byte offset off, returning a fresh segment with
// its count incremented. off must be an entry boundary (from segLocate).
func segInsert(buf []byte, off int, member []byte) []byte {
	entry := 4 + len(member)
	out := make([]byte, len(buf)+entry)
	copy(out, buf[:off])
	binary.BigEndian.PutUint32(out[off:], uint32(len(member)))
	copy(out[off+4:], member)
	copy(out[off+entry:], buf[off:])
	binary.BigEndian.PutUint32(out, uint32(segCount(buf)+1))
	return out
}

// segDelete removes the entry of byte length entryLen at offset off, returning a
// fresh segment with its count decremented.
func segDelete(buf []byte, off, entryLen int) []byte {
	out := make([]byte, len(buf)-entryLen)
	copy(out, buf[:off])
	copy(out[off:], buf[off+entryLen:])
	binary.BigEndian.PutUint32(out, uint32(segCount(buf)-1))
	return out
}

// segSplit cuts a packed segment in half by entry count, returning the lower and
// upper halves as fresh segments. The upper half's first member is its routing
// boundary.
func segSplit(buf []byte) (lower, upper []byte) {
	half := segCount(buf) / 2
	off := 4
	for range half {
		l := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4 + l
	}
	lower = make([]byte, 4+(off-4))
	binary.BigEndian.PutUint32(lower, uint32(half))
	copy(lower[4:], buf[4:off])
	upperEntries := len(buf) - off
	upper = make([]byte, 4+upperEntries)
	binary.BigEndian.PutUint32(upper, uint32(segCount(buf)-half))
	copy(upper[4:], buf[off:])
	return lower, upper
}

// newSeg builds a one-member packed segment.
func newSeg(member []byte) []byte {
	out := make([]byte, 8+len(member))
	binary.BigEndian.PutUint32(out, 1)
	binary.BigEndian.PutUint32(out[4:], uint32(len(member)))
	copy(out[8:], member)
	return out
}

// Add inserts member and reports whether it was newly added (false if already
// present). It rewrites at most one segment, and splits that segment when it
// overflows segCap, which is the only time the metadata boundary array changes.
func (s *Set) Add(member []byte) (added bool, err error) {
	rawMeta, found, err := s.kv.Get(s.metaKey())
	if err != nil {
		return false, err
	}
	if !found {
		// First member: one segment holding it, one boundary.
		m := &meta{count: 1, nextSeg: 1, bounds: []boundary{{first: member, segno: 0}}}
		if err := s.kv.Set(s.segKey(0), newSeg(member)); err != nil {
			return false, err
		}
		return true, s.kv.Set(s.metaKey(), encodeMeta(m))
	}

	boff, segno := routeRawIdx(rawMeta, member)
	raw, found, err := s.kv.Get(s.segKey(segno))
	if err != nil {
		return false, err
	}
	if !found {
		return false, errCorrupt
	}
	off, _, present := segLocate(raw, member)
	if present {
		return false, nil
	}
	seg := segInsert(raw, off, member)

	if segCount(seg) <= segCap {
		if err := s.kv.Set(s.segKey(segno), seg); err != nil {
			return false, err
		}
		// The common path mutates only the count, in place over the engine-owned
		// metadata bytes, no decode and no re-encode. A head insert also lowers the
		// segment's smallest member, which is its routing boundary, so that one
		// boundary is spliced (one allocation, still no full re-encode).
		metaBumpCount(rawMeta, 1)
		if off == 4 {
			rawMeta = metaSpliceFirst(rawMeta, boff, member)
		}
		return true, s.kv.Set(s.metaKey(), rawMeta)
	}

	// Overflow: split the segment in half. The boundary array grows by one, which
	// is the rare case that decodes the metadata row and re-encodes it; the cost is
	// amortized over a full segment of cheap in-place inserts. The lower half keeps
	// segno; the upper half gets a fresh segno so its key never aliases an old one.
	m, err := decodeMeta(rawMeta)
	if err != nil {
		return false, err
	}
	bi := routeIndex(m, member)
	lower, upper := segSplit(seg)
	newSegno := m.nextSeg
	m.nextSeg++
	if err := s.kv.Set(s.segKey(segno), lower); err != nil {
		return false, err
	}
	if err := s.kv.Set(s.segKey(newSegno), upper); err != nil {
		return false, err
	}
	if off == 4 {
		m.bounds[bi].first = append([]byte(nil), segFirst(lower)...)
	}
	// Insert the new upper boundary right after bi.
	nb := boundary{first: append([]byte(nil), segFirst(upper)...), segno: newSegno}
	m.bounds = append(m.bounds, boundary{})
	copy(m.bounds[bi+2:], m.bounds[bi+1:])
	m.bounds[bi+1] = nb
	m.count++
	return true, s.kv.Set(s.metaKey(), encodeMeta(m))
}

// IsMember reports whether member is in the set: route to one segment over the
// packed metadata bytes, then scan that segment in place. Neither the metadata
// row nor the segment is decoded, so a read on a present-or-absent member
// allocates nothing beyond the two row keys.
func (s *Set) IsMember(member []byte) (bool, error) {
	raw, found, err := s.kv.Get(s.metaKey())
	if err != nil || !found {
		return false, err
	}
	if binary.BigEndian.Uint32(raw[12:]) == 0 {
		return false, nil
	}
	segno := routeRaw(raw, member)
	seg, found, err := s.kv.Get(s.segKey(segno))
	if err != nil || !found {
		return false, err
	}
	_, _, present := segLocate(seg, member)
	return present, nil
}

// Remove deletes member and reports whether it was present. It rewrites one
// segment; a segment emptied of its last member is deleted and its boundary
// dropped, so an empty set leaves no rows behind.
func (s *Set) Remove(member []byte) (removed bool, err error) {
	m, err := s.loadMeta()
	if err != nil || m == nil || len(m.bounds) == 0 {
		return false, err
	}
	bi := routeIndex(m, member)
	segno := m.bounds[bi].segno
	raw, found, err := s.kv.Get(s.segKey(segno))
	if err != nil || !found {
		return false, err
	}
	off, entryLen, present := segLocate(raw, member)
	if !present {
		return false, nil
	}
	seg := segDelete(raw, off, entryLen)
	m.count--

	if segCount(seg) == 0 {
		// Drop the empty segment and its boundary. An empty set deletes its
		// metadata row too, so Card is 0 and no rows leak.
		if _, err := s.kv.Delete(s.segKey(segno)); err != nil {
			return false, err
		}
		m.bounds = append(m.bounds[:bi], m.bounds[bi+1:]...)
		if len(m.bounds) == 0 {
			_, err := s.kv.Delete(s.metaKey())
			return true, err
		}
		return true, s.kv.Set(s.metaKey(), encodeMeta(m))
	}

	if err := s.kv.Set(s.segKey(segno), seg); err != nil {
		return false, err
	}
	if off == 4 {
		m.bounds[bi].first = append([]byte(nil), segFirst(seg)...)
	}
	return true, s.kv.Set(s.metaKey(), encodeMeta(m))
}

// Card is the cardinality, read straight from the metadata row in O(1) with no
// segment access.
func (s *Set) Card() (int, error) {
	m, err := s.loadMeta()
	if err != nil || m == nil {
		return 0, err
	}
	return int(m.count), nil
}

// Members returns every member in sorted order by walking the segments in
// boundary order. Cost is proportional to the set size, not the keyspace, which
// is the property the hash index alone cannot give. The caller that wants Redis's
// unspecified SMEMBERS order can shuffle; sorted is a stable superset of correct.
func (s *Set) Members() ([][]byte, error) {
	m, err := s.loadMeta()
	if err != nil || m == nil {
		return nil, err
	}
	out := make([][]byte, 0, m.count)
	for _, b := range m.bounds {
		raw, found, err := s.kv.Get(s.segKey(b.segno))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errCorrupt
		}
		seg, err := decodeSeg(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, seg...)
	}
	return out, nil
}
