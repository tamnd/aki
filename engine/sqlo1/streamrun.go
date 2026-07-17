package sqlo1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// The doc 10 entry run codec, slice 1 of the stream model. Entries live
// in runs (subkey kind 1): consecutive entries in ID order, a shared
// first-seen field name table, varint ID deltas, and a trailing tomb
// bitmap once XDEL has marked an entry.
//
// The encoding is canonical: for a given entry sequence there is
// exactly one legal byte string, and the decoder rejects every
// non-canonical form (non-minimal varints, inline names the table
// covers, table entries never referenced, tomb padding bits). That
// buys the strongest cheap oracle there is, decode-then-reencode
// byte equality, and it is what the codec fuzz asserts.
//
// The cut thresholds are the xadd lab verdicts (#1114), baked here the
// way listnode.go bakes the lnode numbers.
const (
	// streamRunMax is the run cut threshold in encoded payload bytes.
	// Like listNodeMax this is writer policy, not a format fact: the
	// decode puts no upper bound on payload size because a fat entry
	// legitimately owns an oversize run of its own.
	streamRunMax = 4032

	// streamRunMaxEntries caps entries per run. The byte cap binds
	// first for every value size from 100 B up, so this bound only
	// exists for tiny entries, where it keeps the tomb bitmap at 16 B
	// and the per-run decode cost flat.
	streamRunMaxEntries = 128

	// streamRunHdrLen is the run payload header:
	//
	//	u64 base_ms, u64 base_seq  // the first entry's ID
	//	u16 n                      // encoded entries, tombstoned included
	//	u8  nnames                 // name table size
	//	u16 tflags                 // bit0: tomb bitmap present
	//
	// The first entry encodes zero deltas against the base by
	// definition; the redundancy costs two bytes and keeps the entry
	// loop uniform.
	streamRunHdrLen = 21

	// streamNameTableMax bounds the name table. Redis's master-entry
	// insight: most streams carry one schema, so after sixteen
	// distinct names the seventeenth is rare enough to inline.
	streamNameTableMax = 16

	// streamNameMaxLen bounds a table entry's name, which carries a u8
	// length. Longer names inline with a varint length instead.
	streamNameMaxLen = 255

	// streamNameInline is the nameref sentinel for an inline name:
	// varint length plus bytes, the same shape as values.
	streamNameInline = 0xFF

	// sflagTombs marks a run whose payload ends in the ceil(n/8)-byte
	// tomb bitmap. The bitmap is always the trailing bytes, so no
	// offset field is needed and no offset can overflow on a
	// fat-value run.
	sflagTombs = 1 << 0
)

// streamID is an entry ID, the (ms, seq) pair. The zero ID is not a
// legal entry ID (Redis rejects 0-0 in XADD), so runs never carry it.
type streamID struct {
	ms  uint64
	seq uint64
}

// less is the Redis ID order: ms first, seq inside a millisecond.
func (a streamID) less(b streamID) bool {
	return a.ms < b.ms || (a.ms == b.ms && a.seq < b.seq)
}

// streamEntry is one entry: its ID, its fields as name/value pairs in
// XADD argument order (duplicates legal, order preserved on the wire),
// and the tomb mark. Decoded entries alias the read buffer and the
// walker's shared pair scratch; a caller that retains one copies it.
type streamEntry struct {
	id   streamID
	fv   [][]byte
	dead bool
}

// streamRunInfo is what a run walk learns beyond the entries: the ID
// span, the encoded and live counts, and whether a tomb bitmap rides
// the tail.
type streamRunInfo struct {
	base  streamID
	last  streamID
	n     int
	live  int
	tombs bool
}

// streamNameRef finds name in the table, -1 when absent.
func streamNameRef(table [][]byte, name []byte) int {
	for i, nm := range table {
		if bytes.Equal(nm, name) {
			return i
		}
	}
	return -1
}

// streamUvarintLen is the minimal uvarint width of x, the canonical
// form the decoder holds every varint to.
func streamUvarintLen(x uint64) int {
	return (bits.Len64(x|1) + 6) / 7
}

// appendStreamRun encodes entries as one run payload onto dst. The
// contract violations are writer bugs, not data errors, so they panic:
// one to streamRunMaxEntries entries, strictly increasing IDs, no zero
// ID, and fv holding whole pairs.
func appendStreamRun(dst []byte, entries []streamEntry) []byte {
	if len(entries) == 0 || len(entries) > streamRunMaxEntries {
		panic("sqlo1: stream run entry count out of range")
	}
	if entries[0].id == (streamID{}) {
		panic("sqlo1: stream run starts at the zero ID")
	}
	// The name table is first-seen order over the whole run: every
	// distinct name that fits a u8 length, while the table has room.
	var table [][]byte
	tombs := false
	for i := range entries {
		e := &entries[i]
		if len(e.fv)%2 != 0 {
			panic("sqlo1: stream entry holds a torn field pair")
		}
		if e.dead {
			tombs = true
		}
		for f := 0; f < len(e.fv); f += 2 {
			name := e.fv[f]
			if len(name) > streamNameMaxLen || len(table) == streamNameTableMax {
				continue
			}
			if streamNameRef(table, name) < 0 {
				table = append(table, name)
			}
		}
	}

	base := entries[0].id
	var h [streamRunHdrLen]byte
	binary.LittleEndian.PutUint64(h[0:], base.ms)
	binary.LittleEndian.PutUint64(h[8:], base.seq)
	binary.LittleEndian.PutUint16(h[16:], uint16(len(entries)))
	h[18] = uint8(len(table))
	var tflags uint16
	if tombs {
		tflags = sflagTombs
	}
	binary.LittleEndian.PutUint16(h[19:], tflags)
	dst = append(dst, h[:]...)
	for _, nm := range table {
		dst = append(dst, uint8(len(nm)))
		dst = append(dst, nm...)
	}

	prev := base
	for i := range entries {
		e := &entries[i]
		if i > 0 && !prev.less(e.id) {
			panic("sqlo1: stream run IDs not strictly increasing")
		}
		dst = appendStreamRunEntry(dst, table, prev, e.id, e.fv)
		prev = e.id
	}

	if tombs {
		bm := make([]byte, (len(entries)+7)/8)
		for i := range entries {
			if entries[i].dead {
				bm[i/8] |= 1 << (i % 8)
			}
		}
		dst = append(dst, bm...)
	}
	return dst
}

// appendStreamRunEntry encodes one entry onto dst against prev, the
// run's previous ID, resolving names through the run's table. prev
// equal to id marks the first entry, which encodes zero deltas against
// the base by definition. The tail amendment in stream.go appends
// through this too, so the incremental and from-scratch encodes share
// one source of encoding truth.
func appendStreamRunEntry(dst []byte, table [][]byte, prev, id streamID, fv [][]byte) []byte {
	var dms, dseq uint64
	switch {
	case id == prev:
		// The first entry.
	case prev.less(id):
		dms = id.ms - prev.ms
		if dms == 0 {
			dseq = id.seq - prev.seq
		} else {
			// A new millisecond carries its seq absolute; bursts
			// inside one are the delta-to-2-bytes case.
			dseq = id.seq
		}
	default:
		panic("sqlo1: stream run IDs not strictly increasing")
	}
	dst = binary.AppendUvarint(dst, dms)
	dst = binary.AppendUvarint(dst, dseq)
	dst = binary.AppendUvarint(dst, uint64(len(fv)/2))
	for f := 0; f < len(fv); f += 2 {
		name, val := fv[f], fv[f+1]
		if r := streamNameRef(table, name); r >= 0 {
			dst = append(dst, uint8(r))
		} else {
			dst = append(dst, streamNameInline)
			dst = binary.AppendUvarint(dst, uint64(len(name)))
			dst = append(dst, name...)
		}
		dst = binary.AppendUvarint(dst, uint64(len(val)))
		dst = append(dst, val...)
	}
	return dst
}

// streamRunUvarint reads one canonical uvarint, rejecting torn buffers
// and non-minimal encodings.
func streamRunUvarint(p []byte, i int, what string) (uint64, []byte, error) {
	x, k := binary.Uvarint(p)
	if k <= 0 {
		return 0, nil, fmt.Errorf("sqlo1: stream run entry %d torn at %s", i, what)
	}
	if k != streamUvarintLen(x) {
		return 0, nil, fmt.Errorf("sqlo1: stream run entry %d has a non-minimal %s varint", i, what)
	}
	return x, p[k:], nil
}

// walkStreamRun validates a run payload in one pass and hands each
// entry to fn in ID order, tombstoned entries included with dead set.
// fn may be nil for a validate-only walk. The entry's fv aliases both v
// and a pair scratch the walker reuses, so it is dead the moment fn
// returns; retainers copy.
//
// The walk enforces the canonical form in full, so a payload it
// accepts re-encodes byte-identically: minimal varints, first-entry
// zero deltas, strictly increasing IDs without overflow, a distinct
// name table in first-reference order with every entry referenced,
// inline names only where the table could not hold them, and a tomb
// bitmap that is present exactly when a bit is set, with clear padding.
func walkStreamRun(v []byte, fn func(i int, e streamEntry) error) (streamRunInfo, error) {
	if len(v) < streamRunHdrLen {
		return streamRunInfo{}, fmt.Errorf("sqlo1: stream run of %d bytes has no header", len(v))
	}
	base := streamID{
		ms:  binary.LittleEndian.Uint64(v[0:]),
		seq: binary.LittleEndian.Uint64(v[8:]),
	}
	if base == (streamID{}) {
		return streamRunInfo{}, errors.New("sqlo1: stream run starts at the zero ID")
	}
	n := int(binary.LittleEndian.Uint16(v[16:]))
	if n == 0 || n > streamRunMaxEntries {
		return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry count %d out of range", n)
	}
	nnames := int(v[18])
	if nnames > streamNameTableMax {
		return streamRunInfo{}, fmt.Errorf("sqlo1: stream run name table size %d out of range", nnames)
	}
	tflags := binary.LittleEndian.Uint16(v[19:])
	if tflags&^uint16(sflagTombs) != 0 {
		return streamRunInfo{}, fmt.Errorf("sqlo1: stream run has unknown tflags %#x", tflags)
	}

	body := v[streamRunHdrLen:]
	var bm []byte
	tombs := tflags&sflagTombs != 0
	if tombs {
		tl := (n + 7) / 8
		if len(body) < tl {
			return streamRunInfo{}, errors.New("sqlo1: stream run torn at the tomb bitmap")
		}
		bm = body[len(body)-tl:]
		body = body[:len(body)-tl]
		if n%8 != 0 && bm[tl-1]>>(n%8) != 0 {
			return streamRunInfo{}, errors.New("sqlo1: stream run tomb bitmap has padding bits set")
		}
	}

	var table [][]byte
	for j := range nnames {
		if len(body) < 1 {
			return streamRunInfo{}, fmt.Errorf("sqlo1: stream run torn at name table entry %d", j)
		}
		l := int(body[0])
		if len(body) < 1+l {
			return streamRunInfo{}, fmt.Errorf("sqlo1: stream run name table entry %d torn at %d of %d bytes", j, len(body)-1, l)
		}
		name := body[1 : 1+l]
		if streamNameRef(table, name) >= 0 {
			return streamRunInfo{}, fmt.Errorf("sqlo1: stream run name table entry %d duplicates an earlier name", j)
		}
		table = append(table, name)
		body = body[1+l:]
	}

	info := streamRunInfo{base: base, n: n, live: n, tombs: tombs}
	var fv [][]byte
	prev := base
	nextRef := 0
	for i := range n {
		var dms, dseq uint64
		var err error
		if dms, body, err = streamRunUvarint(body, i, "dms"); err != nil {
			return streamRunInfo{}, err
		}
		if dseq, body, err = streamRunUvarint(body, i, "dseq"); err != nil {
			return streamRunInfo{}, err
		}
		id := prev
		switch {
		case i == 0:
			if dms != 0 || dseq != 0 {
				return streamRunInfo{}, errors.New("sqlo1: stream run first entry has nonzero deltas against the base")
			}
		case dms == 0:
			if dseq == 0 {
				return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d repeats the previous ID", i)
			}
			id.seq = prev.seq + dseq
			if id.seq < prev.seq {
				return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d overflows seq", i)
			}
		default:
			id.ms = prev.ms + dms
			if id.ms < prev.ms {
				return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d overflows ms", i)
			}
			id.seq = dseq
		}
		var nf uint64
		if nf, body, err = streamRunUvarint(body, i, "nfields"); err != nil {
			return streamRunInfo{}, err
		}
		fv = fv[:0]
		for f := range int(nf) {
			if len(body) < 1 {
				return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d torn at field %d", i, f)
			}
			ref := body[0]
			body = body[1:]
			var name []byte
			if ref != streamNameInline {
				if int(ref) >= nnames {
					return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d field %d references name %d past the table", i, f, ref)
				}
				if int(ref) > nextRef {
					return streamRunInfo{}, fmt.Errorf("sqlo1: stream run name table is not in first-reference order at entry %d field %d", i, f)
				}
				if int(ref) == nextRef {
					nextRef++
				}
				name = table[ref]
			} else {
				var nl uint64
				if nl, body, err = streamRunUvarint(body, i, "name length"); err != nil {
					return streamRunInfo{}, err
				}
				if uint64(len(body)) < nl {
					return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d field %d name torn at %d of %d bytes", i, f, len(body), nl)
				}
				name = body[:nl]
				body = body[nl:]
				// A short inline name is only canonical past a full,
				// fully-referenced table: first-seen construction
				// means every table name's first occurrence precedes
				// the first name that ever inlines short.
				if nl <= streamNameMaxLen && (nextRef < streamNameTableMax || streamNameRef(table, name) >= 0) {
					return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d field %d inlines a name the table covers", i, f)
				}
			}
			var vl uint64
			if vl, body, err = streamRunUvarint(body, i, "value length"); err != nil {
				return streamRunInfo{}, err
			}
			if uint64(len(body)) < vl {
				return streamRunInfo{}, fmt.Errorf("sqlo1: stream run entry %d field %d value torn at %d of %d bytes", i, f, len(body), vl)
			}
			fv = append(fv, name, body[:vl])
			body = body[vl:]
		}
		dead := false
		if tombs && bm[i/8]&(1<<(i%8)) != 0 {
			dead = true
			info.live--
		}
		if fn != nil {
			if err := fn(i, streamEntry{id: id, fv: fv, dead: dead}); err != nil {
				return streamRunInfo{}, err
			}
		}
		prev = id
	}
	if len(body) != 0 {
		return streamRunInfo{}, fmt.Errorf("sqlo1: stream run has %d trailing bytes", len(body))
	}
	if nextRef != nnames {
		return streamRunInfo{}, fmt.Errorf("sqlo1: stream run name table holds %d names but only %d are referenced", nnames, nextRef)
	}
	if tombs && info.live == n {
		return streamRunInfo{}, errors.New("sqlo1: stream run carries a tomb bitmap with no bits set")
	}
	info.last = prev
	return info, nil
}
