package sqlo1b

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

// Scalar cascade codecs, doc 04 section 11 per the cascade lab
// (#1295) verdicts: dict for low-cardinality values, run-length
// stacked on dict when repeats cluster, frame-of-reference bit
// packing for integer shapes. These are schemes 1..3 of the doc 03
// section 7 registry; scheme selection is the sampled-selection
// slice's job, this file is the codecs alone.
//
// A frame payload is a chain of encoded records, and keys are unique
// by construction, so compressing the chain whole would hand every
// scheme a stream it cannot win on. The cascade schemes therefore
// split each record at its value: the stem (envelope head through
// the key, plus the rcrc tail) rides raw, and the value stream is
// what the scheme encodes, which is exactly the shape the lab
// measured. The compressed payload is self-delimiting, because
// cDecode gets only the bytes and ulen:
//
//	uvarint n          record count
//	stems              n x (prefix, rcrc); prefix = record[0 : rlen-vlen-4]
//	values             scheme-specific over the n value slices
//
// A stem starts with the record envelope header, so rlen and vlen
// delimit it with no extra framing. Value section formats:
//
//	scheme 1 (dict)     uvarint ndict, ndict x (uvarint len, bytes),
//	                    n x uvarint index
//	scheme 2 (dict+rle) same dictionary, then (uvarint index,
//	                    uvarint run) pairs covering the n values
//	scheme 3 (for+pack) u8 mode (0 canonical decimal ASCII, 1 8-byte
//	                    LE u64), then 1024-value blocks of uvarint
//	                    base, u8 width, width-bit deltas packed
//	                    little-endian into 64-bit words
//
// Decode reconstructs the exact uncompressed payload or fails
// loudly; it never trusts a length or an index it has not bounded,
// because scrub feeds it whatever is on disk.

// errCascadeInapplicable reports a payload the scheme cannot encode
// (for+pack on values that are not integer-shaped). The selection
// slice treats it as "this scheme is out", not as damage.
var errCascadeInapplicable = errors.New("sqlo1b: scheme inapplicable to this payload")

// forBlock is the frame-of-reference block: each block packs at its
// own width over its own base, so near-sorted streams get narrow
// words. Lab-swept in labs/sqlo1/b4/01_cascade.
const forBlock = 1024

// cascadeSplit parses a frame payload's record chain into stems and
// values. Slices alias the payload.
type cascadeSplit struct {
	stems  [][]byte // per record: prefix bytes, rcrc excluded
	crcs   [][]byte // per record: the 4-byte rcrc tail
	values [][]byte // per record: the value bytes
}

func splitCascade(payload []byte) (*cascadeSplit, error) {
	var sp cascadeSplit
	for off := 0; off < len(payload); {
		rest := payload[off:]
		if len(rest) < recHdrSize+recTailSize {
			return nil, fmt.Errorf("sqlo1b: cascade payload tail of %d bytes is no record", len(rest))
		}
		rlen := int(binary.LittleEndian.Uint32(rest))
		vlen := int(binary.LittleEndian.Uint32(rest[8:]))
		if rlen < recHdrSize+recTailSize || rlen > len(rest) || vlen > rlen-recHdrSize-recTailSize {
			return nil, fmt.Errorf("sqlo1b: cascade payload record at %d has rlen %d, vlen %d", off, rlen, vlen)
		}
		plen := rlen - vlen - recTailSize
		sp.stems = append(sp.stems, rest[:plen:plen])
		sp.values = append(sp.values, rest[plen:plen+vlen:plen+vlen])
		sp.crcs = append(sp.crcs, rest[rlen-recTailSize:rlen:rlen])
		off += rlen
	}
	return &sp, nil
}

// cEncode is the scheme registry's encode side for the cascade
// schemes: frame payload bytes to compressed bytes. A scheme that
// cannot represent the payload returns errCascadeInapplicable.
func cEncode(scheme uint8, payload []byte) ([]byte, error) {
	sp, err := splitCascade(payload)
	if err != nil {
		return nil, err
	}
	if len(sp.stems) == 0 {
		return nil, errCascadeInapplicable
	}
	return cEncodeSplit(scheme, sp, len(payload))
}

func cEncodeSplit(scheme uint8, sp *cascadeSplit, ulen int) ([]byte, error) {
	out := binary.AppendUvarint(make([]byte, 0, ulen), uint64(len(sp.stems)))
	for i, stem := range sp.stems {
		out = append(out, stem...)
		out = append(out, sp.crcs[i]...)
	}
	switch scheme {
	case SchemeDict:
		return dictEncode(out, sp.values, false), nil
	case SchemeDictRLE:
		return dictEncode(out, sp.values, true), nil
	case SchemeFor:
		return forPackEncode(out, sp.values)
	default:
		return nil, fmt.Errorf("sqlo1b: no cascade encoder for scheme %d", scheme)
	}
}

// cSelectFloor is the doc 03 section 7 minimum win: a group whose
// best scheme saves less than 8 percent of its raw payload stores
// raw, because decode cost must buy real bytes. The cascade lab
// (#1295) found the floor non-binding on every real shape; it stays
// as the degenerate backstop.
const cSelectFloor = 8

// cSelect runs the sampled cascade over one frame group's payload
// and returns the winning scheme with its compressed bytes, or raw
// with nil. The lab's sampling discipline (1 percent with a 40
// absolute minimum) clamps to the whole group at the 4 KiB frame
// granularity, so the sample here is every record: each cascade
// scheme try-encodes the full payload and the smallest one above the
// floor wins, ties to the lower id because dict is the cheapest
// decode. Zstd is the lab's fall-through, tried only when every
// lightweight scheme misses the floor, so shapes the cascade owns
// never pay the zstd encode.
func cSelect(payload []byte) (uint8, []byte) {
	sp, err := splitCascade(payload)
	if err != nil || len(sp.stems) == 0 {
		return SchemeRaw, nil
	}
	scheme, best := SchemeRaw, []byte(nil)
	for _, cand := range []uint8{SchemeDict, SchemeDictRLE, SchemeFor} {
		comp, err := cEncodeSplit(cand, sp, len(payload))
		if err != nil {
			continue
		}
		if 100*len(comp) > (100-cSelectFloor)*len(payload) {
			continue
		}
		if best == nil || len(comp) < len(best) {
			scheme, best = cand, comp
		}
	}
	if scheme == SchemeRaw {
		if comp := zstdEncode(payload); 100*len(comp) <= (100-cSelectFloor)*len(payload) {
			return SchemeZstd, comp
		}
	}
	return scheme, best
}

// cascadeDecode reconstructs the exact frame payload from a cascade
// scheme's compressed bytes. ulen bounds every allocation.
func cascadeDecode(scheme uint8, comp []byte, ulen int) ([]byte, error) {
	if ulen < 0 || ulen > cframeMaxUlen {
		return nil, fmt.Errorf("sqlo1b: cascade ulen %d out of range", ulen)
	}
	n64, k := binary.Uvarint(comp)
	if k <= 0 || n64 > uint64(ulen)/(recHdrSize+recTailSize) {
		return nil, fmt.Errorf("sqlo1b: cascade frame claims %d records in %d bytes", n64, ulen)
	}
	n := int(n64)
	comp = comp[k:]
	// The stems: prefixes carry the envelope headers that delimit
	// them, so this pass sizes every record before values decode.
	stems := make([][]byte, n)
	crcs := make([][]byte, n)
	vlens := make([]int, n)
	total := 0
	for i := range n {
		if len(comp) < recHdrSize {
			return nil, fmt.Errorf("sqlo1b: cascade stem %d of %d truncated", i, n)
		}
		rlen := int(binary.LittleEndian.Uint32(comp))
		vlen := int(binary.LittleEndian.Uint32(comp[8:]))
		if rlen < recHdrSize+recTailSize || vlen > rlen-recHdrSize-recTailSize {
			return nil, fmt.Errorf("sqlo1b: cascade stem %d has rlen %d, vlen %d", i, rlen, vlen)
		}
		plen := rlen - vlen - recTailSize
		if len(comp) < plen+recTailSize {
			return nil, fmt.Errorf("sqlo1b: cascade stem %d of %d truncated", i, n)
		}
		stems[i] = comp[:plen:plen]
		crcs[i] = comp[plen : plen+recTailSize : plen+recTailSize]
		vlens[i] = vlen
		total += rlen
		if total > ulen {
			return nil, fmt.Errorf("sqlo1b: cascade records total %d bytes past ulen %d", total, ulen)
		}
		comp = comp[plen+recTailSize:]
	}
	if total != ulen {
		return nil, fmt.Errorf("sqlo1b: cascade records total %d bytes, ulen %d", total, ulen)
	}
	var values [][]byte
	var rest []byte
	var err error
	switch scheme {
	case SchemeDict:
		values, rest, err = dictDecode(comp, n, false)
	case SchemeDictRLE:
		values, rest, err = dictDecode(comp, n, true)
	case SchemeFor:
		values, rest, err = forPackDecode(comp, n)
	default:
		return nil, fmt.Errorf("sqlo1b: no cascade decoder for scheme %d", scheme)
	}
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("sqlo1b: cascade frame carries %d bytes past the value section", len(rest))
	}
	out := make([]byte, 0, ulen)
	for i := range n {
		if len(values[i]) != vlens[i] {
			return nil, fmt.Errorf("sqlo1b: cascade value %d decoded to %d bytes, stem declares %d", i, len(values[i]), vlens[i])
		}
		out = append(out, stems[i]...)
		out = append(out, values[i]...)
		out = append(out, crcs[i]...)
	}
	return out, nil
}

// dictEncode appends the scheme 1/2 value section: a first-appearance
// dictionary, then the index stream, run-length coded for scheme 2.
func dictEncode(out []byte, values [][]byte, rle bool) []byte {
	seen := make(map[string]uint64, 64)
	var dict [][]byte
	idx := make([]uint64, len(values))
	for i, v := range values {
		id, ok := seen[string(v)]
		if !ok {
			id = uint64(len(dict))
			dict = append(dict, v)
			seen[string(v)] = id
		}
		idx[i] = id
	}
	out = binary.AppendUvarint(out, uint64(len(dict)))
	for _, d := range dict {
		out = binary.AppendUvarint(out, uint64(len(d)))
		out = append(out, d...)
	}
	if !rle {
		for _, id := range idx {
			out = binary.AppendUvarint(out, id)
		}
		return out
	}
	for i := 0; i < len(idx); {
		j := i + 1
		for j < len(idx) && idx[j] == idx[i] {
			j++
		}
		out = binary.AppendUvarint(out, idx[i])
		out = binary.AppendUvarint(out, uint64(j-i))
		i = j
	}
	return out
}

// dictDecode reads the scheme 1/2 value section back into n value
// slices and returns whatever trails it. Every dictionary length,
// index, and run is bounded before use.
func dictDecode(b []byte, n int, rle bool) ([][]byte, []byte, error) {
	nd, k := binary.Uvarint(b)
	if k <= 0 || nd > uint64(n) {
		return nil, nil, fmt.Errorf("sqlo1b: cascade dictionary of %d entries for %d values", nd, n)
	}
	b = b[k:]
	dict := make([][]byte, nd)
	for i := range dict {
		l, k := binary.Uvarint(b)
		if k <= 0 || l > uint64(len(b)-k) {
			return nil, nil, fmt.Errorf("sqlo1b: cascade dictionary entry %d truncated", i)
		}
		b = b[k:]
		dict[i] = b[:l:l]
		b = b[l:]
	}
	values := make([][]byte, 0, n)
	if !rle {
		for i := range n {
			id, k := binary.Uvarint(b)
			if k <= 0 || id >= nd {
				return nil, nil, fmt.Errorf("sqlo1b: cascade value %d has dictionary index %d of %d", i, id, nd)
			}
			b = b[k:]
			values = append(values, dict[id])
		}
		return values, b, nil
	}
	for len(values) < n {
		id, k := binary.Uvarint(b)
		if k <= 0 || id >= nd {
			return nil, nil, fmt.Errorf("sqlo1b: cascade run at value %d has dictionary index %d of %d", len(values), id, nd)
		}
		b = b[k:]
		run, k := binary.Uvarint(b)
		if k <= 0 || run == 0 || run > uint64(n-len(values)) {
			return nil, nil, fmt.Errorf("sqlo1b: cascade run of %d at value %d of %d", run, len(values), n)
		}
		b = b[k:]
		for range run {
			values = append(values, dict[id])
		}
	}
	return values, b, nil
}

// forPackEncode appends the scheme 3 value section. Values must all
// be canonical decimal ASCII (mode 0) or 8-byte LE words (mode 1);
// anything else is not integer-shaped and the scheme is out.
func forPackEncode(out []byte, values [][]byte) ([]byte, error) {
	mode, ok := forApplicable(values)
	if !ok {
		return nil, errCascadeInapplicable
	}
	xs := make([]uint64, len(values))
	for i, v := range values {
		if mode == 1 {
			xs[i] = binary.LittleEndian.Uint64(v)
		} else {
			xs[i], _ = parseCanonicalDec(v)
		}
	}
	out = append(out, byte(mode))
	for b := 0; b < len(xs); b += forBlock {
		blk := xs[b:min(b+forBlock, len(xs))]
		base := blk[0]
		for _, x := range blk {
			base = min(base, x)
		}
		width := 0
		for _, x := range blk {
			width = max(width, bitsLen(x-base))
		}
		out = binary.AppendUvarint(out, base)
		out = append(out, byte(width))
		var word uint64
		fill := 0
		for _, x := range blk {
			d := x - base
			word |= d << fill
			fill += width
			if fill >= 64 {
				out = binary.LittleEndian.AppendUint64(out, word)
				fill -= 64
				if width > 0 && fill > 0 {
					word = d >> (width - fill)
				} else {
					word = 0
				}
			}
		}
		if fill > 0 {
			out = binary.LittleEndian.AppendUint64(out, word)
		}
	}
	return out, nil
}

// forPackDecode reads the scheme 3 value section back into n value
// slices and returns whatever trails it.
func forPackDecode(b []byte, n int) ([][]byte, []byte, error) {
	if len(b) == 0 || b[0] > 1 {
		return nil, nil, fmt.Errorf("sqlo1b: cascade for+pack section has no valid mode")
	}
	mode := int(b[0])
	b = b[1:]
	values := make([][]byte, 0, n)
	for done := 0; done < n; done += forBlock {
		blkN := min(forBlock, n-done)
		base, k := binary.Uvarint(b)
		if k <= 0 || len(b) < k+1 {
			return nil, nil, fmt.Errorf("sqlo1b: cascade for+pack block at value %d truncated", done)
		}
		width := int(b[k])
		b = b[k+1:]
		if width > 64 {
			return nil, nil, fmt.Errorf("sqlo1b: cascade for+pack width %d", width)
		}
		words := (blkN*width + 63) / 64
		if len(b) < words*8 {
			return nil, nil, fmt.Errorf("sqlo1b: cascade for+pack block at value %d wants %d words in %d bytes", done, words, len(b))
		}
		mask := ^uint64(0)
		if width < 64 {
			mask = uint64(1)<<width - 1
		}
		var word uint64
		fill := 0
		wi := 0
		for range blkN {
			var d uint64
			switch {
			case width == 0:
				d = 0
			case fill < width:
				next := binary.LittleEndian.Uint64(b[wi*8:])
				d = (word | next<<fill) & mask
				word = next >> (width - fill)
				fill += 64 - width
				wi++
			default:
				d = word & mask
				word >>= width
				fill -= width
			}
			x := base + d
			if mode == 1 {
				var v [8]byte
				binary.LittleEndian.PutUint64(v[:], x)
				values = append(values, v[:])
			} else {
				values = append(values, strconv.AppendUint(make([]byte, 0, 20), x, 10))
			}
		}
		b = b[words*8:]
	}
	return values, b, nil
}

// forApplicable classifies the value stream for scheme 3: mode 1 when
// every value is exactly 8 bytes, mode 0 when every value is a
// canonical decimal integer, otherwise the scheme is out.
func forApplicable(values [][]byte) (mode int, ok bool) {
	if len(values) == 0 {
		return 0, false
	}
	bin := true
	for _, v := range values {
		if len(v) != 8 {
			bin = false
			break
		}
	}
	if bin {
		return 1, true
	}
	for _, v := range values {
		if _, ok := parseCanonicalDec(v); !ok {
			return 0, false
		}
	}
	return 0, true
}

// parseCanonicalDec parses a canonical decimal u64: digits only, no
// leading zero, no overflow, so decode's strconv round-trips the
// exact bytes.
func parseCanonicalDec(v []byte) (uint64, bool) {
	if len(v) == 0 || len(v) > 20 || (len(v) > 1 && v[0] == '0') {
		return 0, false
	}
	var x uint64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if x > (1<<64-1-d)/10 {
			return 0, false
		}
		x = x*10 + d
	}
	return x, true
}

// bitsLen is the minimum width holding x.
func bitsLen(x uint64) int {
	n := 0
	for x > 0 {
		n++
		x >>= 1
	}
	return n
}
