package store

import (
	"encoding/binary"
	"errors"
)

// The whole-record cold frame (spec 2064/f3/06 section 2.3): the self-delimiting
// unit the migration quantum stages for a shard's cold region and recovery walks
// linearly. Layout:
//
//	[u32 total | u8 kind | u8 flags | u16 klen | u32 vlen | key | value]
//
// total is the whole frame's byte length, so a linear scan re-derives every
// frame boundary with no index, which is how the tier-tagged index rebuilds cold
// entries at open and how a torn tail is detected (07-aki-single-file.md): a
// total that runs past the durable cursor truncates the tail. kind, flags, klen,
// and vlen restore the record header; the key rides along whole so Bitcask-style
// liveness (the index names me or I am dead) works from the frame alone.
//
// value is the record's in-arena value region verbatim, chosen by band: the
// embedded value's vlen bytes, the 8-byte int cell, or the 16-byte separated run
// pointer. A value already in the log while hot frames its pointer, not its
// bytes, so a doubly-cold read is the two preads doc 06 section 2.3 priced rather
// than a re-copy of bytes the log already holds. The u32 vlen field carries the
// record's logical value length (or an int's digit count) for reply presizing;
// the value-region length is total - coldHdr - klen, which the pointer bands make
// distinct from vlen.
//
// A record with a deadline (flagHasTTL) carries its absolute unix-ms expiry as
// a trailing 8-byte LE word after the value. The tail placement keeps the key
// at coldHdr, so every offset the index and the flip bookkeeping derive from
// the header survives, and only the value-region arithmetic subtracts the word.
const coldHdr = 12 // u32 total + u8 kind + u8 flags + u16 klen + u32 vlen

// coldExpSize is the trailing expiry word's width on a flagHasTTL frame.
const coldExpSize = 8

// errColdShort marks a frame whose total runs past the buffer that holds it: a
// torn tail on recovery, or a corrupt length on a linear walk.
var errColdShort = errors.New("store: cold frame runs past its buffer")

// coldFrame is one decoded frame. key and value alias the source buffer and stay
// valid only as long as it does; the migrator copies out before the buffer
// recycles.
type coldFrame struct {
	kind  byte
	flags byte
	vlen  uint32
	key   []byte
	value []byte
	exp   uint64 // absolute unix-ms deadline, 0 unless flags carry flagHasTTL
}

// appendColdFrame writes one cold frame onto dst and returns the extended slice.
// value is the record's raw value region; the caller decides what that is per
// band (frameRecord does the band selection). Field widths match the resident
// header, so a record the store already admitted cannot overflow klen or vlen,
// and total is bounded by the same header caps plus the frame prefix. exp is
// written as the trailing expiry word only when flags carry flagHasTTL and is
// ignored otherwise, so a TTL-free frame stays byte-identical to before the
// projection slice.
func appendColdFrame(dst []byte, kind, flags byte, vlen uint32, key, value []byte, exp uint64) []byte {
	total := coldHdr + len(key) + len(value)
	if flags&flagHasTTL != 0 {
		total += coldExpSize
	}
	var h [coldHdr]byte
	binary.LittleEndian.PutUint32(h[0:], uint32(total))
	h[4] = kind
	h[5] = flags
	binary.LittleEndian.PutUint16(h[6:], uint16(len(key)))
	binary.LittleEndian.PutUint32(h[8:], vlen)
	dst = append(dst, h[:]...)
	dst = append(dst, key...)
	dst = append(dst, value...)
	if flags&flagHasTTL != 0 {
		dst = binary.LittleEndian.AppendUint64(dst, exp)
	}
	return dst
}

// decodeColdFrame parses the frame at the front of buf, returning it and the
// byte count consumed so a linear scan advances by the return value. A total
// shorter than the header or longer than buf is a torn or corrupt frame and
// errors without aliasing past the buffer.
func decodeColdFrame(buf []byte) (coldFrame, int, error) {
	if len(buf) < coldHdr {
		return coldFrame{}, 0, errColdShort
	}
	total := int(binary.LittleEndian.Uint32(buf[0:]))
	if total < coldHdr || total > len(buf) {
		return coldFrame{}, 0, errColdShort
	}
	klen := int(binary.LittleEndian.Uint16(buf[6:]))
	vend := total
	var exp uint64
	if buf[5]&flagHasTTL != 0 {
		vend = total - coldExpSize
		if coldHdr+klen > vend {
			return coldFrame{}, 0, errColdShort
		}
		exp = binary.LittleEndian.Uint64(buf[vend:])
	}
	if coldHdr+klen > vend {
		return coldFrame{}, 0, errColdShort
	}
	f := coldFrame{
		kind:  buf[4],
		flags: buf[5],
		vlen:  binary.LittleEndian.Uint32(buf[8:]),
		key:   buf[coldHdr : coldHdr+klen],
		value: buf[coldHdr+klen : vend],
		exp:   exp,
	}
	return f, total, nil
}

// valueRegion returns the record's in-arena value bytes, the raw region a cold
// frame carries verbatim: the 8-byte int cell, the 16-byte separated or chunked
// run pointer, or the embedded value's vlen bytes. A pointer band frames its
// pointer, not the run it names, so the log-resident bytes are read on promotion.
func (s *Store) valueRegion(off uint64) []byte {
	vs := s.valueStart(off)
	f := s.recFlags(off)
	switch {
	case f&flagInt != 0:
		return s.arena.buf[vs : vs+8]
	case f&(flagSep|flagChunked) != 0:
		return s.arena.buf[vs : vs+ptrSize]
	default:
		return s.arena.buf[vs : vs+s.vlen(off)]
	}
}

// frameRecord appends the record at off as one whole-record cold frame onto dst
// and returns the extended slice. This is the point-data path; a chunked
// collection demotes through the chunk frame variant (doc 06 section 6.2), not
// through here. The header fields, the value region, and the expiry slot are
// framed verbatim, so the migrator's phase-2 flip can rebuild the resident
// record from the frame alone.
func (s *Store) frameRecord(off uint64, dst []byte) []byte {
	kind := s.arena.buf[off+offKind]
	return appendColdFrame(dst, kind, s.recFlags(off), uint32(s.vlen(off)), s.keyAt(off), s.valueRegion(off), uint64(s.expireAt(off)))
}
