package f1srv

import (
	"encoding/binary"
	"strconv"
)

// DUMP and RESTORE move a value out of and back into the keyspace as a self-describing byte blob:
// the type-tagged RDB serialization of the value, a two-byte RDB version, and an eight-byte CRC64
// that seals the two together. A client dumps a key, ships the opaque bytes somewhere, and restores
// them under any name on any compatible server.
//
// DUMP is not byte-identical across engines and cannot be: Redis 8.8 stamps RDB version 14 into the
// footer and Valkey 9.1 stamps 80, so the version bytes differ and, because the CRC64 covers those
// bytes, the checksums differ too, even for the same value. The compatibility contract is therefore
// round-trip and interop, not byte-equality: aki restores what aki dumped, aki restores what Redis
// or Valkey dumped, and both of them restore what aki dumped. Byte-for-byte the payload cannot match
// both servers at once because the two servers do not match each other.
//
// The string body is the standard RDB string encoding, so a short canonical integer is int-encoded
// exactly as Redis does (the DUMP of "12345" is the same leading bytes on all three engines), and any
// other string is length-prefixed. On the load side the decoder accepts every RDB string form a real
// server emits, including the LZF-compressed form Redis uses for a long compressible value, so a
// Redis- or Valkey-produced string blob restores here even though aki's own encoder never compresses.
//
// The hash type is the second slice. aki dumps a hash as RDB_TYPE_HASH (type 4), the plain form: a
// field count then that many field/value string pairs, which both reference servers accept on RESTORE
// at any size. The load side additionally decodes RDB_TYPE_HASH_LISTPACK (type 16), the listpack form
// both Redis 8.8 and Valkey 9.1 actually emit even for large hashes, so a hash blob produced by either
// server restores here. The remaining collection types arrive in the follow-up slices; each reuses
// this file's CRC64, version framing, string primitives, and (for the listpack encodings) lpDecode.

// RDB object type bytes. Only the forms aki serializes or has to load are named here.
const (
	rdbTypeString       = 0x00 // a plain string value
	rdbTypeHash         = 0x04 // a hash as a field count then field/value string pairs
	rdbTypeHashListpack = 0x10 // a hash packed into a single listpack blob, the form both servers emit
)

// rdbVersion is the RDB version stamped into the footer of every DUMP payload aki produces. RESTORE
// on a server refuses a payload whose version is newer than the server's own, so the value is chosen
// low enough that both Redis 8.8 (RDB version 14) and Valkey 9.1 (RDB version 80) accept an
// aki-produced blob, and at the version where the listpack encodings later type slices emit became
// valid, so those slices do not have to raise it.
const rdbVersion = 11

// rdbMaxLoadVersion is the newest RDB version RESTORE will load. It is set to Valkey 9.1's version so
// a blob produced by either reference server is accepted; the string body format is stable across
// every version in that range, so a higher-versioned string blob still decodes.
const rdbMaxLoadVersion = 80

// RDB string encodings signalled in the low six bits of a 0b11xxxxxx length byte.
const (
	rdbEncInt8  = 0 // 0xC0: an 8-bit signed integer follows
	rdbEncInt16 = 1 // 0xC1: a 16-bit signed integer follows, little-endian
	rdbEncInt32 = 2 // 0xC2: a 32-bit signed integer follows, little-endian
	rdbEncLZF   = 3 // 0xC3: an LZF-compressed string follows
)

// crc64Table is the lookup table for Redis's CRC-64 (the "Jones" polynomial 0xad93d23594c935a9,
// reflected input and output, zero initial and final values), the checksum both servers seal a DUMP
// payload with. It is built once at init from the reflected polynomial.
var crc64Table [256]uint64

func init() {
	rp := crc64Reflect(0xad93d23594c935a9)
	for n := 0; n < 256; n++ {
		crc := uint64(n)
		for k := 0; k < 8; k++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ rp
			} else {
				crc >>= 1
			}
		}
		crc64Table[n] = crc
	}
}

// crc64Reflect returns the bit-reversal of a 64-bit value, used to turn the normal-form polynomial
// into the reflected form the reflected table build and update step expect.
func crc64Reflect(v uint64) uint64 {
	var r uint64
	for i := 0; i < 64; i++ {
		if v&1 != 0 {
			r |= 1 << (63 - i)
		}
		v >>= 1
	}
	return r
}

// crc64 folds data into a running CRC-64. The seed is zero at the start of a payload.
func crc64(seed uint64, data []byte) uint64 {
	crc := seed
	for _, b := range data {
		crc = crc64Table[byte(crc)^b] ^ (crc >> 8)
	}
	return crc
}

// cmdDump serializes a key's value to the RDB blob RESTORE consumes, or replies null when the key
// does not exist. The string and hash types are serialized here; the remaining collection types are
// refused rather than answered with a body this file cannot build, and their slices lift that refusal.
func (c *connState) cmdDump(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'dump' command")
		return
	}
	key := argv[1]
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(key)
	}
	switch c.resolveType(key) {
	case keyMissing:
		c.writeNil()
	case keyString:
		v, _ := c.srv.store.Get(key, nil)
		payload := rdbAppendString([]byte{rdbTypeString}, v)
		c.writeBulk(rdbSeal(payload))
	case keyHash:
		c.writeBulk(rdbSeal(c.rdbDumpHash(key)))
	default:
		c.writeErr("ERR DUMP of this type is not supported yet")
	}
}

// rdbDumpHash builds the RDB_TYPE_HASH body for a hash: the type byte, the field count, then each
// field and value as an RDB string. It holds the key's stripe lock for a consistent snapshot and
// walks the fields off the O(1) count and the collection index, the same enumerate path HGETALL uses,
// so it never depends on an in-memory copy of the whole hash.
func (c *connState) rdbDumpHash(hkey []byte) []byte {
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	defer mu.Unlock()

	payload := rdbAppendLen([]byte{rdbTypeHash}, c.hashCount(hkey))
	prefix := c.hashPrefix(hkey)
	plen := len(prefix)
	var after []byte
	scanK := make([][]byte, 0, hashScanBatch)
	scanO := make([]uint64, 0, hashScanBatch)
	var vbuf []byte
	for {
		keys, offs, last := c.srv.store.CollScanKV(prefix, after, hashScanBatch, scanK[:0], scanO[:0])
		if len(keys) == 0 {
			break
		}
		for i, k := range keys {
			payload = rdbAppendString(payload, k[plen:])
			vbuf = c.srv.store.ReadValueAt(offs[i], vbuf[:0])
			payload = rdbAppendString(payload, vbuf)
		}
		if last == nil {
			break
		}
		after = last
	}
	return payload
}

// cmdRestore parses a DUMP blob and writes its value under a key, honoring the TTL and the REPLACE,
// ABSTTL, IDLETIME, and FREQ options. It reproduces both servers' errors: a negative TTL, a blob
// whose version or checksum is wrong, an unparseable body, and a target key that already exists
// without REPLACE.
func (c *connState) cmdRestore(argv [][]byte) {
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'restore' command")
		return
	}
	key := argv[1]
	ttl, ok := parseInt64Strict(argv[2])
	if !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	if ttl < 0 {
		c.writeErr("ERR Invalid TTL value, must be >= 0")
		return
	}
	blob := argv[3]

	var replace, absttl bool
	for i := 4; i < len(argv); {
		switch {
		case eqFold(argv[i], "REPLACE"):
			replace = true
			i++
		case eqFold(argv[i], "ABSTTL"):
			absttl = true
			i++
		case eqFold(argv[i], "IDLETIME"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			// f1srv does not track per-key idle time, so the value is validated for compatibility
			// and otherwise ignored, the way OBJECT IDLETIME already reports zero here.
			if _, ok := parseInt64Strict(argv[i+1]); !ok {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			i += 2
		case eqFold(argv[i], "FREQ"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, ok := parseInt64Strict(argv[i+1])
			if !ok {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			if n < 0 || n > 255 {
				c.writeErr("ERR Invalid frequency value, must be >= 0 and <= 255")
				return
			}
			i += 2
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	// The footer (version + CRC64) is checked before the body, so a truncated or corrupt blob is
	// rejected with the version-or-checksum error rather than a body-parse error, matching Redis.
	body, ok := rdbCheckFooter(blob)
	if !ok {
		c.writeErr("ERR DUMP payload version or checksum are wrong")
		return
	}
	val, ok := rdbLoadValue(body)
	if !ok {
		c.writeErr("ERR Bad data format")
		return
	}

	// Reap an expired target before the existence check so RESTORE onto a key that has just expired
	// succeeds without REPLACE, the same as on both servers. The probe runs before the stripe lock
	// because expireIfNeeded takes that lock itself.
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(key)
	}
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()

	if c.resolveType(key) != keyMissing {
		if !replace {
			c.writeErr("BUSYKEY Target key name already exists.")
			return
		}
		c.dropKeyLocked(key)
	}
	if err := c.rdbWriteValue(key, val); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	if ttl > 0 {
		atMs := ttl
		if !absttl {
			// A relative TTL is milliseconds from now; the overflow guard keeps a huge TTL from
			// wrapping into the past.
			at, ok := addOverflow(c.nowMs, ttl)
			if !ok {
				c.writeErr("ERR Invalid TTL value, must be >= 0")
				return
			}
			atMs = at
		}
		c.setExpiryLocked(key, atMs)
	}
	c.writeSimple("OK")
}

// rdbSeal appends the two-byte RDB version and the eight-byte CRC64 footer to a payload and returns
// the finished blob. The checksum covers the value bytes and the version bytes together.
func rdbSeal(payload []byte) []byte {
	payload = append(payload, byte(rdbVersion), byte(rdbVersion>>8))
	var sum [8]byte
	binary.LittleEndian.PutUint64(sum[:], crc64(0, payload))
	return append(payload, sum[:]...)
}

// rdbCheckFooter validates a blob's trailing version and CRC64 and returns the body (the type byte
// and value, without the footer). It fails when the blob is too short to hold a footer, names a
// version this build will not load, or does not match its own checksum.
func rdbCheckFooter(blob []byte) ([]byte, bool) {
	// A footer is 2 version bytes + 8 CRC bytes; the smallest value is a 1-byte type + a 1-byte
	// empty-string length, so the shortest valid blob is 12 bytes. Anything shorter is corrupt.
	if len(blob) < 12 {
		return nil, false
	}
	n := len(blob)
	ver := binary.LittleEndian.Uint16(blob[n-10 : n-8])
	if ver == 0 || ver > rdbMaxLoadVersion {
		return nil, false
	}
	stored := binary.LittleEndian.Uint64(blob[n-8:])
	if crc64(0, blob[:n-8]) != stored {
		return nil, false
	}
	return blob[: n-10], true
}

// rdbValue is the decoded form of a DUMP body: the value's type and, for a string, its bytes, or for
// a collection, its elements in order (a hash carries field, value, field, value ...). Keeping the
// elements here lets a single loader handle every type and the caller write them through the type's
// own primitives without re-parsing.
type rdbValue struct {
	kind  keyKind
	str   []byte
	elems [][]byte
}

// rdbLoadValue parses a value body (a type byte followed by the type's encoding) into an rdbValue. It
// understands the string type and both hash encodings a real server emits: the plain field-count form
// and the listpack form. Other type bytes are reported as unparseable until their slices land.
func rdbLoadValue(body []byte) (rdbValue, bool) {
	if len(body) < 1 {
		return rdbValue{}, false
	}
	switch body[0] {
	case rdbTypeString:
		s, _, ok := rdbReadString(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		return rdbValue{kind: keyString, str: s}, true
	case rdbTypeHash:
		count, rest, ok := rdbReadLen(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		elems := make([][]byte, 0, count*2)
		for i := uint64(0); i < count*2; i++ {
			s, r, ok := rdbReadString(rest)
			if !ok {
				return rdbValue{}, false
			}
			elems = append(elems, s)
			rest = r
		}
		return rdbValue{kind: keyHash, elems: elems}, true
	case rdbTypeHashListpack:
		lp, _, ok := rdbReadString(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		elems, ok := lpDecode(lp)
		if !ok || len(elems)%2 != 0 {
			return rdbValue{}, false
		}
		return rdbValue{kind: keyHash, elems: elems}, true
	default:
		return rdbValue{}, false
	}
}

// rdbWriteValue lands a decoded value under a key. The caller holds the key's stripe lock and has
// already cleared any prior value, so this only has to insert. A string is a single Set; a hash writes
// each field/value pair through the same field-key primitives HSET uses, then stamps the O(1) count.
func (c *connState) rdbWriteValue(key []byte, v rdbValue) error {
	switch v.kind {
	case keyString:
		return c.srv.store.Set(key, v.str)
	case keyHash:
		for i := 0; i+1 < len(v.elems); i += 2 {
			fk := c.fieldKey(key, v.elems[i])
			isNew, err := c.srv.store.PutKind(fk, v.elems[i+1], kindHashField)
			if err != nil {
				return err
			}
			if isNew {
				c.srv.store.CollInsert(fk, kindHashField)
			}
		}
		return c.setHashCount(key, uint64(len(v.elems)/2))
	}
	return nil
}

// lpDecode parses a listpack blob into its elements in order. A listpack is a 6-byte header (a 4-byte
// total length and a 2-byte element count, both little-endian), a run of entries, and a single 0xFF
// terminator. Each entry is an encoding byte or two, its data, and a back-length that a forward walk
// skips, so the walk only has to size each entry to reach the next. An integer entry is rendered to
// its decimal text, the shape a hash field or value takes once loaded.
func lpDecode(b []byte) ([][]byte, bool) {
	// Six header bytes and one terminator are the smallest possible listpack.
	if len(b) < 7 {
		return nil, false
	}
	p := 6
	var out [][]byte
	for p < len(b) {
		if b[p] == 0xFF {
			return out, true
		}
		val, n, ok := lpGet(b[p:])
		if !ok {
			return nil, false
		}
		out = append(out, val)
		p += n + lpBacklenSize(n)
	}
	return nil, false
}

// lpGet decodes one listpack entry from the front of b and returns its value, the number of bytes the
// encoding and data occupy (not counting the trailing back-length), and whether it decoded. The
// encodings follow the listpack format: a 7-bit small uint, 6-, 12-, and 32-bit string lengths, and
// 13-, 16-, 24-, 32-, and 64-bit signed integers, every one a hash field or value can take.
func lpGet(b []byte) ([]byte, int, bool) {
	if len(b) < 1 {
		return nil, 0, false
	}
	c := b[0]
	switch {
	case c&0x80 == 0: // 0xxxxxxx: 7-bit unsigned int
		return strconv.AppendInt(nil, int64(c&0x7f), 10), 1, true
	case c&0xC0 == 0x80: // 10xxxxxx: 6-bit string length
		n := int(c & 0x3f)
		if len(b) < 1+n {
			return nil, 0, false
		}
		return append([]byte(nil), b[1:1+n]...), 1 + n, true
	case c&0xE0 == 0xC0: // 110xxxxx yyyyyyyy: 13-bit signed int
		if len(b) < 2 {
			return nil, 0, false
		}
		v := int(c&0x1f)<<8 | int(b[1])
		if v >= 1<<12 {
			v -= 1 << 13
		}
		return strconv.AppendInt(nil, int64(v), 10), 2, true
	case c&0xF0 == 0xE0: // 1110xxxx yyyyyyyy: 12-bit string length
		if len(b) < 2 {
			return nil, 0, false
		}
		n := int(c&0x0f)<<8 | int(b[1])
		if len(b) < 2+n {
			return nil, 0, false
		}
		return append([]byte(nil), b[2:2+n]...), 2 + n, true
	case c == 0xF0: // 32-bit string length, little-endian
		if len(b) < 5 {
			return nil, 0, false
		}
		n := int(binary.LittleEndian.Uint32(b[1:5]))
		if n < 0 || len(b) < 5+n {
			return nil, 0, false
		}
		return append([]byte(nil), b[5:5+n]...), 5 + n, true
	case c == 0xF1: // 16-bit signed int, little-endian
		if len(b) < 3 {
			return nil, 0, false
		}
		return strconv.AppendInt(nil, int64(int16(binary.LittleEndian.Uint16(b[1:3]))), 10), 3, true
	case c == 0xF2: // 24-bit signed int, little-endian
		if len(b) < 4 {
			return nil, 0, false
		}
		u := uint32(b[1]) | uint32(b[2])<<8 | uint32(b[3])<<16
		v := int32(u<<8) >> 8 // sign-extend the 24-bit value
		return strconv.AppendInt(nil, int64(v), 10), 4, true
	case c == 0xF3: // 32-bit signed int, little-endian
		if len(b) < 5 {
			return nil, 0, false
		}
		return strconv.AppendInt(nil, int64(int32(binary.LittleEndian.Uint32(b[1:5]))), 10), 5, true
	case c == 0xF4: // 64-bit signed int, little-endian
		if len(b) < 9 {
			return nil, 0, false
		}
		return strconv.AppendInt(nil, int64(binary.LittleEndian.Uint64(b[1:9])), 10), 9, true
	}
	return nil, 0, false
}

// lpBacklenSize returns how many bytes the back-length field occupies for an entry whose encoding and
// data span l bytes. The listpack stores the back-length in 7-bit groups, so the count steps up at
// each power-of-128 boundary; the forward walk adds it to reach the next entry.
func lpBacklenSize(l int) int {
	switch {
	case l < 128:
		return 1
	case l < 16384:
		return 2
	case l < 2097152:
		return 3
	case l < 268435456:
		return 4
	default:
		return 5
	}
}

// rdbAppendString appends the RDB string encoding of val to dst. A short canonical integer is
// int-encoded exactly as Redis does, so the same value dumps to the same leading bytes on every
// engine; any other string is written length-prefixed and uncompressed.
func rdbAppendString(dst, val []byte) []byte {
	if enc, ok := rdbIntEncode(val); ok {
		return append(dst, enc...)
	}
	dst = rdbAppendLen(dst, uint64(len(val)))
	return append(dst, val...)
}

// rdbIntEncode returns the RDB int-encoding of val when val is the canonical decimal form of an
// integer that fits in 32 signed bits, the same test Redis applies before it int-encodes a string.
func rdbIntEncode(val []byte) ([]byte, bool) {
	if len(val) == 0 || len(val) > 11 {
		return nil, false
	}
	n, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return nil, false
	}
	// Only a canonical decimal round-trips; a value with leading zeros or a plus sign is stored
	// verbatim as a raw string so DUMP is reversible.
	if strconv.FormatInt(n, 10) != string(val) {
		return nil, false
	}
	switch {
	case n >= -128 && n <= 127:
		return []byte{0xC0, byte(int8(n))}, true
	case n >= -32768 && n <= 32767:
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(int16(n)))
		return []byte{0xC1, b[0], b[1]}, true
	case n >= -2147483648 && n <= 2147483647:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(int32(n)))
		return []byte{0xC2, b[0], b[1], b[2], b[3]}, true
	}
	return nil, false
}

// rdbAppendLen appends an RDB length prefix: 6 bits for a small length, 14 bits for a medium one, or
// a marker byte plus a 32- or 64-bit big-endian length for a large one.
func rdbAppendLen(dst []byte, n uint64) []byte {
	switch {
	case n < 1<<6:
		return append(dst, byte(n))
	case n < 1<<14:
		return append(dst, byte(n>>8)|(1<<6), byte(n))
	case n <= 0xffffffff:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(n))
		return append(append(dst, 0x80), b[:]...)
	default:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], n)
		return append(append(dst, 0x81), b[:]...)
	}
}

// rdbReadString decodes one RDB string from b and returns it, the remaining bytes, and whether the
// decode succeeded. It accepts every form a real server emits: the 6- and 14-bit and 32- and 64-bit
// length prefixes, the three int encodings, and the LZF-compressed form.
func rdbReadString(b []byte) ([]byte, []byte, bool) {
	if len(b) < 1 {
		return nil, nil, false
	}
	first := b[0]
	switch first >> 6 {
	case 0: // 6-bit length
		n := int(first & 0x3f)
		b = b[1:]
		if len(b) < n {
			return nil, nil, false
		}
		return b[:n], b[n:], true
	case 1: // 14-bit length
		if len(b) < 2 {
			return nil, nil, false
		}
		n := int(first&0x3f)<<8 | int(b[1])
		b = b[2:]
		if len(b) < n {
			return nil, nil, false
		}
		return b[:n], b[n:], true
	case 2: // 32- or 64-bit length
		switch first {
		case 0x80:
			if len(b) < 5 {
				return nil, nil, false
			}
			n := int(binary.BigEndian.Uint32(b[1:5]))
			b = b[5:]
			if n < 0 || len(b) < n {
				return nil, nil, false
			}
			return b[:n], b[n:], true
		case 0x81:
			if len(b) < 9 {
				return nil, nil, false
			}
			n := binary.BigEndian.Uint64(b[1:9])
			b = b[9:]
			if n > uint64(len(b)) {
				return nil, nil, false
			}
			return b[:n], b[n:], true
		default:
			return nil, nil, false
		}
	default: // 3: encoded value
		switch first & 0x3f {
		case rdbEncInt8:
			if len(b) < 2 {
				return nil, nil, false
			}
			return strconv.AppendInt(nil, int64(int8(b[1])), 10), b[2:], true
		case rdbEncInt16:
			if len(b) < 3 {
				return nil, nil, false
			}
			v := int64(int16(binary.LittleEndian.Uint16(b[1:3])))
			return strconv.AppendInt(nil, v, 10), b[3:], true
		case rdbEncInt32:
			if len(b) < 5 {
				return nil, nil, false
			}
			v := int64(int32(binary.LittleEndian.Uint32(b[1:5])))
			return strconv.AppendInt(nil, v, 10), b[5:], true
		case rdbEncLZF:
			return rdbReadLZF(b[1:])
		default:
			return nil, nil, false
		}
	}
}

// rdbReadLen decodes a plain RDB length (no int or LZF encoding), used for the compressed and
// uncompressed byte counts inside an LZF string.
func rdbReadLen(b []byte) (uint64, []byte, bool) {
	if len(b) < 1 {
		return 0, nil, false
	}
	first := b[0]
	switch first >> 6 {
	case 0:
		return uint64(first & 0x3f), b[1:], true
	case 1:
		if len(b) < 2 {
			return 0, nil, false
		}
		return uint64(first&0x3f)<<8 | uint64(b[1]), b[2:], true
	case 2:
		switch first {
		case 0x80:
			if len(b) < 5 {
				return 0, nil, false
			}
			return uint64(binary.BigEndian.Uint32(b[1:5])), b[5:], true
		case 0x81:
			if len(b) < 9 {
				return 0, nil, false
			}
			return binary.BigEndian.Uint64(b[1:9]), b[9:], true
		}
	}
	return 0, nil, false
}

// rdbReadLZF decodes the LZF-compressed string form: a compressed length, an uncompressed length,
// then the compressed bytes. Redis writes this form for a long compressible value, so decoding it is
// what lets a Redis- or Valkey-produced blob restore here even though aki's encoder never compresses.
func rdbReadLZF(b []byte) ([]byte, []byte, bool) {
	clen, b, ok := rdbReadLen(b)
	if !ok {
		return nil, nil, false
	}
	ulen, b, ok := rdbReadLen(b)
	if !ok {
		return nil, nil, false
	}
	if uint64(len(b)) < clen {
		return nil, nil, false
	}
	out, ok := lzfDecompress(b[:clen], int(ulen))
	if !ok {
		return nil, nil, false
	}
	return out, b[clen:], true
}

// lzfDecompress expands one liblzf-compressed block into exactly ulen bytes. A control byte below 32
// introduces a literal run of ctrl+1 bytes; a higher control byte introduces a back-reference of
// length+2 bytes to an earlier offset, copied byte by byte so an overlapping run repeats correctly.
func lzfDecompress(in []byte, ulen int) ([]byte, bool) {
	out := make([]byte, 0, ulen)
	for i := 0; i < len(in); {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			n := ctrl + 1
			if i+n > len(in) {
				return nil, false
			}
			out = append(out, in[i:i+n]...)
			i += n
			continue
		}
		length := ctrl >> 5
		if length == 7 {
			if i >= len(in) {
				return nil, false
			}
			length += int(in[i])
			i++
		}
		if i >= len(in) {
			return nil, false
		}
		ref := len(out) - ((ctrl&0x1f)<<8) - int(in[i]) - 1
		i++
		if ref < 0 {
			return nil, false
		}
		for k := 0; k < length+2; k++ {
			out = append(out, out[ref+k])
		}
	}
	if len(out) != ulen {
		return nil, false
	}
	return out, true
}
