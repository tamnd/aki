package akifile

// The meta-kv codec (spec 2064/f3/07 section 3, the meta_kv segment kind, and section
// 8's import provenance). A meta_kv payload is the file's small string map: a config
// snapshot, RDB-import provenance, and operator notes. It is where `aki import` stamps
// the source RDB version and content hash so `file-info` can say "imported from RDB
// v12" honestly, and where a config snapshot rides along with the data so a copied
// file carries the settings it was written under.
//
// Unlike the fixed-row roots (checkpoint, seg-stats, free map, TTL index), the values
// here are variable-length strings, so a pair is length-prefixed: a u32 key length,
// the key bytes, a u32 value length, the value bytes. The map is small and written
// whole at each commit, not a delta chain. The payload carries no checksum of its own;
// the meta_kv segment header's payload CRC covers it, so the MKV3 magic is a kind
// cross-check, not an integrity field. Codec only: it frames into and reads out of a
// caller-owned payload and never touches a File. The config surface that populates it
// and the import path that stamps provenance are separate slices.

// MetaKVMagic is the meta_kv payload sentinel.
const MetaKVMagic = "MKV3"

const (
	// MetaKVHeaderLen is the fixed meta_kv header size.
	MetaKVHeaderLen = 16
	// MetaKVPairOverhead is the fixed cost of one pair before its bytes: a u32 key
	// length and a u32 value length. A pair is at least this many bytes, which bounds
	// a corrupt entry_count before the walk.
	MetaKVPairOverhead = 8
)

// MetaKVHeader counts the pairs that follow. Like the free map, the header carries no
// derived total, so there is no header-versus-body invariant to tear.
type MetaKVHeader struct {
	EntryCount uint64 // key-value pairs that follow the header
}

// MetaKVPair is one config or provenance entry. Key and Value are arbitrary bytes,
// though in practice both are short UTF-8 strings.
type MetaKVPair struct {
	Key   []byte
	Value []byte
}

// AppendMetaKVHeader frames a meta_kv header onto dst. Pairs follow with
// AppendMetaKVPair, so a map streams out in bounded slices.
func AppendMetaKVHeader(dst []byte, h MetaKVHeader) []byte {
	var b [MetaKVHeaderLen]byte
	copy(b[0:4], MetaKVMagic)
	// b[4:8] reserved, left zero.
	le.PutUint64(b[8:16], h.EntryCount)
	return append(dst, b[:]...)
}

// AppendMetaKVPair frames one length-prefixed key-value pair onto dst: a u32 key
// length, the key, a u32 value length, the value.
func AppendMetaKVPair(dst []byte, key, value []byte) []byte {
	var hdr [4]byte
	le.PutUint32(hdr[:], uint32(len(key)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, key...)
	le.PutUint32(hdr[:], uint32(len(value)))
	dst = append(dst, hdr[:]...)
	return append(dst, value...)
}

// ParseMetaKVHeader decodes and validates a meta_kv header: only the magic, since the
// header carries no invariant beyond its count.
func ParseMetaKVHeader(b []byte) (MetaKVHeader, error) {
	if len(b) < MetaKVHeaderLen {
		return MetaKVHeader{}, ErrShort
	}
	if string(b[0:4]) != MetaKVMagic {
		return MetaKVHeader{}, ErrMagic
	}
	return MetaKVHeader{EntryCount: le.Uint64(b[8:16])}, nil
}

// MetaKVPairs decodes every pair in a meta_kv payload after its header, the load path
// that restores the config and provenance map. It bounds every length against the
// remaining payload so a corrupt count or length cannot over-read: any overrun is
// ErrLength. Keys and values are copied out, so they do not alias the payload.
func MetaKVPairs(payload []byte, h MetaKVHeader) ([]MetaKVPair, error) {
	n := uint64(len(payload))
	if n < MetaKVHeaderLen {
		return nil, ErrShort
	}
	// Every pair costs at least its two length words, so a count beyond the remaining
	// bytes divided by that floor is corrupt before we read a single string.
	if h.EntryCount > (n-MetaKVHeaderLen)/MetaKVPairOverhead {
		return nil, ErrLength
	}
	pairs := make([]MetaKVPair, h.EntryCount)
	off := uint64(MetaKVHeaderLen)
	for i := range pairs {
		key, next, err := readMetaKVField(payload, off, n)
		if err != nil {
			return nil, err
		}
		value, next, err := readMetaKVField(payload, next, n)
		if err != nil {
			return nil, err
		}
		pairs[i] = MetaKVPair{Key: key, Value: value}
		off = next
	}
	return pairs, nil
}

// readMetaKVField decodes one length-prefixed field at off and returns a fresh copy of
// its bytes and the offset past it. It bounds the u32 length against the remaining
// payload so neither the length word nor the body can over-read.
func readMetaKVField(payload []byte, off, n uint64) (field []byte, next uint64, err error) {
	if off+4 > n {
		return nil, 0, ErrLength
	}
	l := uint64(le.Uint32(payload[off : off+4]))
	off += 4
	if l > n-off {
		return nil, 0, ErrLength
	}
	return append([]byte(nil), payload[off:off+l]...), off + l, nil
}

// MetaKVLookup returns the value for a key, the config or provenance read `file-info`
// and the startup path use. It reports the first match; ok is false when the key is
// absent.
func MetaKVLookup(pairs []MetaKVPair, key string) (value []byte, ok bool) {
	for i := range pairs {
		if string(pairs[i].Key) == key {
			return pairs[i].Value, true
		}
	}
	return nil, false
}
