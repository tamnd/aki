package sqlo1

import (
	"encoding/binary"
	"fmt"
)

// Subkeys (doc 03 section 6.3): segment and fence records index
// through the cold index under a 16-byte synthetic key instead of the
// user key, so RENAME moves only the root record and DEL of a huge
// collection is O(1) at command time. The codec lives here rather than
// in the Track B package because the type layer mints rooths and
// builds subkeys on the hot path; sqlo1b re-exports these names so the
// format package and its consumers see one definition. The kind byte
// is namespaced by the root's type: only the two values below mean the
// same thing in every per-type doc, the rest (popcount caches, score
// runs, stream groups, PELs) belong to docs 05..10.
const (
	SubkindSeg   uint8 = 1 // primary segment records, every collection type
	SubkindFence uint8 = 3 // fence pages, every paged collection type
)

// SubkeySize is the synthetic key length for segment and fence
// records.
const SubkeySize = 16

// maxSegid is the 56-bit segid ceiling; the subkey layout gives segid
// seven bytes.
const maxSegid = 1<<56 - 1

// maxRoothCounter is the 48-bit per-shard mint counter ceiling; the
// mint packs the shard into the top 16 bits of splitmix64's input.
const maxRoothCounter = 1<<48 - 1

// Subkey is the decoded 16-byte synthetic key: minted root identity,
// per-type kind, 56-bit per-type segment identifier.
type Subkey struct {
	Rooth uint64
	Kind  uint8
	Segid uint64
}

// NewSubkey builds a subkey, rejecting a segid past its seven bytes
// and the reserved kind 0.
func NewSubkey(rooth uint64, kind uint8, segid uint64) (Subkey, error) {
	if kind == 0 {
		return Subkey{}, fmt.Errorf("sqlo1: subkey kind 0 is reserved")
	}
	if segid > maxSegid {
		return Subkey{}, fmt.Errorf("sqlo1: segid %d exceeds 56 bits", segid)
	}
	return Subkey{Rooth: rooth, Kind: kind, Segid: segid}, nil
}

// Encode lays the subkey out per the doc 6.3 table: u64 rooth, u8
// kind, 7-byte little-endian segid. The result is a record Key.
func (s Subkey) Encode() []byte {
	b := make([]byte, SubkeySize)
	binary.LittleEndian.PutUint64(b[0:], s.Rooth)
	b[8] = s.Kind
	var seg [8]byte
	binary.LittleEndian.PutUint64(seg[:], s.Segid)
	copy(b[9:], seg[:7])
	return b
}

// DecodeSubkey parses a record Key that the envelope already sized:
// exactly SubkeySize bytes, matching what validateEnvelope enforces
// for seg and fence records.
func DecodeSubkey(b []byte) (Subkey, error) {
	if len(b) != SubkeySize {
		return Subkey{}, fmt.Errorf("sqlo1: subkey of %d bytes, want %d", len(b), SubkeySize)
	}
	var seg [8]byte
	copy(seg[:7], b[9:])
	s := Subkey{
		Rooth: binary.LittleEndian.Uint64(b[0:]),
		Kind:  b[8],
		Segid: binary.LittleEndian.Uint64(seg[:]),
	}
	if s.Kind == 0 {
		return Subkey{}, fmt.Errorf("sqlo1: subkey kind 0 is reserved")
	}
	return s, nil
}

// MintRooth mints a root identity from the shard and its monotonic
// root counter: splitmix64 over shard<<48|counter. splitmix64 is a
// bijection, distinct inputs give distinct rooths, so the mint is
// collision-free by construction across shards and time, and a rooth
// never depends on the current key name (RENAME stability, doc 12
// section 2.2). The counter's persistence is the store layer's job;
// it lives in root payload headers and replays with them.
func MintRooth(shard uint16, counter uint64) (uint64, error) {
	if counter > maxRoothCounter {
		return 0, fmt.Errorf("sqlo1: rooth counter %d exceeds 48 bits", counter)
	}
	return splitmix64(uint64(shard)<<48 | counter), nil
}

// GenKey returns the reserved key under which a backend records
// rooth's current generation: rooth in the subkey layout with kind 0
// and a zero segid. NewSubkey and DecodeSubkey reject kind 0, so no
// per-type segment can collide. Both tracks store generation state
// under the same bytes, which keeps the drain batch's Bumps
// backend-agnostic.
func GenKey(rooth uint64) []byte {
	b := make([]byte, SubkeySize)
	binary.LittleEndian.PutUint64(b, rooth)
	return b
}

// LeaseEnd validates a mint lease of n counters starting at start and
// returns the new lease mark, start+n. A mark counts counters ever
// leased, so the space is spent when the mark reaches the 48-bit
// counter ceiling. Every Minter backend hands this its recorded mark
// and the caller's n and stores the result, which keeps the range
// arithmetic and its overflow care in one place.
func LeaseEnd(start, n uint64) (uint64, error) {
	const space = maxRoothCounter + 1
	if n == 0 {
		return 0, fmt.Errorf("sqlo1: mint lease of zero counters")
	}
	if start > space || n > space-start {
		return 0, fmt.Errorf("sqlo1: mint lease of %d at mark %d passes the 48-bit counter space", n, start)
	}
	return start + n, nil
}

// splitmix64 is the Steele-Lea-Flood mix. The reference generator
// from seed 0 emits splitmix64(k*gamma) for k = 0, 1, 2, ..., which
// the golden test pins; the mint feeds it plain packed integers, the
// bijection is what matters, not the stream.
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}
