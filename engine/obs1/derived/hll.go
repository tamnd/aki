package derived

// HyperLogLog (spec 2064/f3/15 section 7) is a string holding a fixed-size
// cardinality sketch: 16384 six-bit registers, dense form exactly 12304 bytes,
// sparse form an opcode stream up to hll-sparse-max-bytes. GET returns the raw
// sketch, SET can write it, TYPE reports string, so the byte layout is a Redis
// wire interop surface, not an internal detail: PFCOUNT on identical bytes must
// return the number a same-version Redis returns. The encodings, the
// MurmurHash64A element hash, and the estimator are all ported byte-for-byte
// from Redis hyperloglog.c for that reason.
//
// This file carries the header contract, the HYLL validity check, the sparse
// creation of a fresh sketch, and the PFADD / single-key PFCOUNT commands.
// Multi-key PFCOUNT and PFMERGE (the register-merge fold and the F17 hop plan of
// sections 8 and 9) land in the following slices, the same co-located-first split
// BITOP took.

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/obs1/shard"
)

const (
	hllP         = 14               // register-index bits
	hllRegisters = 1 << hllP        // 16384 registers
	hllBits      = 6                // bits per register
	hllRegMax    = 63               // 6-bit register ceiling
	hllQ         = 64 - hllP        // 50, the sentinel-bounded value ceiling is Q+1
	hllPMask     = hllRegisters - 1 // low-14-bit register-index mask
	hllHashSeed  = 0xadc83b19       // Redis HLL MurmurHash64A seed

	hllHdrSize   = 16                           // magic(4)+encoding(1)+pad(3)+card(8)
	hllRegBytes  = (hllRegisters * hllBits) / 8 // 12288 packed register bytes
	hllDenseSize = hllHdrSize + hllRegBytes     // 12304 bytes exactly

	hllDense       = 0 // encoding byte values
	hllSparse      = 1
	hllMaxEncoding = 1

	// Sparse opcode limits, Redis names kept.
	hllSparseXZeroMaxLen = 16384
	hllSparseValMaxValue = 32
	hllSparseValMaxLen   = 4
	hllSparseZeroMaxLen  = 64

	// hllSparseMaxBytes is the Redis default hll-sparse-max-bytes, kept at the
	// Redis value for layout interop: a sparse sketch past this budget promotes to
	// dense. A tuning knob lands with the config surface; the constant is the
	// default until then.
	hllSparseMaxBytes = 3000
)

const (
	errNotHLL     = "WRONGTYPE Key is not a valid HyperLogLog string value."
	errCorruptHLL = "INVALIDOBJ Corrupted HLL object detected"
)

// isHLL reports whether blob is a structurally valid HYLL sketch: the "HYLL"
// magic, a known encoding byte, and, for dense, the exact 12304-byte length.
// Sparse sketches carry no length check here; a malformed opcode stream is caught
// later when it is decoded, the same split Redis draws between structural
// validation (before any mutation) and the estimator's corruption signal. This is
// the check every HLL command runs before it touches a byte.
func isHLL(blob []byte) bool {
	if len(blob) < hllHdrSize {
		return false
	}
	if blob[0] != 'H' || blob[1] != 'Y' || blob[2] != 'L' || blob[3] != 'L' {
		return false
	}
	if blob[4] > hllMaxEncoding {
		return false
	}
	if blob[4] == hllDense && len(blob) != hllDenseSize {
		return false
	}
	return true
}

// cacheValid reports whether the cached cardinality in the header is current. The
// stale bit is the MSB of the last card byte; clear means PFCOUNT can return the
// cached count with no recompute.
func cacheValid(blob []byte) bool { return blob[15]&0x80 == 0 }

// invalidateCache sets the stale bit so the next PFCOUNT recomputes.
func invalidateCache(blob []byte) { blob[15] |= 0x80 }

// readCache returns the cached cardinality. It is only meaningful when
// cacheValid, where the stale bit (the MSB of the top byte) is clear, so the
// eight little-endian bytes are the count directly.
func readCache(blob []byte) uint64 { return binary.LittleEndian.Uint64(blob[8:16]) }

// writeCache stores card in the header and, because a real cardinality never uses
// the top bit, clears the stale bit as a side effect, exactly as Redis does.
func writeCache(blob []byte, card uint64) { binary.LittleEndian.PutUint64(blob[8:16], card) }

// createSparse builds a fresh empty sketch: the 16-byte header plus the XZERO
// opcodes that cover all 16384 registers with zero. Since HLL_REGISTERS equals
// the XZERO run ceiling, that is a single XZERO(16384). The zeroed header means a
// valid cache holding cardinality zero, the correct answer for an empty HLL.
func createSparse() []byte {
	blob := make([]byte, hllHdrSize, hllHdrSize+2)
	blob[0], blob[1], blob[2], blob[3] = 'H', 'Y', 'L', 'L'
	blob[4] = hllSparse
	aux := hllRegisters
	for aux > 0 {
		run := hllSparseXZeroMaxLen
		if run > aux {
			run = aux
		}
		blob = appendXZero(blob, run)
		aux -= run
	}
	return blob
}

// PfAdd answers PFADD key [element ...]: hash each element into its register and
// keep the max, returning 1 if the sketch (or the key) changed and 0 otherwise.
// A missing key is created as an empty sparse sketch, so PFADD with no elements
// is the touch-or-create idiom: 1 when it created the key, 0 when it already
// existed. Validation runs before any mutation, so a key holding non-HYLL bytes
// is refused without a write.
func PfAdd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	blob, ok := cx.St.GetString(key, cx.NowMs, nil)
	updated := 0
	if ok {
		if !isHLL(blob) {
			r.Err(errNotHLL)
			return
		}
	} else {
		blob = createSparse()
		updated++
	}

	for _, ele := range args[1:] {
		newBlob, ret := hllAdd(blob, ele)
		if ret < 0 {
			r.Err(errCorruptHLL)
			return
		}
		blob = newBlob
		updated += ret
	}

	if updated > 0 {
		invalidateCache(blob)
		if err := cx.St.SetString(key, blob, cx.NowMs, 0, true); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
		r.Int(1)
		return
	}
	r.Int(0)
}

// hllAdd applies one element to the sketch and returns the (possibly reallocated)
// sketch and the change flag: 1 if a register grew, 0 if not, -1 on a corrupt
// sketch. The sparse path can splice, grow, and promote to dense, so it returns a
// fresh slice; the dense path edits in place and returns the same slice.
func hllAdd(blob, ele []byte) ([]byte, int) {
	index, count := hllPatLen(ele)
	switch blob[4] {
	case hllDense:
		return blob, denseSet(blob[hllHdrSize:], index, count)
	case hllSparse:
		return sparseSet(blob, index, count)
	default:
		return blob, -1
	}
}

// PfCount answers PFCOUNT key [key ...]: the estimated cardinality of one key,
// or of the union of several. The single-key path keeps the cache fast path; a
// clear cache bit returns the cached count at point-read cost, a set bit
// recomputes over all 16384 registers, writes the count back into the header,
// and clears the bit, the one path that makes PFCOUNT a write. The multi-key
// path folds every co-located key into one register scratch and estimates from
// it; the union count belongs to no single key's header, so nothing is cached
// and nothing is written. A key set spanning shards routes to PfCountCross.
func PfCount(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args) > 1 {
		pfCountUnion(cx, args, r)
		return
	}
	blob, ok := cx.St.GetString(args[0], cx.NowMs, nil)
	if !ok {
		r.Int(0)
		return
	}
	if !isHLL(blob) {
		r.Err(errNotHLL)
		return
	}
	if cacheValid(blob) {
		r.Int(int64(readCache(blob)))
		return
	}
	card, ok := hllCount(blob)
	if !ok {
		r.Err(errCorruptHLL)
		return
	}
	writeCache(blob, card)
	if err := cx.St.SetString(args[0], blob, cx.NowMs, 0, true); err != nil {
		r.Err("ERR " + err.Error())
		return
	}
	r.Int(int64(card))
}
