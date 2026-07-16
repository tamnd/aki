package derived

// The HLL debug surface (spec 2064/f3/15 section 7.4): PFDEBUG and PFSELFTEST,
// ported with their Redis shapes because the differential suite greps them.
// PFDEBUG introspects one sketch (its registers, its opcode stream, its
// encoding) and can force it dense; PFSELFTEST is the standing correctness gate
// on the estimator and the encoding equivalence, a self-contained check that
// touches no key.

import (
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// errNoSuchKey is the Redis wording PFDEBUG returns for a missing key, distinct
// from the count commands' silent zero: PFDEBUG is an introspection verb, so a
// missing key is an error, not an empty sketch.
const errNoSuchKey = "ERR The specified key does not exist"

// eqFold reports whether b equals the uppercase token s, case-insensitively, the
// subcommand match every option-parsing handler in the tree uses.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

// PfDebug answers PFDEBUG <subcommand> <key>. The key must exist and hold a valid
// HYLL sketch, checked before any subcommand runs; a missing key is an error and
// a non-HYLL value is the WRONGTYPE refusal, the same order Redis uses.
func PfDebug(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	sub := args[0]
	key := args[1]
	blob, ok := cx.St.GetString(key, cx.NowMs, cx.Val[:0])
	cx.Val = blob
	if !ok {
		r.Err(errNoSuchKey)
		return
	}
	if !isHLL(blob) {
		r.Err(errNotHLL)
		return
	}
	switch {
	case eqFold(sub, "GETREG"):
		pfDebugGetReg(cx, key, blob, r)
	case eqFold(sub, "DECODE"):
		pfDebugDecode(blob, r)
	case eqFold(sub, "ENCODING"):
		if blob[4] == hllDense {
			r.Status("dense")
		} else {
			r.Status("sparse")
		}
	case eqFold(sub, "TODENSE"):
		pfDebugToDense(cx, key, blob, r)
	default:
		r.Err("ERR unknown PFDEBUG subcommand or wrong number of arguments")
	}
}

// pfDebugGetReg replies the array of all 16384 register values. Redis requires a
// dense sketch here, so a sparse one is promoted in place first (a write), then
// the packed registers are unpacked once and emitted as integers.
func pfDebugGetReg(cx *shard.Ctx, key, blob []byte, r shard.Reply) {
	if blob[4] == hllSparse {
		dense, ok := sparseToDense(blob)
		if !ok {
			r.Err(errCorruptHLL)
			return
		}
		if err := cx.St.SetString(key, dense, cx.NowMs, 0, true); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
		blob = dense
	}
	regs := make([]byte, hllRegisters)
	unpackDenseInto(blob[hllHdrSize:], regs)
	out := resp.AppendArrayHeader(cx.Aux[:0], hllRegisters)
	for _, v := range regs {
		out = resp.AppendInt(out, int64(v))
	}
	cx.Aux = out
	r.Raw(out)
}

// pfDebugDecode replies the sparse opcode stream as one space-joined bulk string,
// the Redis format: "z:<len>" for ZERO, "Z:<len>" for XZERO, "v:<val>,<len>" for
// VAL. It is defined only for a sparse sketch; a dense one is an error.
func pfDebugDecode(blob []byte, r shard.Reply) {
	if blob[4] != hllSparse {
		r.Err("ERR HLL encoding is not sparse")
		return
	}
	opcodes := blob[hllHdrSize:]
	out := make([]byte, 0, len(opcodes)*6)
	p := 0
	for p < len(opcodes) {
		if len(out) > 0 {
			out = append(out, ' ')
		}
		op := opcodes[p]
		switch {
		case sparseIsZero(op):
			out = append(out, 'z', ':')
			out = strconv.AppendInt(out, int64(sparseZeroLen(op)), 10)
			p++
		case sparseIsXZero(op):
			if p+1 >= len(opcodes) {
				r.Err(errCorruptHLL)
				return
			}
			out = append(out, 'Z', ':')
			out = strconv.AppendInt(out, int64(sparseXZeroLen(opcodes[p], opcodes[p+1])), 10)
			p += 2
		default: // VAL
			out = append(out, 'v', ':')
			out = strconv.AppendInt(out, int64(sparseValValue(op)), 10)
			out = append(out, ',')
			out = strconv.AppendInt(out, int64(sparseValLen(op)), 10)
			p++
		}
	}
	r.Bulk(out)
}

// pfDebugToDense forces a sparse sketch dense in place and replies OK. An already
// dense sketch is left untouched, no write, still OK, the Redis contract.
func pfDebugToDense(cx *shard.Ctx, key, blob []byte, r shard.Reply) {
	if blob[4] == hllSparse {
		dense, ok := sparseToDense(blob)
		if !ok {
			r.Err(errCorruptHLL)
			return
		}
		if err := cx.St.SetString(key, dense, cx.NowMs, 0, true); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
	}
	r.Status("OK")
}

// PfSelfTest runs the estimator-and-encoding self check and replies OK, or an
// error naming the first failing invariant. It touches no key: it builds sketches
// in local scratch, so it is a keyless command. Two invariants are checked, the
// pair the port can regress on: sparse and dense sketches fed the same elements
// must hold byte-identical registers, and the estimate must track the true
// cardinality within the HLL error envelope across a range of sizes.
func PfSelfTest(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if msg := hllSelfTest(); msg != "" {
		r.Err("ERR " + msg)
		return
	}
	r.Status("OK")
}

// hllSelfTest returns "" on success or the failing invariant's message.
func hllSelfTest() string {
	var buf [24]byte
	ele := func(i int) []byte { return strconv.AppendInt(buf[:0], int64(i), 10) }

	// Sparse and dense agreement: the same elements into a sparse sketch and a
	// fresh dense sketch must promote to the same registers.
	sparse := createSparse()
	dense := denseFromScratch(make([]byte, hllRegisters))
	for i := 0; i < 1000; i++ {
		e := ele(i)
		var ret int
		if sparse, ret = hllAdd(sparse, e); ret < 0 {
			return "sparse add failed during self test"
		}
		if dense, ret = hllAdd(dense, e); ret < 0 {
			return "dense add failed during self test"
		}
	}
	promoted, ok := sparseToDense(sparse)
	if !ok {
		return "sparse to dense conversion failed during self test"
	}
	if string(promoted[hllHdrSize:]) != string(dense[hllHdrSize:]) {
		return "sparse and dense registers disagree"
	}

	// Estimator accuracy: add distinct elements to a dense sketch and check the
	// running estimate stays within a generous multiple of the standard error at
	// a set of checkpoints. p=14 gives ~0.81 percent standard error; the 5 percent
	// gate is loose enough to never flap yet tight enough to catch a broken port.
	checks := map[int]bool{1000: true, 10000: true, 50000: true, 100000: true}
	acc := denseFromScratch(make([]byte, hllRegisters))
	for i := 0; i < 100000; i++ {
		var ret int
		if acc, ret = hllAdd(acc, ele(i)); ret < 0 {
			return "dense add failed during self test"
		}
		n := i + 1
		if !checks[n] {
			continue
		}
		card, ok := hllCount(acc)
		if !ok {
			return "count failed during self test"
		}
		diff := int64(card) - int64(n)
		if diff < 0 {
			diff = -diff
		}
		if diff*100 > int64(n)*5 {
			return "estimate out of tolerance at " + strconv.Itoa(n)
		}
	}
	return ""
}
