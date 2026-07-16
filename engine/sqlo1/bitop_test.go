package sqlo1

import (
	"bytes"
	"context"
	"math/rand/v2"
	"testing"
)

// oracleBitop computes BITOP the slow byte-at-a-time way, zero-padding
// shorter sources to the longest, independently of the streamed
// implementation.
func oracleBitop(op int, srcs [][]byte) []byte {
	maxLen := 0
	for _, s := range srcs {
		maxLen = max(maxLen, len(s))
	}
	out := make([]byte, maxLen)
	for i := range out {
		var b byte
		for j, s := range srcs {
			var v byte
			if i < len(s) {
				v = s[i]
			}
			if j == 0 {
				b = v
				continue
			}
			switch op {
			case bitopAnd:
				b &= v
			case bitopOr:
				b |= v
			case bitopXor:
				b ^= v
			}
		}
		if op == bitopNot {
			b = ^b
		}
		out[i] = b
	}
	return out
}

func (r *strRig) bitop(op int, dest string, srcs ...string) int64 {
	r.t.Helper()
	keys := make([][]byte, len(srcs))
	for i, k := range srcs {
		keys[i] = []byte(k)
	}
	n, err := r.s.BitOp(context.Background(), op, []byte(dest), keys)
	if err != nil {
		r.t.Fatalf("BitOp(%d, %q, %v): %v", op, dest, srcs, err)
	}
	return n
}

// TestStrBitOpOracle drives random BITOPs over a pool of keys that
// spans the whole ladder (missing, plain, blob-sized, rope, rope with
// lazy gaps) against the byte oracle, with destinations drawn from
// the same pool so overwrite, dest-among-sources, and empty-result
// deletion all occur, then reopens cold and checks every key.
func TestStrBitOpOracle(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	oracle := map[string][]byte{}
	setKey := func(k string, v []byte) {
		r.set(k, v)
		oracle[k] = bytes.Clone(v)
	}
	setKey("plain", pat(100, 3))
	setKey("blob", pat(6<<10, 4))
	setKey("rope", pat(20<<10, 5))
	setKey("rope2", pat(11<<10, 6))
	// A rope with lazy gap chunks: grow far past the end with SETRANGE
	// so the middle chunks never exist.
	setKey("gappy", pat(9<<10, 7))
	if _, err := r.s.SetRange(ctx, []byte("gappy"), 30<<10, pat(512, 8)); err != nil {
		t.Fatalf("SetRange: %v", err)
	}
	g := oracle["gappy"]
	g = append(g, make([]byte, 30<<10+512-len(g))...)
	copy(g[30<<10:], pat(512, 8))
	oracle["gappy"] = g
	oracle["missing"] = nil

	pool := []string{"plain", "blob", "rope", "rope2", "gappy", "missing", "dst1", "dst2"}
	rng := rand.New(rand.NewPCG(21, 43))
	for round := range 48 {
		op := int(rng.Uint64() % 4)
		n := 1
		if op != bitopNot {
			n = 1 + int(rng.Uint64()%3)
		}
		srcs := make([]string, n)
		vals := make([][]byte, n)
		for i := range srcs {
			srcs[i] = pool[rng.Uint64()%uint64(len(pool))]
			vals[i] = oracle[srcs[i]]
		}
		dest := pool[rng.Uint64()%uint64(len(pool))]
		want := oracleBitop(op, vals)
		got := r.bitop(op, dest, srcs...)
		if got != int64(len(want)) {
			t.Fatalf("round %d: BitOp(%d, %q, %v) = %d, want %d", round, op, dest, srcs, got, len(want))
		}
		if len(want) == 0 {
			oracle[dest] = nil
			if v, ok := r.get(r.s, dest); ok {
				t.Fatalf("round %d: empty result left %q holding %d bytes", round, dest, len(v))
			}
		} else {
			oracle[dest] = want
			r.want(r.s, dest, want)
		}
		if rng.Uint64()%4 == 0 {
			r.flush()
		}
	}
	r.flush()
	s2 := r.reopen()
	for _, k := range pool {
		want := oracle[k]
		if len(want) == 0 {
			if v, ok := r.get(s2, k); ok && len(v) != 0 {
				t.Fatalf("cold: %q holds %d bytes, want missing or empty", k, len(v))
			}
			continue
		}
		r.want(s2, k, want)
	}
}

// TestStrBitOpLazyZeroSkip pins the all-zero chunk elision: result
// chunks that come out zero are never written, because an absent
// chunk under the root's total_len already reads as zeros.
func TestStrBitOpLazyZeroSkip(t *testing.T) {
	r := newStrRig(t)

	// Two 32 KiB bitmaps whose set bits live in different chunks.
	a := make([]byte, 32<<10)
	a[5<<10] = 0x80
	b := make([]byte, 32<<10)
	b[20<<10] = 0x01
	r.set("a", a)
	r.set("b", b)
	r.flush()

	before := r.rs.chunkPuts
	if n := r.bitop(bitopAnd, "zero", "a", "b"); n != 32<<10 {
		t.Fatalf("AND length = %d, want %d", n, 32<<10)
	}
	r.flush()
	if got := r.rs.chunkPuts - before; got != 0 {
		t.Fatalf("all-zero AND result wrote %d chunks, want 0", got)
	}
	r.want(r.s, "zero", make([]byte, 32<<10))

	before = r.rs.chunkPuts
	if n := r.bitop(bitopOr, "or", "a", "b"); n != 32<<10 {
		t.Fatalf("OR length = %d, want %d", n, 32<<10)
	}
	r.flush()
	if got := r.rs.chunkPuts - before; got != 2 {
		t.Fatalf("two-bit OR result wrote %d chunks, want 2", got)
	}
	r.want(r.s, "or", oracleBitop(bitopOr, [][]byte{a, b}))
}

// TestStrBitOpStreamMemory is the milestone's constant-memory
// assertion: BITOP over multi-MiB ropes must hold its scratch to one
// stripe (strReadRound chunks), never the result length. The caps are
// checked before any whole-value Get can legitimately grow s.val.
func TestStrBitOpStreamMemory(t *testing.T) {
	r := newStrRig(t)

	const valLen = 2 << 20
	stripe := strReadRound << 10 // rig chunks are 1 KiB
	av, bv := pat(valLen, 9), pat(valLen, 10)
	r.set("a", av)
	r.set("b", bv)
	r.flush()

	if n := r.bitop(bitopXor, "x", "a", "b"); n != valLen {
		t.Fatalf("XOR length = %d, want %d", n, valLen)
	}
	if n := r.bitop(bitopNot, "n", "a"); n != valLen {
		t.Fatalf("NOT length = %d, want %d", n, valLen)
	}
	if got := cap(r.s.bitopAcc); got > stripe {
		t.Fatalf("accumulator grew to %d bytes, stripe bound is %d", got, stripe)
	}
	if got := cap(r.s.val); got > stripe {
		t.Fatalf("rope read scratch grew to %d bytes, stripe bound is %d", got, stripe)
	}
	r.flush()
	r.want(r.s, "x", oracleBitop(bitopXor, [][]byte{av, bv}))
	r.want(r.s, "n", oracleBitop(bitopNot, [][]byte{av}))
}

// TestServerBitOp pins the wire semantics: the documented examples,
// zero-padding, dest deletion on empty results, the TTL discard, and
// every error text.
func TestServerBitOp(t *testing.T) {
	do, _ := dispatchServer(t)

	do("SET", "k1", "foobar")
	do("SET", "k2", "abcdef")
	if got := do("BITOP", "AND", "dest", "k1", "k2"); got != ":6\r\n" {
		t.Fatalf("AND = %q", got)
	}
	if got := do("GET", "dest"); got != "$6\r\n`bc`ab\r\n" {
		t.Fatalf("AND value = %q", got)
	}
	if got := do("BITOP", "or", "dest", "k1", "k2"); got != ":6\r\n" {
		t.Fatalf("lowercase OR = %q", got)
	}
	if got := do("GET", "dest"); got != "$6\r\ngoofev\r\n" {
		t.Fatalf("OR value = %q", got)
	}
	do("BITOP", "XOR", "dest", "k1", "k2")
	if got := do("GET", "dest"); got != "$6\r\n\x07\x0d\x0c\x06\x04\x14\r\n" {
		t.Fatalf("XOR value = %q", got)
	}
	do("BITOP", "NOT", "dest", "k1")
	if got := do("GET", "dest"); got != "$6\r\n\x99\x90\x90\x9d\x9e\x8d\r\n" {
		t.Fatalf("NOT value = %q", got)
	}

	// Zero-padding: shorter source reads as zeros past its end.
	do("SET", "s1", "abc")
	do("SET", "s2", "a")
	if got := do("BITOP", "XOR", "d", "s1", "s2"); got != ":3\r\n" {
		t.Fatalf("padded XOR = %q", got)
	}
	if got := do("GET", "d"); got != "$3\r\n\x00bc\r\n" {
		t.Fatalf("padded XOR value = %q", got)
	}

	// Dest among its own sources.
	if got := do("BITOP", "OR", "s1", "s1", "s1"); got != ":3\r\n" {
		t.Fatalf("self OR = %q", got)
	}
	if got := do("GET", "s1"); got != "$3\r\nabc\r\n" {
		t.Fatalf("self OR value = %q", got)
	}

	// Empty result deletes the destination.
	do("SET", "gone", "x")
	if got := do("BITOP", "AND", "gone", "no1", "no2"); got != ":0\r\n" {
		t.Fatalf("missing AND = %q", got)
	}
	if got := do("GET", "gone"); got != "$-1\r\n" {
		t.Fatalf("dest after empty result = %q", got)
	}

	// The destination's old TTL is discarded, like SET.
	do("SET", "t", "v")
	do("EXPIRE", "t", "100")
	do("BITOP", "NOT", "t", "k1")
	if got := do("TTL", "t"); got != ":-1\r\n" {
		t.Fatalf("TTL after BITOP = %q", got)
	}

	// Errors.
	if got := do("BITOP", "AND", "d"); got != "-ERR wrong number of arguments for 'bitop' command\r\n" {
		t.Fatalf("arity = %q", got)
	}
	if got := do("BITOP", "NAND", "d", "k1"); got != "-ERR syntax error\r\n" {
		t.Fatalf("bad op = %q", got)
	}
	if got := do("BITOP", "NOT", "d", "k1", "k2"); got != "-ERR BITOP NOT must be called with a single source key.\r\n" {
		t.Fatalf("NOT arity = %q", got)
	}
}
