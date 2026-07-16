package sqlo1

import (
	"bytes"
	"context"
	"math"
	"testing"
)

func (r *strRig) setbit(s *Str, key string, off int64, bit int) int {
	r.t.Helper()
	old, err := s.SetBit(context.Background(), []byte(key), off, bit)
	if err != nil {
		r.t.Fatalf("SetBit(%q, %d, %d): %v", key, off, bit, err)
	}
	return old
}

func (r *strRig) getbit(s *Str, key string, off int64) int {
	r.t.Helper()
	b, err := s.GetBit(context.Background(), []byte(key), off)
	if err != nil {
		r.t.Fatalf("GetBit(%q, %d): %v", key, off, err)
	}
	return b
}

// TestStrBitPoint walks SETBIT and GETBIT against a byte oracle on a
// plain key: previous-bit returns, growth to the bit's byte even when
// writing a zero, zero reads past the end and on missing keys, and
// the no-op skip that leaves nothing to drain.
func TestStrBitPoint(t *testing.T) {
	r := newStrRig(t)

	if r.getbit(r.s, "missing", 100) != 0 {
		t.Fatal("GETBIT on a missing key read nonzero")
	}
	if _, ok := r.get(r.s, "missing"); ok {
		t.Fatal("GETBIT created the key")
	}

	var want []byte
	steps := []struct {
		off int64
		bit int
	}{
		{0, 1}, {7, 1}, {3, 1}, {0, 0}, {100, 1}, {101, 0}, {50, 1}, {100, 0}, {12, 1},
	}
	for _, st := range steps {
		wantOld := 0
		if b := int(st.off >> 3); b < len(want) && want[b]&(1<<(7-uint(st.off)&7)) != 0 {
			wantOld = 1
		}
		if old := r.setbit(r.s, "k", st.off, st.bit); old != wantOld {
			t.Fatalf("SETBIT(%d, %d) returned %d, want %d", st.off, st.bit, old, wantOld)
		}
		for int64(len(want)) <= st.off>>3 {
			want = append(want, 0)
		}
		mask := byte(1) << (7 - uint(st.off)&7)
		if st.bit != 0 {
			want[st.off>>3] |= mask
		} else {
			want[st.off>>3] &^= mask
		}
		got, ok := r.get(r.s, "k")
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("after SETBIT(%d, %d): value %x, want %x", st.off, st.bit, got, want)
		}
		if b := r.getbit(r.s, "k", st.off); b != st.bit {
			t.Fatalf("GETBIT(%d) read %d after writing %d", st.off, b, st.bit)
		}
	}
	if r.getbit(r.s, "k", int64(len(want))*8+9) != 0 {
		t.Fatal("GETBIT past the end read nonzero")
	}

	// A bit write that changes neither a byte nor the length drains
	// nothing.
	r.flush()
	base := r.rs.chunkPuts + r.rs.rootPuts + r.rs.plainPuts
	for _, st := range steps[len(steps)-3:] {
		cur := r.getbit(r.s, "k", st.off)
		if old := r.setbit(r.s, "k", st.off, cur); old != cur {
			t.Fatalf("no-op SETBIT(%d) returned %d, want %d", st.off, old, cur)
		}
	}
	r.flush()
	if got := r.rs.chunkPuts + r.rs.rootPuts + r.rs.plainPuts; got != base {
		t.Fatalf("no-op SETBITs drained %d records", got-base)
	}

	// Cold continuation: the same bytes answer from the store.
	s2 := r.reopen()
	if got, ok := r.get(s2, "k"); !ok || !bytes.Equal(got, want) {
		t.Fatalf("cold value %x, want %x", got, want)
	}
	if r.getbit(s2, "k", 3) != 1 || r.getbit(s2, "k", 100) != 0 {
		t.Fatal("cold GETBIT disagrees with the oracle")
	}
}

// TestStrSetBitFarOffset pins the doc 05 capability claim: a SETBIT
// far past any existing byte costs one chunk record plus the root,
// never the gap, and every reader sees the gap as zeros (S-I4).
func TestStrSetBitFarOffset(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	const off = int64(1<<20)*8 + 3 // bit 3 of byte 1 MiB
	r.flush()
	c0, r0 := r.rs.chunkPuts, r.rs.rootPuts
	if old := r.setbit(r.s, "bm", off, 1); old != 0 {
		t.Fatalf("far SETBIT returned %d", old)
	}
	r.flush()
	if got := r.rs.chunkPuts - c0; got != 1 {
		t.Fatalf("far SETBIT drained %d chunk records, want 1", got)
	}
	if got := r.rs.rootPuts - r0; got != 1 {
		t.Fatalf("far SETBIT drained %d root records, want 1", got)
	}

	n, ok, err := r.s.Strlen(ctx, []byte("bm"))
	if err != nil || !ok || n != 1<<20+1 {
		t.Fatalf("Strlen after far SETBIT: %d, %v, %v", n, ok, err)
	}
	if r.getbit(r.s, "bm", off) != 1 {
		t.Fatal("far bit reads 0")
	}
	if r.getbit(r.s, "bm", 12345) != 0 || r.getbit(r.s, "bm", off-8) != 0 {
		t.Fatal("lazy gap reads nonzero")
	}

	// Cold: the gap is still zeros and the bit still set through a
	// fresh tier.
	s2 := r.reopen()
	if r.getbit(s2, "bm", off) != 1 || r.getbit(s2, "bm", 999999) != 0 {
		t.Fatal("cold far bit or gap disagrees")
	}
	v, err := s2.Range(ctx, []byte("bm"), 1<<20, 1<<20)
	if err != nil || len(v) != 1 || v[0] != 0x10 {
		t.Fatalf("far byte reads %x, want 10", v)
	}
}

// TestStrSetBitChunkCoalescing is the milestone assert: a storm of
// SETBITs inside one chunk drains as one chunk record per drain
// cycle, because the hot tier redirties one slot instead of queueing
// per-operation writes.
func TestStrSetBitChunkCoalescing(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	want := pat(16<<10, 9) // 16 chunks of 1 KiB under the rig config
	r.set("bm", want)
	r.flush()

	// Two full cycles: each scatters writes across chunk 3 and drains
	// exactly one image of it, and nothing else.
	for cycle := range int64(2) {
		c0, r0, p0 := r.rs.chunkPuts, r.rs.rootPuts, r.rs.plainPuts
		for i := range int64(100) {
			off := 3*8192 + (i*37+cycle*11)%8192
			bit := int(i & 1)
			r.setbit(r.s, "bm", off, bit)
			mask := byte(1) << (7 - uint(off)&7)
			if bit != 0 {
				want[off>>3] |= mask
			} else {
				want[off>>3] &^= mask
			}
		}
		r.flush()
		if got := r.rs.chunkPuts - c0; got != 1 {
			t.Fatalf("cycle %d drained %d chunk records, want 1", cycle, got)
		}
		if r.rs.rootPuts != r0 || r.rs.plainPuts != p0 {
			t.Fatalf("cycle %d drained root or plain records", cycle)
		}
	}

	v, err := r.s.Range(ctx, []byte("bm"), 3072, 4095)
	if err != nil || !bytes.Equal(v, want[3072:4096]) {
		t.Fatalf("chunk 3 bytes diverged from the oracle")
	}
	if got, ok := r.get(r.reopen(), "bm"); !ok || !bytes.Equal(got, want) {
		t.Fatal("cold value diverged from the oracle")
	}
}

// TestStrBitfield drives the op executor: the overflow matrix for
// both signednesses at both poles, WRAP and SAT and FAIL, unaligned
// and full-width fields, sequential visibility inside one call, and
// GET's no-create rule.
func TestStrBitfield(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	bf := func(s *Str, key string, ops ...BitfieldOp) ([]int64, []bool) {
		t.Helper()
		res, nulls, err := s.Bitfield(ctx, []byte(key), ops)
		if err != nil {
			t.Fatalf("Bitfield(%q): %v", key, err)
		}
		if len(res) != len(ops) || len(nulls) != len(ops) {
			t.Fatalf("Bitfield returned %d results for %d ops", len(res), len(ops))
		}
		return res, nulls
	}

	steps := []struct {
		op   BitfieldOp
		res  int64
		null bool
	}{
		{BitfieldOp{'s', false, 8, 'w', 0, 255}, 0, false},
		{BitfieldOp{'g', false, 8, 'w', 0, 0}, 255, false},
		{BitfieldOp{'i', false, 8, 'w', 0, 10}, 9, false},
		{BitfieldOp{'i', false, 8, 's', 0, 250}, 255, false},
		{BitfieldOp{'i', false, 8, 'f', 0, 1}, 0, true},
		{BitfieldOp{'g', false, 8, 'w', 0, 0}, 255, false},
		{BitfieldOp{'i', false, 8, 'w', 0, -255}, 0, false},
		{BitfieldOp{'i', false, 8, 's', 0, -5}, 0, false},
		{BitfieldOp{'s', false, 8, 's', 0, -3}, 0, false},
		{BitfieldOp{'g', false, 8, 'w', 0, 0}, 0, false},
		{BitfieldOp{'s', false, 8, 'w', 0, 300}, 0, false},
		{BitfieldOp{'g', false, 8, 'w', 0, 0}, 44, false},

		{BitfieldOp{'s', true, 8, 'w', 8, 200}, 0, false},
		{BitfieldOp{'g', true, 8, 'w', 8, 0}, -56, false},
		{BitfieldOp{'g', false, 8, 'w', 8, 0}, 200, false},
		{BitfieldOp{'s', true, 8, 's', 8, 200}, -56, false},
		{BitfieldOp{'g', true, 8, 'w', 8, 0}, 127, false},
		{BitfieldOp{'i', true, 8, 's', 8, -1000}, -128, false},
		{BitfieldOp{'s', true, 8, 'f', 8, 128}, 0, true},
		{BitfieldOp{'g', true, 8, 'w', 8, 0}, -128, false},
		{BitfieldOp{'i', true, 8, 'w', 8, -1}, 127, false},
		{BitfieldOp{'i', true, 8, 'f', 8, 1}, 0, true},

		{BitfieldOp{'s', false, 4, 'w', 20, 15}, 0, false},
		{BitfieldOp{'g', false, 4, 'w', 20, 0}, 15, false},
		{BitfieldOp{'i', false, 4, 'w', 20, 2}, 1, false},

		{BitfieldOp{'s', true, 64, 'w', 24, -1}, 0, false},
		{BitfieldOp{'g', true, 64, 'w', 24, 0}, -1, false},
		{BitfieldOp{'i', true, 64, 'w', 24, math.MinInt64}, math.MaxInt64, false},
		{BitfieldOp{'i', true, 64, 's', 24, 1}, math.MaxInt64, false},
		{BitfieldOp{'i', true, 64, 'f', 24, 1}, 0, true},
	}
	for i, st := range steps {
		res, nulls := bf(r.s, "f", st.op)
		if nulls[0] != st.null || (!st.null && res[0] != st.res) {
			t.Fatalf("step %d %+v: got (%d, null=%v), want (%d, null=%v)", i, st.op, res[0], nulls[0], st.res, st.null)
		}
	}
	// The i64 field at bit 24 makes the value 11 bytes.
	if n, _, _ := r.s.Strlen(ctx, []byte("f")); n != 11 {
		t.Fatalf("value length %d, want 11", n)
	}

	// Later ops in one call see earlier ops' writes.
	res, nulls := bf(r.s, "seq",
		BitfieldOp{'s', false, 8, 'w', 0, 100},
		BitfieldOp{'i', false, 8, 'w', 0, 28},
		BitfieldOp{'g', false, 8, 'w', 0, 0},
	)
	if nulls[0] || nulls[1] || nulls[2] || res[0] != 0 || res[1] != 128 || res[2] != 128 {
		t.Fatalf("sequential ops: %v %v", res, nulls)
	}

	// GET alone reads zeros and creates nothing.
	res, nulls = bf(r.s, "absent", BitfieldOp{'g', true, 16, 'w', 40, 0})
	if nulls[0] || res[0] != 0 {
		t.Fatalf("GET on missing key: %v %v", res, nulls)
	}
	if _, ok := r.get(r.s, "absent"); ok {
		t.Fatal("BITFIELD GET created the key")
	}
}

// TestStrBitfieldRope runs fields against a rope: a field straddling
// a chunk boundary, growth past the tail through the lazy gap, and
// the cold read of both.
func TestStrBitfieldRope(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	p := pat(16<<10, 5)
	r.set("rope", p)

	// Bits 8188..8195 straddle chunks 0 and 1 (1 KiB chunks): the low
	// nibble of byte 1023 and the high nibble of byte 1024.
	wide := uint64(p[1023])<<8 | uint64(p[1024])
	wantOld := int64(wide >> 4 & 0xff)
	res, nulls, err := r.s.Bitfield(ctx, []byte("rope"), []BitfieldOp{
		{'g', false, 8, 'w', 8188, 0},
		{'s', false, 8, 'w', 8188, 0xA5},
	})
	if err != nil || nulls[0] || nulls[1] || res[0] != wantOld || res[1] != wantOld {
		t.Fatalf("straddling field: %v %v %v, want old %d", res, nulls, err, wantOld)
	}
	p[1023] = p[1023]&0xF0 | 0xA5>>4
	p[1024] = 0x50 | p[1024]&0x0F
	v, err := r.s.Range(ctx, []byte("rope"), 1020, 1027)
	if err != nil || !bytes.Equal(v, p[1020:1028]) {
		t.Fatalf("bytes around the straddle: %x, want %x", v, p[1020:1028])
	}

	// Growth: a field past the tail lands one chunk and moves the
	// length to its last byte; the gap stays lazy.
	r.flush()
	c0 := r.rs.chunkPuts
	if _, _, err := r.s.Bitfield(ctx, []byte("rope"), []BitfieldOp{{'s', false, 8, 'w', 20000 * 8, 7}}); err != nil {
		t.Fatal(err)
	}
	r.flush()
	if got := r.rs.chunkPuts - c0; got != 1 {
		t.Fatalf("growth drained %d chunk records, want 1", got)
	}
	if n, _, _ := r.s.Strlen(ctx, []byte("rope")); n != 20001 {
		t.Fatalf("length after growth %d, want 20001", n)
	}

	s2 := r.reopen()
	res, nulls, err = s2.Bitfield(ctx, []byte("rope"), []BitfieldOp{
		{'g', false, 8, 'w', 8188, 0},
		{'g', false, 8, 'w', 20000 * 8, 0},
		{'g', false, 8, 'w', 18000 * 8, 0},
	})
	if err != nil || nulls[0] || nulls[1] || nulls[2] {
		t.Fatalf("cold reads: %v %v", nulls, err)
	}
	if res[0] != 0xA5 || res[1] != 7 || res[2] != 0 {
		t.Fatalf("cold fields %v, want [165 7 0]", res)
	}
}

// TestServerBitOps pins the wire surface: reply shapes, every error
// text, OVERFLOW switching mid-command, typed indexing, the RO gate,
// and TTL survival across bit writes.
func TestServerBitOps(t *testing.T) {
	do, _ := dispatchServer(t)

	if got := do("SETBIT", "k", "7", "1"); got != ":0\r\n" {
		t.Fatalf("SETBIT: %q", got)
	}
	if got := do("GET", "k"); got != "$1\r\n\x01\r\n" {
		t.Fatalf("GET after SETBIT: %q", got)
	}
	if got := do("SETBIT", "k", "7", "0"); got != ":1\r\n" {
		t.Fatalf("SETBIT old bit: %q", got)
	}
	if got := do("GETBIT", "k", "7"); got != ":0\r\n" {
		t.Fatalf("GETBIT: %q", got)
	}
	if got := do("GETBIT", "k", "100"); got != ":0\r\n" {
		t.Fatalf("GETBIT past end: %q", got)
	}
	if got := do("GETBIT", "nosuch", "0"); got != ":0\r\n" {
		t.Fatalf("GETBIT missing: %q", got)
	}

	// The 512 MiB cap: the last addressable bit works, one past fails.
	if got := do("SETBIT", "big", "4294967295", "1"); got != ":0\r\n" {
		t.Fatalf("SETBIT at cap: %q", got)
	}
	if got := do("STRLEN", "big"); got != ":536870912\r\n" {
		t.Fatalf("STRLEN at cap: %q", got)
	}
	wantOffErr := "-ERR bit offset is not an integer or out of range\r\n"
	for _, bad := range []string{"4294967296", "-1", "01", "x"} {
		if got := do("SETBIT", "k", bad, "1"); got != wantOffErr {
			t.Fatalf("SETBIT offset %q: %q", bad, got)
		}
		if got := do("GETBIT", "k", bad); got != wantOffErr {
			t.Fatalf("GETBIT offset %q: %q", bad, got)
		}
	}
	for _, bad := range []string{"2", "-1", "01", ""} {
		if got := do("SETBIT", "k", "0", bad); got != "-ERR bit is not an integer or out of range\r\n" {
			t.Fatalf("SETBIT bit %q: %q", bad, got)
		}
	}
	if got := do("SETBIT", "k", "0"); got != "-ERR wrong number of arguments for 'setbit' command\r\n" {
		t.Fatalf("SETBIT arity: %q", got)
	}
	if got := do("GETBIT", "k"); got != "-ERR wrong number of arguments for 'getbit' command\r\n" {
		t.Fatalf("GETBIT arity: %q", got)
	}

	// BITFIELD basics, lowercase tokens, typed indexing.
	if got := do("BITFIELD", "bf"); got != "*0\r\n" {
		t.Fatalf("BITFIELD no ops: %q", got)
	}
	if got := do("BITFIELD", "bf", "set", "u8", "0", "255", "get", "u8", "0"); got != "*2\r\n:0\r\n:255\r\n" {
		t.Fatalf("BITFIELD set+get: %q", got)
	}
	if got := do("BITFIELD", "bf", "SET", "u8", "#1", "7", "GET", "u8", "8"); got != "*2\r\n:0\r\n:7\r\n" {
		t.Fatalf("BITFIELD typed index: %q", got)
	}
	if got := do("BITFIELD", "bf", "INCRBY", "u8", "0", "10"); got != "*1\r\n:9\r\n" {
		t.Fatalf("BITFIELD wrap incrby: %q", got)
	}
	if got := do("BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "250", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "1", "GET", "u8", "0"); got != "*3\r\n:255\r\n$-1\r\n:255\r\n" {
		t.Fatalf("BITFIELD overflow switch: %q", got)
	}
	if got := do("BITFIELD", "bf2", "SET", "i8", "0", "-120", "INCRBY", "i8", "0", "-100"); got != "*2\r\n:0\r\n:36\r\n" {
		t.Fatalf("BITFIELD signed wrap: %q", got)
	}

	// Error surface.
	if got := do("BITFIELD", "bf", "GET", "u64", "0"); got != "-"+bitfieldTypeErr+"\r\n" {
		t.Fatalf("BITFIELD u64: %q", got)
	}
	for _, bad := range []string{"i0", "i65", "u0", "w8", "i", "i08"} {
		if got := do("BITFIELD", "bf", "GET", bad, "0"); got != "-"+bitfieldTypeErr+"\r\n" {
			t.Fatalf("BITFIELD type %q: %q", bad, got)
		}
	}
	if got := do("BITFIELD", "bf", "GET", "u8", "-1"); got != wantOffErr {
		t.Fatalf("BITFIELD negative offset: %q", got)
	}
	if got := do("BITFIELD", "bf", "SET", "u8", "4294967289", "1"); got != wantOffErr {
		t.Fatalf("BITFIELD offset past cap: %q", got)
	}
	if got := do("BITFIELD", "bf", "OVERFLOW", "BANANA", "GET", "u8", "0"); got != "-ERR Invalid OVERFLOW type specified\r\n" {
		t.Fatalf("BITFIELD overflow arg: %q", got)
	}
	if got := do("BITFIELD", "bf", "SET", "u8", "0", "abc"); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("BITFIELD bad value: %q", got)
	}
	for _, bad := range [][]string{
		{"BITFIELD", "bf", "SET", "u8", "0"},
		{"BITFIELD", "bf", "GET", "u8"},
		{"BITFIELD", "bf", "OVERFLOW"},
		{"BITFIELD", "bf", "BANANA"},
	} {
		if got := do(bad...); got != "-ERR syntax error\r\n" {
			t.Fatalf("%v: %q", bad, got)
		}
	}
	// A parse error anywhere means nothing executed.
	if got := do("BITFIELD", "bf3", "SET", "u8", "0", "9", "GET", "u99", "0"); got != "-"+bitfieldTypeErr+"\r\n" {
		t.Fatalf("BITFIELD mixed parse error: %q", got)
	}
	if got := do("GET", "bf3"); got != "$-1\r\n" {
		t.Fatalf("BITFIELD wrote before parse error: %q", got)
	}

	// BITFIELD_RO reads, and only reads.
	if got := do("BITFIELD_RO", "bf", "GET", "u8", "0"); got != "*1\r\n:255\r\n" {
		t.Fatalf("BITFIELD_RO get: %q", got)
	}
	wantRO := "-ERR BITFIELD_RO only supports the GET subcommand\r\n"
	if got := do("BITFIELD_RO", "bf", "SET", "u8", "0", "1"); got != wantRO {
		t.Fatalf("BITFIELD_RO set: %q", got)
	}
	if got := do("BITFIELD_RO", "bf", "OVERFLOW", "SAT", "GET", "u8", "0"); got != wantRO {
		t.Fatalf("BITFIELD_RO overflow: %q", got)
	}

	// Bit writes preserve TTLs like every non-SET write.
	do("SET", "t", "x", "EX", "100")
	do("SETBIT", "t", "3", "1")
	do("BITFIELD", "t", "SET", "u8", "8", "5")
	if got := do("TTL", "t"); got != ":100\r\n" {
		t.Fatalf("TTL after bit writes: %q", got)
	}
}
