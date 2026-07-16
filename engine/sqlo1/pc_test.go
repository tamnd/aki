package sqlo1

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"
)

// Oracle helpers, deliberately bit-at-a-time and independent of the
// popcount and window code they check.

func oracleBitCount(v []byte, lo, hi int64) int64 {
	var n int64
	for i := lo; i <= hi; i++ {
		if v[i>>3]&(1<<(7-uint(i)&7)) != 0 {
			n++
		}
	}
	return n
}

func oracleBitPos(v []byte, bit int, lo, hi int64) int64 {
	for i := lo; i <= hi; i++ {
		b := 0
		if v[i>>3]&(1<<(7-uint(i)&7)) != 0 {
			b = 1
		}
		if b == bit {
			return i
		}
	}
	return -1
}

// oracleWindow resolves a range the way Redis documents it, written
// out separately from bitRange.resolve.
func oracleWindow(start, end, byteLen int64, bitUnit bool) (lo, hi int64, ok bool) {
	d := byteLen
	if bitUnit {
		d = byteLen * 8
	}
	if start < 0 {
		start = max(d+start, 0)
	}
	if end < 0 {
		end = d + end
	}
	end = min(end, d-1)
	if d == 0 || start > end {
		return 0, 0, false
	}
	if bitUnit {
		return start, end, true
	}
	return start * 8, end*8 + 7, true
}

func (r *strRig) bitcount(s *Str, key string, br bitRange) int64 {
	r.t.Helper()
	n, err := s.BitCount(context.Background(), []byte(key), br)
	if err != nil {
		r.t.Fatalf("BitCount(%q, %+v): %v", key, br, err)
	}
	return n
}

func (r *strRig) bitpos(s *Str, key string, bit int, br bitRange) int64 {
	r.t.Helper()
	p, err := s.BitPos(context.Background(), []byte(key), bit, br)
	if err != nil {
		r.t.Fatalf("BitPos(%q, %d, %+v): %v", key, bit, br, err)
	}
	return p
}

// checkPC recomputes every chunk's popcount from the drained store
// and compares it to the drained cache segments, the direct statement
// that write-time maintenance kept the cache exact. Call it only
// after a flush.
func (r *strRig) checkPC(key string) {
	r.t.Helper()
	ctx := context.Background()
	root, ok := r.storedRoot(key)
	if !ok {
		r.t.Fatalf("checkPC(%q): no rope root in the store", key)
	}
	if root.pcSegCount == 0 {
		r.t.Fatalf("checkPC(%q): rope has no popcount cache", key)
	}
	var kb [SubkeySize]byte
	for c := range root.chunkCount {
		var cnt uint32
		putChunkKey(kb[:], root.rooth, c)
		if rec, err := r.rs.MemStore.Get(ctx, kb[:]); err == nil {
			for _, b := range rec.Value {
				for m := byte(0x80); m != 0; m >>= 1 {
					if b&m != 0 {
						cnt++
					}
				}
			}
		}
		var entry uint32
		putPCKey(kb[:], root.rooth, c/pcChunksPerSeg)
		if rec, err := r.rs.MemStore.Get(ctx, kb[:]); err == nil {
			idx := int(c%pcChunksPerSeg) * 4
			if idx+4 <= len(rec.Value) {
				entry = uint32(rec.Value[idx]) | uint32(rec.Value[idx+1])<<8 |
					uint32(rec.Value[idx+2])<<16 | uint32(rec.Value[idx+3])<<24
			}
		}
		if entry != cnt {
			r.t.Fatalf("checkPC(%q): chunk %d stores %d set bits, cache entry says %d", key, c, cnt, entry)
		}
	}
}

// TestStrPopcountExactness is the slice's property test: random bit
// and range writes against a byte oracle, with BITCOUNT and BITPOS
// queried while the touched chunks are still hot and dirty, after
// flushes, and cold. The cache is installed up front and every write
// afterwards must keep it exact through growth and boundary chunks.
func TestStrPopcountExactness(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(11, 17))
	oracle := append([]byte(nil), pat(20<<10, 5)...)
	r.set("prop", oracle)

	check := func(s *Str, tag string) {
		n := int64(len(oracle))
		if got, want := r.bitcount(s, "prop", bitRange{}), oracleBitCount(oracle, 0, n*8-1); got != want {
			t.Fatalf("%s: whole BITCOUNT %d, oracle %d", tag, got, want)
		}
		for q := range 4 {
			bitUnit := q%2 == 0
			d := n
			if bitUnit {
				d = n * 8
			}
			st, en := rng.Int64N(3*d)-d, rng.Int64N(3*d)-d
			br := bitRange{start: st, end: en, ranged: true, endGiven: true, bitUnit: bitUnit}
			var want int64
			if lo, hi, ok := oracleWindow(st, en, n, bitUnit); ok {
				want = oracleBitCount(oracle, lo, hi)
			}
			if got := r.bitcount(s, "prop", br); got != want {
				t.Fatalf("%s: BITCOUNT %d %d bit=%v got %d, oracle %d", tag, st, en, bitUnit, got, want)
			}
			bit := rng.IntN(2)
			wantPos := int64(-1)
			if lo, hi, ok := oracleWindow(st, en, n, bitUnit); ok {
				wantPos = oracleBitPos(oracle, bit, lo, hi)
			}
			if got := r.bitpos(s, "prop", bit, br); got != wantPos {
				t.Fatalf("%s: BITPOS %d %d %d bit-unit=%v got %d, oracle %d", tag, bit, st, en, bitUnit, got, wantPos)
			}
		}
		// The start-only shape, where an exhausted clear-bit scan
		// answers just past the value.
		st := rng.Int64N(2*n) - n
		br := bitRange{start: st, end: -1, ranged: true}
		for _, bit := range []int{0, 1} {
			wantPos := int64(-1)
			if lo, hi, ok := oracleWindow(st, -1, n, false); ok {
				wantPos = oracleBitPos(oracle, bit, lo, hi)
				if wantPos == -1 && bit == 0 {
					wantPos = n * 8
				}
			}
			if got := r.bitpos(s, "prop", bit, br); got != wantPos {
				t.Fatalf("%s: BITPOS %d %d got %d, oracle %d", tag, bit, st, got, wantPos)
			}
		}
	}

	// The first whole-value query inside this check installs the cache;
	// everything after holds with it in place.
	check(r.s, "initial")

	for i := range 48 {
		switch rng.IntN(6) {
		case 0, 1:
			off := rng.Int64N(int64(len(oracle))*8 + 512)
			bit := rng.IntN(2)
			if _, err := r.s.SetBit(ctx, []byte("prop"), off, bit); err != nil {
				t.Fatalf("op %d SetBit(%d, %d): %v", i, off, bit, err)
			}
			if b := int(off >> 3); b >= len(oracle) {
				oracle = append(oracle, make([]byte, b+1-len(oracle))...)
			}
			m := byte(1) << (7 - uint(off)&7)
			if bit == 1 {
				oracle[off>>3] |= m
			} else {
				oracle[off>>3] &^= m
			}
		case 2, 3:
			off := rng.Int64N(int64(len(oracle)) + 3000)
			p := pat(1+rng.IntN(2500), byte(i*13+1))
			if _, err := r.s.SetRange(ctx, []byte("prop"), off, p); err != nil {
				t.Fatalf("op %d SetRange(%d, %d bytes): %v", i, off, len(p), err)
			}
			if end := int(off) + len(p); end > len(oracle) {
				oracle = append(oracle, make([]byte, end-len(oracle))...)
			}
			copy(oracle[off:], p)
		case 4:
			p := pat(1+rng.IntN(1500), byte(i*7+3))
			if _, err := r.s.Append(ctx, []byte("prop"), p); err != nil {
				t.Fatalf("op %d Append(%d bytes): %v", i, len(p), err)
			}
			oracle = append(oracle, p...)
		case 5:
			r.flush()
			check(r.reopen(), "cold")
		}
		check(r.s, "hot")
	}
	r.flush()
	r.checkPC("prop")
	check(r.reopen(), "final cold")
}

// TestStrPopcountReadCost pins S-I3 at the store: once the cache is
// drained, a cold whole-value BITCOUNT reads segments and no chunks,
// a ranged one reads only its edge chunks, and a cold BITPOS reads
// exactly the one mixed chunk the cache could not skip.
//
// Write ordering matters here: a whole-value query on an uncached
// rope WRITES (it installs the cache), and the store's replay guard
// drops batches whose sequence fell behind, so every write moves
// forward through the newest reopened tier and the original rig tier
// is never written through again after s2 flushes.
func TestStrPopcountReadCost(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	v := pat(32<<10, 6)
	whole := oracleBitCount(v, 0, int64(len(v))*8-1)
	sparse := make([]byte, 32<<10)
	sparse[20<<10+5] = 0x04
	r.set("rc", v)
	r.set("sparse", sparse)
	r.flush()

	// First cold whole-value counts: the build pass reads every chunk
	// once and installs each cache.
	s2 := r.reopen()
	c0, p0 := r.rs.chunkReads, r.rs.pcReads
	if got := r.bitcount(s2, "rc", bitRange{}); got != whole {
		t.Fatalf("building BITCOUNT %d, oracle %d", got, whole)
	}
	if d := r.rs.chunkReads - c0; d != 32 {
		t.Fatalf("cache build read %d chunks cold, want 32", d)
	}
	if got, want := r.bitcount(s2, "sparse", bitRange{}), int64(1); got != want {
		t.Fatalf("sparse build BITCOUNT %d, want %d", got, want)
	}
	if r.rs.pcReads != p0 {
		t.Fatalf("cache builds read %d pc segments cold, want 0", r.rs.pcReads-p0)
	}
	if err := s2.t.Flush(ctx); err != nil {
		t.Fatalf("flush after builds: %v", err)
	}
	for _, k := range []string{"rc", "sparse"} {
		if root, ok := r.storedRoot(k); !ok || root.pcSegCount != 1 {
			t.Fatalf("drained %s root claims %d cache segments, want 1", k, root.pcSegCount)
		}
	}

	// Second cold reader: the whole count comes from one segment.
	s3 := r.reopen()
	c0, p0 = r.rs.chunkReads, r.rs.pcReads
	if got := r.bitcount(s3, "rc", bitRange{}); got != whole {
		t.Fatalf("cached BITCOUNT %d, oracle %d", got, whole)
	}
	if d := r.rs.chunkReads - c0; d != 0 {
		t.Fatalf("cached whole BITCOUNT read %d chunks, want 0", d)
	}
	if d := r.rs.pcReads - p0; d != 1 {
		t.Fatalf("cached whole BITCOUNT read %d pc segments, want 1", d)
	}

	// A byte range with partial edges in chunks 1 and 2 reads exactly
	// those two; the segment round is already hot on s3.
	c0 = r.rs.chunkReads
	br := bitRange{start: 1500, end: 2600, ranged: true, endGiven: true}
	if got, want := r.bitcount(s3, "rc", br), oracleBitCount(v, 1500*8, 2600*8+7); got != want {
		t.Fatalf("ranged BITCOUNT %d, oracle %d", got, want)
	}
	if d := r.rs.chunkReads - c0; d != 2 {
		t.Fatalf("ranged BITCOUNT read %d chunks cold, want the 2 edges", d)
	}

	// BITPOS over the one-bit bitmap: zero chunks skip via the cache
	// and only the mixed chunk is read.
	c0, p0 = r.rs.chunkReads, r.rs.pcReads
	wantPos := int64(20<<10+5)*8 + 5
	if got := r.bitpos(s3, "sparse", 1, bitRange{}); got != wantPos {
		t.Fatalf("sparse BITPOS %d, want %d", got, wantPos)
	}
	if d := r.rs.chunkReads - c0; d != 1 {
		t.Fatalf("sparse BITPOS read %d chunks cold, want 1", d)
	}
	if d := r.rs.pcReads - p0; d != 1 {
		t.Fatalf("sparse BITPOS read %d pc segments cold, want 1", d)
	}
	// All-zero scan for a clear bit answers at the window start with
	// no chunk reads at all.
	c0 = r.rs.chunkReads
	if got := r.bitpos(s3, "sparse", 0, bitRange{}); got != 0 {
		t.Fatalf("sparse BITPOS 0 = %d, want 0", got)
	}
	if d := r.rs.chunkReads - c0; d != 0 {
		t.Fatalf("clear-bit BITPOS read %d chunks, want 0", d)
	}
}

// TestStrPopcountGrowth walks the cache across segment boundaries:
// appends and far writes extend pc_seg_count with the chunk count,
// cold counts stay chunk-free, and a full rewrite drops the cache
// with its plane.
func TestStrPopcountGrowth(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	oracle := append([]byte(nil), pat(1040<<10, 9)...)
	r.set("big", oracle)
	if got, want := r.bitcount(r.s, "big", bitRange{}), oracleBitCount(oracle, 0, int64(len(oracle))*8-1); got != want {
		t.Fatalf("build BITCOUNT %d, oracle %d", got, want)
	}
	r.flush()
	if root, ok := r.storedRoot("big"); !ok || root.pcSegCount != 2 {
		t.Fatalf("1040-chunk rope claims %d cache segments, want 2", root.pcSegCount)
	}
	r.checkPC("big")

	// Append across the next segment boundary.
	app := pat(1010<<10, 3)
	if _, err := r.s.Append(ctx, []byte("big"), app); err != nil {
		t.Fatalf("Append: %v", err)
	}
	oracle = append(oracle, app...)
	if got, want := r.bitcount(r.s, "big", bitRange{}), oracleBitCount(oracle, 0, int64(len(oracle))*8-1); got != want {
		t.Fatalf("post-append BITCOUNT %d, oracle %d", got, want)
	}
	r.flush()
	if root, ok := r.storedRoot("big"); !ok || root.pcSegCount != 3 {
		t.Fatalf("2050-chunk rope claims %d cache segments, want 3", root.pcSegCount)
	}
	r.checkPC("big")

	// A far SETBIT grows lazily; the gap chunks never exist and their
	// entries stay zero.
	farBit := int64(3<<20)*8 + 9
	if _, err := r.s.SetBit(ctx, []byte("big"), farBit, 1); err != nil {
		t.Fatalf("far SetBit: %v", err)
	}
	oracle = append(oracle, make([]byte, int(farBit>>3)+1-len(oracle))...)
	oracle[farBit>>3] |= 1 << (7 - uint(farBit)&7)
	if got, want := r.bitcount(r.s, "big", bitRange{}), oracleBitCount(oracle, 0, int64(len(oracle))*8-1); got != want {
		t.Fatalf("post-growth BITCOUNT %d, oracle %d", got, want)
	}
	r.flush()
	if root, ok := r.storedRoot("big"); !ok || root.pcSegCount != 4 {
		t.Fatalf("3073-chunk rope claims %d cache segments, want 4", root.pcSegCount)
	}
	r.checkPC("big")

	// Cold, the whole count is four segment reads and no chunks, and
	// BITPOS across the lazy gap lands on the far bit.
	s2 := r.reopen()
	c0, p0 := r.rs.chunkReads, r.rs.pcReads
	if got, want := r.bitcount(s2, "big", bitRange{}), oracleBitCount(oracle, 0, int64(len(oracle))*8-1); got != want {
		t.Fatalf("cold BITCOUNT %d, oracle %d", got, want)
	}
	if d := r.rs.chunkReads - c0; d != 0 {
		t.Fatalf("cold cached BITCOUNT read %d chunks, want 0", d)
	}
	if d := r.rs.pcReads - p0; d != 4 {
		t.Fatalf("cold cached BITCOUNT read %d pc segments, want 4", d)
	}
	gapStart := int64(2050<<10) * 8
	if got := r.bitpos(s2, "big", 1, bitRange{start: gapStart, end: -1, ranged: true, bitUnit: true, endGiven: false}); got != farBit {
		t.Fatalf("BITPOS across the gap %d, want %d", got, farBit)
	}

	// A full rewrite mints a fresh plane with no cache.
	r.set("big", pat(9<<10, 1))
	r.flush()
	if root, ok := r.storedRoot("big"); !ok || root.pcSegCount != 0 {
		t.Fatalf("rewritten rope claims %d cache segments, want 0", root.pcSegCount)
	}
}

// TestServerBitCountPos pins the wire surface to Redis's documented
// replies and error texts, including the BIT range form and BITPOS's
// implicit-end rule for clear bits.
func TestServerBitCountPos(t *testing.T) {
	do, _ := dispatchServer(t)
	if got := do("SET", "mykey", "foobar"); got != "+OK\r\n" {
		t.Fatalf("SET: %q", got)
	}
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"BITCOUNT", "mykey"}, ":26\r\n"},
		{[]string{"BITCOUNT", "mykey", "0", "0"}, ":4\r\n"},
		{[]string{"BITCOUNT", "mykey", "1", "1"}, ":6\r\n"},
		{[]string{"BITCOUNT", "mykey", "1", "1", "BYTE"}, ":6\r\n"},
		{[]string{"BITCOUNT", "mykey", "5", "30", "BIT"}, ":17\r\n"},
		{[]string{"BITCOUNT", "mykey", "5", "30", "bit"}, ":17\r\n"},
		{[]string{"BITCOUNT", "mykey", "0", "-5"}, ":10\r\n"},
		{[]string{"BITCOUNT", "mykey", "0", "-5", "BIT"}, ":25\r\n"},
		{[]string{"BITCOUNT", "mykey", "26", "30"}, ":0\r\n"},
		{[]string{"BITCOUNT", "nosuch"}, ":0\r\n"},
		{[]string{"BITCOUNT", "mykey", "1"}, "-ERR syntax error\r\n"},
		{[]string{"BITCOUNT", "mykey", "0", "0", "BIT", "x"}, "-ERR syntax error\r\n"},
		{[]string{"BITCOUNT", "mykey", "0", "0", "BANANA"}, "-ERR syntax error\r\n"},
		{[]string{"BITCOUNT", "mykey", "a", "0"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"BITCOUNT", "mykey", "0", "01"}, "-ERR value is not an integer or out of range\r\n"},
	} {
		if got := do(c.args...); got != c.want {
			t.Fatalf("%v: %q, want %q", c.args, got, c.want)
		}
	}

	do("SET", "k1", "\xff\xf0\x00")
	do("SET", "k2", "\x00\xff\xf0")
	do("SET", "k3", "\x00\x00\x00")
	do("SET", "k4", "\xff\xff\xff")
	do("SET", "k5", "")
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"BITPOS", "k1", "0"}, ":12\r\n"},
		{[]string{"BITPOS", "k2", "1", "0"}, ":8\r\n"},
		{[]string{"BITPOS", "k2", "1", "2"}, ":16\r\n"},
		{[]string{"BITPOS", "k2", "1", "0", "-1", "BIT"}, ":8\r\n"},
		{[]string{"BITPOS", "k2", "1", "-12", "-1", "BIT"}, ":12\r\n"},
		{[]string{"BITPOS", "k3", "1"}, ":-1\r\n"},
		{[]string{"BITPOS", "k3", "0"}, ":0\r\n"},
		{[]string{"BITPOS", "k4", "0"}, ":24\r\n"},
		{[]string{"BITPOS", "k4", "0", "2"}, ":24\r\n"},
		{[]string{"BITPOS", "k4", "0", "0", "-1"}, ":-1\r\n"},
		{[]string{"BITPOS", "k4", "0", "0", "-1", "BIT"}, ":-1\r\n"},
		{[]string{"BITPOS", "k5", "0"}, ":-1\r\n"},
		{[]string{"BITPOS", "k5", "1"}, ":-1\r\n"},
		{[]string{"BITPOS", "nosuch", "0"}, ":0\r\n"},
		{[]string{"BITPOS", "nosuch", "1"}, ":-1\r\n"},
		{[]string{"BITPOS", "k1", "2"}, "-ERR The bit argument must be 1 or 0.\r\n"},
		{[]string{"BITPOS", "k1", "x"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"BITPOS", "k1", "0", "a"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"BITPOS", "k1", "0", "0", "b"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"BITPOS", "k1", "0", "0", "0", "JUNK"}, "-ERR syntax error\r\n"},
		{[]string{"BITPOS", "k1", "0", "0", "0", "BIT", "BIT"}, "-ERR syntax error\r\n"},
	} {
		if got := do(c.args...); got != c.want {
			t.Fatalf("%v: %q, want %q", c.args, got, c.want)
		}
	}

	// The cache build restamps: a rope key keeps its TTL through the
	// whole-value BITCOUNT that installs the cache.
	big := pat(9<<10, 8)
	if got := do("SET", "rope", string(big)); got != "+OK\r\n" {
		t.Fatalf("SET rope: %q", got)
	}
	if got := do("EXPIRE", "rope", "100"); got != ":1\r\n" {
		t.Fatalf("EXPIRE rope: %q", got)
	}
	want := oracleBitCount(big, 0, int64(len(big))*8-1)
	if got := do("BITCOUNT", "rope"); got != ":"+strconv.FormatInt(want, 10)+"\r\n" {
		t.Fatalf("BITCOUNT rope: %q", got)
	}
	if got := do("TTL", "rope"); got != ":100\r\n" {
		t.Fatalf("TTL after cache build: %q", got)
	}
}
