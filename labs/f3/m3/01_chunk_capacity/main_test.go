package main

import (
	"math/rand"
	"testing"
)

// model is a shadow list of value lengths, the ground truth the chunked deque is
// checked against. Every value here is distinct-length-agnostic: the lab prices
// bytes and positions, so the model tracks length per position, which is all the
// deque stores structurally beyond the payload.
type model struct{ v []int }

func (m *model) rpush(n int)     { m.v = append(m.v, n) }
func (m *model) lpop() (int, bool) {
	if len(m.v) == 0 {
		return 0, false
	}
	x := m.v[0]
	m.v = m.v[1:]
	return x, true
}
func (m *model) insert(k, n int) {
	if k >= len(m.v) {
		m.v = append(m.v, n)
		return
	}
	m.v = append(m.v, 0)
	copy(m.v[k+1:], m.v[k:])
	m.v[k] = n
}

// checkInvariants walks the deque and asserts the section 2.8 invariants a
// chunked-deque slice must encode as tests: count equals the sum of live chunk
// counts, the flat directory agrees, firstLive is within range, directory
// offsets are strictly increasing across live frames, and every position maps to
// exactly one (chunk, ordinal).
func (l *listNative) checkInvariants(tb testing.TB) {
	var sum int
	for i := l.headChunk; i < len(l.chunks); i++ {
		c := l.chunks[i]
		live := c.live()
		if live != l.counts[i] {
			tb.Fatalf("chunk %d: live %d != counts %d", i, live, l.counts[i])
		}
		if live < 0 {
			tb.Fatalf("chunk %d: negative live %d", i, live)
		}
		if c.firstLive < 0 || c.firstLive > len(c.dir) {
			tb.Fatalf("chunk %d: firstLive %d out of [0,%d]", i, c.firstLive, len(c.dir))
		}
		for j := 1; j < len(c.dir); j++ {
			if c.dir[j] <= c.dir[j-1] {
				tb.Fatalf("chunk %d: dir not strictly increasing at %d", i, j)
			}
		}
		if i > l.headChunk && live == 0 {
			tb.Fatalf("chunk %d: interior chunk drained to empty, should be recycled", i)
		}
		sum += live
	}
	if sum != l.count {
		tb.Fatalf("sum of live counts %d != count %d", sum, l.count)
	}
}

// TestAgainstModel drives rpush/lpop/insert against the shadow model across all
// geometry arms, checking the invariants and the position mapping after every
// batch so a budget-specific split or recycle bug surfaces.
func TestAgainstModel(t *testing.T) {
	arms := []geoArm{
		{"512/128", 512, 128},
		{"1024/128", 1024, 128},
		{"4096/128", 4096, 128},
		{"4096/64", 4096, 64},
		{"8192/128", 8192, 128},
	}
	for _, a := range arms {
		t.Run(a.name, func(t *testing.T) {
			l := newList(a.capBytes, a.elemCap, 0)
			m := &model{}
			rng := rand.New(rand.NewSource(int64(a.capBytes*131 + a.elemCap)))

			check := func() {
				l.checkInvariants(t)
				if l.count != len(m.v) {
					t.Fatalf("count %d != model %d", l.count, len(m.v))
				}
				// position mapping: every external index resolves and lands on a
				// live slot in the right chunk.
				for probe := 0; probe < 100 && l.count > 0; probe++ {
					k := rng.Intn(l.count)
					ci, ord := l.locate(k)
					if ci < 0 || ord < 0 || ord >= l.counts[ci] {
						t.Fatalf("locate(%d) = (%d,%d) invalid", k, ci, ord)
					}
				}
			}

			// grow by RPUSH with a spread of value sizes across the byte budget.
			for i := 0; i < 4000; i++ {
				vl := 1 + rng.Intn(a.capBytes/2)
				l.rpush(vl)
				m.rpush(vl)
				if i%400 == 0 {
					check()
				}
			}
			check()

			// interior inserts, which drive splits at every budget.
			for i := 0; i < 2000; i++ {
				k := rng.Intn(l.count)
				vl := 1 + rng.Intn(a.capBytes/2)
				l.insert(k, vl)
				m.insert(k, vl)
				if i%200 == 0 {
					check()
				}
			}
			check()

			// churn: mixed pop and push.
			for i := 0; i < 6000; i++ {
				if rng.Intn(2) == 0 && l.count > 0 {
					l.lpop()
					m.lpop()
				} else {
					vl := 1 + rng.Intn(a.capBytes/2)
					l.rpush(vl)
					m.rpush(vl)
				}
				if i%400 == 0 {
					check()
				}
			}
			check()

			// drain to empty; the last pop must leave a valid empty structure.
			for l.count > 0 {
				l.lpop()
				m.lpop()
			}
			check()
			if l.count != 0 {
				t.Fatalf("not drained: %d left", l.count)
			}
		})
	}
}

// TestReferenceBand checks the element-inline cap plumbing: with a reference
// threshold set, oversized values occupy a fixed reference frame in the chunk and
// their bytes are booked to the value log, so a chunk of big values still packs
// many frames instead of one.
func TestReferenceBand(t *testing.T) {
	// inline: a 2KiB value in a 4KiB chunk packs one per chunk.
	inl := buildSeq(4096, 128, 0, 200, 2048)
	if got := inl.elemsPerChunk(); got >= 2 {
		t.Fatalf("inline 2KiB in 4KiB: elems/chunk %.2f, expected < 2", got)
	}
	// reference: the same value as a 16B reference packs many per chunk and books
	// its bytes to the log.
	ref := buildSeq(4096, 128, 1, 200, 2048)
	if got := ref.elemsPerChunk(); got < 20 {
		t.Fatalf("reference 2KiB: elems/chunk %.2f, expected many", got)
	}
	if ref.logByteTotal() == 0 {
		t.Fatalf("reference band booked no value-log bytes")
	}
}

// TestElemCapBinds checks the element cap binds before the byte budget when
// values are tiny: a 4KiB chunk of 1-byte values seals at 128 elements, not at
// the ~1300 the byte budget alone would allow, keeping the u16 directory bounded.
func TestElemCapBinds(t *testing.T) {
	l := buildSeq(4096, 128, 0, 1000, 1)
	for i, c := range l.chunks {
		if i == len(l.chunks)-1 {
			break // tail may be partial
		}
		if len(c.dir) > 128 {
			t.Fatalf("chunk %d holds %d elements, over the 128 cap", i, len(c.dir))
		}
	}
	// with 1B values the element cap must be the binding limit, so full chunks
	// hold exactly 128.
	if len(l.chunks) > 1 && len(l.chunks[0].dir) != 128 {
		t.Fatalf("first chunk holds %d, expected the 128 element cap to bind", len(l.chunks[0].dir))
	}
}

// Benchmarks: go test -bench . exercises the edge push, edge pop, and interior
// insert per geometry arm, the three cost shapes the sweep scores.

var benchArms = []geoArm{
	{"512/128", 512, 128},
	{"1024/128", 1024, 128},
	{"2048/128", 2048, 128},
	{"4096/128", 4096, 128},
	{"8192/128", 8192, 128},
}

func BenchmarkRPush(b *testing.B) {
	for _, vb := range []int{64, 1024} {
		for _, a := range benchArms {
			b.Run(a.name+"/v"+itoa(vb), func(b *testing.B) {
				l := newList(a.capBytes, a.elemCap, 0)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					l.rpush(vb)
				}
			})
		}
	}
}

func BenchmarkLPop(b *testing.B) {
	for _, vb := range []int{64, 1024} {
		for _, a := range benchArms {
			b.Run(a.name+"/v"+itoa(vb), func(b *testing.B) {
				l := buildSeq(a.capBytes, a.elemCap, 0, b.N+1, vb)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					l.lpop()
				}
			})
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	for _, vb := range []int{64, 1024} {
		for _, a := range benchArms {
			b.Run(a.name+"/v"+itoa(vb), func(b *testing.B) {
				l := buildSeq(a.capBytes, a.elemCap, 0, 100_000, vb)
				r := uint64(0x1234 ^ uint64(a.capBytes))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r = xorshift(r)
					l.insert(int(r%uint64(l.count)), vb)
				}
			})
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
