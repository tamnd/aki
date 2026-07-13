package main

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// refBitOp is the plain byte oracle: the result is as long as the longest source, shorter
// sources zero-pad, and NOT is the single-source complement at that source's length.
func refBitOp(op int, srcs [][]byte) []byte {
	if op == bitNot {
		out := make([]byte, len(srcs[0]))
		for i := range out {
			out[i] = ^srcs[0][i]
		}
		return out
	}
	ml := maxLen(srcs)
	if ml == 0 {
		return nil
	}
	out := make([]byte, ml)
	for i := 0; i < ml; i++ {
		var acc byte
		if op == bitAnd {
			acc = 0xFF
		}
		for _, s := range srcs {
			var v byte
			if i < len(s) {
				v = s[i]
			}
			switch op {
			case bitAnd:
				acc &= v
			case bitOr:
				acc |= v
			case bitXor:
				acc ^= v
			}
		}
		out[i] = acc
	}
	return out
}

// gatherStreaming reassembles the streaming form's chunk writes into one buffer so its
// answer can be compared. The assembly buffer is the test's own, not the engine residency
// the lab prices.
func gatherStreaming(op int, srcs [][]byte) []byte {
	ml := maxLen(srcs)
	if ml == 0 {
		return nil
	}
	res := make([]byte, ml)
	applyStreaming(op, srcs, func(off int, b []byte) { copy(res[off:], b) })
	return res
}

// TestFormsAgree pins that the streaming form, the materialize form, and the byte oracle all
// agree bit for bit over every op and a spread of lengths that straddle the chunk boundary,
// so the memory numbers compare one answer.
func TestFormsAgree(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x5170, 0x0b17))
	lens := []int{0, 1, 7, chunkSize - 1, chunkSize, chunkSize + 1, 3*chunkSize + 123}
	ops := []int{bitAnd, bitOr, bitXor, bitNot}
	for _, op := range ops {
		for iter := 0; iter < 60; iter++ {
			nsrc := 1 + rng.IntN(3)
			if op == bitNot {
				nsrc = 1
			}
			srcs := make([][]byte, nsrc)
			for i := range srcs {
				b := make([]byte, lens[rng.IntN(len(lens))])
				for j := range b {
					b[j] = byte(rng.Uint32())
				}
				srcs[i] = b
			}
			want := refBitOp(op, srcs)
			if got := gatherStreaming(op, srcs); !bytes.Equal(got, want) {
				t.Fatalf("op %d iter %d streaming mismatch: %d vs %d bytes", op, iter, len(got), len(want))
			}
			if got := applyMaterialize(op, srcs); !bytes.Equal(got, want) {
				t.Fatalf("op %d iter %d materialize mismatch: %d vs %d bytes", op, iter, len(got), len(want))
			}
		}
	}
}

func benchOp(b *testing.B, apply func(int, [][]byte)) {
	const n = 4 << 20
	srcs := make([][]byte, 3)
	for i := range srcs {
		buf := make([]byte, n)
		for j := range buf {
			buf[j] = byte(j*7 + i)
		}
		srcs[i] = buf
	}
	b.SetBytes(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		apply(bitAnd, srcs)
	}
}

func BenchmarkStreaming(b *testing.B) {
	var sink byte
	benchOp(b, func(op int, srcs [][]byte) {
		applyStreaming(op, srcs, func(_ int, c []byte) { sink ^= c[0] })
	})
	_ = sink
}

func BenchmarkMaterialize(b *testing.B) {
	var sink byte
	benchOp(b, func(op int, srcs [][]byte) {
		sink ^= applyMaterialize(op, srcs)[0]
	})
	_ = sink
}
