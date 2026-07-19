package zset

import (
	"math"
	"testing"
)

func TestInlineScoreCodec(t *testing.T) {
	cases := []float64{
		0, 1, -1, 7, 127, -128, 128, -129, 32767, -32768, 32768, -32769,
		2147483647, -2147483648, 2147483648, -2147483649,
		1.5, -0.25, 3.14159, math.MaxFloat64, -math.MaxFloat64,
		math.Inf(1), math.Inf(-1), 9007199254740992,
	}
	widths := map[float64]int{0: 2, 1: 2, 127: 2, -128: 2, 128: 3, 32767: 3, 32768: 5, 2147483647: 5, 2147483648: 9, 1.5: 9, math.Inf(1): 9}
	for _, s := range cases {
		var b [9]byte
		w := putScore(b[:], s)
		if ew := encScoreWidth(s); ew != w {
			t.Fatalf("score %v: encScoreWidth=%d putScore=%d", s, ew, w)
		}
		if sw := scoreWidthAt(b[:], 0); sw != w {
			t.Fatalf("score %v: scoreWidthAt=%d putScore=%d", s, sw, w)
		}
		got, rw := readScore(b[:])
		if rw != w {
			t.Fatalf("score %v: readScore width=%d putScore=%d", s, rw, w)
		}
		if got != s {
			t.Fatalf("score %v round-trip got %v", s, got)
		}
		if want, ok := widths[s]; ok && w != want {
			t.Fatalf("score %v width %d want %d", s, w, want)
		}
	}
}
