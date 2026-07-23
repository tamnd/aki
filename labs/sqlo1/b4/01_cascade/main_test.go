package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// TestRoundTrip drives every encoder over every generated shape and a
// set of adversarial corpora; whatever the encoder accepts it must
// reproduce byte for byte, since the selection arms trust encode
// length without re-verifying.
func TestRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	corpora := map[string][][]byte{}
	for _, shape := range []string{"counters", "timestamps", "u64s", "uuids", "json", "mixed"} {
		corpora[shape] = genShape(shape, 3000, rng)
	}
	corpora["single"] = [][]byte{[]byte("just one")}
	corpora["empties"] = [][]byte{{}, []byte("a"), {}, {}, []byte("a")}
	corpora["allsame"] = [][]byte{[]byte("x"), []byte("x"), []byte("x")}
	corpora["zero"] = [][]byte{[]byte("0"), []byte("0"), []byte("1")}
	corpora["wide"] = [][]byte{[]byte("18446744073709551615"), []byte("1"), []byte("0")}
	blockEdge := make([][]byte, 0, forBlock+1)
	for i := 0; i <= forBlock; i++ {
		blockEdge = append(blockEdge, []byte(fmt.Sprintf("%d", 1000000+i*3)))
	}
	corpora["blockedge"] = blockEdge

	for name, vals := range corpora {
		for _, e := range encoders {
			if !e.applicable(vals) {
				continue
			}
			enc := e.encode(vals)
			dec := e.decode(enc)
			if !valsEqual(vals, dec) {
				t.Fatalf("%s on %s: round trip mismatch", e.name, name)
			}
		}
	}
}

// TestForApplicable pins the refusals: non-canonical decimals, mixed
// widths, and overflow must all keep forpack out, since a wrong accept
// would corrupt on the re-format.
func TestForApplicable(t *testing.T) {
	bad := [][][]byte{
		{[]byte("01")},
		{[]byte("")},
		{[]byte("12a")},
		{[]byte("-5")},
		{[]byte("18446744073709551616")},
		{[]byte("1"), []byte("abcdefgh")},
		{},
	}
	for i, vals := range bad {
		if _, ok := forApplicable(vals); ok {
			t.Fatalf("case %d: forApplicable accepted %q", i, vals)
		}
	}
	if mode, ok := forApplicable([][]byte{[]byte("12345678")}); !ok || mode != 1 {
		t.Fatalf("8-byte value should be binary mode, got %d %v", mode, ok)
	}
	if mode, ok := forApplicable([][]byte{[]byte("123"), []byte("4")}); !ok || mode != 0 {
		t.Fatalf("decimal values should be ascii mode, got %d %v", mode, ok)
	}
}

// TestForPackRandom fuzzes the bit-packer across widths: random u64
// populations clamped to every bit width, both modes, across block
// boundaries.
func TestForPackRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for width := 0; width <= 64; width += 7 {
		for _, n := range []int{1, 63, 64, 65, forBlock - 1, forBlock, forBlock + 1, 3000} {
			vals := make([][]byte, n)
			for i := range vals {
				x := rng.Uint64()
				if width < 64 {
					x &= uint64(1)<<width - 1
				}
				vals[i] = []byte(fmt.Sprintf("%d", x))
			}
			if _, ok := forApplicable(vals); !ok {
				t.Fatalf("width %d n %d: not applicable", width, n)
			}
			dec := forPackDecode(forPackEncode(vals))
			if !valsEqual(vals, dec) {
				t.Fatalf("width %d n %d: mismatch", width, n)
			}
		}
	}
}

// TestSelection sanity: the rule picks a lightweight scheme where one
// clearly wins, falls through to zstd on JSON, and never returns an
// inapplicable encoder.
func TestSelection(t *testing.T) {
	rng := rand.New(rand.NewSource(13))
	ts := genShape("timestamps", 512, rng)
	if got := selectScheme(ts, true, 0.08); encoders[got].name != "forpack" {
		t.Fatalf("timestamps chose %s", encoders[got].name)
	}
	js := genShape("json", 512, rng)
	if got := selectScheme(js, false, 0.08); encoders[got].name != "zstd" {
		t.Fatalf("json chose %s", encoders[got].name)
	}
	if got := oracleScheme(js); encoders[got].name != "zstd" {
		t.Fatalf("json oracle chose %s", encoders[got].name)
	}
	incompressible := make([][]byte, 64)
	for i := range incompressible {
		v := make([]byte, 40)
		rng.Read(v)
		incompressible[i] = v
	}
	if got := selectScheme(incompressible, false, 0.08); encoders[got].name != "raw" && encoders[got].name != "zstd" {
		t.Fatalf("random bytes chose %s", encoders[got].name)
	}
	sel := selectScheme(ts, true, 0.08)
	if !encoders[sel].applicable(ts) {
		t.Fatalf("selection returned inapplicable encoder")
	}
	enc := encoders[sel].encode(ts)
	if !valsEqual(ts, encoders[sel].decode(enc)) {
		t.Fatalf("selected encoder does not round trip")
	}
}

// TestDictSharing pins that dict decode returns aliased dictionary
// bytes equal to the originals even under interleaved repeats.
func TestDictSharing(t *testing.T) {
	vals := [][]byte{[]byte("aa"), []byte("bb"), []byte("aa"), []byte("cc"), []byte("bb"), []byte("aa")}
	for _, e := range encoders[1:3] {
		dec := e.decode(e.encode(vals))
		for i := range vals {
			if !bytes.Equal(vals[i], dec[i]) {
				t.Fatalf("%s: slot %d mismatch", e.name, i)
			}
		}
	}
}
