package main

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestFrameRoundTrip pins the group framing against value slices the
// generators produce plus edge shapes, since measure trusts unframe
// blindly after the byte-equality check.
func TestFrameRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	corpora := map[string][][]byte{
		"empties": {{}, []byte("a"), {}},
		"single":  {[]byte("just one")},
	}
	for _, shape := range []string{"json", "sess", "user", "rand"} {
		corpora[shape] = genCorpus(shape, 500, 1, rng)
	}
	for name, vals := range corpora {
		dec := unframe(frame(vals))
		if len(dec) != len(vals) {
			t.Fatalf("%s: count %d != %d", name, len(dec), len(vals))
		}
		for i := range vals {
			if !bytes.Equal(vals[i], dec[i]) {
				t.Fatalf("%s: slot %d mismatch", name, i)
			}
		}
	}
}

// TestDictRoundTrip trains a real dictionary and holds compressed
// groups to byte-exact decode through the dict decoder, on every shape
// the sweep uses.
func TestDictRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for _, shape := range []string{"json", "sess", "user"} {
		d := trainDict(sampleBytes(shape, 1, 200*1024, rng), 16*1024)
		if d == nil {
			t.Fatalf("%s: training failed", shape)
		}
		c := newCodec(d)
		vals := genCorpus(shape, 2000, 1, rng)
		ratio, _, _ := measure(vals, 64, c)
		if ratio <= 0 || ratio > 1.5 {
			t.Fatalf("%s: implausible ratio %f", shape, ratio)
		}
	}
}

// TestDictBeatsPlainOnSmallGroups pins the lab's premise on the shape
// it should be strongest for: templated json in 16-value groups must
// compress strictly smaller with a trained dictionary than without.
func TestDictBeatsPlainOnSmallGroups(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	d := trainDict(sampleBytes("json", 1, 1024*1024, rng), 64*1024)
	if d == nil {
		t.Fatal("training failed")
	}
	vals := genCorpus("json", 4000, 1, rng)
	withDict := groupBytes(vals, 16, newCodec(d))
	plain := groupBytes(vals, 16, newCodec(nil))
	if withDict >= plain {
		t.Fatalf("dict %d >= plain %d on 16-value json groups", withDict, plain)
	}
}

// TestTrainFallback: a degenerate corpus (one repeated byte) must not
// panic; either training succeeds or returns nil, and nil is the
// fallback the engine slice takes.
func TestTrainFallback(t *testing.T) {
	samples := make([][]byte, 100)
	for i := range samples {
		samples[i] = bytes.Repeat([]byte{'x'}, 10)
	}
	d := trainDict(samples, 16*1024)
	if d != nil {
		c := newCodec(d)
		vals := [][]byte{bytes.Repeat([]byte{'x'}, 10)}
		if r, _, _ := measure(vals, 1, c); r <= 0 {
			t.Fatalf("bad ratio %f", r)
		}
	}
}

// TestMixDrift sanity: at p=0 the mix generator produces only set 1
// output shapes, at p=1 only set 2, checked via the sess scope vocab.
func TestMixDrift(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	for _, v := range genMix("sess", 200, 0, rng) {
		if bytes.Contains(v, []byte("scope=cart")) || bytes.Contains(v, []byte("scope=checkout")) {
			t.Fatalf("set 2 vocab at p=0: %s", v)
		}
	}
	seen2 := false
	for _, v := range genMix("sess", 200, 1, rng) {
		if bytes.Contains(v, []byte("scope=read")) || bytes.Contains(v, []byte("scope=admin")) {
			t.Fatalf("set 1 vocab at p=1: %s", v)
		}
		if bytes.Contains(v, []byte("scope=cart")) {
			seen2 = true
		}
	}
	if !seen2 {
		t.Fatal("no set 2 vocab at p=1")
	}
}
