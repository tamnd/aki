package sqlo1

import (
	"bytes"
	"fmt"
	"testing"
)

// TestSubkeyLayoutGolden pins the doc 6.3 byte layout by hand.
func TestSubkeyLayoutGolden(t *testing.T) {
	s, err := NewSubkey(0x1122334455667788, 3, 0xAABBCCDDEEFF)
	if err != nil {
		t.Fatal(err)
	}
	got := s.Encode()
	want := []byte{
		0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // rooth LE
		3,                                        // kind
		0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x00, // segid LE, 7 bytes
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("subkey layout\n got %x\nwant %x", got, want)
	}
}

func TestSubkeyRoundtrip(t *testing.T) {
	cases := []Subkey{
		{Rooth: 1, Kind: SubkindSeg, Segid: 0},
		{Rooth: ^uint64(0), Kind: SubkindFence, Segid: maxSegid},
		{Rooth: 0, Kind: 5, Segid: 1 << 55},
	}
	for _, want := range cases {
		s, err := NewSubkey(want.Rooth, want.Kind, want.Segid)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeSubkey(s.Encode())
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("roundtrip %+v, want %+v", got, want)
		}
	}
}

func TestSubkeyRejects(t *testing.T) {
	if _, err := NewSubkey(1, 0, 1); err == nil {
		t.Error("kind 0 minted")
	}
	if _, err := NewSubkey(1, 1, maxSegid+1); err == nil {
		t.Error("segid past 56 bits minted")
	}
	if _, err := DecodeSubkey(make([]byte, SubkeySize-1)); err == nil {
		t.Error("short subkey decoded")
	}
	if _, err := DecodeSubkey(make([]byte, SubkeySize+1)); err == nil {
		t.Error("long subkey decoded")
	}
	if _, err := DecodeSubkey(make([]byte, SubkeySize)); err == nil {
		t.Error("kind 0 decoded")
	}
}

// TestSplitmix64Golden pins the mix against the reference stream from
// seed 0 (Vigna's splitmix64.c), whose k-th output is the mix of
// (k+1)*gamma: a wrong constant or a dropped shift step fails here,
// not in a collision three months in. MintRooth(0, 0) feeds input 0
// and lands on the stream's first output.
func TestSplitmix64Golden(t *testing.T) {
	const gamma = 0x9E3779B97F4A7C15
	stream := []uint64{0xE220A8397B1DCDAF, 0x6E789E6AA1B965F4, 0x06C45D188009454F}
	for k, want := range stream {
		if got := splitmix64(uint64(k) * gamma); got != want {
			t.Fatalf("splitmix64(%d*gamma) = %#x, want %#x", k, got, want)
		}
	}
	if got, err := MintRooth(0, 0); err != nil || got != stream[0] {
		t.Fatalf("MintRooth(0, 0) = %#x, %v, want %#x", got, err, stream[0])
	}
}

func TestMintRoothCollisionFree(t *testing.T) {
	seen := make(map[uint64]string, 1<<17)
	shards := []uint16{0, 1, 255, 65535}
	for _, sh := range shards {
		for c := range uint64(1 << 15) {
			r, err := MintRooth(sh, c)
			if err != nil {
				t.Fatal(err)
			}
			if prev, dup := seen[r]; dup {
				t.Fatalf("rooth %#x minted twice: shard %d counter %d and %s", r, sh, c, prev)
			}
			seen[r] = fmt.Sprintf("shard %d counter %d", sh, c)
		}
	}
	// The counter ceiling and the boundary just below it.
	if _, err := MintRooth(0, maxRoothCounter); err != nil {
		t.Fatal(err)
	}
	if _, err := MintRooth(0, maxRoothCounter+1); err == nil {
		t.Error("counter past 48 bits minted")
	}
}

func TestLeaseEnd(t *testing.T) {
	const space = uint64(1) << 48
	ok := []struct{ start, n, want uint64 }{
		{0, 5, 5},
		{5, 3, 8},
		{space - 1, 1, space},
		{0, space, space},
	}
	for _, c := range ok {
		got, err := LeaseEnd(c.start, c.n)
		if err != nil || got != c.want {
			t.Errorf("LeaseEnd(%d, %d) = %d, %v, want %d", c.start, c.n, got, err, c.want)
		}
	}
	bad := []struct{ start, n uint64 }{
		{0, 0},
		{space, 1},
		{1, space},
		{^uint64(0), 2},
	}
	for _, c := range bad {
		if _, err := LeaseEnd(c.start, c.n); err == nil {
			t.Errorf("LeaseEnd(%d, %d) accepted", c.start, c.n)
		}
	}
}
