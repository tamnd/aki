package main

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// oneMemberBlob packs the same listpack shape the engine and the lab use.
func TestOneMemberBlob(t *testing.T) {
	b := oneMemberBlob([]byte("hi"))
	if !bytes.Equal(b, []byte{2, 'h', 'h', 'i'}) {
		t.Fatalf("oneMemberBlob = %v", b)
	}
}

// The arena route round-trips a tiny set through the real store: a PeekCollBlob
// resolve then a PutCollBlob commit republish leaves the record readable with the
// same kind and blob, the invariant the routing flip depends on.
func TestArenaRouteRoundTrips(t *testing.T) {
	s := store.New(8<<20, 1<<20)
	blob := oneMemberBlob([]byte("hello"))
	if err := s.PutCollBlob(key(0), kindSet, 1, blob, 0, 0); err != nil {
		t.Fatalf("seed PutCollBlob: %v", err)
	}

	rec, kind, bits, _, ok := s.PeekCollBlob(key(0))
	if !ok || kind != kindSet {
		t.Fatalf("PeekCollBlob ok=%v kind=%#x, want true %#x", ok, kind, kindSet)
	}
	scratch := append([]byte(nil), rec...)
	if err := s.PutCollBlob(key(0), kindSet, bits, scratch, 0, 0); err != nil {
		t.Fatalf("republish PutCollBlob: %v", err)
	}

	got, kind, _, ok := s.GetCollBlob(key(0), 0)
	if !ok || kind != kindSet {
		t.Fatalf("GetCollBlob ok=%v kind=%#x, want true %#x", ok, kind, kindSet)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("round-tripped blob = %v, want %v", got, blob)
	}
}

// Both timing arms complete over a small population without panicking, so the lab
// body is exercised under go test. The absolute numbers are read from the binary;
// this only proves the arms run and return positive per-command costs.
func TestRouteArmsRun(t *testing.T) {
	member := []byte("hello")
	if w := wallRoute(1000, 5000, member); w <= 0 {
		t.Fatalf("wallRoute = %.2f ns/cmd, want positive", w)
	}
	if a := arenaRoute(1000, 5000, member); a <= 0 {
		t.Fatalf("arenaRoute = %.2f ns/cmd, want positive", a)
	}
}
