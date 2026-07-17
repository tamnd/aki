package akifile

import (
	"bytes"
	"testing"
)

// TestCollOpPayloadRoundTrips frames effect payloads across the shapes the types
// use, an add with a sub-value (a zset member plus score), a remove with a bare
// sub-key (a set member), and a trim with a sub-value but no sub-key (a list
// bound), then decodes each and checks every field survives.
func TestCollOpPayloadRoundTrips(t *testing.T) {
	cases := []CollOpRow{
		{Kind: CollKindZset, Op: 1, SubKey: []byte("member-a"), SubValue: []byte("3.5")},
		{Kind: CollKindSet, Op: 2, SubKey: []byte("gone"), SubValue: nil},
		{Kind: CollKindList, Op: 4, SubKey: nil, SubValue: []byte("0..10")},
		{Kind: CollKindHash, Op: 1, SubKey: []byte("field"), SubValue: []byte("value")},
	}
	for _, want := range cases {
		payload := AppendCollOp(nil, want)
		got, err := ParseCollOp(payload)
		if err != nil {
			t.Fatalf("parse %+v: %v", want, err)
		}
		if got.Kind != want.Kind || got.Op != want.Op {
			t.Fatalf("kind/op = %d/%d, want %d/%d", got.Kind, got.Op, want.Kind, want.Op)
		}
		if !bytes.Equal(got.SubKey, want.SubKey) {
			t.Fatalf("sub-key = %q, want %q", got.SubKey, want.SubKey)
		}
		if !bytes.Equal(got.SubValue, want.SubValue) {
			t.Fatalf("sub-value = %q, want %q", got.SubValue, want.SubValue)
		}
	}
}

// TestCollSnapPayloadRoundTrips frames a snapshot payload with a header and an
// element run, decodes it, and checks the split lands where it was framed. A
// header-only payload (empty element run) and a run-only payload (empty header)
// both round-trip, the boundaries the length-prefix-then-tail layout has to hold.
func TestCollSnapPayloadRoundTrips(t *testing.T) {
	cases := []CollSnapRow{
		{Kind: CollKindStream, Header: []byte("counters+groups"), ElementRun: []byte("entry-blocks")},
		{Kind: CollKindSet, Header: nil, ElementRun: []byte("member-run")},
		{Kind: CollKindHash, Header: []byte("expire+band"), ElementRun: nil},
	}
	for _, want := range cases {
		payload := AppendCollSnap(nil, want)
		got, err := ParseCollSnap(payload)
		if err != nil {
			t.Fatalf("parse %+v: %v", want, err)
		}
		if got.Kind != want.Kind {
			t.Fatalf("kind = %d, want %d", got.Kind, want.Kind)
		}
		if !bytes.Equal(got.Header, want.Header) {
			t.Fatalf("header = %q, want %q", got.Header, want.Header)
		}
		if !bytes.Equal(got.ElementRun, want.ElementRun) {
			t.Fatalf("element run = %q, want %q", got.ElementRun, want.ElementRun)
		}
	}
}

// TestCollectionFrameCarriesPayload proves a collection payload rides an ordinary
// record frame: an op frame and a snapshot frame each frame with the collection
// flag set and the payload in the value slot, and a linear walk reads both back
// with the flag and the payload intact. This is the seam that lets the existing
// record writer, walk, and CRC path carry a collection frame unchanged.
func TestCollectionFrameCarriesPayload(t *testing.T) {
	opPayload := AppendCollOp(nil, CollOpRow{Kind: CollKindSet, Op: 1, SubKey: []byte("m1")})
	snapPayload := AppendCollSnap(nil, CollSnapRow{Kind: CollKindSet, Header: []byte("ttl"), ElementRun: []byte("m1m2")})

	var buf []byte
	buf, _ = AppendRecordFrame(buf, RecordRow{
		Flags:    RecFlagCollectionOp,
		ValueLen: uint32(len(opPayload)),
		Value:    opPayload,
		Key:      []byte("myset"),
	})
	buf, _ = AppendRecordFrame(buf, RecordRow{
		Flags:    RecFlagCollectionSnap,
		ValueLen: uint32(len(snapPayload)),
		Value:    snapPayload,
		Key:      []byte("myset"),
	})

	var off uint64
	_, op, next, err := NextRecordFrame(buf, off)
	if err != nil {
		t.Fatalf("walk op frame: %v", err)
	}
	if op.Flags&RecFlagCollectionOp == 0 || string(op.Key) != "myset" {
		t.Fatalf("op frame = %+v, want collection-op key myset", op)
	}
	if !bytes.Equal(op.Value, opPayload) {
		t.Fatalf("op payload = %q, want %q", op.Value, opPayload)
	}
	decoded, err := ParseCollOp(op.Value)
	if err != nil || decoded.Kind != CollKindSet || string(decoded.SubKey) != "m1" {
		t.Fatalf("decoded op = %+v err %v, want set m1", decoded, err)
	}

	_, snap, _, err := NextRecordFrame(buf, next)
	if err != nil {
		t.Fatalf("walk snap frame: %v", err)
	}
	if snap.Flags&RecFlagCollectionSnap == 0 {
		t.Fatalf("snap frame lost its flag: %#x", snap.Flags)
	}
	ds, err := ParseCollSnap(snap.Value)
	if err != nil || ds.Kind != CollKindSet || !bytes.Equal(ds.ElementRun, []byte("m1m2")) {
		t.Fatalf("decoded snap = %+v err %v, want set element run m1m2", ds, err)
	}
}

// TestCollPayloadRejectsTornLength proves a CRC-clean but malformed payload, a
// sub-key or header length that outruns the bytes, fails closed rather than
// slicing out of range.
func TestCollPayloadRejectsTornLength(t *testing.T) {
	// kind, op, then a sub-key length of 9 over a 3-byte remainder.
	if _, err := ParseCollOp([]byte{byte(CollKindSet), 1, 9, 'a', 'b', 'c'}); err != ErrLength {
		t.Fatalf("op over-long sub-key = %v, want ErrLength", err)
	}
	// kind, then a header length of 9 over a 2-byte remainder.
	if _, err := ParseCollSnap([]byte{byte(CollKindSet), 9, 'a', 'b'}); err != ErrLength {
		t.Fatalf("snap over-long header = %v, want ErrLength", err)
	}
	if _, err := ParseCollOp([]byte{byte(CollKindSet)}); err != ErrShort {
		t.Fatalf("op too short = %v, want ErrShort", err)
	}
}
