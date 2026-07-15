package main

import "testing"

func TestClassify(t *testing.T) {
	v1 := []byte("w0-k0-v1")
	v2 := []byte("w0-k0-v2")
	stray := []byte("w3-k9-v7")

	cases := []struct {
		name     string
		st       keyState
		observed []byte
		found    bool
		want     verdict
	}{
		{"acked value present", keyState{acked: v1}, v1, true, verdictMatch},
		{"absent key still absent", keyState{}, nil, false, verdictMatch},
		{"pending set not applied", keyState{acked: v1, pending: &pendingOp{val: v2}}, v1, true, verdictMatch},
		{"pending set applied", keyState{acked: v1, pending: &pendingOp{val: v2}}, v2, true, verdictPendingApplied},
		{"pending set on absent key applied", keyState{pending: &pendingOp{val: v2}}, v2, true, verdictPendingApplied},
		{"pending del applied", keyState{acked: v1, pending: &pendingOp{del: true}}, nil, false, verdictPendingApplied},
		{"acked value gone", keyState{acked: v1}, nil, false, verdictLost},
		{"acked gone with pending set", keyState{acked: v1, pending: &pendingOp{val: v2}}, nil, false, verdictLost},
		{"foreign bytes under the key", keyState{acked: v1}, stray, true, verdictCorrupt},
		{"stale generation resurfaced", keyState{acked: v2, pending: &pendingOp{del: true}}, v1, true, verdictCorrupt},
		{"deleted key resurrected", keyState{}, v1, true, verdictCorrupt},
	}
	for _, c := range cases {
		if got := classify(c.st, c.observed, c.found); got != c.want {
			t.Errorf("%s: classify = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestDigest(t *testing.T) {
	a := map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")}
	b := map[string][]byte{"k2": []byte("v2"), "k1": []byte("v1")}
	if digest(a) != digest(b) {
		t.Fatal("digest must not depend on map iteration order")
	}
	c := map[string][]byte{"k1": []byte("v1"), "k2": []byte("vX")}
	if digest(a) == digest(c) {
		t.Fatal("digest must change when a value changes")
	}
	if digest(map[string][]byte{}) == digest(a) {
		t.Fatal("digest of an empty keyspace must differ from a populated one")
	}
}

func TestIterationResultPass(t *testing.T) {
	clean := iterationResult{matched: 10}
	lossy := iterationResult{matched: 4, lost: 6}
	corrupt := iterationResult{matched: 9, corrupt: 1}

	if !clean.pass(false) || !clean.pass(true) {
		t.Fatal("a clean iteration passes both modes")
	}
	if !lossy.pass(false) {
		t.Fatal("lost writes are legal for a non-durable store")
	}
	if lossy.pass(true) {
		t.Fatal("lost writes fail a durable store")
	}
	if corrupt.pass(false) || corrupt.pass(true) {
		t.Fatal("corruption fails both modes")
	}
}
