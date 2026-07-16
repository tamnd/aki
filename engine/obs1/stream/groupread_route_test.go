package stream

import (
	"testing"
)

// Routing helpers the dispatcher calls to co-locate an XREADGROUP's stream keys on
// one shard and pick the single routing key. They parse the GROUP prefix and the
// COUNT/BLOCK/NOACK option clauses the same way Xreadgroup does, so a malformed
// tail returns nil/-1 and the handler answers the exact error in place.

func toArgs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestGroupReadKeysWellFormed(t *testing.T) {
	tail := toArgs("GROUP", "g", "c", "COUNT", "2", "NOACK", "STREAMS", "a", "b", ">", ">")
	keys := GroupReadKeys(tail)
	if len(keys) != 2 || string(keys[0]) != "a" || string(keys[1]) != "b" {
		t.Fatalf("keys = %v, want [a b]", keys)
	}
	if at := GroupReadKeyAt(tail); at != 7 {
		t.Fatalf("key-at = %d, want 7", at)
	}
}

func TestGroupReadKeysWithBlock(t *testing.T) {
	tail := toArgs("GROUP", "g", "c", "BLOCK", "50", "STREAMS", "only", ">")
	keys := GroupReadKeys(tail)
	if len(keys) != 1 || string(keys[0]) != "only" {
		t.Fatalf("keys = %v, want [only]", keys)
	}
	if at := GroupReadKeyAt(tail); at != 6 {
		t.Fatalf("key-at = %d, want 6", at)
	}
}

func TestGroupReadKeysMalformed(t *testing.T) {
	cases := [][]string{
		{"NOTGROUP", "g", "c", "STREAMS", "a", ">"}, // missing GROUP keyword
		{"GROUP", "g"},                                    // too short for the prefix
		{"GROUP", "g", "c", "STREAMS", "a"},               // odd key/id count
		{"GROUP", "g", "c", "COUNT"},                      // dangling option, no STREAMS
		{"GROUP", "g", "c", "BOGUS", "STREAMS", "a", ">"}, // unknown option token
	}
	for _, cs := range cases {
		tail := toArgs(cs...)
		if keys := GroupReadKeys(tail); keys != nil {
			t.Fatalf("keys(%v) = %v, want nil", cs, keys)
		}
		if at := GroupReadKeyAt(tail); at != -1 {
			t.Fatalf("key-at(%v) = %d, want -1", cs, at)
		}
	}
}
