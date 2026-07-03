package f1srv

import (
	"fmt"
	"testing"
)

func TestParseExecModel(t *testing.T) {
	cases := []struct {
		in     string
		want   execModel
		wantOK bool
	}{
		{"", execModelShared, true},
		{"shared", execModelShared, true},
		{"affinity", execModelAffinity, true},
		{"Shared", execModelShared, false},   // case-sensitive on purpose: the flag help spells them lowercase
		{"AFFINITY", execModelShared, false}, // an unrecognized value falls back to shared, ok=false
		{"garbage", execModelShared, false},
	}
	for _, tc := range cases {
		got, ok := parseExecModel(tc.in)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("parseExecModel(%q) = (%v, %v), want (%v, %v)", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestExecModelString(t *testing.T) {
	if got := execModelShared.String(); got != "shared" {
		t.Errorf("execModelShared.String() = %q, want %q", got, "shared")
	}
	if got := execModelAffinity.String(); got != "affinity" {
		t.Errorf("execModelAffinity.String() = %q, want %q", got, "affinity")
	}
	// A round trip through parse and back must be stable for the recognized spellings,
	// because String feeds the startup banner and INFO and parse reads the flag.
	for _, s := range []string{"shared", "affinity"} {
		m, ok := parseExecModel(s)
		if !ok {
			t.Fatalf("parseExecModel(%q) not ok", s)
		}
		if m.String() != s {
			t.Errorf("round trip %q -> %v -> %q", s, m, m.String())
		}
	}
}

func TestShardForRange(t *testing.T) {
	// Every key must land in [0, nShards) for a spread of shard counts, including the
	// non-power-of-two counts the Lemire reduction is chosen to support so the worker
	// count can track an arbitrary core count.
	for _, n := range []int{1, 2, 3, 5, 7, 8, 12, 16, 31, 64, 100} {
		for i := 0; i < 5000; i++ {
			key := []byte(fmt.Sprintf("key:%d", i))
			s := shardFor(key, n)
			if s < 0 || s >= n {
				t.Fatalf("shardFor(%q, %d) = %d, out of [0,%d)", key, n, s, n)
			}
		}
	}
}

func TestShardForDeterministic(t *testing.T) {
	// The routing decision must be a pure function of the key bytes: the home loop
	// computes it before touching the store and every worker must agree without a lookup.
	keys := []string{"", "a", "user:1000", "hll:sessions", "zset:{leaderboard}", "\x00\x01\x02"}
	for _, k := range keys {
		first := shardFor([]byte(k), 16)
		for i := 0; i < 100; i++ {
			if got := shardFor([]byte(k), 16); got != first {
				t.Fatalf("shardFor(%q, 16) not stable: %d then %d", k, first, got)
			}
		}
	}
}

func TestShardForSingleShard(t *testing.T) {
	// nShards <= 1 collapses to the degenerate single-owner store; callers rely on this
	// so a one-worker server needs no special case.
	for _, n := range []int{-1, 0, 1} {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("k%d", i))
			if got := shardFor(key, n); got != 0 {
				t.Errorf("shardFor(%q, %d) = %d, want 0", key, n, got)
			}
		}
	}
}

func TestShardForDistribution(t *testing.T) {
	// The hash plus Lemire reduction must spread keys roughly evenly, or the whole point
	// of one-shard-per-core is lost to a hot worker. This is a loose sanity bound, not a
	// statistical test: with 100k distinct keys over 16 shards the expected count is 6250
	// per shard, and we allow any shard to sit within 25% of that.
	const nKeys = 100000
	const nShards = 16
	counts := make([]int, nShards)
	for i := 0; i < nKeys; i++ {
		counts[shardFor([]byte(fmt.Sprintf("key:%d", i)), nShards)]++
	}
	expected := nKeys / nShards
	lo, hi := expected*3/4, expected*5/4
	for s, c := range counts {
		if c < lo || c > hi {
			t.Errorf("shard %d got %d keys, want within [%d,%d] of expected %d", s, c, lo, hi, expected)
		}
	}
}

func TestShardHashAvalanche(t *testing.T) {
	// Adjacent keys that differ only in the low byte must not cluster in one shard, which
	// is the failure the splitmix64 finalizer on top of FNV-1a is there to prevent. Hash a
	// run of sequential keys and require they touch most of the shards rather than a few.
	const nShards = 64
	seen := make(map[int]bool)
	for i := 0; i < 256; i++ {
		seen[shardFor([]byte(fmt.Sprintf("seq%d", i)), nShards)] = true
	}
	if len(seen) < nShards/2 {
		t.Errorf("256 sequential keys touched only %d of %d shards, hash is clustering", len(seen), nShards)
	}
}
