package command

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// argvOf turns string args into the [][]byte the rewrite helper takes.
func argvOf(args ...string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}

// strsOf turns a rewritten command back into strings for comparison.
func strsOf(argv [][]byte) []string {
	out := make([]string, len(argv))
	for i, a := range argv {
		out[i] = string(a)
	}
	return out
}

// TestRewriteForAOFExactExpiry covers the rewrites whose timestamp is fully
// determined by the input, so the result can be checked exactly.
func TestRewriteForAOFExactExpiry(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"expireat", []string{"EXPIREAT", "k", "100"}, []string{"PEXPIREAT", "k", "100000"}},
		{"set exat", []string{"SET", "k", "v", "EXAT", "100"}, []string{"SET", "k", "v", "PXAT", "100000"}},
		{"set pxat", []string{"SET", "k", "v", "PXAT", "12345"}, []string{"SET", "k", "v", "PXAT", "12345"}},
		{"getex exat", []string{"GETEX", "k", "EXAT", "100"}, []string{"PEXPIREAT", "k", "100000"}},
		{"getex pxat", []string{"GETEX", "k", "PXAT", "777"}, []string{"PEXPIREAT", "k", "777"}},
		{"getex persist", []string{"GETEX", "k", "PERSIST"}, []string{"PERSIST", "k"}},
		// No expiry option means propagate verbatim.
		{"plain set", []string{"SET", "k", "v"}, []string{"SET", "k", "v"}},
		{"set keepttl", []string{"SET", "k", "v", "KEEPTTL"}, []string{"SET", "k", "v", "KEEPTTL"}},
		{"set nx", []string{"SET", "k", "v", "NX"}, []string{"SET", "k", "v", "NX"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := strsOf(rewriteForAOF(tc.in[0], argvOf(tc.in...)))
			if len(got) != len(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v want %v", got, tc.want)
				}
			}
		})
	}
}

// TestRewriteForAOFRelativeExpiry covers the rewrites whose timestamp is now
// plus an offset. The exact value depends on the clock, so the test checks the
// shape and that the timestamp lands in a tight window around the expected one.
func TestRewriteForAOFRelativeExpiry(t *testing.T) {
	cases := []struct {
		name     string
		in       []string
		wantHead []string // command up to but not including the timestamp
		offsetMs int64
	}{
		{"setex", []string{"SETEX", "k", "100", "v"}, []string{"SET", "k", "v", "PXAT"}, 100 * 1000},
		{"psetex", []string{"PSETEX", "k", "5000", "v"}, []string{"SET", "k", "v", "PXAT"}, 5000},
		{"set ex", []string{"SET", "k", "v", "EX", "100"}, []string{"SET", "k", "v", "PXAT"}, 100 * 1000},
		{"set px", []string{"SET", "k", "v", "PX", "5000"}, []string{"SET", "k", "v", "PXAT"}, 5000},
		{"expire", []string{"EXPIRE", "k", "100"}, []string{"PEXPIREAT", "k"}, 100 * 1000},
		{"pexpire", []string{"PEXPIRE", "k", "5000"}, []string{"PEXPIREAT", "k"}, 5000},
		{"getex ex", []string{"GETEX", "k", "EX", "100"}, []string{"PEXPIREAT", "k"}, 100 * 1000},
		{"set ex with nx dropped", []string{"SET", "k", "v", "NX", "EX", "100"}, []string{"SET", "k", "v", "PXAT"}, 100 * 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := time.Now().UnixMilli()
			got := strsOf(rewriteForAOF(tc.in[0], argvOf(tc.in...)))
			after := time.Now().UnixMilli()

			if len(got) != len(tc.wantHead)+1 {
				t.Fatalf("got %v want head %v plus a timestamp", got, tc.wantHead)
			}
			for i, w := range tc.wantHead {
				if got[i] != w {
					t.Fatalf("got %v want head %v", got, tc.wantHead)
				}
			}
			ts, err := strconv.ParseInt(got[len(got)-1], 10, 64)
			if err != nil {
				t.Fatalf("timestamp %q not an integer: %v", got[len(got)-1], err)
			}
			lo, hi := before+tc.offsetMs, after+tc.offsetMs
			if ts < lo || ts > hi {
				t.Fatalf("timestamp %d outside [%d,%d]", ts, lo, hi)
			}
		})
	}
}

// TestAOFSetexRewritten confirms SETEX reaches the incr file as an absolute SET
// PXAT, the same wiring TestAOFExpireRewritten checks for EXPIRE.
func TestAOFSetexRewritten(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")
	if got := sendLine(t, r, c, "SETEX k 100 v"); got != "+OK" {
		t.Fatalf("SETEX = %q", got)
	}

	incr := readIncrFile(t, filepath.Join(dir, "appendonlydir"))
	if !strings.Contains(incr, "PXAT") {
		t.Fatalf("incr missing PXAT: %q", incr)
	}
	if strings.Contains(incr, "SETEX") {
		t.Fatalf("incr kept relative SETEX: %q", incr)
	}
}
