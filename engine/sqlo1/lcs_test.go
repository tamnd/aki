package sqlo1

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// TestServerLcs pins the wire bytes of every LCS shape against replies
// captured from a live redis-server 8.8.0, including the RESP2
// rendering of the IDX map, the option spellings, and the error texts.
func TestServerLcs(t *testing.T) {
	do, _ := dispatchServer(t)

	if got := do("SET", "key1", "ohmytext"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("SET", "key2", "mynewtext"); got != "+OK\r\n" {
		t.Fatal(got)
	}

	cases := []struct {
		args []string
		want string
	}{
		{[]string{"LCS", "key1", "key2"}, "$6\r\nmytext\r\n"},
		{[]string{"LCS", "key1", "key2", "LEN"}, ":6\r\n"},
		{[]string{"LCS", "key1", "key2", "IDX"},
			"*4\r\n$7\r\nmatches\r\n*2\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n$3\r\nlen\r\n:6\r\n"},
		{[]string{"LCS", "key1", "key2", "IDX", "MINMATCHLEN", "4"},
			"*4\r\n$7\r\nmatches\r\n*1\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n$3\r\nlen\r\n:6\r\n"},
		{[]string{"LCS", "key1", "key2", "IDX", "WITHMATCHLEN"},
			"*4\r\n$7\r\nmatches\r\n*2\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:6\r\n"},
		{[]string{"LCS", "key1", "key2", "IDX", "MINMATCHLEN", "4", "WITHMATCHLEN"},
			"*4\r\n$7\r\nmatches\r\n*1\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n$3\r\nlen\r\n:6\r\n"},
		// Options are case-insensitive and order-free.
		{[]string{"LCS", "key1", "key2", "idx", "withmatchlen", "minmatchlen", "4"},
			"*4\r\n$7\r\nmatches\r\n*1\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n$3\r\nlen\r\n:6\r\n"},
		// Negative MINMATCHLEN clamps to zero.
		{[]string{"LCS", "key1", "key2", "MINMATCHLEN", "-5", "IDX", "WITHMATCHLEN"},
			"*4\r\n$7\r\nmatches\r\n*2\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:6\r\n"},
		// Missing keys read as empty strings.
		{[]string{"LCS", "key1", "missing"}, "$0\r\n\r\n"},
		{[]string{"LCS", "missing1", "missing2"}, "$0\r\n\r\n"},
		{[]string{"LCS", "missing1", "missing2", "IDX"},
			"*4\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n"},
		// A key against itself is the whole string.
		{[]string{"LCS", "key1", "key1"}, "$8\r\nohmytext\r\n"},
		// Errors.
		{[]string{"LCS", "key1", "key2", "LEN", "IDX"},
			"-ERR If you want both the length and indexes, please just use IDX.\r\n"},
		{[]string{"LCS", "key1", "key2", "BOGUS"}, "-ERR syntax error\r\n"},
		{[]string{"LCS", "key1", "key2", "MINMATCHLEN"}, "-ERR syntax error\r\n"},
		{[]string{"LCS", "key1", "key2", "MINMATCHLEN", "x"},
			"-ERR value is not an integer or out of range\r\n"},
		{[]string{"LCS", "key1"}, "-ERR wrong number of arguments for 'lcs' command\r\n"},
	}
	for _, tc := range cases {
		if got := do(tc.args...); got != tc.want {
			t.Errorf("%v = %q, want %q", tc.args, got, tc.want)
		}
	}
}

// TestServerLcsTransientCap pins the Redis guard on the DP table: two
// 12000-byte strings need a 576 MB table, past the proto-max-bulk-len
// default, and the error text matches redis-server 8.8.0 verbatim.
func TestServerLcsTransientCap(t *testing.T) {
	do, _ := dispatchServer(t)
	if got := do("SET", "big1", strings.Repeat("a", 12000)); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("SET", "big2", strings.Repeat("b", 12000)); got != "+OK\r\n" {
		t.Fatal(got)
	}
	want := "-ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len\r\n"
	if got := do("LCS", "big1", "big2", "LEN"); got != want {
		t.Fatalf("LCS over cap = %q, want %q", got, want)
	}
}

// lcsRefLen is an independent two-row DP, the cross-check for the full
// table the production code carries for its backtrack.
func lcsRefLen(a, b []byte) int {
	prev := make([]int, len(b)+1)
	row := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		prev, row = row, prev
		for j := 1; j <= len(b); j++ {
			switch {
			case a[i-1] == b[j-1]:
				row[j] = prev[j-1] + 1
			case prev[j] > row[j-1]:
				row[j] = prev[j]
			default:
				row[j] = row[j-1]
			}
		}
	}
	return row[len(b)]
}

// isSubsequence reports whether sub appears in s in order.
func isSubsequence(sub, s []byte) bool {
	i := 0
	for _, c := range s {
		if i < len(sub) && sub[i] == c {
			i++
		}
	}
	return i == len(sub)
}

// TestStrLcsRope drives LCS through the layer with both values over
// the rope boundary, so the reads stream chunk subkeys, and checks the
// result against an independent DP: the string must be a common
// subsequence of the reference length, and every IDX range must pair
// equal slices whose lengths sum to the total.
func TestStrLcsRope(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	// Two strings just over the 8 KiB rope boundary with planted
	// common runs; the pseudo-random filler bytes come from disjoint
	// alphabets so the interesting structure is the planted runs.
	// Sized to stay under the DP table cap, which two 12 KiB values
	// would already blow.
	common := []byte("the-quick-brown-fox-jumps-over-the-lazy-dog-0123456789")
	var a, b []byte
	sa, sb := uint32(1), uint32(2)
	next := func(s *uint32) byte {
		*s = *s*1664525 + 1013904223
		return byte(*s >> 24)
	}
	for len(a) < 8300 {
		for range 40 {
			a = append(a, 'A'+next(&sa)%8)
		}
		a = append(a, common...)
	}
	for len(b) < 8300 {
		for range 25 {
			b = append(b, 'a'+next(&sb)%8)
		}
		b = append(b, common...)
	}
	r.set("ka", a)
	r.set("kb", b)

	va, vb, err := r.s.LcsRead(ctx, []byte("ka"), []byte("kb"))
	if err != nil {
		t.Fatalf("LcsRead: %v", err)
	}
	if !bytes.Equal(va, a) || !bytes.Equal(vb, b) {
		t.Fatalf("LcsRead returned wrong bytes: %d/%d vs %d/%d", len(va), len(vb), len(a), len(b))
	}

	refLen := lcsRefLen(a, b)
	total, result, _, err := lcsRun(va, vb, false, false, 0)
	if err != nil {
		t.Fatalf("lcsRun: %v", err)
	}
	if int(total) != refLen {
		t.Fatalf("LCS length = %d, reference DP says %d", total, refLen)
	}
	if len(result) != refLen {
		t.Fatalf("result length = %d, want %d", len(result), refLen)
	}
	if !isSubsequence(result, a) || !isSubsequence(result, b) {
		t.Fatal("result is not a common subsequence of both inputs")
	}

	// IDX over the same values: ranges must pair equal slices and
	// cover the whole LCS when unfiltered.
	va, vb, err = r.s.LcsRead(ctx, []byte("ka"), []byte("kb"))
	if err != nil {
		t.Fatalf("LcsRead: %v", err)
	}
	total2, _, matches, err := lcsRun(va, vb, false, true, 0)
	if err != nil {
		t.Fatalf("lcsRun IDX: %v", err)
	}
	if total2 != total {
		t.Fatalf("IDX total = %d, want %d", total2, total)
	}
	var covered uint32
	for _, m := range matches {
		if m.aEnd < m.aStart || m.bEnd < m.bStart {
			t.Fatalf("inverted range %+v", m)
		}
		if m.aEnd-m.aStart != m.bEnd-m.bStart {
			t.Fatalf("unequal range lengths %+v", m)
		}
		if !bytes.Equal(a[m.aStart:m.aEnd+1], b[m.bStart:m.bEnd+1]) {
			t.Fatalf("range %+v pairs unequal slices", m)
		}
		covered += m.aEnd - m.aStart + 1
	}
	if covered != total {
		t.Fatalf("ranges cover %d bytes, want %d", covered, total)
	}

	// MINMATCHLEN keeps exactly the long ranges.
	va, vb, err = r.s.LcsRead(ctx, []byte("ka"), []byte("kb"))
	if err != nil {
		t.Fatalf("LcsRead: %v", err)
	}
	_, _, longOnly, err := lcsRun(va, vb, false, true, 10)
	if err != nil {
		t.Fatalf("lcsRun MINMATCHLEN: %v", err)
	}
	wantLong := 0
	for _, m := range matches {
		if m.aEnd-m.aStart+1 >= 10 {
			wantLong++
		}
	}
	if len(longOnly) != wantLong {
		t.Fatalf("MINMATCHLEN 10 kept %d ranges, want %d", len(longOnly), wantLong)
	}
}
