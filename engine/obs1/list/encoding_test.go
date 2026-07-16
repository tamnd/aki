package list

import (
	"os"
	"strings"
	"testing"
)

// The differential encoding suite for the list bands (spec 2064/f3/13 section
// 4.4). Every case carries the encoding real Redis reports for the same elements
// under the default config:
//
//	list-max-listpack-size -2   (an 8 KiB budget on the packed listpack bytes)
//
// The boundaries below were bisected live against redis-server 8.8.0. Unlike the
// set and zset bands, the list flip is a byte budget, not an element count and
// not a per-element value cap: 2000 tiny elements stay listpack while a single
// 9 KiB element is already quicklist. The expectations are vendored so the table
// runs offline; TestEncodingAgainstRedis replays the same table against a live
// server when AKI_REDIS_ADDR points at one, so a Redis bump that moves the
// budget shows up as a failure here rather than as silent parity drift.
//
// The live check pushes one element per command, the incremental path aki
// mirrors element by element. Redis has a second, looser path for a single
// multi-element RPUSH: it can leave a listpack over the byte budget until the
// next single-element op reconverges it to quicklist. aki converts eagerly on
// the budget within a multi-arg push, so its OBJECT ENCODING can read quicklist
// where Redis still reads listpack right after one bulk command; a following
// single-element push brings Redis to quicklist too. That transient bulk-path
// divergence is the one encoding gap this slice does not chase, since matching
// it would mean holding oversized listpacks, against the whole point of the
// budget. Element data, order, and every command result stay identical.

// gen builds n copies of an each-byte string of width w.
func gen(n, w int) []string {
	out := make([]string, n)
	s := strings.Repeat("x", w)
	for i := range out {
		out[i] = s
	}
	return out
}

// buildList appends elems to a fresh list in order, the same one-at-a-time build
// RPUSH does, so the conversion path is the live one.
func buildList(elems []string) *list {
	l := newList()
	for _, e := range elems {
		l.pushBack([]byte(e))
	}
	return l
}

var encodingCases = []struct {
	name  string
	elems []string
	want  string
}{
	{"three tiny", []string{"a", "b", "c"}, "listpack"},
	{"int elements", []string{"1", "2", "3", "-9999999"}, "listpack"},
	{"98 x80B stays listpack", gen(98, 80), "listpack"},
	{"99 x80B flips quicklist", gen(99, 80), "quicklist"},
	{"194 x40B stays listpack", gen(194, 40), "listpack"},
	{"195 x40B flips quicklist", gen(195, 40), "quicklist"},
	{"2729 x1B stays listpack", gen(2729, 1), "listpack"},
	{"2730 x1B flips quicklist", gen(2730, 1), "quicklist"},
	{"one 9000B element", []string{strings.Repeat("y", 9000)}, "quicklist"},
}

func TestEncodingParity(t *testing.T) {
	for _, tc := range encodingCases {
		t.Run(tc.name, func(t *testing.T) {
			l := buildList(tc.elems)
			if got := l.encoding().String(); got != tc.want {
				t.Fatalf("encoding = %q, Redis reports %q for %d elements",
					got, tc.want, len(tc.elems))
			}
		})
	}
}

// TestEncodingAgainstRedis replays the vendored table against a live Redis when
// AKI_REDIS_ADDR is set (host:port), so the frozen boundaries stay honest.
// Skipped by default; it is the confirmation lever, not a required gate. It
// pushes in batches so a large case stays under the server's argument limits.
func TestEncodingAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check encodings against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()
	for _, tc := range encodingCases {
		key := "aki:lenc:" + tc.name
		c.cmd("DEL", key)
		for _, e := range tc.elems {
			if _, err := c.cmd("RPUSH", key, e); err != nil {
				t.Fatalf("%s: RPUSH: %v", tc.name, err)
			}
		}
		got, err := c.cmd("OBJECT", "ENCODING", key)
		if err != nil {
			t.Fatalf("%s: OBJECT ENCODING: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: live Redis encoding %q, vendored table says %q",
				tc.name, got, tc.want)
		}
		c.cmd("DEL", key)
	}
}
