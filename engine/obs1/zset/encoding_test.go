package zset

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// The differential encoding suite. Every case carries the encoding real Redis
// reports for the same members under the default config (spec 2064/f3/12
// section 4):
//
//	zset-max-listpack-entries 128
//	zset-max-listpack-value   64
//
// The expectations are vendored so the table runs offline; TestEncodingAgainstRedis
// replays the same table against a live server when AKI_REDIS_ADDR points at one,
// so a Redis version bump that moved a threshold shows up as a failure here rather
// than as a silent parity drift.

// buildZset applies pairs to a fresh zset one at a time, mirroring how ZADD
// builds a member at a time so the conversion path is the one under test.
func buildZset(members []string) *zset {
	z := newZset()
	for i, m := range members {
		z.update([]byte(m), float64(i), flags{})
	}
	return z
}

func words(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "m" + strconv.Itoa(i)
	}
	return out
}

func repeat(s string, n int) string { return strings.Repeat(s, n) }

var encodingCases = []struct {
	name    string
	members []string
	want    string
}{
	{"few members", []string{"a", "b", "c"}, "listpack"},
	{"at entry cap", words(maxListpackEntries), "listpack"},
	{"over entry cap", words(maxListpackEntries + 1), "skiplist"},
	{"member at value cap", []string{"seed", repeat("a", maxListpackValue)}, "listpack"},
	{"member over value cap", []string{"seed", repeat("a", maxListpackValue+1)}, "skiplist"},
	{"single long member", []string{repeat("z", maxListpackValue+1)}, "skiplist"},
	{"overshoot 200 pairs", words(200), "skiplist"},
}

func TestEncodingParity(t *testing.T) {
	for _, tc := range encodingCases {
		t.Run(tc.name, func(t *testing.T) {
			z := buildZset(tc.members)
			if got := z.enc.String(); got != tc.want {
				t.Fatalf("encoding = %q, Redis reports %q for %d members", got, tc.want, len(tc.members))
			}
		})
	}
}

// TestEncodingAgainstRedis replays the vendored table against a live Redis when
// AKI_REDIS_ADDR is set (host:port), so the frozen expectations stay honest.
// Skipped by default; it is the confirmation lever, not a required gate.
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
		key := "aki:zenc:" + tc.name
		c.cmd("DEL", key)
		args := []string{"ZADD", key}
		for i, m := range tc.members {
			args = append(args, strconv.Itoa(i), m)
		}
		if _, err := c.cmd(args...); err != nil {
			t.Fatalf("%s: ZADD: %v", tc.name, err)
		}
		got, err := c.cmd("OBJECT", "ENCODING", key)
		if err != nil {
			t.Fatalf("%s: OBJECT ENCODING: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: live Redis encoding %q, vendored table says %q", tc.name, got, tc.want)
		}
		c.cmd("DEL", key)
	}
}
