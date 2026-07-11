package set

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// The differential encoding suite. Every case below carries the encoding real
// Redis reports for the same members under the default config (spec 2064/f3/11
// section 3):
//
//	set-max-intset-entries   512
//	set-max-listpack-entries 128
//	set-max-listpack-value   64
//
// The expectations are vendored so the table runs offline; TestEncodingAgainstRedis
// replays the same table against a live server when AKI_REDIS_ADDR points at one,
// so a Redis version bump that moved a threshold shows up as a failure here rather
// than as a silent parity drift.

// buildSet applies members to a fresh set in order and returns it, mirroring how
// SADD builds one member at a time (order matters for the conversion path).
func buildSet(members []string) *set {
	if len(members) == 0 {
		return nil
	}
	s := newSet([]byte(members[0]))
	for _, m := range members {
		s.add([]byte(m))
	}
	return s
}

// rep expands a generator into a member list for the large cases without
// bloating the table literal.
func ints(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = strconv.Itoa(i)
	}
	return out
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
	{"few ints", []string{"1", "2", "3"}, "intset"},
	{"negative ints", []string{"-5", "0", "9999999999"}, "intset"},
	{"ints at cap", ints(maxIntsetEntries), "intset"},
	{"ints past 128 stay intset", ints(200), "intset"},
	{"ints over cap", ints(maxIntsetEntries + 1), "hashtable"},
	{"non-int forces listpack", []string{"1", "2", "apple"}, "listpack"},
	{"all words", []string{"apple", "banana"}, "listpack"},
	{"word first then ints", []string{"tag", "1", "2"}, "listpack"},
	{"listpack at entry cap", append(words(maxListpackEntries-1), "seed"), "listpack"},
	{"listpack over entry cap", append(words(maxListpackEntries), "seed"), "hashtable"},
	{"member at value cap", []string{"seed", repeat("a", maxListpackValue)}, "listpack"},
	{"member over value cap", []string{"seed", repeat("a", maxListpackValue+1)}, "hashtable"},
	{"huge int string is a word", []string{"seed", repeat("9", 40)}, "listpack"},
}

func TestEncodingParity(t *testing.T) {
	for _, tc := range encodingCases {
		t.Run(tc.name, func(t *testing.T) {
			s := buildSet(tc.members)
			if got := s.enc.String(); got != tc.want {
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
		key := "aki:enc:" + tc.name
		c.cmd("DEL", key)
		args := append([]string{"SADD", key}, tc.members...)
		if _, err := c.cmd(args...); err != nil {
			t.Fatalf("%s: SADD: %v", tc.name, err)
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
