package zset

import (
	"bytes"
	"math"
	"testing"
)

func bb(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// The ZADD option grammar: flag parsing, illegal combinations, and pair-count
// checks all report the exact Redis errors (spec 2064/f3/12 section 6.1).
func TestParseZaddFlags(t *testing.T) {
	cases := []struct {
		name string
		tail []string
		err  string
		fl   flags
		rest []string
	}{
		{"plain pair", []string{"1", "m"}, "", flags{}, []string{"1", "m"}},
		{"nx ch pair", []string{"NX", "CH", "2", "m"}, "", flags{nx: true, ch: true}, []string{"2", "m"}},
		{"gt incr single", []string{"GT", "INCR", "3", "m"}, "", flags{gt: true, incr: true}, []string{"3", "m"}},
		{"case folded", []string{"xx", "1", "m"}, "", flags{xx: true}, []string{"1", "m"}},
		{"nx xx", []string{"NX", "XX", "1", "m"}, "ERR XX and NX options at the same time are not compatible", flags{}, nil},
		{"gt lt", []string{"GT", "LT", "1", "m"}, "ERR GT, LT, and/or NX options at the same time are not compatible", flags{}, nil},
		{"gt nx", []string{"GT", "NX", "1", "m"}, "ERR GT, LT, and/or NX options at the same time are not compatible", flags{}, nil},
		{"odd pairs", []string{"1", "m", "2"}, "ERR syntax error", flags{}, nil},
		{"no pairs", []string{"NX"}, "ERR syntax error", flags{}, nil},
		{"incr two pairs", []string{"INCR", "1", "a", "2", "b"}, "ERR INCR option supports a single increment-element pair", flags{}, nil},
		{"score named like flag", []string{"1", "NX"}, "", flags{}, []string{"1", "NX"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fl, rest, err := parseZaddFlags(bb(tc.tail...))
			if err != tc.err {
				t.Fatalf("err = %q, want %q", err, tc.err)
			}
			if err != "" {
				return
			}
			if fl != tc.fl {
				t.Fatalf("flags = %+v, want %+v", fl, tc.fl)
			}
			if len(rest) != len(tc.rest) {
				t.Fatalf("rest = %v, want %v", rest, tc.rest)
			}
			for i := range rest {
				if !bytes.Equal(rest[i], []byte(tc.rest[i])) {
					t.Fatalf("rest[%d] = %q, want %q", i, rest[i], tc.rest[i])
				}
			}
		})
	}
}

func TestParseScore(t *testing.T) {
	ok := map[string]float64{
		"1": 1, "-2.5": -2.5, "3e2": 300, "inf": math.Inf(1),
		"+inf": math.Inf(1), "-inf": math.Inf(-1), "0": 0,
	}
	for in, want := range ok {
		got, valid := parseScore([]byte(in))
		if !valid || got != want {
			t.Errorf("parseScore(%q) = %v,%v, want %v,true", in, got, valid, want)
		}
	}
	for _, in := range []string{"nan", "", "abc", "1.2.3", " 1", "1 "} {
		if _, valid := parseScore([]byte(in)); valid {
			t.Errorf("parseScore(%q) accepted, want reject", in)
		}
	}
}

func TestParseIndexAndClamp(t *testing.T) {
	if _, ok := parseIndex([]byte("12")); !ok {
		t.Fatal("12 should parse")
	}
	if n, ok := parseIndex([]byte("-3")); !ok || n != -3 {
		t.Fatalf("-3 = %d,%v", n, ok)
	}
	for _, bad := range []string{"", "1a", "-", "+"} {
		if _, ok := parseIndex([]byte(bad)); ok {
			t.Errorf("parseIndex(%q) accepted", bad)
		}
	}
	cases := []struct {
		start, stop, card int
		lo, hi            int
		empty             bool
	}{
		{0, -1, 5, 0, 4, false},   // whole set
		{-2, -1, 5, 3, 4, false},  // last two
		{0, 100, 3, 0, 2, false},  // stop clamps
		{-100, 1, 3, 0, 1, false}, // start clamps to 0
		{2, 1, 5, 0, 0, true},     // inverted
		{5, 6, 5, 0, 0, true},     // start past end
		{0, 0, 0, 0, 0, true},     // empty set
	}
	for _, c := range cases {
		lo, hi, empty := clampRange(c.start, c.stop, c.card)
		if empty != c.empty || (!empty && (lo != c.lo || hi != c.hi)) {
			t.Errorf("clampRange(%d,%d,%d) = %d,%d,%v want %d,%d,%v",
				c.start, c.stop, c.card, lo, hi, empty, c.lo, c.hi, c.empty)
		}
	}
}
