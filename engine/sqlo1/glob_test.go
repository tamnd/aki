package sqlo1

import (
	"strings"
	"testing"
)

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		// Literals and the empty corners. Upstream's while loop needs
		// both sides non-empty, so * alone does not match the empty
		// string; the port keeps the quirk.
		{"", "", true},
		{"", "x", false},
		{"x", "", false},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"*", "", false},
		{"*", "x", true},

		// Stars.
		{"h*llo", "hello", true},
		{"h*llo", "hllo", true},
		{"h*llo", "heeeello", true},
		{"h*llo", "hell", false},
		{"a*", "a", true},
		{"a*b*", "ab", true},
		{"**", "anything", true},
		{"a*b", "a", false},
		{"*x*", "axb", true},
		{"*x*", "ab", false},

		// Question marks.
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"???", "abc", true},
		{"???", "ab", false},

		// Classes, negation, ranges (including a reversed range).
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"[a-c]", "b", true},
		{"[a-c]", "d", false},
		{"[c-a]", "b", true},
		{"[\\]]", "]", true},
		{"[\\-]", "-", true},

		// The unclosed-class quirk: the class ends at the pattern's
		// end as if the ] were there.
		{"[abc", "a", true},
		{"[abc", "d", false},
		{"h[", "h", false},

		// Escapes.
		{"\\*", "*", true},
		{"\\*", "x", false},
		{"a\\?c", "a?c", true},
		{"a\\?c", "abc", false},
		{"a\\", "a\\", true},

		// Field-name shapes HSCAN MATCH sees.
		{"f0*", "f077", true},
		{"f0*", "f177", false},
		{"*:cart", "user:42:cart", true},
	}
	for _, c := range cases {
		if got := globMatch([]byte(c.pattern), []byte(c.s)); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

// TestGlobMatchNoBlowup is the skipLongerMatches guard: the classic
// exponential-backtrack pattern must fail fast, not hang the server
// on a hostile MATCH argument.
func TestGlobMatchNoBlowup(t *testing.T) {
	pattern := []byte(strings.Repeat("a*", 20) + "b")
	s := []byte(strings.Repeat("a", 200))
	if globMatch(pattern, s) {
		t.Fatal("pattern cannot match a string with no b")
	}
}
