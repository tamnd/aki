package set

import "testing"

// globMatch is SSCAN's MATCH filter, the same operator set as Redis's
// stringmatchlen. These cases cover the literal, wildcard, class, and escape
// paths the scan relies on.
func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, str string
		want         bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*o", "hello", true},
		{"h*o", "hero", true},
		{"h*o", "hi", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]t", "hbt", true},
		{"h[a-c]t", "hdt", false},
		{`h\*o`, "h*o", true},
		{`h\*o`, "hxo", false},
		{"user:*", "user:42", true},
		{"user:*", "admin:42", false},
		{"**", "collapse", true},
		{"abc", "abc", true},
		{"abc", "abcd", false},
	}
	for _, tc := range cases {
		if got := globMatch([]byte(tc.pattern), []byte(tc.str)); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pattern, tc.str, got, tc.want)
		}
	}
}

func TestStringEncoding(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"12345", "int"},
		{"-99", "int"},
		{"hello", "embstr"},
		{"", "embstr"},
	}
	for _, tc := range cases {
		if got := stringEncoding([]byte(tc.in)); got != tc.want {
			t.Errorf("stringEncoding(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	long := make([]byte, 45)
	for i := range long {
		long[i] = 'x'
	}
	if got := stringEncoding(long); got != "raw" {
		t.Errorf("stringEncoding(45 bytes) = %q, want raw", got)
	}
}
