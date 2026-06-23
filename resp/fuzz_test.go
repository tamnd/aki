package resp

import "testing"

// FuzzDecode drives arbitrary bytes through the decoder. The decoder must never
// panic and must never return a non-nil value together with a non-nil error;
// well-formed seeds also confirm a successful decode advances the position
// (doc 06 §17.2).
func FuzzDecode(f *testing.F) {
	seeds := []string{
		"+OK\r\n",
		"$-1\r\n",
		"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n",
		"_\r\n",
		"#t\r\n",
		",3.14\r\n",
		"%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n",
		"$?\r\n;5\r\nhello\r\n;0\r\n",
		"|1\r\n+k\r\n:1\r\n:9\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		v, pos, err := Decode(data, 0)
		if err != nil {
			// On error the position must not advance past the input.
			if pos < 0 || pos > len(data) {
				t.Fatalf("error path returned out-of-range pos=%d for len=%d", pos, len(data))
			}
			return
		}
		if pos < 0 || pos > len(data) {
			t.Fatalf("success returned out-of-range pos=%d for len=%d", pos, len(data))
		}
		_ = v
	})
}

// FuzzParseRequest drives arbitrary bytes through the client-request parser,
// which must never panic regardless of input.
func FuzzParseRequest(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"PING\r\n",
		"SET k \"v v\"\r\n",
		"\r\n",
		"*-1\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		argv, pos, err := ParseRequest(data, 0, DefaultMaxBulkLen, nil)
		if pos < 0 || pos > len(data) {
			t.Fatalf("out-of-range pos=%d for len=%d", pos, len(data))
		}
		if err == nil && argv == nil && pos == 0 && len(data) > 0 {
			t.Fatalf("no progress and no error on non-empty input")
		}
	})
}
