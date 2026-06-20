package resp

import (
	"errors"
	"reflect"
	"testing"
)

func argvStrings(argv [][]byte) []string {
	out := make([]string, len(argv))
	for i, a := range argv {
		out[i] = string(a)
	}
	return out
}

func TestParseMultibulk(t *testing.T) {
	in := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	argv, pos, err := ParseRequest(in, 0, DefaultMaxBulkLen)
	if err != nil {
		t.Fatal(err)
	}
	if pos != len(in) {
		t.Fatalf("pos=%d want %d", pos, len(in))
	}
	if got := argvStrings(argv); !reflect.DeepEqual(got, []string{"SET", "foo", "bar"}) {
		t.Fatalf("argv=%v", got)
	}
}

func TestParsePipeline(t *testing.T) {
	in := []byte("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	pos := 0
	var cmds [][]string
	for pos < len(in) {
		argv, newPos, err := ParseRequest(in, pos, DefaultMaxBulkLen)
		if err != nil {
			t.Fatal(err)
		}
		if argv != nil {
			cmds = append(cmds, argvStrings(argv))
		}
		pos = newPos
	}
	want := [][]string{{"PING"}, {"GET", "k"}}
	if !reflect.DeepEqual(cmds, want) {
		t.Fatalf("cmds=%v want %v", cmds, want)
	}
}

func TestParsePartialResumes(t *testing.T) {
	full := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	// A split mid-bulk must return ErrNeedMore without consuming anything.
	for i := 1; i < len(full); i++ {
		_, pos, err := ParseRequest(full[:i], 0, DefaultMaxBulkLen)
		if !errors.Is(err, ErrNeedMore) {
			t.Fatalf("prefix %d: err=%v want ErrNeedMore", i, err)
		}
		if pos != 0 {
			t.Fatalf("prefix %d: pos=%d want 0", i, pos)
		}
	}
}

func TestParseInlineSimple(t *testing.T) {
	argv, pos, err := ParseRequest([]byte("PING\r\n"), 0, DefaultMaxBulkLen)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 6 || !reflect.DeepEqual(argvStrings(argv), []string{"PING"}) {
		t.Fatalf("argv=%v pos=%d", argvStrings(argv), pos)
	}

	// Bare LF terminator (no CR) is accepted.
	argv, _, err = ParseRequest([]byte("SET foo bar\n"), 0, DefaultMaxBulkLen)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(argvStrings(argv), []string{"SET", "foo", "bar"}) {
		t.Fatalf("argv=%v", argvStrings(argv))
	}
}

func TestParseInlineQuoting(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"SET key \"hello world\"\r\n", []string{"SET", "key", "hello world"}},
		{"SET key 'foo bar'\r\n", []string{"SET", "key", "foo bar"}},
		{"k \"a\\tb\"\r\n", []string{"k", "a\tb"}},
		{"k \"a\\x41b\"\r\n", []string{"k", "aAb"}},
		{"a    b\tc\r\n", []string{"a", "b", "c"}},
		{"k \"\"\r\n", []string{"k", ""}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			argv, _, err := ParseRequest([]byte(tc.in), 0, DefaultMaxBulkLen)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(argvStrings(argv), tc.want) {
				t.Fatalf("argv=%v want %v", argvStrings(argv), tc.want)
			}
		})
	}
}

func TestParseInlineUnbalancedQuotes(t *testing.T) {
	_, _, err := ParseRequest([]byte("SET key \"unterminated\r\n"), 0, DefaultMaxBulkLen)
	var pe ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("err=%v want ProtocolError", err)
	}
}

func TestParseBlankLineSkipped(t *testing.T) {
	in := []byte("\r\nPING\r\n")
	argv, pos, err := ParseRequest(in, 0, DefaultMaxBulkLen)
	if err != nil || argv != nil || pos != 2 {
		t.Fatalf("blank line: argv=%v pos=%d err=%v", argv, pos, err)
	}
	argv, _, err = ParseRequest(in, pos, DefaultMaxBulkLen)
	if err != nil || !reflect.DeepEqual(argvStrings(argv), []string{"PING"}) {
		t.Fatalf("after blank: argv=%v err=%v", argvStrings(argv), err)
	}
}

func TestParseProtocolErrors(t *testing.T) {
	var pe ProtocolError
	// An element that is not a bulk string is the "expected '$'" fatal error.
	_, _, err := ParseRequest([]byte("*2\r\n+notbulk\r\n"), 0, DefaultMaxBulkLen)
	if !errors.As(err, &pe) {
		t.Fatalf("expected-dollar: err=%v want ProtocolError", err)
	}

	// Multibulk count over the cap is a hard error.
	_, _, err = ParseRequest([]byte("*1048577\r\n"), 0, DefaultMaxBulkLen)
	if !errors.As(err, &pe) {
		t.Fatalf("over-cap: err=%v want ProtocolError", err)
	}

	// Bulk length over the configured cap is a hard error.
	_, _, err = ParseRequest([]byte("*1\r\n$100\r\n"), 0, 10)
	if !errors.As(err, &pe) {
		t.Fatalf("over-bulk-cap: err=%v want ProtocolError", err)
	}
}

func TestParseTooBigInline(t *testing.T) {
	big := make([]byte, MaxInlineLen+10)
	for i := range big {
		big[i] = 'x'
	}
	_, _, err := ParseRequest(big, 0, DefaultMaxBulkLen)
	var pe ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("err=%v want ProtocolError", err)
	}
}
