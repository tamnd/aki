package resp

import (
	"bytes"
	"testing"
)

func parseOne(t *testing.T, in string) ([][]byte, int, Status) {
	t.Helper()
	var p Parser
	return p.Next([]byte(in))
}

func TestParseArrayForm(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", []string{"ECHO", "hello"}},
		{"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n", []string{"SET", "k", ""}},
		{"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n", []string{"SET", "k", "v", "EX", "10"}},
	}
	for _, c := range cases {
		args, consumed, st := parseOne(t, c.in)
		if st != OK {
			t.Fatalf("%q: status %v", c.in, st)
		}
		if consumed != len(c.in) {
			t.Fatalf("%q: consumed %d, want %d", c.in, consumed, len(c.in))
		}
		if len(args) != len(c.want) {
			t.Fatalf("%q: %d args, want %d", c.in, len(args), len(c.want))
		}
		for i := range args {
			if string(args[i]) != c.want[i] {
				t.Fatalf("%q: arg %d = %q, want %q", c.in, i, args[i], c.want[i])
			}
		}
	}
}

func TestParseInlineForm(t *testing.T) {
	args, consumed, st := parseOne(t, "SET foo bar\r\n")
	if st != OK || consumed != 13 || len(args) != 3 {
		t.Fatalf("inline: args %q consumed %d status %v", args, consumed, st)
	}
	// Bare LF, extra whitespace.
	args, consumed, st = parseOne(t, "  PING\t \n")
	if st != OK || consumed != 9 || len(args) != 1 || string(args[0]) != "PING" {
		t.Fatalf("inline ws: args %q consumed %d status %v", args, consumed, st)
	}
	// A blank line consumes and yields an empty command the caller skips.
	args, consumed, st = parseOne(t, "\r\n")
	if st != OK || consumed != 2 || len(args) != 0 {
		t.Fatalf("blank line: args %q consumed %d status %v", args, consumed, st)
	}
}

func TestParseSkippedArrays(t *testing.T) {
	for _, in := range []string{"*0\r\n", "*-1\r\n"} {
		args, consumed, st := parseOne(t, in)
		if st != OK || consumed != len(in) || len(args) != 0 {
			t.Fatalf("%q: args %q consumed %d status %v", in, args, consumed, st)
		}
	}
}

func TestParseNeedMore(t *testing.T) {
	whole := "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
	for i := 0; i < len(whole); i++ {
		_, consumed, st := parseOne(t, whole[:i])
		if st != NeedMore || consumed != 0 {
			t.Fatalf("prefix %d: consumed %d status %v", i, consumed, st)
		}
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		in  string
		msg string
	}{
		{"*abc\r\n", "invalid multibulk length"},
		{"*2x\r\n", "invalid multibulk length"},
		{"*\r\n", "invalid multibulk length"},
		{"*2000000\r\n", "invalid multibulk length"},
		{"*111111111111111111111\r\n", "invalid multibulk length"},
		{"*1\r\n@4\r\nPING\r\n", "expected '$', got '@'"},
		{"*1\r\n$-1\r\n", "invalid bulk length"},
		{"*1\r\n$abc\r\n", "invalid bulk length"},
		{"*1\r\n$4x\r\nPING\r\n", "invalid bulk length"},
		{"*1\r\n$999999999999\r\n", "invalid bulk length"},
	}
	for _, c := range cases {
		var p Parser
		_, consumed, st := p.Next([]byte(c.in))
		if st != ProtoErr || consumed != 0 {
			t.Fatalf("%q: consumed %d status %v", c.in, consumed, st)
		}
		if p.LastError() != c.msg {
			t.Fatalf("%q: error %q, want %q", c.in, p.LastError(), c.msg)
		}
	}
}

func TestParseTooBigInline(t *testing.T) {
	var p Parser
	long := bytes.Repeat([]byte{'a'}, maxInline+1)
	if _, _, st := p.Next(long); st != ProtoErr {
		t.Fatalf("unterminated oversize line: status %v", st)
	}
	// The cap is on the line, not the buffer fill: a newline past the cap is
	// the same error, so chopped and whole-buffer parses agree.
	if _, _, st := p.Next(append(long, '\n')); st != ProtoErr {
		t.Fatalf("terminated oversize line: status %v", st)
	}
	if p.LastError() != "too big inline request" {
		t.Fatalf("error %q", p.LastError())
	}
}

// TestParsePipelineEverySplit parses a canonical pipeline chopped at every
// byte offset and checks each chop parses the same command sequence as the
// whole buffer, the restart-from-head contract the driver leans on.
func TestParsePipelineEverySplit(t *testing.T) {
	pipeline := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n" +
		"PING\r\n" +
		"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n" +
		"*0\r\n" +
		"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n")
	want := parseStream(t, pipeline, len(pipeline))
	for chunk := 1; chunk < len(pipeline); chunk++ {
		got := parseStream(t, pipeline, chunk)
		if len(got) != len(want) {
			t.Fatalf("chunk %d: %d commands, want %d", chunk, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("chunk %d command %d = %q, want %q", chunk, i, got[i], want[i])
			}
		}
	}
}

// parseStream feeds data in chunk-sized reads through a fresh parser and
// returns each parsed command as a joined string.
func parseStream(t *testing.T, data []byte, chunk int) []string {
	t.Helper()
	var p Parser
	var out []string
	buf := make([]byte, 0, len(data))
	fed, pos := 0, 0
	for {
		args, consumed, st := p.Next(buf[pos:])
		switch st {
		case OK:
			if consumed <= 0 {
				t.Fatal("OK with nothing consumed")
			}
			pos += consumed
			if len(args) > 0 {
				var b bytes.Buffer
				for i, a := range args {
					if i > 0 {
						b.WriteByte(' ')
					}
					b.Write(a)
				}
				out = append(out, b.String())
			}
		case NeedMore:
			if fed == len(data) {
				if pos != len(buf) {
					t.Fatalf("stream ended with %d unparsed bytes", len(buf)-pos)
				}
				return out
			}
			n := fed + chunk
			if n > len(data) {
				n = len(data)
			}
			buf = append(buf, data[fed:n]...)
			fed = n
		default:
			t.Fatalf("protocol error mid-stream: %s", p.LastError())
		}
	}
}
