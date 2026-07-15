package sqlo1

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseCommandMultibulk(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"set", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n", []string{"SET", "k", "hello"}},
		{"empty arg", "*2\r\n$3\r\nGET\r\n$0\r\n\r\n", []string{"GET", ""}},
		{"binary arg", "*2\r\n$3\r\nGET\r\n$3\r\n\x00\r\t\r\n", []string{"GET", "\x00\r\t"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args, n, err := ParseCommand([]byte(tc.in), nil)
			if err != nil {
				t.Fatal(err)
			}
			if n != len(tc.in) {
				t.Fatalf("consumed %d bytes, want %d", n, len(tc.in))
			}
			if len(args) != len(tc.want) {
				t.Fatalf("args = %q, want %q", args, tc.want)
			}
			for i := range args {
				if string(args[i]) != tc.want[i] {
					t.Fatalf("arg %d = %q, want %q", i, args[i], tc.want[i])
				}
			}
		})
	}
}

func TestParseCommandIncremental(t *testing.T) {
	// Feed a pipelined pair of commands one byte at a time; the parser must
	// answer ErrIncomplete until each command is whole, and must never
	// consume a partial one.
	full := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nvv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	firstLen := len("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nvv\r\n")

	for i := range firstLen {
		if _, _, err := ParseCommand(full[:i], nil); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("prefix of %d bytes: err = %v, want ErrIncomplete", i, err)
		}
	}
	args, n, err := ParseCommand(full, nil)
	if err != nil || n != firstLen {
		t.Fatalf("first command: n = %d err = %v, want %d nil", n, err, firstLen)
	}
	if string(args[0]) != "SET" {
		t.Fatalf("first command args = %q", args)
	}
	args, n, err = ParseCommand(full[firstLen:], nil)
	if err != nil || n != len(full)-firstLen {
		t.Fatalf("second command: n = %d err = %v", n, err)
	}
	if string(args[0]) != "GET" || string(args[1]) != "k" {
		t.Fatalf("second command args = %q", args)
	}
}

func TestParseCommandInline(t *testing.T) {
	args, n, err := ParseCommand([]byte("PING\r\n"), nil)
	if err != nil || n != 6 || len(args) != 1 || string(args[0]) != "PING" {
		t.Fatalf("inline PING: args %q n %d err %v", args, n, err)
	}
	args, n, err = ParseCommand([]byte("SET  k   v\n"), nil)
	if err != nil || n != 11 || len(args) != 3 {
		t.Fatalf("inline with runs of spaces: args %q n %d err %v", args, n, err)
	}
	// A bare newline is consumed as an empty command.
	args, n, err = ParseCommand([]byte("\r\n"), nil)
	if err != nil || n != 2 || len(args) != 0 {
		t.Fatalf("empty inline: args %q n %d err %v", args, n, err)
	}
}

func TestParseCommandProtocolErrors(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"bulk without dollar", "*1\r\n:4\r\nPING\r\n"},
		{"negative bulk length", "*1\r\n$-1\r\n"},
		{"junk length", "*x\r\n"},
		{"bad bulk terminator", "*1\r\n$4\r\nPINGXX"},
		{"oversized multibulk", "*99999999999\r\n"},
		{"oversized bulk", "*1\r\n$99999999999\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var pe *ProtoError
			_, _, err := ParseCommand([]byte(tc.in), nil)
			if !errors.As(err, &pe) {
				t.Fatalf("err = %v, want a ProtoError", err)
			}
		})
	}
}

func TestParseCommandEmptyMultibulk(t *testing.T) {
	// *0 and negative counts are consumed as empty commands, like Redis.
	for _, in := range []string{"*0\r\n", "*-1\r\n"} {
		args, n, err := ParseCommand([]byte(in), nil)
		if err != nil || n != len(in) || len(args) != 0 {
			t.Fatalf("%q: args %q n %d err %v", in, args, n, err)
		}
	}
}

func TestParseCommandReusesArgs(t *testing.T) {
	// The connection loop passes args[:0] every command; the slice must
	// come back on every path, including errors, so its capacity is never
	// lost, and a second parse must fully overwrite the first.
	args, _, err := ParseCommand([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n"), nil)
	if err != nil || len(args) != 3 {
		t.Fatalf("first parse: args %q err %v", args, err)
	}
	got := cap(args)

	args, _, err = ParseCommand([]byte("*2\r\n$3\r\n"), args[:0])
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("partial parse err = %v, want ErrIncomplete", err)
	}
	if cap(args) != got {
		t.Fatalf("capacity after ErrIncomplete = %d, want %d kept", cap(args), got)
	}

	args, _, err = ParseCommand([]byte("*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"), args[:0])
	if err != nil || len(args) != 2 || string(args[0]) != "GET" || string(args[1]) != "x" {
		t.Fatalf("reused parse: args %q err %v", args, err)
	}
	if cap(args) != got {
		t.Fatalf("capacity after reuse = %d, want %d kept", cap(args), got)
	}
}

func TestAppendReplies(t *testing.T) {
	cases := []struct {
		got  []byte
		want string
	}{
		{AppendSimple(nil, "OK"), "+OK\r\n"},
		{AppendError(nil, "ERR boom"), "-ERR boom\r\n"},
		{AppendInt(nil, 0), ":0\r\n"},
		{AppendInt(nil, -42), ":-42\r\n"},
		{AppendBulk(nil, []byte("hello")), "$5\r\nhello\r\n"},
		{AppendBulk(nil, nil), "$0\r\n\r\n"},
		{AppendNullBulk(nil), "$-1\r\n"},
		{AppendArray(nil, 3), "*3\r\n"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("built %q, want %q", tc.got, tc.want)
		}
	}
}

func TestBulkSize(t *testing.T) {
	for _, vlen := range []int{0, 1, 9, 10, 99, 100, 12345, maxBulkLen} {
		got := BulkSize(vlen)
		want := len(AppendBulk(nil, make([]byte, vlen)))
		if got != want {
			t.Fatalf("BulkSize(%d) = %d, want %d", vlen, got, want)
		}
	}
}

// encodeCommand renders args as a multibulk request using the reply
// builder, which doubles as the round-trip half of the fuzz target.
func encodeCommand(args [][]byte) []byte {
	b := AppendArray(nil, len(args))
	for _, a := range args {
		b = AppendBulk(b, a)
	}
	return b
}

func FuzzParseCommand(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n"))
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	f.Add([]byte("*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n10\r\n"))
	f.Add([]byte("*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n"))
	f.Add([]byte("*2\r\n$4\r\nECHO\r\n$0\r\n\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("SET k v\n"))
	f.Add([]byte("\r\n"))
	f.Add([]byte("*0\r\n"))
	f.Add([]byte("*-1\r\n"))
	f.Add([]byte("*1\r\n$0\r\n\r\n"))
	f.Add([]byte("$4\r\nPING\r\n"))
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$3\r\n\x00\r\t\r\n"))
	f.Add(bytes.Repeat([]byte("a"), 70000))

	f.Fuzz(func(t *testing.T, data []byte) {
		args, n, err := ParseCommand(data, nil)
		if err != nil {
			if n != 0 {
				t.Fatalf("error path consumed %d bytes", n)
			}
			return
		}
		if n <= 0 || n > len(data) {
			t.Fatalf("consumed %d of %d bytes", n, len(data))
		}
		for _, a := range args {
			if len(a) > maxBulkLen {
				t.Fatalf("argument longer than the bulk limit: %d", len(a))
			}
		}
		if len(args) == 0 {
			return
		}
		// Round trip: re-encode as multibulk and parse again; the argument
		// vectors must match byte for byte.
		enc := encodeCommand(args)
		args2, n2, err := ParseCommand(enc, nil)
		if err != nil || n2 != len(enc) {
			t.Fatalf("re-encoded command failed to parse: n %d err %v", n2, err)
		}
		if len(args2) != len(args) {
			t.Fatalf("round trip arg count %d, want %d", len(args2), len(args))
		}
		for i := range args {
			if !bytes.Equal(args[i], args2[i]) {
				t.Fatalf("round trip arg %d = %q, want %q", i, args2[i], args[i])
			}
		}
	})
}
