package drivers

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"testing"
)

// readRESP reads one full RESP2 value and returns it in a generic shape: a bulk
// string as string, an integer as int64, an array as []any, and either null form
// as nil. The COMMAND replies nest arrays inside arrays, so the command tests
// walk this rather than the flat readArrayBulks helper.
func readRESP(t *testing.T, br *bufio.Reader) any {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read RESP line: %v", err)
	}
	if len(line) < 3 {
		t.Fatalf("short RESP line %q", line)
	}
	body := strings.TrimSuffix(line[1:], "\r\n")
	switch line[0] {
	case '+':
		return body
	case '-':
		return errorReply(body)
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			t.Fatalf("integer %q: %v", body, err)
		}
		return n
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			t.Fatalf("bulk length %q: %v", body, err)
		}
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			t.Fatalf("bulk payload: %v", err)
		}
		return string(buf[:n])
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			t.Fatalf("array length %q: %v", body, err)
		}
		if n < 0 {
			return nil
		}
		out := make([]any, n)
		for i := range out {
			out[i] = readRESP(t, br)
		}
		return out
	default:
		t.Fatalf("unknown RESP prefix %q", line)
		return nil
	}
}

// errorReply marks a RESP error payload so a test can tell it apart from a status
// bulk of the same text.
type errorReply string

func sendCmd(t *testing.T, br *bufio.Reader, nc interface{ Write([]byte) (int, error) }, args ...string) any {
	t.Helper()
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	if _, err := nc.Write([]byte(b.String())); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
	return readRESP(t, br)
}

// specOf finds the ten-element spec whose name matches want in a COMMAND or
// COMMAND INFO reply array and returns it, or nil if absent.
func specOf(reply any, want string) []any {
	arr, ok := reply.([]any)
	if !ok {
		return nil
	}
	for _, e := range arr {
		spec, ok := e.([]any)
		if !ok || len(spec) < 6 {
			continue
		}
		if name, ok := spec[0].(string); ok && name == want {
			return spec
		}
	}
	return nil
}

// assertSpec checks a spec's name, arity, and first/last/step key positions.
func assertSpec(t *testing.T, spec []any, name string, arity, first, last, step int64) {
	t.Helper()
	if spec == nil {
		t.Fatalf("spec for %q missing", name)
	}
	if len(spec) != 10 {
		t.Fatalf("spec %q has %d elements, want 10", name, len(spec))
	}
	if got := spec[0]; got != name {
		t.Fatalf("name = %v, want %q", got, name)
	}
	if got := spec[1]; got != arity {
		t.Fatalf("%s arity = %v, want %d", name, got, arity)
	}
	if got := spec[3]; got != first {
		t.Fatalf("%s first key = %v, want %d", name, got, first)
	}
	if got := spec[4]; got != last {
		t.Fatalf("%s last key = %v, want %d", name, got, last)
	}
	if got := spec[5]; got != step {
		t.Fatalf("%s step = %v, want %d", name, got, step)
	}
	// The four unmodeled fields are empty arrays.
	for _, i := range []int{2, 6, 7, 8, 9} {
		if a, ok := spec[i].([]any); !ok || len(a) != 0 {
			t.Fatalf("%s field %d = %v, want empty array", name, i, spec[i])
		}
	}
}

// TestCommandCount checks COMMAND COUNT returns a positive integer, the size of
// the dispatch table.
func TestCommandCount(t *testing.T) {
	_, nc, br := startServer(t)
	n, ok := sendCmd(t, br, nc, "COMMAND", "COUNT").(int64)
	if !ok || n <= 0 {
		t.Fatalf("COMMAND COUNT = %v, want positive integer", n)
	}
	// The full COMMAND listing must carry exactly that many specs.
	all, ok := sendCmd(t, br, nc, "COMMAND").([]any)
	if !ok {
		t.Fatalf("COMMAND = %T, want array", all)
	}
	if int64(len(all)) != n {
		t.Fatalf("COMMAND listed %d specs, COUNT said %d", len(all), n)
	}
}

// TestCommandInfoSpecs checks the arity and key positions COMMAND INFO reports
// for a fixed single-key command, a variadic single-key command, a fan command,
// and the paired-tail MSET.
func TestCommandInfoSpecs(t *testing.T) {
	_, nc, br := startServer(t)
	reply := sendCmd(t, br, nc, "COMMAND", "INFO", "GET", "SET", "DEL", "MGET", "MSET")
	assertSpec(t, specOf(reply, "get"), "get", 2, 1, 1, 1)
	assertSpec(t, specOf(reply, "set"), "set", -3, 1, 1, 1)
	assertSpec(t, specOf(reply, "del"), "del", -2, 1, -1, 1)
	assertSpec(t, specOf(reply, "mget"), "mget", -2, 1, -1, 1)
	assertSpec(t, specOf(reply, "mset"), "mset", -3, 1, -1, 2)
}

// TestCommandInfoUnknown checks a name the table does not hold answers with a
// null array in its slot, not an error, matching Redis.
func TestCommandInfoUnknown(t *testing.T) {
	_, nc, br := startServer(t)
	reply, ok := sendCmd(t, br, nc, "COMMAND", "INFO", "nosuchcmd").([]any)
	if !ok || len(reply) != 1 {
		t.Fatalf("COMMAND INFO nosuchcmd = %v, want one-element array", reply)
	}
	if reply[0] != nil {
		t.Fatalf("unknown command slot = %v, want null array", reply[0])
	}
}

// TestCommandDocs checks COMMAND DOCS answers the empty map (f3 ships no static
// help), which clients tolerate as no hints rather than an error.
func TestCommandDocs(t *testing.T) {
	_, nc, br := startServer(t)
	reply, ok := sendCmd(t, br, nc, "COMMAND", "DOCS").([]any)
	if !ok || len(reply) != 0 {
		t.Fatalf("COMMAND DOCS = %v, want empty array", reply)
	}
}

// TestCommandGetKeys checks key extraction for a single-key command, a paired
// MSET, a multi-key MGET, and the no-key and unknown-command error paths.
func TestCommandGetKeys(t *testing.T) {
	_, nc, br := startServer(t)

	if got := sendCmd(t, br, nc, "COMMAND", "GETKEYS", "SET", "foo", "bar"); !keysEqual(got, "foo") {
		t.Fatalf("GETKEYS SET = %v, want [foo]", got)
	}
	if got := sendCmd(t, br, nc, "COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"); !keysEqual(got, "a", "b") {
		t.Fatalf("GETKEYS MSET = %v, want [a b]", got)
	}
	if got := sendCmd(t, br, nc, "COMMAND", "GETKEYS", "MGET", "a", "b", "c"); !keysEqual(got, "a", "b", "c") {
		t.Fatalf("GETKEYS MGET = %v, want [a b c]", got)
	}
	if _, ok := sendCmd(t, br, nc, "COMMAND", "GETKEYS", "PING").(errorReply); !ok {
		t.Fatalf("GETKEYS PING did not error")
	}
	if _, ok := sendCmd(t, br, nc, "COMMAND", "GETKEYS", "nosuchcmd", "x").(errorReply); !ok {
		t.Fatalf("GETKEYS nosuchcmd did not error")
	}
}

// TestCommandBadSubcommand checks an unknown subcommand errors.
func TestCommandBadSubcommand(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "COMMAND", "NOPE").(errorReply); !ok {
		t.Fatalf("COMMAND NOPE did not error")
	}
}

func keysEqual(reply any, want ...string) bool {
	arr, ok := reply.([]any)
	if !ok || len(arr) != len(want) {
		return false
	}
	for i, w := range want {
		if arr[i] != w {
			return false
		}
	}
	return true
}
