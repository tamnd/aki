package command

import (
	"bufio"
	"net"
	"testing"
)

// readReply reads one full RESP reply already waiting in the buffer and returns
// its leaf tokens flattened in order. Arrays, sets and maps are walked
// recursively. Nulls become "<nil>".
func readReply(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	line := sendLineRead(t, r)
	if line == "" {
		t.Fatal("empty reply line")
	}
	switch line[0] {
	case '+', '-', ':', ',', '#':
		return []string{line[1:]}
	case '_':
		return []string{"<nil>"}
	case '$':
		if line == "$-1" {
			return []string{"<nil>"}
		}
		payload, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulk payload: %v", err)
		}
		return []string{payload[:len(payload)-2]}
	case '*', '~':
		if line == "*-1" {
			return []string{"<nil>"}
		}
		n := arrayLen(t, "*"+line[1:])
		var out []string
		for range n {
			out = append(out, readReply(t, r)...)
		}
		return out
	case '%':
		n := arrayLen(t, "*"+line[1:])
		var out []string
		for range n * 2 {
			out = append(out, readReply(t, r)...)
		}
		return out
	default:
		t.Fatalf("unexpected reply prefix %q", line)
		return nil
	}
}

// xinfoReply sends a command and flattens its reply.
func xinfoReply(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) []string {
	t.Helper()
	if _, err := c.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	return readReply(t, r)
}

// valueAfter returns the token following the first occurrence of name.
func valueAfter(t *testing.T, toks []string, name string) string {
	t.Helper()
	for i, tok := range toks {
		if tok == name && i+1 < len(toks) {
			return toks[i+1]
		}
	}
	t.Fatalf("field %q not found in %v", name, toks)
	return ""
}

func TestXAddMaxLen(t *testing.T) {
	r, c := startData(t)
	for _, id := range []string{"1-1", "2-1", "3-1", "4-1"} {
		_ = bulk(t, r, c, "XADD s MAXLEN 2 "+id+" a 1")
	}
	if got := sendLine(t, r, c, "XLEN s"); got != ":2" {
		t.Fatalf("XLEN after MAXLEN = %q want :2", got)
	}
	// The two highest entries survive.
	got := xentries(t, r, c, "XRANGE s - +")
	if len(got) != 2 || got[0][0] != "3-1" || got[1][0] != "4-1" {
		t.Fatalf("XRANGE after MAXLEN = %v", got)
	}
}

func TestXAddMinID(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = bulk(t, r, c, "XADD s MINID 2 3-1 c 3")
	got := xentries(t, r, c, "XRANGE s - +")
	if len(got) != 2 || got[0][0] != "2-1" {
		t.Fatalf("XRANGE after MINID = %v", got)
	}
}

func TestXAddMaxLenApproxAndLimit(t *testing.T) {
	r, c := startData(t)
	for _, id := range []string{"1-1", "2-1", "3-1"} {
		_ = bulk(t, r, c, "XADD s "+id+" a 1")
	}
	// Approximate form with a LIMIT cap removes at most one here.
	_ = bulk(t, r, c, "XADD s MAXLEN ~ 1 LIMIT 1 4-1 a 1")
	if got := sendLine(t, r, c, "XLEN s"); got != ":3" {
		t.Fatalf("XLEN after LIMIT 1 = %q want :3", got)
	}
}

func TestXAddBadMaxLen(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "XADD s MAXLEN -1 1-1 a 1"); got != "-"+errStreamMaxLenArg {
		t.Fatalf("XADD MAXLEN -1 = %q", got)
	}
	if got := sendLine(t, r, c, "XADD s MAXLEN abc 1-1 a 1"); got != "-"+errStreamMaxLenArg {
		t.Fatalf("XADD MAXLEN abc = %q", got)
	}
}

func TestXTrim(t *testing.T) {
	r, c := startData(t)
	for _, id := range []string{"1-1", "2-1", "3-1", "4-1"} {
		_ = bulk(t, r, c, "XADD s "+id+" a 1")
	}
	if got := sendLine(t, r, c, "XTRIM s MAXLEN 2"); got != ":2" {
		t.Fatalf("XTRIM MAXLEN 2 = %q want :2", got)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != ":2" {
		t.Fatalf("XLEN after XTRIM = %q", got)
	}
	if got := sendLine(t, r, c, "XTRIM s MINID 9"); got != ":2" {
		t.Fatalf("XTRIM MINID 9 = %q want :2", got)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != ":0" {
		t.Fatalf("XLEN after MINID = %q want :0", got)
	}
}

func TestXTrimLimitNoApprox(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	// LIMIT without ~ is a syntax error.
	if got := sendLine(t, r, c, "XTRIM s MAXLEN 1 LIMIT 1"); got != "-ERR syntax error" {
		t.Fatalf("XTRIM LIMIT without ~ = %q", got)
	}
}

func TestXSetID(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 5-5 a 1")
	if got := sendLine(t, r, c, "XSETID s 10-0"); got != "+OK" {
		t.Fatalf("XSETID = %q", got)
	}
	// Next auto entry follows the new last ID.
	id := bulk(t, r, c, "XADD s 10-* a 1")
	if id != "10-1" {
		t.Fatalf("XADD after XSETID = %q want 10-1", id)
	}
	// Lowering below the top present entry fails.
	if got := sendLine(t, r, c, "XSETID s 1-0"); got != "-"+errStreamSetIDSmall {
		t.Fatalf("XSETID lower = %q", got)
	}
}

func TestXSetIDOptions(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	if got := sendLine(t, r, c, "XSETID s 100-0 ENTRIESADDED 50 MAXDELETEDID 2-2"); got != "+OK" {
		t.Fatalf("XSETID options = %q", got)
	}
	toks := xinfoReply(t, r, c, "XINFO STREAM s")
	if got := valueAfter(t, toks, "entries-added"); got != "50" {
		t.Fatalf("entries-added = %q want 50", got)
	}
	if got := valueAfter(t, toks, "max-deleted-entry-id"); got != "2-2" {
		t.Fatalf("max-deleted-entry-id = %q want 2-2", got)
	}
}

func TestXSetIDNoKey(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "XSETID missing 1-0"); got != "-"+errStreamNoSuchKey {
		t.Fatalf("XSETID missing = %q", got)
	}
}

func TestXInfoStreamSummary(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	toks := xinfoReply(t, r, c, "XINFO STREAM s")
	if got := valueAfter(t, toks, "length"); got != "2" {
		t.Fatalf("length = %q want 2", got)
	}
	if got := valueAfter(t, toks, "last-generated-id"); got != "2-1" {
		t.Fatalf("last-generated-id = %q want 2-1", got)
	}
	if got := valueAfter(t, toks, "entries-added"); got != "2" {
		t.Fatalf("entries-added = %q want 2", got)
	}
	if got := valueAfter(t, toks, "recorded-first-entry-id"); got != "1-1" {
		t.Fatalf("recorded-first-entry-id = %q want 1-1", got)
	}
	if got := valueAfter(t, toks, "groups"); got != "0" {
		t.Fatalf("groups = %q want 0", got)
	}
}

func TestXInfoStreamFull(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	toks := xinfoReply(t, r, c, "XINFO STREAM s FULL")
	if got := valueAfter(t, toks, "length"); got != "2" {
		t.Fatalf("FULL length = %q want 2", got)
	}
	// The entries field is present and the IDs appear in the flattened reply.
	if got := valueAfter(t, toks, "entries"); got != "1-1" {
		t.Fatalf("FULL first entry id = %q want 1-1", got)
	}
}

func TestXInfoNoKey(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "XINFO STREAM missing"); got != "-"+errStreamNoSuchKey {
		t.Fatalf("XINFO missing = %q", got)
	}
}

func TestXTrimWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	for _, cmd := range []string{"XTRIM k MAXLEN 1", "XSETID k 1-0", "XINFO STREAM k"} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
