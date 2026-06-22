package command

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// cmdErr marks a string as an error reply so a test can tell it apart from a
// simple string.
type cmdErr string

// readReplyValue reads one full RESP reply and returns it as a Go value: a string
// for simple strings and bulk strings, int64 for integers, a []any for arrays,
// and nil for null. It is enough to assert on the nested COMMAND replies.
func readReplyValue(t *testing.T, r *bufio.Reader) any {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		t.Fatalf("empty reply line")
	}
	switch line[0] {
	case '+':
		return line[1:]
	case '-':
		return cmdErr(line[1:])
	case ':':
		n, _ := strconv.ParseInt(line[1:], 10, 64)
		return n
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil
		}
		out := make([]any, n)
		for i := range out {
			out[i] = readReplyValue(t, r)
		}
		return out
	default:
		t.Fatalf("unexpected reply type %q", line)
		return nil
	}
}

// sendReply writes an inline command and returns the parsed reply.
func sendReply(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatalf("write %q: %v", cmd, err)
	}
	return readReplyValue(t, r)
}

// asArray fails unless v is a RESP array.
func asArray(t *testing.T, v any) []any {
	t.Helper()
	a, ok := v.([]any)
	if !ok {
		t.Fatalf("expected array, got %T (%v)", v, v)
	}
	return a
}

// bulkSlice turns an array of bulk strings into a []string.
func bulkSlice(t *testing.T, v any) []string {
	t.Helper()
	a := asArray(t, v)
	out := make([]string, len(a))
	for i, e := range a {
		s, ok := e.(string)
		if !ok {
			t.Fatalf("element %d not a bulk string: %T", i, e)
		}
		out[i] = s
	}
	return out
}

// hasStr reports whether s contains want.
func hasStr(s []string, want string) bool {
	for _, v := range s {
		if v == want {
			return true
		}
	}
	return false
}

func TestCommandInfoGet(t *testing.T) {
	r, c := startData(t)
	reply := sendReply(t, r, c, "COMMAND INFO get")
	outer := asArray(t, reply)
	if len(outer) != 1 {
		t.Fatalf("COMMAND INFO get returned %d entries", len(outer))
	}
	info := asArray(t, outer[0])
	if len(info) != 10 {
		t.Fatalf("info entry has %d fields, want 10", len(info))
	}
	if info[0] != "get" {
		t.Fatalf("name = %v", info[0])
	}
	if info[1] != int64(2) {
		t.Fatalf("arity = %v", info[1])
	}
	if info[3] != int64(1) || info[4] != int64(1) || info[5] != int64(1) {
		t.Fatalf("key spec = %v %v %v", info[3], info[4], info[5])
	}
	flags := bulkSlice(t, info[2])
	if !hasStr(flags, "readonly") {
		t.Fatalf("flags missing readonly: %v", flags)
	}
}

func TestCommandInfoUnknown(t *testing.T) {
	r, c := startData(t)
	reply := sendReply(t, r, c, "COMMAND INFO nosuchcmd")
	outer := asArray(t, reply)
	if len(outer) != 1 || outer[0] != nil {
		t.Fatalf("unknown command should give one null element, got %v", outer)
	}
}

func TestCommandInfoContainerHasSubcommands(t *testing.T) {
	r, c := startData(t)
	reply := sendReply(t, r, c, "COMMAND INFO config")
	info := asArray(t, asArray(t, reply)[0])
	subs := asArray(t, info[9])
	if len(subs) == 0 {
		t.Fatalf("config should report its subcommands")
	}
}

func TestCommandListAll(t *testing.T) {
	r, c := startData(t)
	names := bulkSlice(t, sendReply(t, r, c, "COMMAND LIST"))
	if !hasStr(names, "get") || !hasStr(names, "set") {
		t.Fatalf("COMMAND LIST missing core commands")
	}
}

func TestCommandListPattern(t *testing.T) {
	r, c := startData(t)
	names := bulkSlice(t, sendReply(t, r, c, "COMMAND LIST FILTERBY PATTERN ge*"))
	if !hasStr(names, "get") || !hasStr(names, "getset") {
		t.Fatalf("pattern filter missing matches: %v", names)
	}
	if hasStr(names, "set") {
		t.Fatalf("pattern filter let a non-match through: %v", names)
	}
}

func TestCommandListAclcat(t *testing.T) {
	r, c := startData(t)
	names := bulkSlice(t, sendReply(t, r, c, "COMMAND LIST FILTERBY ACLCAT string"))
	if !hasStr(names, "get") || !hasStr(names, "set") {
		t.Fatalf("@string category missing core string commands: %v", names)
	}
}

func TestCommandListModuleEmpty(t *testing.T) {
	r, c := startData(t)
	names := bulkSlice(t, sendReply(t, r, c, "COMMAND LIST FILTERBY MODULE foo"))
	if len(names) != 0 {
		t.Fatalf("module filter should be empty, got %v", names)
	}
}

func TestCommandGetKeysFixed(t *testing.T) {
	r, c := startData(t)
	keys := bulkSlice(t, sendReply(t, r, c, "COMMAND GETKEYS set k v"))
	if len(keys) != 1 || keys[0] != "k" {
		t.Fatalf("GETKEYS set = %v", keys)
	}
}

func TestCommandGetKeysMSet(t *testing.T) {
	r, c := startData(t)
	keys := bulkSlice(t, sendReply(t, r, c, "COMMAND GETKEYS mset k1 v1 k2 v2"))
	if len(keys) != 2 || keys[0] != "k1" || keys[1] != "k2" {
		t.Fatalf("GETKEYS mset = %v", keys)
	}
}

func TestCommandGetKeysDel(t *testing.T) {
	r, c := startData(t)
	keys := bulkSlice(t, sendReply(t, r, c, "COMMAND GETKEYS del a b c"))
	if len(keys) != 3 {
		t.Fatalf("GETKEYS del = %v", keys)
	}
}

func TestCommandGetKeysZUnionStore(t *testing.T) {
	r, c := startData(t)
	keys := bulkSlice(t, sendReply(t, r, c, "COMMAND GETKEYS zunionstore dest 2 src1 src2"))
	want := []string{"dest", "src1", "src2"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("GETKEYS zunionstore = %v", keys)
	}
}

func TestCommandGetKeysLMPop(t *testing.T) {
	r, c := startData(t)
	keys := bulkSlice(t, sendReply(t, r, c, "COMMAND GETKEYS lmpop 2 l1 l2 LEFT"))
	if strings.Join(keys, ",") != "l1,l2" {
		t.Fatalf("GETKEYS lmpop = %v", keys)
	}
}

func TestCommandGetKeysSortStore(t *testing.T) {
	r, c := startData(t)
	keys := bulkSlice(t, sendReply(t, r, c, "COMMAND GETKEYS sort mylist STORE dest"))
	if strings.Join(keys, ",") != "mylist,dest" {
		t.Fatalf("GETKEYS sort = %v", keys)
	}
}

func TestCommandGetKeysXRead(t *testing.T) {
	r, c := startData(t)
	keys := bulkSlice(t, sendReply(t, r, c, "COMMAND GETKEYS xread COUNT 2 STREAMS s1 s2 0 0"))
	if strings.Join(keys, ",") != "s1,s2" {
		t.Fatalf("GETKEYS xread = %v", keys)
	}
}

func TestCommandGetKeysNoKeys(t *testing.T) {
	r, c := startData(t)
	reply := sendReply(t, r, c, "COMMAND GETKEYS ping")
	e, ok := reply.(cmdErr)
	if !ok || !strings.Contains(string(e), "no key arguments") {
		t.Fatalf("GETKEYS ping = %v", reply)
	}
}

func TestCommandGetKeysUnknown(t *testing.T) {
	r, c := startData(t)
	reply := sendReply(t, r, c, "COMMAND GETKEYS nosuchcmd a b")
	e, ok := reply.(cmdErr)
	if !ok || !strings.Contains(string(e), "Invalid command") {
		t.Fatalf("GETKEYS unknown = %v", reply)
	}
}

func TestCommandGetKeysAndFlags(t *testing.T) {
	r, c := startData(t)
	reply := sendReply(t, r, c, "COMMAND GETKEYSANDFLAGS set k v")
	outer := asArray(t, reply)
	if len(outer) != 1 {
		t.Fatalf("GETKEYSANDFLAGS set returned %d entries", len(outer))
	}
	pair := asArray(t, outer[0])
	if pair[0] != "k" {
		t.Fatalf("key = %v", pair[0])
	}
	flags := bulkSlice(t, pair[1])
	if !hasStr(flags, "RW") || !hasStr(flags, "update") {
		t.Fatalf("flags = %v", flags)
	}
}
