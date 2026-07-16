package stream

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// The command-level harness: the real stream handlers on a shard runtime, driven
// the way the hash and list slices drive theirs. It exercises routing, reply
// framing, WRONGTYPE, and the registry the way a client would, without a socket.

const (
	opXadd byte = iota + 1
	opXlen
	opXdel
	opXsetid
	opXtrim
	opXrange
	opXrevrange
	opXread
	opXgroup
	opXinfo
	opXreadgroup
	opXack
	opXpending
	opXclaim
	opXautoclaim
	opXnack
	opObject
	opSet // seed a string key to test WRONGTYPE and the OBJECT fallthrough
	opLast
)

func harnessHandlers() []shard.Handler {
	h := make([]shard.Handler, opLast)
	h[opXadd] = Xadd
	h[opXlen] = Xlen
	h[opXdel] = Xdel
	h[opXsetid] = Xsetid
	h[opXtrim] = Xtrim
	h[opXrange] = Xrange
	h[opXrevrange] = Xrevrange
	h[opXread] = Xread
	h[opXgroup] = Xgroup
	h[opXinfo] = Xinfo
	h[opXreadgroup] = Xreadgroup
	h[opXack] = Xack
	h[opXpending] = Xpending
	h[opXclaim] = Xclaim
	h[opXautoclaim] = Xautoclaim
	h[opXnack] = Xnack
	h[opObject] = Object
	h[opSet] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		if err := cx.St.Set(args[0], args[1]); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
		r.Status("OK")
	}
	return h
}

func newHarness(t *testing.T) *shard.Runtime {
	t.Helper()
	rt := shard.New(1, 8<<20, 1<<18)
	rt.Use(harnessHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// do sends one first-argument-keyed command and returns its whole raw reply.
func do(t *testing.T, c *shard.Conn, op byte, a ...string) []byte {
	t.Helper()
	return doAt(t, c, op, 0, a...)
}

// doAt sends a command routed by args[keyIdx] (OBJECT keys on args[1]).
func doAt(t *testing.T, c *shard.Conn, op byte, keyIdx int, a ...string) []byte {
	t.Helper()
	args := make([][]byte, len(a))
	for i := range a {
		args[i] = []byte(a[i])
	}
	if err := c.DoAt(op, keyIdx, args); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	var rep []byte
	deadline := time.Now().Add(10 * time.Second)
	for rep == nil {
		c.DrainReplies(func(b []byte) { rep = append([]byte(nil), b...) })
		if rep == nil {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for a reply")
			}
			runtime.Gosched()
		}
	}
	return rep
}

// --- RESP decoding for assertions ----------------------------------------

type errReply struct{ msg string }

func decodeReply(t *testing.T, b []byte) any {
	t.Helper()
	v, rest := decodeOne(t, b)
	if len(rest) != 0 {
		t.Fatalf("trailing bytes after reply: %q", rest)
	}
	return v
}

func decodeOne(t *testing.T, b []byte) (any, []byte) {
	t.Helper()
	if len(b) == 0 {
		t.Fatal("empty reply buffer")
	}
	line, rest := splitLine(t, b)
	switch line[0] {
	case '+', ':':
		return string(line[1:]), rest
	case '-':
		return errReply{string(line[1:])}, rest
	case '$':
		n, _ := strconv.Atoi(string(line[1:]))
		if n < 0 {
			return nil, rest
		}
		v := string(rest[:n])
		return v, rest[n+2:] // skip payload + CRLF
	case '*':
		n, _ := strconv.Atoi(string(line[1:]))
		if n < 0 {
			return nil, rest
		}
		out := make([]any, n)
		for i := 0; i < n; i++ {
			out[i], rest = decodeOne(t, rest)
		}
		return out, rest
	default:
		t.Fatalf("unexpected reply byte %q", line)
		return nil, nil
	}
}

func splitLine(t *testing.T, b []byte) (line, rest []byte) {
	t.Helper()
	for i := 0; i+1 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' {
			return b[:i], b[i+2:]
		}
	}
	t.Fatalf("no CRLF in %q", b)
	return nil, nil
}

// --- assertion helpers ----------------------------------------------------

func wantInt(t *testing.T, raw []byte, n int64) {
	t.Helper()
	got := decodeReply(t, raw)
	s, ok := got.(string)
	if !ok || s != strconv.FormatInt(n, 10) {
		t.Fatalf("reply = %v, want integer %d", render(got), n)
	}
}

func wantBulk(t *testing.T, raw []byte, want string) {
	t.Helper()
	got := decodeReply(t, raw)
	s, ok := got.(string)
	if !ok || s != want {
		t.Fatalf("reply = %v, want bulk %q", render(got), want)
	}
}

// bulkReply returns a bulk-string reply's payload, failing on any other shape.
func bulkReply(t *testing.T, raw []byte) string {
	t.Helper()
	got := decodeReply(t, raw)
	s, ok := got.(string)
	if !ok {
		t.Fatalf("reply = %v, want bulk", render(got))
	}
	return s
}

func wantStatus(t *testing.T, raw []byte, want string) { wantBulk(t, raw, want) }

func wantNil(t *testing.T, raw []byte) {
	t.Helper()
	if got := decodeReply(t, raw); got != nil {
		t.Fatalf("reply = %v, want nil", render(got))
	}
}

func wantErr(t *testing.T, raw []byte, want string) {
	t.Helper()
	got := decodeReply(t, raw)
	e, ok := got.(errReply)
	if !ok || e.msg != want {
		t.Fatalf("reply = %v, want error %q", render(got), want)
	}
}

func render(v any) string {
	switch x := v.(type) {
	case nil:
		return "(nil)"
	case errReply:
		return "(error) " + x.msg
	case []any:
		return fmt.Sprintf("%v", x)
	default:
		return fmt.Sprintf("%q", x)
	}
}
