package command

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// startBlockingServer brings up a data-backed server and returns its address so a
// test can open several connections and have one block while another pushes.
func startBlockingServer(t *testing.T) string {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	d := New(Config{Engine: NewEngine(ks)})
	srv := networking.New(networking.Config{Addr: "127.0.0.1:0"}, d)
	d.SetServer(srv)
	go func() { _ = srv.ListenAndServe(networking.Config{Addr: "127.0.0.1:0"}) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	return srv.Addr().String()
}

// dialBlocking opens one client connection to addr.
func dialBlocking(t *testing.T, addr string) (*bufio.Reader, net.Conn) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return bufio.NewReader(c), c
}

// writeCmd sends an inline command without waiting for a reply, so the caller can
// let the connection block.
func writeCmd(t *testing.T, c net.Conn, cmd string) {
	t.Helper()
	if _, err := c.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatal(err)
	}
}

// readLineWait reads one reply line with a generous deadline so a parked reply
// has time to arrive.
func readLineWait(t *testing.T, r *bufio.Reader, c net.Conn) string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// readArrayWait reads an array header and its bulk elements with a wait deadline.
func readArrayWait(t *testing.T, r *bufio.Reader, c net.Conn) []string {
	t.Helper()
	head := readLineWait(t, r, c)
	if head == "" || head[0] != '*' {
		t.Fatalf("expected array header, got %q", head)
	}
	n, err := strconv.Atoi(head[1:])
	if err != nil {
		t.Fatalf("bad array len %q: %v", head, err)
	}
	out := make([]string, n)
	for i := range out {
		h := readLineWait(t, r, c)
		if h == "$-1" || h == "_" {
			out[i] = "<nil>"
			continue
		}
		if h == "" || h[0] != '$' {
			t.Fatalf("expected bulk header, got %q", h)
		}
		out[i] = readLineWait(t, r, c)
	}
	return out
}

func TestBlpopServedByPush(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BLPOP k 5")
	// Let the blocking client register before the push arrives.
	time.Sleep(100 * time.Millisecond)
	if got := sendLine(t, rb, cb, "RPUSH k hello"); got != ":1" {
		t.Fatalf("RPUSH = %q want :1", got)
	}
	got := readArrayWait(t, ra, ca)
	if len(got) != 2 || got[0] != "k" || got[1] != "hello" {
		t.Fatalf("BLPOP = %v want [k hello]", got)
	}
}

func TestBlpopImmediateWhenDataPresent(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b")
	got := array(t, r, c, "BLPOP k 0")
	if len(got) != 2 || got[0] != "k" || got[1] != "a" {
		t.Fatalf("BLPOP present = %v want [k a]", got)
	}
}

func TestBrpopServedByPush(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BRPOP k 5")
	time.Sleep(100 * time.Millisecond)
	_ = sendLine(t, rb, cb, "RPUSH k x y")
	got := readArrayWait(t, ra, ca)
	if len(got) != 2 || got[0] != "k" || got[1] != "y" {
		t.Fatalf("BRPOP = %v want [k y]", got)
	}
}

func TestBlpopTimeout(t *testing.T) {
	r, c := startData(t)
	start := time.Now()
	if got := sendLine(t, r, c, "BLPOP k 0.2"); got != "*-1" {
		t.Fatalf("BLPOP timeout = %q want *-1", got)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("BLPOP returned too soon: %v", elapsed)
	}
}

func TestBlpopMultiKeyFirstNonEmpty(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH second v")
	got := array(t, r, c, "BLPOP first second 0")
	if len(got) != 2 || got[0] != "second" || got[1] != "v" {
		t.Fatalf("BLPOP multi = %v want [second v]", got)
	}
}

func TestBlpopFifoFairness(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)
	rc, cc := dialBlocking(t, addr)

	// First blocker arrives, then the second, so the first must be served first.
	writeCmd(t, ca, "BLPOP k 5")
	time.Sleep(80 * time.Millisecond)
	writeCmd(t, cb, "BLPOP k 5")
	time.Sleep(80 * time.Millisecond)

	_ = sendLine(t, rc, cc, "RPUSH k one two")

	gotA := readArrayWait(t, ra, ca)
	gotB := readArrayWait(t, rb, cb)
	if gotA[1] != "one" {
		t.Fatalf("first blocker got %v want value one", gotA)
	}
	if gotB[1] != "two" {
		t.Fatalf("second blocker got %v want value two", gotB)
	}
}

func TestBlmoveServedByPush(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BLMOVE src dst LEFT RIGHT 5")
	time.Sleep(100 * time.Millisecond)
	_ = sendLine(t, rb, cb, "RPUSH src v")
	if got := readLineWait(t, ra, ca); got != "$1" {
		t.Fatalf("BLMOVE header = %q want $1", got)
	}
	if got := readLineWait(t, ra, ca); got != "v" {
		t.Fatalf("BLMOVE body = %q want v", got)
	}
	// The element landed on dst.
	if got := array(t, rb, cb, "LRANGE dst 0 -1"); len(got) != 1 || got[0] != "v" {
		t.Fatalf("dst after BLMOVE = %v want [v]", got)
	}
}

func TestBrpoplpushServedByPush(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BRPOPLPUSH src dst 5")
	time.Sleep(100 * time.Millisecond)
	_ = sendLine(t, rb, cb, "RPUSH src a b")
	if got := readLineWait(t, ra, ca); got != "$1" {
		t.Fatalf("BRPOPLPUSH header = %q want $1", got)
	}
	if got := readLineWait(t, ra, ca); got != "b" {
		t.Fatalf("BRPOPLPUSH body = %q want b", got)
	}
}

func TestBlmpopServedByPush(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BLMPOP 5 2 k1 k2 LEFT COUNT 10")
	time.Sleep(100 * time.Millisecond)
	_ = sendLine(t, rb, cb, "RPUSH k2 a b c")

	head := readLineWait(t, ra, ca)
	if head != "*2" {
		t.Fatalf("BLMPOP header = %q want *2", head)
	}
	if got := readLineWait(t, ra, ca); got != "$2" {
		t.Fatalf("BLMPOP key header = %q want $2", got)
	}
	if got := readLineWait(t, ra, ca); got != "k2" {
		t.Fatalf("BLMPOP key = %q want k2", got)
	}
	if got := readLineWait(t, ra, ca); got != "*3" {
		t.Fatalf("BLMPOP elems header = %q want *3", got)
	}
	for _, want := range []string{"a", "b", "c"} {
		_ = readLineWait(t, ra, ca) // $1
		if got := readLineWait(t, ra, ca); got != want {
			t.Fatalf("BLMPOP elem = %q want %q", got, want)
		}
	}
}

func TestBlmpopTimeout(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BLMPOP 0.1 1 k LEFT"); got != "*-1" {
		t.Fatalf("BLMPOP timeout = %q want *-1", got)
	}
}

func TestClientUnblockTimeout(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	idLine := sendLine(t, ra, ca, "CLIENT ID")
	id := idLine[1:]

	writeCmd(t, ca, "BLPOP k 0")
	time.Sleep(100 * time.Millisecond)
	if got := sendLine(t, rb, cb, "CLIENT UNBLOCK "+id); got != ":1" {
		t.Fatalf("CLIENT UNBLOCK = %q want :1", got)
	}
	if got := readLineWait(t, ra, ca); got != "*-1" {
		t.Fatalf("unblocked BLPOP = %q want *-1", got)
	}
}

func TestClientUnblockError(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	id := sendLine(t, ra, ca, "CLIENT ID")[1:]
	writeCmd(t, ca, "BLPOP k 0")
	time.Sleep(100 * time.Millisecond)
	if got := sendLine(t, rb, cb, "CLIENT UNBLOCK "+id+" ERROR"); got != ":1" {
		t.Fatalf("CLIENT UNBLOCK ERROR = %q want :1", got)
	}
	got := readLineWait(t, ra, ca)
	if !strings.HasPrefix(got, "-UNBLOCKED") {
		t.Fatalf("unblocked BLPOP = %q want -UNBLOCKED...", got)
	}
}

func TestClientUnblockNotBlocked(t *testing.T) {
	r, c := startData(t)
	id := sendLine(t, r, c, "CLIENT ID")[1:]
	if got := sendLine(t, r, c, "CLIENT UNBLOCK "+id); got != ":0" {
		t.Fatalf("CLIENT UNBLOCK idle = %q want :0", got)
	}
}

func TestBlpopInsideMultiDoesNotBlock(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "BLPOP k 0")
	// EXEC replies an array of one element: the BLPOP result, a null array because
	// the key is empty and the transaction must not park.
	if got := sendLine(t, r, c, "EXEC"); got != "*1" {
		t.Fatalf("EXEC header = %q want *1", got)
	}
	if got := sendLineRead(t, r); got != "*-1" {
		t.Fatalf("BLPOP in MULTI = %q want *-1", got)
	}
}

func TestBlpopNegativeTimeout(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BLPOP k -1"); got != "-ERR timeout is negative" {
		t.Fatalf("BLPOP -1 = %q", got)
	}
}

func TestBlpopBadTimeout(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BLPOP k notanumber"); got != "-ERR timeout is not a float or out of range" {
		t.Fatalf("BLPOP bad timeout = %q", got)
	}
}

func TestBlpopWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "BLPOP k 0"); got != "-"+wrongTypeError {
		t.Fatalf("BLPOP wrongtype = %q", got)
	}
}
