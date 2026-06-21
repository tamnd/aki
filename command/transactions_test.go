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

// startDataTwo brings up one server backed by a shared keyspace and returns two
// separate client connections to it, so a test can have one client watch a key
// while another modifies it.
func startDataTwo(t *testing.T) (*bufio.Reader, net.Conn, *bufio.Reader, net.Conn) {
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
	go func() { _ = srv.ListenAndServe(networking.Config{Addr: "127.0.0.1:0"}) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	dial := func() (*bufio.Reader, net.Conn) {
		conn, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		return bufio.NewReader(conn), conn
	}
	r1, c1 := dial()
	r2, c2 := dial()
	return r1, c1, r2, c2
}

// readResp reads one full RESP reply and returns it as a flattened string slice.
// Aggregate types contribute their header line followed by their elements, which
// is enough to assert on shape and leaf values in these tests.
func readResp(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	line := sendLineRead(t, r)
	if line == "" {
		t.Fatal("empty reply line")
	}
	switch line[0] {
	case '+', '-', ':', '_', ',', '#':
		return []string{line}
	case '$':
		if line == "$-1" {
			return []string{line}
		}
		return []string{line, sendLineRead(t, r)}
	case '*':
		out := []string{line}
		if line == "*-1" {
			return out
		}
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			t.Fatalf("bad array len %q", line)
		}
		for range n {
			out = append(out, readResp(t, r)...)
		}
		return out
	default:
		t.Fatalf("unexpected reply prefix %q", line)
		return nil
	}
}

func TestMultiExecReplies(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "MULTI"); got != "+OK" {
		t.Fatalf("MULTI = %q", got)
	}
	if got := sendLine(t, r, c, "SET k 1"); got != "+QUEUED" {
		t.Fatalf("queued SET = %q", got)
	}
	if got := sendLine(t, r, c, "INCR k"); got != "+QUEUED" {
		t.Fatalf("queued INCR = %q", got)
	}
	if _, err := c.Write([]byte("EXEC\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	want := []string{"*2", "+OK", ":2"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("EXEC reply = %v want %v", got, want)
	}
}

func TestExecWithoutMulti(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "EXEC"); got != "-ERR EXEC without MULTI" {
		t.Fatalf("EXEC without MULTI = %q", got)
	}
}

func TestDiscardWithoutMulti(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "DISCARD"); got != "-ERR DISCARD without MULTI" {
		t.Fatalf("DISCARD without MULTI = %q", got)
	}
}

func TestDiscard(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "SET k 1")
	if got := sendLine(t, r, c, "DISCARD"); got != "+OK" {
		t.Fatalf("DISCARD = %q", got)
	}
	// The queued SET was thrown away, so the key is absent.
	if got := bulk(t, r, c, "GET k"); got != "<nil>" {
		t.Fatalf("GET after DISCARD = %q", got)
	}
}

func TestNestedMulti(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	if got := sendLine(t, r, c, "MULTI"); got != "-ERR MULTI calls can not be nested" {
		t.Fatalf("nested MULTI = %q", got)
	}
}

func TestWatchInsideMulti(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	if got := sendLine(t, r, c, "WATCH k"); got != "-ERR WATCH inside MULTI is not allowed" {
		t.Fatalf("WATCH inside MULTI = %q", got)
	}
}

func TestExecAbortOnBadCommand(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "SET k 1")
	if got := sendLine(t, r, c, "NOSUCHCMD"); !strings.HasPrefix(got, "-ERR unknown command") {
		t.Fatalf("queued bad command = %q", got)
	}
	got := sendLine(t, r, c, "EXEC")
	if got != "-EXECABORT Transaction discarded because of previous errors." {
		t.Fatalf("EXEC = %q", got)
	}
}

func TestExecAbortOnBadArity(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	if got := sendLine(t, r, c, "GET"); !strings.HasPrefix(got, "-ERR wrong number of arguments") {
		t.Fatalf("queued bad arity = %q", got)
	}
	if got := sendLine(t, r, c, "EXEC"); got != "-EXECABORT Transaction discarded because of previous errors." {
		t.Fatalf("EXEC = %q", got)
	}
}

func TestWatchUnchangedRuns(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k 1")
	_ = sendLine(t, r, c, "WATCH k")
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "INCR k")
	if _, err := c.Write([]byte("EXEC\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	want := []string{"*1", ":2"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("EXEC after unchanged WATCH = %v want %v", got, want)
	}
}

func TestWatchChangedAborts(t *testing.T) {
	r, c, r2, c2 := startDataTwo(t)
	_ = sendLine(t, r, c, "SET k 1")
	_ = sendLine(t, r, c, "WATCH k")
	// A second connection modifies the watched key.
	if got := sendLine(t, r2, c2, "SET k 99"); got != "+OK" {
		t.Fatalf("second client SET = %q", got)
	}
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "INCR k")
	if got := sendLine(t, r, c, "EXEC"); got != "*-1" {
		t.Fatalf("EXEC after changed WATCH = %q want *-1", got)
	}
}

func TestWatchSelfModifyAborts(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k 1")
	_ = sendLine(t, r, c, "WATCH k")
	// The same client modifies the watched key before MULTI.
	_ = sendLine(t, r, c, "SET k 2")
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "INCR k")
	if got := sendLine(t, r, c, "EXEC"); got != "*-1" {
		t.Fatalf("EXEC after self-modify = %q want *-1", got)
	}
}

func TestUnwatchClears(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k 1")
	_ = sendLine(t, r, c, "WATCH k")
	if got := sendLine(t, r, c, "UNWATCH"); got != "+OK" {
		t.Fatalf("UNWATCH = %q", got)
	}
	// k changes, but it is no longer watched, so EXEC still runs.
	_ = sendLine(t, r, c, "SET k 5")
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "INCR k")
	if _, err := c.Write([]byte("EXEC\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	want := []string{"*1", ":6"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("EXEC after UNWATCH = %v want %v", got, want)
	}
}
