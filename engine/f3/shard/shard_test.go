package shard

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"
)

const (
	testArena = 4 << 20
	testSeg   = 1 << 18
)

// collect drains replies from c until want replies have been emitted or the
// deadline passes, returning them in emit order.
func collect(t *testing.T, c *Conn, want int) [][]byte {
	t.Helper()
	var got [][]byte
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < want {
		c.DrainReplies(func(rep []byte) {
			got = append(got, append([]byte(nil), rep...))
		})
		if len(got) < want {
			if time.Now().After(deadline) {
				t.Fatalf("timed out with %d of %d replies", len(got), want)
			}
			runtime.Gosched()
		}
	}
	return got
}

func TestSingleShardSmoke(t *testing.T) {
	rt := New(1, testArena, testSeg)
	rt.Start()
	defer rt.Stop()

	c := rt.NewConn()
	mustDo := func(op byte, key, arg string) {
		t.Helper()
		if err := c.Do(op, []byte(key), []byte(arg)); err != nil {
			t.Fatal(err)
		}
	}
	mustDo(OpPing, "", "")
	mustDo(OpEcho, "", "hello")
	mustDo(OpSet, "k1", "v1")
	mustDo(OpGet, "k1", "")
	mustDo(OpGet, "missing", "")
	mustDo(OpError, "", "unknown command 'NOPE'")
	c.Flush()

	got := collect(t, c, 6)
	want := [][]byte{
		[]byte("+PONG\r\n"),
		[]byte("$5\r\nhello\r\n"),
		[]byte("+OK\r\n"),
		[]byte("$2\r\nv1\r\n"),
		[]byte("$-1\r\n"),
		[]byte("-ERR unknown command 'NOPE'\r\n"),
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("reply %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestWakeFromPark parks every worker by going quiet well past the spin
// window, then pushes and expects the reply: the producer-side wake rule has
// to bring a parked worker back.
func TestWakeFromPark(t *testing.T) {
	rt := New(2, testArena, testSeg)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	for round := 0; round < 3; round++ {
		time.Sleep(5 * time.Millisecond) // well past spinWindow: workers parked
		if err := c.Do(OpEcho, nil, []byte("wake")); err != nil {
			t.Fatal(err)
		}
		if err := c.Do(OpSet, []byte(fmt.Sprintf("wk%d", round)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		c.Flush()
		got := collect(t, c, 2)
		if !bytes.Equal(got[0], []byte("$4\r\nwake\r\n")) || !bytes.Equal(got[1], []byte("+OK\r\n")) {
			t.Fatalf("round %d replies = %q, %q", round, got[0], got[1])
		}
	}
}

// TestBatchOverflowSplits fills past one node's command capacity in a single
// flush window and checks nothing is lost or reordered.
func TestBatchOverflowSplits(t *testing.T) {
	rt := New(1, testArena, testSeg)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	const n = batchCap*3 + 5
	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%03d", i)
		if err := c.Do(OpSet, []byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
		want[i] = []byte("+OK\r\n")
	}
	c.Flush()
	got := collect(t, c, n)
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("reply %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCommandTooBig(t *testing.T) {
	rt := New(1, testArena, testSeg)
	c := rt.NewConn()
	if err := c.Do(OpSet, []byte("k"), make([]byte, batchDataCap+1)); err != ErrTooBig {
		t.Fatalf("err = %v, want ErrTooBig", err)
	}
}
