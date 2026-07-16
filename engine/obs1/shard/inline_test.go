package shard

import (
	"bytes"
	"fmt"
	"testing"
)

// TestInlineDrainWindowOverrun drives one connection from a single goroutine,
// the doc 08 section 4.1 transport shape, through several reorder rings'
// worth of commands with no separate drainer. Without SetInlineDrain the Do
// throttle would deadlock the moment the issue runs a full ring ahead; with
// it, the throttle drains inline and every reply comes back in issue order.
// The tail past the last throttle entry is drained the way the transport's
// boundary does it: Wait then DrainReplies until nothing is owed.
func TestInlineDrainWindowOverrun(t *testing.T) {
	const shards = 4
	n := 4 * replyRing

	rt := testRuntime(shards)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	var got [][]byte
	emit := func(rep []byte) { got = append(got, append([]byte(nil), rep...)) }
	c.SetInlineDrain(emit)

	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		switch i % 2 {
		case 0: // keyless, round-robins across shards
			p := fmt.Sprintf("e%05d", i)
			want[i] = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p))
			if err := c.Do(opEcho, false, args(p)); err != nil {
				t.Fatal(err)
			}
		case 1: // keyed write, spread over the keyspace
			k := fmt.Sprintf("k%03d", i%251)
			want[i] = []byte("+OK\r\n")
			if err := c.Do(opSet, true, args(k, "v")); err != nil {
				t.Fatal(err)
			}
		}
	}
	c.Flush()
	for c.Owes() {
		if !c.Wait() {
			break
		}
		c.DrainReplies(emit)
	}

	if len(got) != n {
		t.Fatalf("got %d of %d replies", len(got), n)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("reply %d wrong or out of order: got %q, want %q", i, got[i], want[i])
		}
	}
}
