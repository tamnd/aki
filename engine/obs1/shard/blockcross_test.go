package shard

import (
	"sync"
	"testing"
	"time"
)

// The cross-shard blocking substrate (txnroute.go DoBlockCross, intent.go
// PostOwner and the opAsync owner hop, handler.go the Ctx runtime accessors):
// DoBlockCross is DoTxn with a park option, so its ordering properties are the
// route's, and the two new primitives are the serve-time side-hop a parked
// cross-shard block leans on. The list package proves the verb semantics on top;
// here the properties are the transport's.

// TestDoBlockCrossServesInOrder proves the immediate-serve case is exactly
// DoTxn's loopback: a body that returns a reply lands it at the command's
// pipeline slot, between the write before it and the read after it.
func TestDoBlockCrossServesInOrder(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	c := rt.NewConn()
	if err := c.Do(opSet, true, args(k1, "v1")); err != nil {
		t.Fatal(err)
	}
	err := c.DoBlockCross(args(k1, k2), func(tx *Txn, conn *Conn, seq uint32) []byte {
		txnSet(tx, k2, "moved")
		return []byte(":1\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	if err := c.Do(opGet, true, args(k2)); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	got := collect(t, c, 3)
	want := []string{"+OK\r\n", ":1\r\n", "$5\r\nmoved\r\n"}
	for i, exp := range want {
		if string(got[i]) != exp {
			t.Fatalf("reply %d = %q, want %q", i, got[i], exp)
		}
	}
}

// TestDoBlockCrossParkDefersReply proves the park case: a body that returns nil
// delivers no reply now, holds the reorder cursor (a read enqueued after it
// stalls), and completes at its own sequence only when CompleteBlocked fires,
// exactly the seam a co-located BLPOP rides.
func TestDoBlockCrossParkDefersReply(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	c := rt.NewConn()
	if err := c.Do(opSet, true, args(k1, "before")); err != nil {
		t.Fatal(err)
	}
	target := make(chan blockInfo, 1)
	err := c.DoBlockCross(args(k1, k2), func(tx *Txn, conn *Conn, seq uint32) []byte {
		// The intents are held here; park by capturing the target and returning nil.
		target <- blockInfo{conn: conn, seq: seq}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	if err := c.Do(opGet, true, args(k1)); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	var got [][]byte
	emit := func(rep []byte) { got = append(got, append([]byte(nil), rep...)) }
	// The SET emits; the park holds its own slot and the read behind it.
	deadline := time.Now().Add(2 * time.Second)
	for len(got) < 1 {
		c.DrainReplies(emit)
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the pre-block reply")
		}
	}
	if len(got) != 1 || string(got[0]) != "+OK\r\n" {
		t.Fatalf("before completion got %v, want only +OK", got)
	}
	if !c.Blocked() {
		t.Fatal("barrier disarmed while the parked cross block is still owed")
	}

	info := <-target
	info.conn.CompleteBlocked(info.seq, []byte(":7\r\n"))
	got = got[:0]
	rest := collect(t, c, 2)
	if string(rest[0]) != ":7\r\n" || string(rest[1]) != "$6\r\nbefore\r\n" {
		t.Fatalf("after completion got %q %q, want :7 then before", rest[0], rest[1])
	}
	if c.Blocked() {
		t.Fatal("barrier still armed after the parked reply emitted")
	}
}

// TestPostOwnerRunsOnTarget proves PostOwner runs its closure on the named
// shard's owner: the closure reads ShardID and reports it, and it matches the
// shard the key routes to, for every shard.
func TestPostOwnerRunsOnTarget(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()

	for sh := 0; sh < 4; sh++ {
		got := make(chan int, 1)
		rt.PostOwner(sh, func(cx *Ctx) { got <- cx.ShardID() })
		select {
		case id := <-got:
			if id != sh {
				t.Fatalf("PostOwner(%d) ran on shard %d", sh, id)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("PostOwner(%d) never ran", sh)
		}
	}
}

// TestPostOwnerOrderedAfterEnqueue proves a PostOwner closure posted after a
// transaction's work runs on the owner after that work, the ordering a serve's
// cancel hop relies on: the closure observes a value a just-run Do wrote.
func TestPostOwnerOrderedAfterEnqueue(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k := keyOnShard(t, rt, 2)
	sh := rt.ShardOf([]byte(k))

	c := rt.NewConn()
	got := make(chan []byte, 1)
	err := c.DoBlockCross(args(k), func(tx *Txn, conn *Conn, seq uint32) []byte {
		txnSet(tx, k, "written")
		rt.PostOwner(sh, func(cx *Ctx) {
			v, _ := cx.St.Get([]byte(k), nil)
			got <- append([]byte(nil), v...)
		})
		return []byte("+OK\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	c.Flush()
	collect(t, c, 1)
	select {
	case v := <-got:
		if string(v) != "written" {
			t.Fatalf("PostOwner saw %q, want the transaction's write", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PostOwner closure never ran")
	}
}

// TestDoBlockCrossParkConcurrent races many parked cross blocks against their
// completions from a separate goroutine, meaningful under the race detector: the
// substrate must deliver every one exactly once and in sequence order.
func TestDoBlockCrossParkConcurrent(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	c := rt.NewConn()
	const k = 40
	targets := make(chan blockInfo, k)
	for i := 0; i < k; i++ {
		if err := c.DoBlockCross(args(k1, k2), func(tx *Txn, conn *Conn, seq uint32) []byte {
			targets <- blockInfo{conn: conn, seq: seq}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	c.Flush()

	seqs := make([]uint32, 0, k)
	for i := 0; i < k; i++ {
		seqs = append(seqs, (<-targets).seq)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := len(seqs) - 1; i >= 0; i-- {
			c.CompleteBlocked(seqs[i], []byte(":1\r\n"))
		}
	}()
	got := collect(t, c, k)
	wg.Wait()
	for i := 0; i < k; i++ {
		if string(got[i]) != ":1\r\n" {
			t.Fatalf("reply %d = %q, want :1", i, got[i])
		}
	}
}
