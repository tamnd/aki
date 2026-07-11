package shard

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// The cross-shard tier-two route (txnroute.go): DoTxn's ordering against the
// point traffic around it, the deferral of point commands on keys with queued
// intents, and deadlock freedom with transactions arming in opposite orders.
// The set-level SMOVE semantics ride this substrate and are proven in the set
// package; here the properties are the transport's.

// twoShardKeys returns one key on each of two distinct shards of rt.
func twoShardKeys(t *testing.T, rt *Runtime) (string, string) {
	t.Helper()
	k1 := keyOnShard(t, rt, 0)
	k2 := keyOnShard(t, rt, 1)
	return k1, k2
}

// txnGet reads key inside the transaction through the owner's store,
// returning a copy.
func txnGet(t *Txn, key string) []byte {
	var out []byte
	t.Do([]byte(key), func(cx *Ctx) {
		v, ok := cx.St.Get([]byte(key), nil)
		if ok {
			out = append([]byte(nil), v...)
		}
	})
	return out
}

// txnSet writes key inside the transaction through the owner's store.
func txnSet(t *Txn, key, val string) {
	t.Do([]byte(key), func(cx *Ctx) {
		_ = cx.St.Set([]byte(key), []byte(val))
	})
}

// TestDoTxnReplyInOrder proves the loopback reply lands at the command's
// pipeline slot: commands enqueued before and after the transaction keep
// their positions around it.
func TestDoTxnReplyInOrder(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	c := rt.NewConn()
	if err := c.Do(opSet, true, args(k1, "v1")); err != nil {
		t.Fatal(err)
	}
	err := c.DoTxn(args(k1, k2), func(tx *Txn) []byte {
		txnSet(tx, k2, "moved")
		return []byte(":1\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opGet, true, args(k2)); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	got := collect(t, c, 3)
	if string(got[0]) != "+OK\r\n" {
		t.Fatalf("reply 0 = %q, want +OK", got[0])
	}
	if string(got[1]) != ":1\r\n" {
		t.Fatalf("reply 1 = %q, want the transaction's loopback", got[1])
	}
	if string(got[2]) != "$5\r\nmoved\r\n" {
		t.Fatalf("reply 2 = %q, want the value the transaction wrote", got[2])
	}
}

// TestDoTxnSeesPriorWrites is the ordering-in fence: a write the connection
// enqueued before the transaction is visible inside it, because the arm rides
// the inbound path behind it.
func TestDoTxnSeesPriorWrites(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	c := rt.NewConn()
	for i := 0; i < 50; i++ {
		if err := c.Do(opSet, true, args(k1, fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	var seen []byte
	err := c.DoTxn(args(k1, k2), func(tx *Txn) []byte {
		seen = txnGet(tx, k1)
		return []byte("+DONE\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	collect(t, c, 51)
	if string(seen) != "v49" {
		t.Fatalf("transaction saw %q, want v49 (the last write enqueued before it)", seen)
	}
}

// TestLaterSameConnCommandDefers is the ordering-out fence: a write the
// connection enqueued after the transaction is invisible inside it and lands
// after it, even though the transaction's critical section runs long after
// the write reached its owner.
func TestLaterSameConnCommandDefers(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	c := rt.NewConn()
	if err := c.Do(opSet, true, args(k1, "before")); err != nil {
		t.Fatal(err)
	}
	var seen []byte
	err := c.DoTxn(args(k1, k2), func(tx *Txn) []byte {
		// Give the later SET every chance to reach its owner first; the
		// deferral must still hold it out of the critical section.
		time.Sleep(20 * time.Millisecond)
		seen = txnGet(tx, k1)
		return []byte("+DONE\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opSet, true, args(k1, "after")); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opGet, true, args(k1)); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	got := collect(t, c, 4)
	if string(seen) != "before" {
		t.Fatalf("transaction saw %q, want the pre-transaction value", seen)
	}
	if string(got[3]) != "$5\r\nafter\r\n" {
		t.Fatalf("final read = %q, want the post-transaction value", got[3])
	}
}

// TestPointOpsDeferDuringTxn proves exclusion and its scope: while a
// transaction holds its keys, another connection's point command on one of
// them waits for the release, and a command on an uninvolved key of the same
// shard sails through.
func TestPointOpsDeferDuringTxn(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)
	free := keyOnShard(t, rt, 0)
	for free == k1 {
		free += "x"
		if rt.ShardOf([]byte(free)) != 0 {
			free = keyOnShard(t, rt, 0)
		}
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	c := rt.NewConn()
	err := c.DoTxn(args(k1, k2), func(tx *Txn) []byte {
		txnSet(tx, k1, "held")
		close(entered)
		<-release
		return []byte("+DONE\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	<-entered

	// The uninvolved key answers while the barrier is held.
	c2 := rt.NewConn()
	if err := c2.Do(opSet, true, args(free, "ok")); err != nil {
		t.Fatal(err)
	}
	c2.Flush()
	collect(t, c2, 1)

	// The involved key does not.
	if err := c2.Do(opGet, true, args(k1)); err != nil {
		t.Fatal(err)
	}
	c2.Flush()
	gotReply := false
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if c2.DrainReplies(func([]byte) { gotReply = true }) > 0 {
			break
		}
	}
	if gotReply {
		t.Fatal("a point command on a held key answered inside the critical section")
	}
	close(release)
	got := collect(t, c2, 1)
	if string(got[0]) != "$4\r\nheld\r\n" {
		t.Fatalf("deferred read = %q, want the value the transaction wrote", got[0])
	}
	collect(t, c, 1)
}

// TestDeferredNodeCompletes proves a node that mixes deferred and free
// commands answers everything once the barrier lifts, and that per-key order
// within the deferred run holds.
func TestDeferredNodeCompletes(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	entered := make(chan struct{})
	release := make(chan struct{})
	c := rt.NewConn()
	err := c.DoTxn(args(k1, k2), func(tx *Txn) []byte {
		close(entered)
		<-release
		return []byte("+DONE\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	<-entered

	c2 := rt.NewConn()
	for i := 0; i < 3; i++ {
		if err := c2.Do(opSet, true, args(k1, fmt.Sprintf("d%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := c2.Do(opGet, true, args(k1)); err != nil {
		t.Fatal(err)
	}
	c2.Flush()
	time.Sleep(10 * time.Millisecond)
	close(release)
	got := collect(t, c2, 4)
	if string(got[3]) != "$2\r\nd2\r\n" {
		t.Fatalf("deferred run broke per-key order: final read = %q, want d2", got[3])
	}
	collect(t, c, 1)
}

// TestDoTxnOppositeOrders is the deadlock-freedom stress: transactions over
// the same two keys arming from both directions, interleaved with point
// traffic on both keys, all under the race detector in the -race run. The
// ticket order plus ascending-shard acquisition must let every one complete.
func TestDoTxnOppositeOrders(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	const rounds = 200
	var wg sync.WaitGroup
	run := func(a, b string) {
		defer wg.Done()
		c := rt.NewConn()
		for i := 0; i < rounds; i++ {
			err := c.DoTxn(args(a, b), func(tx *Txn) []byte {
				txnSet(tx, a, "x")
				txnSet(tx, b, "y")
				return []byte("+OK\r\n")
			})
			if err != nil {
				t.Error(err)
				return
			}
			c.Flush()
			collect(t, c, 1)
		}
	}
	point := func(key string) {
		defer wg.Done()
		c := rt.NewConn()
		for i := 0; i < rounds; i++ {
			if err := c.Do(opGet, true, args(key)); err != nil {
				t.Error(err)
				return
			}
			c.Flush()
			collect(t, c, 1)
		}
	}
	wg.Add(4)
	go run(k1, k2)
	go run(k2, k1)
	go point(k1)
	go point(k2)
	wg.Wait()
}

// TestFanSubDefers proves a tier-one fan sub-command parks when one of its
// keys is held: a two-key fan where one key rides free and the other is under
// a barrier answers only after the release, with both keys' work done.
func TestFanSubDefers(t *testing.T) {
	handlers := testHandlers()
	opCount := byte(len(handlers))
	handlers = append(handlers, func(cx *Ctx, a [][]byte, r Reply) {
		n := int64(0)
		for _, k := range a {
			if _, ok := cx.St.Get(k, nil); ok {
				n++
			}
		}
		r.FanCount(n)
	})
	rt := New(4, testArena, testSeg)
	rt.Use(handlers)
	rt.Start()
	defer rt.Stop()
	k1, k2 := twoShardKeys(t, rt)

	seed := rt.NewConn()
	if err := seed.Do(opSet, true, args(k1, "a")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Do(opSet, true, args(k2, "b")); err != nil {
		t.Fatal(err)
	}
	seed.Flush()
	collect(t, seed, 2)

	entered := make(chan struct{})
	release := make(chan struct{})
	c := rt.NewConn()
	err := c.DoTxn(args(k1, k2), func(tx *Txn) []byte {
		close(entered)
		<-release
		return []byte("+DONE\r\n")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	<-entered

	c2 := rt.NewConn()
	if err := c2.DoFan(opCount, FanCount, args(k1, k2), nil); err != nil {
		t.Fatal(err)
	}
	c2.Flush()
	gotReply := false
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if c2.DrainReplies(func([]byte) { gotReply = true }) > 0 {
			break
		}
	}
	if gotReply {
		t.Fatal("a fan sub-command on a held key answered inside the critical section")
	}
	close(release)
	got := collect(t, c2, 1)
	if !bytes.Equal(got[0], []byte(":2\r\n")) {
		t.Fatalf("fan reply = %q, want :2", got[0])
	}
	collect(t, c, 1)
}
