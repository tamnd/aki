package drivers

// The boot pipeline at the server level (spec 2064/obs1 doc 02 section
// 2.5): a node writes over the socket, closes, and a later incarnation
// boots off the same bucket and serves back exactly what was acked,
// with seqs continuing where the last incarnation stopped.

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// bootServer runs Listen with the boot seam over bucket as incarnation
// inc: the production composition, explicit flush cuts only so the test
// controls every commit.
func bootServer(t *testing.T, bucket *sim.Sim, inc uint32) (*Booted, *Server, net.Conn, *bufio.Reader) {
	t.Helper()
	var booted *Booted
	srv, err := Listen(Options{
		Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18,
		ConnShape: testConnShape(), NetDriver: testNetDriver(),
		Boot: func(rt *shard.Runtime) error {
			b, err := BootDurability(context.Background(), BootConfig{
				Store: bucket, Prefix: "p", Node: 0xE7, Incarnation: inc,
				FlushAge: time.Hour, FoldAge: -1,
			}, rt)
			if err != nil {
				return err
			}
			booted = b
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	return booted, srv, nc, bufio.NewReader(nc)
}

// commitAndStop cuts the buffered frames, waits for the chain commit,
// and stops the incarnation in dependency order.
func commitAndStop(t *testing.T, b *Booted, srv *Server, nc net.Conn) {
	t.Helper()
	b.WL.Barrier()
	done := make(chan struct{})
	b.WL.NotifyAllCommitted(func() { close(done) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("commit barrier never fired")
	}
	nc.Close()
	srv.Close()
	if err := b.Close(); err != nil {
		t.Fatalf("pipeline close: %v", err)
	}
}

func TestBootServesAcrossRestart(t *testing.T) {
	bucket := sim.New(sim.Config{})

	// Incarnation 1: a fresh bucket, so boot creates the root and
	// self-grants every group.
	b1, srv1, nc1, r1 := bootServer(t, bucket, 1)
	if b1.Replay.Frames != 0 {
		t.Fatalf("fresh boot replayed %d frames", b1.Replay.Frames)
	}
	send(t, nc1, "SET", "alpha", "one")
	expect(t, r1, "+OK\r\n")
	send(t, nc1, "SET", "bravo", "b1", "PX", "3600000")
	expect(t, r1, "+OK\r\n")
	send(t, nc1, "SET", "gone", "x")
	expect(t, r1, "+OK\r\n")
	send(t, nc1, "DEL", "gone")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "INCR", "ctr")
	expect(t, r1, ":1\r\n")
	commitAndStop(t, b1, srv1, nc1)

	// Incarnation 2: recovery replays the tail into the stores and the
	// runtime serves it back, then keeps writing on continued seqs.
	b2, srv2, nc2, r2 := bootServer(t, bucket, 2)
	if b2.Replay.StrSets == 0 {
		t.Fatalf("reboot replay stats %+v, want strsets", b2.Replay)
	}
	send(t, nc2, "GET", "alpha")
	expect(t, r2, "$3\r\none\r\n")
	send(t, nc2, "GET", "bravo")
	expect(t, r2, "$2\r\nb1\r\n")
	send(t, nc2, "GET", "gone")
	expect(t, r2, "$-1\r\n")
	send(t, nc2, "INCR", "ctr")
	expect(t, r2, ":2\r\n")
	send(t, nc2, "SET", "delta", "d2")
	expect(t, r2, "+OK\r\n")
	commitAndStop(t, b2, srv2, nc2)

	// Incarnation 3: the second incarnation's writes survived too, so
	// StartSeq landed its flushes where recovery can find them.
	b3, srv3, nc3, r3 := bootServer(t, bucket, 3)
	send(t, nc3, "GET", "delta")
	expect(t, r3, "$2\r\nd2\r\n")
	send(t, nc3, "GET", "alpha")
	expect(t, r3, "$3\r\none\r\n")
	send(t, nc3, "INCR", "ctr")
	expect(t, r3, ":3\r\n")
	commitAndStop(t, b3, srv3, nc3)
}

// TestBootServesSetsAcrossRestart drives the set plane through the boot
// seam: creates, removals, an emptying SMOVE over one hash tag so both
// sides ride one keyed run, and a DEL over a set key, all served back by
// the next incarnation from the replayed registries.
func TestBootServesSetsAcrossRestart(t *testing.T) {
	bucket := sim.New(sim.Config{})

	b1, srv1, nc1, r1 := bootServer(t, bucket, 1)
	send(t, nc1, "SADD", "s1", "a", "b", "c")
	expect(t, r1, ":3\r\n")
	send(t, nc1, "SREM", "s1", "b")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "SADD", "nums", "1", "2", "3")
	expect(t, r1, ":3\r\n")
	send(t, nc1, "SADD", "gone", "x")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "DEL", "gone")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "SADD", "{t}src", "m")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "SADD", "{t}dst", "z")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "SMOVE", "{t}src", "{t}dst", "m")
	expect(t, r1, ":1\r\n")
	commitAndStop(t, b1, srv1, nc1)

	b2, srv2, nc2, r2 := bootServer(t, bucket, 2)
	if b2.Replay.SAdds == 0 || b2.Replay.SRems == 0 || b2.Replay.CollDrops == 0 {
		t.Fatalf("reboot replay stats %+v, want set plane counts", b2.Replay)
	}
	send(t, nc2, "SISMEMBER", "s1", "a")
	expect(t, r2, ":1\r\n")
	send(t, nc2, "SISMEMBER", "s1", "b")
	expect(t, r2, ":0\r\n")
	send(t, nc2, "SCARD", "s1")
	expect(t, r2, ":2\r\n")
	send(t, nc2, "SCARD", "nums")
	expect(t, r2, ":3\r\n")
	send(t, nc2, "SISMEMBER", "nums", "2")
	expect(t, r2, ":1\r\n")
	send(t, nc2, "SCARD", "gone")
	expect(t, r2, ":0\r\n")
	send(t, nc2, "SCARD", "{t}src")
	expect(t, r2, ":0\r\n")
	send(t, nc2, "SISMEMBER", "{t}dst", "m")
	expect(t, r2, ":1\r\n")
	send(t, nc2, "SCARD", "{t}dst")
	expect(t, r2, ":2\r\n")
	send(t, nc2, "SADD", "s1", "d")
	expect(t, r2, ":1\r\n")
	commitAndStop(t, b2, srv2, nc2)

	b3, srv3, nc3, r3 := bootServer(t, bucket, 3)
	send(t, nc3, "SCARD", "s1")
	expect(t, r3, ":3\r\n")
	send(t, nc3, "SISMEMBER", "s1", "d")
	expect(t, r3, ":1\r\n")
	commitAndStop(t, b3, srv3, nc3)
}

// TestBootServesHashesAcrossRestart drives the hash plane through the
// boot seam: creates, an overwrite, deletes, a field deadline set and
// persisted, the TTL restore HINCRBY emits behind its hset, and a
// DEL-emptied hash, all served back by the next incarnation.
func TestBootServesHashesAcrossRestart(t *testing.T) {
	bucket := sim.New(sim.Config{})

	b1, srv1, nc1, r1 := bootServer(t, bucket, 1)
	send(t, nc1, "HSET", "h", "f1", "v1", "f2", "v2")
	expect(t, r1, ":2\r\n")
	send(t, nc1, "HDEL", "h", "f2")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "HEXPIREAT", "h", "7000000000", "FIELDS", "1", "f1")
	expect(t, r1, "*1\r\n:1\r\n")
	send(t, nc1, "HINCRBY", "hc", "a", "5")
	expect(t, r1, ":5\r\n")
	send(t, nc1, "HEXPIREAT", "hc", "7000000000", "FIELDS", "1", "a")
	expect(t, r1, "*1\r\n:1\r\n")
	send(t, nc1, "HINCRBY", "hc", "a", "2")
	expect(t, r1, ":7\r\n")
	send(t, nc1, "HSET", "hp", "p", "v")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "HEXPIREAT", "hp", "7000000000", "FIELDS", "1", "p")
	expect(t, r1, "*1\r\n:1\r\n")
	send(t, nc1, "HPERSIST", "hp", "FIELDS", "1", "p")
	expect(t, r1, "*1\r\n:1\r\n")
	send(t, nc1, "HSET", "hd", "x", "1")
	expect(t, r1, ":1\r\n")
	send(t, nc1, "HDEL", "hd", "x")
	expect(t, r1, ":1\r\n")
	commitAndStop(t, b1, srv1, nc1)

	b2, srv2, nc2, r2 := bootServer(t, bucket, 2)
	if b2.Replay.HSets == 0 || b2.Replay.HDels == 0 || b2.Replay.HExpires == 0 {
		t.Fatalf("reboot replay stats %+v, want hash plane counts", b2.Replay)
	}
	send(t, nc2, "HGET", "h", "f1")
	expect(t, r2, "$2\r\nv1\r\n")
	send(t, nc2, "HGET", "h", "f2")
	expect(t, r2, "$-1\r\n")
	send(t, nc2, "HEXPIRETIME", "h", "FIELDS", "1", "f1")
	expect(t, r2, "*1\r\n:7000000000\r\n")
	send(t, nc2, "HGET", "hc", "a")
	expect(t, r2, "$1\r\n7\r\n")
	send(t, nc2, "HEXPIRETIME", "hc", "FIELDS", "1", "a")
	expect(t, r2, "*1\r\n:7000000000\r\n")
	send(t, nc2, "HEXPIRETIME", "hp", "FIELDS", "1", "p")
	expect(t, r2, "*1\r\n:-1\r\n")
	send(t, nc2, "HLEN", "hd")
	expect(t, r2, ":0\r\n")
	send(t, nc2, "HSET", "h", "f3", "v3")
	expect(t, r2, ":1\r\n")
	commitAndStop(t, b2, srv2, nc2)

	b3, srv3, nc3, r3 := bootServer(t, bucket, 3)
	send(t, nc3, "HGET", "h", "f3")
	expect(t, r3, "$2\r\nv3\r\n")
	send(t, nc3, "HLEN", "h")
	expect(t, r3, ":2\r\n")
	commitAndStop(t, b3, srv3, nc3)
}
