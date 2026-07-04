package f1srv

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// Slice 5d covers the key-level operations over a set that engaged intra-key partitioning (spec
// 2064/f1_rewrite_ltm/19 section 6.12): DEL/UNLINK, RENAME/RENAMENX, COPY, EXPIRE-to-the-past, TYPE,
// and OBJECT ENCODING. The audit found these already partition-correct without new code, because the
// whole-set prefix bounds all P partition ranges contiguously (the partition byte sits between the
// set key and the member), rename and copy carry the partition byte verbatim as part of each row
// suffix, and the header copy carries P, while CollRandDrop tears down every partition vector. These
// tests pin that by asserting each op leaves byte-identical observable state across P=1, 2, 4, 8. The
// one real hole slice 5d closes is the pipelined-delete coalescer, which folds a run of same-key
// SREMs under one whole-key stripe lock that does not apply to a partitioned set; TestSetRemCoalesced
// PartitionIdentical drives that run through the real drain loop at every P.

// setEncoding runs OBJECT ENCODING through the routed path and returns the encoding string.
func setEncoding(c *connState, key string) string {
	return call(c, func(c *connState, a [][]byte) { c.cmdObject(a) }, "OBJECT", "ENCODING", key)
}

// setType runs TYPE through the routed path and returns the reply.
func setType(c *connState, key string) string {
	return call(c, func(c *connState, a [][]byte) { c.cmdType(a) }, "TYPE", key)
}

// TestSetDelPartitionIdentical drops a partitioned set with DEL and with UNLINK and asserts the set is
// gone at every P: DEL/UNLINK report 1, and a follow-up SCARD/TYPE see nothing. The whole-set prefix
// bounds every partition's rows, so the range delete plus CollRandDrop must sweep all P partitions and
// their draw vectors; a leftover partition row would surface as a non-zero SCARD after the drop.
func TestSetDelPartitionIdentical(t *testing.T) {
	for _, verb := range []string{"DEL", "UNLINK"} {
		for _, p := range []int{1, 2, 4, 8} {
			srv := newPartServer(t, p)
			c := bareConn(srv)
			var members []string
			for i := 0; i < 200; i++ {
				members = append(members, fmt.Sprintf("m%04d", i))
			}
			loadSet(t, c, "s", members)

			got := call(c, func(c *connState, a [][]byte) { c.cmdDel(a) }, verb, "s")
			if got != ":1\r\n" {
				t.Fatalf("%s P=%d = %q, want :1", verb, p, got)
			}
			if card := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "s"); card != ":0\r\n" {
				t.Fatalf("%s P=%d SCARD after drop = %q, want :0", verb, p, card)
			}
			if ty := setType(c, "s"); ty != "+none\r\n" {
				t.Fatalf("%s P=%d TYPE after drop = %q, want +none", verb, p, ty)
			}
			// Re-adding one member rebuilds the set cleanly: no ghost partition rows survived the drop,
			// so the fresh cardinality is exactly 1.
			loadSet(t, c, "s", []string{"fresh"})
			if card := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "s"); card != ":1\r\n" {
				t.Fatalf("%s P=%d SCARD after rebuild = %q, want :1", verb, p, card)
			}
			srv.Close()
		}
	}
}

// TestSetRenamePartitionIdentical renames a partitioned set (both RENAME onto a fresh key and RENAMENX
// onto a free key) and asserts the destination holds the exact same members, cardinality, and encoding
// at every P while the source is gone. moveIndexedFamily re-keys each row under the destination header
// and carries the partition byte verbatim, so every member stays in its partition id, and moveHeader
// carries P; matching P=1 proves the routed destination reads see all P partitions.
func TestSetRenamePartitionIdentical(t *testing.T) {
	type snap struct {
		members string
		card    string
		enc     string
		srcType string
	}
	run := func(p int, nx bool) snap {
		srv := newPartServer(t, p)
		defer srv.Close()
		c := bareConn(srv)
		var members []string
		for i := 0; i < 200; i++ {
			members = append(members, fmt.Sprintf("m%04d", i))
		}
		loadSet(t, c, "src", members)
		if nx {
			call(c, func(c *connState, a [][]byte) { c.cmdRenameNx(a) }, "RENAMENX", "src", "dst")
		} else {
			call(c, func(c *connState, a [][]byte) { c.cmdRename(a) }, "RENAME", "src", "dst")
		}
		return snap{
			members: strings.Join(smembersSorted(t, c, "dst"), "\x00"),
			card:    call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "dst"),
			enc:     setEncoding(c, "dst"),
			srcType: setType(c, "src"),
		}
	}
	for _, nx := range []bool{false, true} {
		ref := run(1, nx)
		if ref.srcType != "+none\r\n" {
			t.Fatalf("nx=%v P=1 source still present after rename: TYPE=%q", nx, ref.srcType)
		}
		for _, p := range []int{2, 4, 8} {
			got := run(p, nx)
			if got != ref {
				t.Fatalf("nx=%v P=%d rename result %+v differs from P=1 %+v", nx, p, got, ref)
			}
		}
	}
}

// TestSetCopyPartitionIdentical copies a partitioned set and asserts the copy holds identical members,
// cardinality, and encoding while the source stays intact at every P. copyIndexedFamily writes each
// destination row with the partition byte carried verbatim and copyHeader carries P, so the copy is a
// faithful per-partition duplicate; matching P=1 proves every partition was copied and both keys read
// back whole.
func TestSetCopyPartitionIdentical(t *testing.T) {
	type snap struct {
		srcMembers string
		dstMembers string
		srcCard    string
		dstCard    string
		dstEnc     string
	}
	run := func(p int) snap {
		srv := newPartServer(t, p)
		defer srv.Close()
		c := bareConn(srv)
		var members []string
		for i := 0; i < 200; i++ {
			members = append(members, fmt.Sprintf("m%04d", i))
		}
		loadSet(t, c, "src", members)
		if r := call(c, func(c *connState, a [][]byte) { c.cmdCopy(a) }, "COPY", "src", "dst"); r != ":1\r\n" {
			t.Fatalf("P=%d COPY = %q, want :1", p, r)
		}
		return snap{
			srcMembers: strings.Join(smembersSorted(t, c, "src"), "\x00"),
			dstMembers: strings.Join(smembersSorted(t, c, "dst"), "\x00"),
			srcCard:    call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "src"),
			dstCard:    call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "dst"),
			dstEnc:     setEncoding(c, "dst"),
		}
	}
	ref := run(1)
	if ref.srcMembers != ref.dstMembers {
		t.Fatalf("P=1 copy diverged from source: src and dst members differ")
	}
	for _, p := range []int{2, 4, 8} {
		got := run(p)
		if got != ref {
			t.Fatalf("P=%d copy result %+v differs from P=1 %+v", p, got, ref)
		}
	}
}

// TestSetExpirePartitionIdentical sets a past TTL on a partitioned set, which deletes it now, and
// asserts the whole set is gone at every P. The delete runs through dropKeyLocked, whose keySet case
// drops the whole-set index range and every partition vector via CollRandDrop; a partition left behind
// would show up as a non-zero SCARD after the reap.
func TestSetExpirePartitionIdentical(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		var members []string
		for i := 0; i < 200; i++ {
			members = append(members, fmt.Sprintf("m%04d", i))
		}
		loadSet(t, c, "s", members)
		if r := call(c, func(c *connState, a [][]byte) { c.cmdExpire(a) }, "EXPIRE", "s", "-1"); r != ":1\r\n" {
			t.Fatalf("P=%d EXPIRE s -1 = %q, want :1", p, r)
		}
		if card := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "s"); card != ":0\r\n" {
			t.Fatalf("P=%d SCARD after past-TTL expire = %q, want :0", p, card)
		}
		if ty := setType(c, "s"); ty != "+none\r\n" {
			t.Fatalf("P=%d TYPE after expire = %q, want +none", p, ty)
		}
		srv.Close()
	}
}

// TestSetTypeEncodingPartitionIdentical asserts TYPE and OBJECT ENCODING report the same thing for a
// partitioned set as for the unpartitioned one: the header carries the type and the hashtable encoding
// regardless of how many partitions the members scattered across, so partitioning must be invisible to
// both introspection commands.
func TestSetTypeEncodingPartitionIdentical(t *testing.T) {
	var refType, refEnc string
	for i, p := range []int{1, 2, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		var members []string
		for j := 0; j < 200; j++ {
			members = append(members, fmt.Sprintf("m%04d", j))
		}
		loadSet(t, c, "s", members)
		ty := setType(c, "s")
		enc := setEncoding(c, "s")
		if ty != "+set\r\n" {
			t.Fatalf("P=%d TYPE = %q, want +set", p, ty)
		}
		if i == 0 {
			refType, refEnc = ty, enc
		} else if ty != refType || enc != refEnc {
			t.Fatalf("P=%d TYPE/ENCODING (%q/%q) differ from P=1 (%q/%q)", p, ty, enc, refType, refEnc)
		}
		srv.Close()
	}
}

// dialPartServer starts a real socket server with the partition count forced to p and returns a
// connected buffered reader/writer plus a cleanup. It is dialTestServerMode with forceP set, so a test
// can drive the full per-connection drain loop (and its delete coalescer) against a partitioned set
// rather than calling command methods directly. The coalescer only fires from the real drain path, so
// a socket, not a bareConn, is required to cover it.
func dialPartServer(t *testing.T, p int) (*bufio.ReadWriter, func()) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64}
	srv := New(cfg)
	srv.forceP.Store(int64(p))
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	cleanup := func() {
		conn.Close()
		srv.Close()
	}
	return rw, cleanup
}

// TestSetRemCoalescedPartitionIdentical drives a pipelined run of same-key SREMs against a partitioned
// set through the real drain loop and asserts the per-command replies plus the surviving membership and
// cardinality are byte-identical across P=1, 2, 4, 8. This is the slice-5d fix: the coalescer folds a
// same-key SREM run under one whole-key stripe lock, which a partitioned set does not have (its members
// scatter across P partition locks), so drainDelete routes the run to cmdSRemCoalescedPart when p>1.
// The run re-removes an already-removed member so the per-command zero-count branch is exercised, and
// spreads members across every partition of P=8. Matching P=1 proves the routed coalescer removes the
// right member from the right partition and keeps the shared cardinality in step.
func TestSetRemCoalescedPartitionIdentical(t *testing.T) {
	type snap struct {
		replies  []string
		survivor string
		card     string
	}
	run := func(p int) snap {
		rw, cleanup := dialPartServer(t, p)
		defer cleanup()

		// Seed 300 members so they scatter across all P partitions of P=8.
		args := []string{"SADD", "s"}
		for i := 0; i < 300; i++ {
			args = append(args, fmt.Sprintf("m%04d", i))
		}
		cmd(t, rw, args...)
		expect(t, rw, ":300")

		// Pipeline 120 single-member SREMs from one connection so they fold into one coalesced run:
		// remove m0000..m0099, then re-remove m0000..m0019 (already gone, counting 0), so the run
		// carries both hit and miss commands. Each SREM is its own command, so each gets one reply.
		var want []string
		for i := 0; i < 100; i++ {
			writeCmd(t, rw, "SREM", "s", fmt.Sprintf("m%04d", i))
			want = append(want, ":1")
		}
		for i := 0; i < 20; i++ {
			writeCmd(t, rw, "SREM", "s", fmt.Sprintf("m%04d", i))
			want = append(want, ":0")
		}
		if err := rw.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		replies := make([]string, len(want))
		for i := range want {
			replies[i] = readReply(t, rw)
		}

		// A survivor probe (m0100 was never removed) and the shrunk cardinality (300 - 100 = 200).
		cmd(t, rw, "SISMEMBER", "s", "m0100")
		survivor := readReply(t, rw)
		cmd(t, rw, "SCARD", "s")
		card := readReply(t, rw)
		return snap{replies: replies, survivor: survivor, card: card}
	}

	ref := run(1)
	// The unpartitioned run is the oracle: 100 removes then 20 no-ops, survivor present, card 200.
	for i, r := range ref.replies {
		wantVal := ":1"
		if i >= 100 {
			wantVal = ":0"
		}
		if r != wantVal {
			t.Fatalf("P=1 reply %d = %q, want %q", i, r, wantVal)
		}
	}
	if ref.survivor != ":1" {
		t.Fatalf("P=1 survivor SISMEMBER = %q, want :1", ref.survivor)
	}
	if ref.card != ":200" {
		t.Fatalf("P=1 SCARD = %q, want :200", ref.card)
	}
	for _, p := range []int{2, 4, 8} {
		got := run(p)
		if len(got.replies) != len(ref.replies) {
			t.Fatalf("P=%d produced %d replies, want %d", p, len(got.replies), len(ref.replies))
		}
		for i := range ref.replies {
			if got.replies[i] != ref.replies[i] {
				t.Fatalf("P=%d coalesced SREM reply %d = %q, want %q (P=1)", p, i, got.replies[i], ref.replies[i])
			}
		}
		if got.survivor != ref.survivor {
			t.Fatalf("P=%d survivor = %q, want %q (P=1)", p, got.survivor, ref.survivor)
		}
		if got.card != ref.card {
			t.Fatalf("P=%d SCARD = %q, want %q (P=1)", p, got.card, ref.card)
		}
	}
}
