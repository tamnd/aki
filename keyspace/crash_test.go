package keyspace

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// These tests are the crash-injection and durability layer of doc 23 section 8,
// adapted to the engine as it stands today. aki commits with a double meta page:
// every commit writes its dirty pages, fsyncs, writes a fresh meta to the
// non-live slot, and fsyncs again. That second fsync is the linearization point.
// A crash before it leaves the previous meta live, so the transaction rolls back
// whole. These tests drive a crash at every fsync of a multi-round workload and
// check that recovery always lands on a clean commit boundary, never a torn one.
//
// One thing these tests do not yet cover is a torn write to a data page that the
// live snapshot still references. The B-tree updates pages in place, so that case
// needs the redo WAL to be recoverable, and the WAL sidecar is not wired into the
// pager yet (doc 04). When it lands, the torn-data-page scenarios from doc 23
// section 8.5 go here too.

const (
	crashRounds   = 6 // commits in the workload
	crashPerRound = 8 // payload keys written per round
)

// roundKey and roundVal name the payload key and value for entry i of round r.
func roundKey(r, i int) []byte { return fmt.Appendf(nil, "r%d:k%d", r, i) }
func roundVal(r, i int) []byte { return fmt.Appendf(nil, "r%d:v%d", r, i) }

// runCrashWorkload writes crashRounds commits to db0. Each round sets its payload
// keys and then a "gen" marker holding the round number, all in one commit, so a
// reader after recovery can tell exactly how many rounds committed. It stops at
// the first commit that returns an injected-crash error and returns nil.
func runCrashWorkload(ks *Keyspace) {
	db := ks.dbs[0]
	for r := 1; r <= crashRounds; r++ {
		for i := range crashPerRound {
			if err := db.Set(roundKey(r, i), roundVal(r, i), TypeString, EncRaw, -1); err != nil {
				return
			}
		}
		if err := db.Set([]byte("gen"), []byte(strconv.Itoa(r)), TypeString, EncRaw, -1); err != nil {
			return
		}
		if err := ks.Commit(); err != nil {
			return
		}
	}
}

// verifyCleanPrefix reopens the file behind mem and checks that the visible state
// is exactly the keys of some whole number of committed rounds: rounds 1..g all
// present with their expected values, rounds past g entirely absent. A torn commit
// would show up as a partial round or a gen ahead of its payload.
func verifyCleanPrefix(t *testing.T, mem vfs.VFS) {
	t.Helper()
	p, err := pager.Open(mem, "crash.aki", pager.Options{})
	if err != nil {
		t.Fatalf("reopen pager: %v", err)
	}
	defer func() { _ = p.Close() }()
	ks, err := Open(p)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}
	db := ks.dbs[0]

	g := 0
	if body, _, found, err := db.Peek([]byte("gen")); err != nil {
		t.Fatalf("peek gen: %v", err)
	} else if found {
		if g, err = strconv.Atoi(string(body)); err != nil {
			t.Fatalf("gen marker %q is not a number: %v", body, err)
		}
	}
	if g < 0 || g > crashRounds {
		t.Fatalf("gen %d out of range 0..%d", g, crashRounds)
	}

	for r := 1; r <= g; r++ {
		for i := range crashPerRound {
			body, _, found, err := db.Peek(roundKey(r, i))
			if err != nil {
				t.Fatalf("peek r%d k%d: %v", r, i, err)
			}
			if !found {
				t.Fatalf("committed round %d missing key %d (gen=%d)", r, i, g)
			}
			if want := roundVal(r, i); string(body) != string(want) {
				t.Fatalf("r%d k%d = %q want %q", r, i, body, want)
			}
		}
	}
	for r := g + 1; r <= crashRounds; r++ {
		for i := range crashPerRound {
			if _, _, found, err := db.Peek(roundKey(r, i)); err != nil {
				t.Fatalf("peek uncommitted r%d k%d: %v", r, i, err)
			} else if found {
				t.Fatalf("uncommitted round %d key %d is present (gen=%d): torn commit", r, i, g)
			}
		}
	}

	checks, err := ks.Check()
	if err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	for _, c := range checks {
		if c.StructErr != nil {
			t.Fatalf("db%d structural error after recovery: %v", c.Index, c.StructErr)
		}
		if c.OrderErrors != 0 {
			t.Fatalf("db%d has %d out-of-order entries after recovery", c.Index, c.OrderErrors)
		}
		if c.BadHeaders != 0 {
			t.Fatalf("db%d has %d bad value headers after recovery", c.Index, c.BadHeaders)
		}
	}
}

// TestCrashRecoveryAtEachFsync crashes the workload at every fsync from the second
// onward and checks that recovery always lands on a clean commit boundary. The
// first fsync cannot be targeted with CrashAfterSyncs (0 disarms the injector),
// and crashing there would mean nothing committed yet, which the no-crash run
// already covers.
func TestCrashRecoveryAtEachFsync(t *testing.T) {
	const totalFsyncs = 2 * crashRounds // two fsyncs per commit
	for n := 1; n <= totalFsyncs; n++ {
		t.Run(fmt.Sprintf("after_fsync_%d", n), func(t *testing.T) {
			mem := vfs.NewMem()
			fl := vfs.NewFault(mem)
			p, err := pager.Create(fl, "crash.aki", pager.Options{PageSize: 4096, DBCount: 16})
			if err != nil {
				t.Fatalf("create pager: %v", err)
			}
			ks, err := Open(p)
			if err != nil {
				t.Fatalf("open keyspace: %v", err)
			}
			fl.CrashAfterSyncs(n)
			runCrashWorkload(ks)
			// The pager is abandoned, not closed: a crash does not get to run
			// cleanup. Recovery reads the raw bytes left in mem.
			verifyCleanPrefix(t, mem)
		})
	}
}

// TestTornMetaWriteFallsBack tears the write of the new meta page so it lands with
// a bad checksum, then reopens. Recovery must reject the torn meta and fall back
// to the previous live snapshot. This is the double-meta guarantee on its own: a
// half-written meta never makes a partial commit visible.
func TestTornMetaWriteFallsBack(t *testing.T) {
	mem := vfs.NewMem()
	fl := vfs.NewFault(mem)
	p, err := pager.Create(fl, "crash.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	ks, err := Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	db := ks.dbs[0]
	if err := db.Set([]byte("k"), []byte("v1"), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// With the catalog now clean, a bare pager commit writes nothing but the new
	// meta page, so tearing the next write tears exactly that meta page.
	fl.TornNextWrite(16)
	if err := p.Commit(pager.CommitInfo{}); !errors.Is(err, vfs.ErrInjectedCrash) {
		t.Fatalf("torn meta commit: got %v want ErrInjectedCrash", err)
	}

	p2, err := pager.Open(mem, "crash.aki", pager.Options{})
	if err != nil {
		t.Fatalf("reopen after torn meta: %v", err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := Open(p2)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}
	body, _, found, err := ks2.dbs[0].Peek([]byte("k"))
	if err != nil || !found {
		t.Fatalf("after torn meta, key k = found %v err %v", found, err)
	}
	if string(body) != "v1" {
		t.Fatalf("after torn meta, k = %q want v1", body)
	}
}
