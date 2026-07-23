package drivers

// The O1c crash suite at the O1 points (spec 2064/obs1 doc 06 section 4,
// doc 11): kills are modeled at the atomic-PUT store boundary, so the
// mid-fold and mid-manifest points collapse to the bucket states they
// leave behind. A segment PUT that never happened leaves nothing; a
// segment PUT without its manifest CAS leaves an orphan object in a slot
// no manifest names; a chain outage under live fold load leaves committed
// history plus an unreferenced WAL tail. Every state must boot into a
// correct sweep, and the orphan-slot state additionally forced the
// incarnation salt on the segment write tag, because a same-node retry
// into an orphan slot must read as a foreign occupant, not as our own
// ambiguous PUT.

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// crashFaults scripts the two kill mechanisms: manDown blocks the
// manifest CAS so folds strand orphan segments, chainDown blocks the
// chain append so flushed WAL objects never commit, and frozen models
// the process kill itself by refusing every write to the bucket while
// the incarnation tears down.
type crashFaults struct {
	manDown   atomic.Bool
	chainDown atomic.Bool
	frozen    atomic.Bool
}

func (f *crashFaults) fn(op sim.Op, key string) *sim.Fault {
	if f.frozen.Load() && op != sim.OpGet {
		return &sim.Fault{Err: errors.New("sim: bucket frozen at the kill point")}
	}
	if f.manDown.Load() && op == sim.OpPutIfAbsent && strings.Contains(key, "/man/") {
		return &sim.Fault{Err: errors.New("sim: scripted manifest outage")}
	}
	if f.chainDown.Load() && op == sim.OpPutIfAbsent && strings.Contains(key, "/chain/") {
		return &sim.Fault{Err: errors.New("sim: scripted chain outage")}
	}
	return nil
}

// abandon is the kill: no barrier, no clean handshake, the bucket stops
// taking writes and the incarnation is torn down with errors tolerated.
func abandon(f *crashFaults, b *Booted, srv *Server, nc net.Conn) {
	f.frozen.Store(true)
	nc.Close()
	srv.Close()
	_ = b.Close()
	f.frozen.Store(false)
}

// getMaybe reads one GET reply that is allowed to be null: the tail of a
// crashed relaxed-ack window is each key either fully present or fully
// absent, never corrupt.
func getMaybe(t *testing.T, nc net.Conn, r *bufio.Reader, key string) (string, bool) {
	t.Helper()
	send(t, nc, "GET", key)
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	if line == "$-1\r\n" {
		return "", false
	}
	if !strings.HasPrefix(line, "$") {
		t.Fatalf("GET %s: unexpected reply %q", key, line)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(line, "$"), "\r\n"))
	if err != nil {
		t.Fatalf("GET %s: bulk length in %q: %v", key, line, err)
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("GET %s: bulk body: %v", key, err)
	}
	return string(buf[:n]), true
}

// setRange writes prefix:from..to-1 with value prefix v-, batched.
func setRange(t *testing.T, nc net.Conn, r *bufio.Reader, prefix string, from, to int) {
	t.Helper()
	const batch = 500
	for base := from; base < to; base += batch {
		end := min(base+batch, to)
		for i := base; i < end; i++ {
			send(t, nc, "SET", prefix+strconv.Itoa(i), "v-"+strconv.Itoa(i))
		}
		for i := base; i < end; i++ {
			expect(t, r, "+OK\r\n")
		}
	}
}

// sweepRange asserts prefix:from..to-1 all answer their exact values.
func sweepRange(t *testing.T, nc net.Conn, r *bufio.Reader, prefix string, from, to int) {
	t.Helper()
	const batch = 500
	for base := from; base < to; base += batch {
		end := min(base+batch, to)
		for i := base; i < end; i++ {
			send(t, nc, "GET", prefix+strconv.Itoa(i))
		}
		for i := base; i < end; i++ {
			v := "v-" + strconv.Itoa(i)
			expect(t, r, "$"+strconv.Itoa(len(v))+"\r\n"+v+"\r\n")
		}
	}
}

// waitCommittedSrv is the commit half of commitAndStop: barrier the
// buffered frames and wait for the chain to cover them, leaving the
// incarnation running.
func waitCommittedSrv(t *testing.T, b *Booted) {
	t.Helper()
	b.WL.Barrier()
	done := make(chan struct{})
	b.WL.NotifyAllCommitted(func() { close(done) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("commit barrier never fired")
	}
}

// waitRecordPublish waits for a record-bearing fold to publish, the
// signal that the fold pipeline is live under the write load.
func waitRecordPublish(t *testing.T, b *Booted) {
	t.Helper()
	pollFor(t, "a record-bearing publish", func() bool {
		if b.Pub.Stats().Published == 0 {
			return false
		}
		for _, led := range b.Folder.Ledger() {
			if led.NRecords > 0 {
				return true
			}
		}
		return false
	})
}

// TestCrashMidManifest is the mid-fold and mid-manifest point: segment
// PUTs land, the manifest CAS never does, and the process dies. The
// orphan segments must survive in the bucket, stay invisible to the next
// boot's rebuild, and not wedge or corrupt the next incarnation's own
// folding when it retries into their slots.
func TestCrashMidManifest(t *testing.T) {
	var faults crashFaults
	bucket := sim.New(sim.Config{Fault: faults.fn})
	const phase1, phase2, phase3 = 4000, 3000, 1500
	ctx := context.Background()

	// Incarnation 1: a folded baseline, then the manifest CAS goes dark
	// while writes keep folding, stranding orphan segments above the
	// published cursor.
	b1, srv1, nc1, r1 := bootColdServer(t, bucket, 1)
	setRange(t, nc1, r1, "k:", 0, phase1)
	waitRecordPublish(t, b1)
	faults.manDown.Store(true)
	// A CAS already past the fault check can still land; wait for the
	// publisher to quiesce before taking the baseline.
	var pubBase uint64
	pollFor(t, "the manifest publisher to quiesce", func() bool {
		n := b1.Pub.Stats().Published
		if n == pubBase {
			return true
		}
		pubBase = n
		return false
	})
	segBase := b1.Folder.Stats().SegmentsPut
	ledBase := len(b1.Folder.Ledger())
	setRange(t, nc1, r1, "k:", phase1, phase1+phase2)
	pollFor(t, "orphan segment PUTs past the dark manifest CAS", func() bool {
		return b1.Folder.Stats().SegmentsPut > segBase
	})
	// The kill point sits between the segment PUT and the manifest CAS,
	// with the stream itself committed: barrier the WAL, then die.
	waitCommittedSrv(t, b1)
	if got := b1.Pub.Stats().Published; got != pubBase {
		t.Fatalf("manifest CAS advanced %d -> %d under the outage", pubBase, got)
	}
	orphans := b1.Folder.Ledger()[ledBase:]
	if len(orphans) == 0 {
		t.Fatal("the outage window folded no segments to strand")
	}
	abandon(&faults, b1, srv1, nc1)
	faults.manDown.Store(false)

	// The orphans are durably in the bucket, in slots no manifest names.
	for _, led := range orphans {
		if _, _, err := bucket.Get(ctx, led.Key); err != nil {
			t.Fatalf("orphan segment %s not in the bucket: %v", led.Key, err)
		}
	}

	// Incarnation 2: boots from the phase-1 manifests, replays the full
	// committed stream, and every key answers; the orphans changed
	// nothing.
	b2, srv2, nc2, r2 := bootColdServer(t, bucket, 2)
	if b2.Resident.Segments == 0 {
		t.Fatalf("resident rebuild %+v, want the phase-1 segments", b2.Resident)
	}
	sweepRange(t, nc2, r2, "k:", 0, phase1+phase2)

	// The teeth: incarnation 2 folds fresh writes into slots the orphans
	// occupy. The incarnation-salted tag makes the occupant read as
	// foreign, so the slot advances instead of adopting orphan bytes.
	setRange(t, nc2, r2, "f:", 0, phase3)
	pollFor(t, "publishing resumed past the orphan slots", func() bool {
		if b2.Pub.Stats().Published == 0 {
			return false
		}
		for _, led := range b2.Folder.Ledger() {
			if led.NRecords > 0 {
				return true
			}
		}
		return false
	})
	if errs := b2.Folder.Stats().BuildErrs; errs != 0 {
		t.Fatalf("folding over orphan slots hit %d build errors", errs)
	}
	commitAndStop(t, b2, srv2, nc2)

	// The direct invariant: no incarnation 2 row may sit on an orphan's
	// object key. A same-tag Recheck would have adopted the orphan's
	// bytes under incarnation 2's placements right here.
	orphanKeys := map[string]bool{}
	for _, led := range orphans {
		orphanKeys[led.Key] = true
	}
	for _, led := range b2.Folder.Ledger() {
		if orphanKeys[led.Key] {
			t.Fatalf("incarnation 2 published %s, a slot its predecessor's orphan occupies", led.Key)
		}
	}

	// Incarnation 3: the rebuild now includes incarnation 2's segments,
	// cut past the orphans; a wrong adoption would surface here as a
	// footer that does not parse or a sweep that misses.
	b3, srv3, nc3, r3 := bootColdServer(t, bucket, 3)
	defer func() { commitAndStop(t, b3, srv3, nc3) }()
	sweepRange(t, nc3, r3, "k:", 0, phase1+phase2)
	sweepRange(t, nc3, r3, "f:", 0, phase3)
	if st := b3.Cold.Stats(); st.Errs != 0 || st.Unresolved != 0 || st.Misses != 0 {
		t.Fatalf("cold reader stats after the orphan boot: %+v", st)
	}
}

// TestCrashChainOutageUnderFoldLoad re-runs the O1b post-PUT-pre-commit
// point with the fold pipeline live: a folded baseline commits, the
// chain goes dark, a relaxed-ack tail flushes WAL objects that never
// commit, and the process dies. The next boot must serve the committed
// prefix exactly and treat every tail key as all or nothing.
func TestCrashChainOutageUnderFoldLoad(t *testing.T) {
	var faults crashFaults
	bucket := sim.New(sim.Config{Fault: faults.fn})
	const prefix, tail = 4000, 400

	b1, srv1, nc1, r1 := bootColdServer(t, bucket, 1)
	setRange(t, nc1, r1, "p:", 0, prefix)
	waitRecordPublish(t, b1)
	waitCommittedSrv(t, b1)

	// The outage: relaxed acks keep answering while flushed WAL objects
	// pile up without a commit, then the pipeline error surfaces and the
	// process dies with folds still cutting underneath.
	faults.chainDown.Store(true)
	setRange(t, nc1, r1, "q:", 0, tail)
	b1.WL.Barrier()
	deadline := time.Now().Add(10 * time.Second)
	for b1.WL.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the chain outage never surfaced as a pipeline error")
		}
		time.Sleep(time.Millisecond)
	}
	abandon(&faults, b1, srv1, nc1)
	faults.chainDown.Store(false)

	// The next boot: fold state rebuilt, the committed prefix exact, and
	// each tail key either its full value or absent, an uncommitted WAL
	// object being invisible history.
	b2, srv2, nc2, r2 := bootColdServer(t, bucket, 2)
	defer func() { commitAndStop(t, b2, srv2, nc2) }()
	if b2.Resident.Segments == 0 {
		t.Fatalf("resident rebuild %+v, want the baseline segments", b2.Resident)
	}
	sweepRange(t, nc2, r2, "p:", 0, prefix)
	survived := 0
	for i := range tail {
		key := "q:" + strconv.Itoa(i)
		v, ok := getMaybe(t, nc2, r2, key)
		if !ok {
			continue
		}
		if want := "v-" + strconv.Itoa(i); v != want {
			t.Fatalf("tail key %s holds %q, want %q or absent", key, v, want)
		}
		survived++
	}
	t.Logf("chain outage tail: %d of %d relaxed-ack keys survived", survived, tail)
}
