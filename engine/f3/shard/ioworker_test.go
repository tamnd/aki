package shard

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

// tempWriteAt returns an ioworker write seam backed by a fresh temp file plus a
// reader over the same file, the stand-in for the shard cold region the
// migration quantum wires in PR 4. The write runs from the off-owner goroutine
// (a real pwrite), the read from the test.
func tempWriteAt(t *testing.T) (func(off int64, b []byte) (int, error), func(off int64, n int) []byte) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "drain"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open drain file: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	write := func(off int64, b []byte) (int, error) { return f.WriteAt(b, off) }
	read := func(off int64, n int) []byte {
		got := make([]byte, n)
		if _, err := f.ReadAt(got, off); err != nil {
			t.Fatalf("read back at %d: %v", off, err)
		}
		return got
	}
	return write, read
}

// drainCompletions runs the owner's completion drain (advanceIntents, the same
// step run in the loop after every batch) until done reports true or the
// deadline passes. The test goroutine stands in for the owner here: submit and
// this drain both run on it, so the pool and the recorded results are touched by
// one goroutine exactly as they are in production.
func drainCompletions(t *testing.T, w *worker, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		w.advanceIntents()
		if time.Now().After(deadline) {
			t.Fatal("completion never posted")
		}
		runtime.Gosched()
	}
}

// TestIOWorkerRoundTrip is the buffer round-trip: the owner stages bytes in a
// pooled buffer and submits a drain, the off-owner goroutine pwrites them, the
// completion posts back, and the bytes read back byte-identical. It also pins
// the result the completion carries and that the buffer returns to the pool.
func TestIOWorkerRoundTrip(t *testing.T) {
	w := newWorker(0, store.New(testArena, testSeg))
	write, read := tempWriteAt(t)
	w.io.write = write

	buf := w.io.pool.get()
	if buf == nil {
		t.Fatal("pool handed out nothing at the start")
	}
	payload := []byte("cold-frame-bytes-0123456789")
	buf = append(buf, payload...)

	const off = 4096
	var res ioResult
	done := false
	w.io.submit(ioJob{buf: buf, off: off, onDone: func(cx *Ctx, r ioResult) {
		res = r
		done = true
	}})
	drainCompletions(t, w, func() bool { return done })
	w.io.stop()

	if res.err != nil {
		t.Fatalf("pwrite reported %v", res.err)
	}
	if res.n != len(payload) {
		t.Fatalf("pwrite moved %d bytes, want %d", res.n, len(payload))
	}
	if got := read(off, len(payload)); !bytes.Equal(got, payload) {
		t.Fatalf("round-trip read %q, want %q", got, payload)
	}
	if w.io.pool.out != 0 {
		t.Fatalf("pool has %d buffers still checked out, want 0", w.io.pool.out)
	}
}

// TestIOWorkerCompletionOrder pins that completions apply in submission order:
// the goroutine drains the job channel FIFO and posts each completion onto the
// single-producer control queue in that order, so advanceIntents runs them in
// the owner's program order. This is the ordering the phase-2 flips depend on.
func TestIOWorkerCompletionOrder(t *testing.T) {
	w := newWorker(0, store.New(testArena, testSeg))
	write, _ := tempWriteAt(t)
	w.io.write = write

	const n = 32
	var order []int
	for i := 0; i < n; i++ {
		i := i
		w.io.submit(ioJob{buf: []byte{byte(i)}, off: int64(i), onDone: func(cx *Ctx, res ioResult) {
			if res.err != nil {
				t.Errorf("job %d pwrite: %v", i, res.err)
			}
			order = append(order, i)
		}})
	}
	drainCompletions(t, w, func() bool { return len(order) == n })
	w.io.stop()

	for i := 0; i < n; i++ {
		if order[i] != i {
			t.Fatalf("completion %d ran out of order: got job %d", i, order[i])
		}
	}
}

// TestIOWorkerNoTarget pins the skeleton's guard: a job submitted before a
// producer wires the write seam completes with errNoDrainTarget rather than
// panicking, and still returns its buffer. No production path submits without a
// seam; this only proves the inert state is safe.
func TestIOWorkerNoTarget(t *testing.T) {
	w := newWorker(0, store.New(testArena, testSeg))
	done := false
	var res ioResult
	w.io.submit(ioJob{buf: []byte("x"), onDone: func(cx *Ctx, r ioResult) {
		res = r
		done = true
	}})
	drainCompletions(t, w, func() bool { return done })
	w.io.stop()
	if res.err != errNoDrainTarget {
		t.Fatalf("result err = %v, want errNoDrainTarget", res.err)
	}
}

// TestIOWorkerCompletionOnOwner is the -race gate on re-serialization: the
// completion must run on the owner goroutine, not the I/O goroutine. It bumps
// w.sink, an owner-only word the batch drain also touches, while a client
// pipelines foreground commands; if a completion ran off-owner the two accesses
// to w.sink would race. Submits come through PostOwner so the checkout and the
// hand-off run on the owner, exactly as the migration quantum will.
func TestIOWorkerCompletionOnOwner(t *testing.T) {
	rt := testRuntime(1)
	w := rt.workers[0]
	write, _ := tempWriteAt(t)
	w.io.write = write // set before Start: no goroutine reads it yet
	rt.Start()
	defer rt.Stop()

	c := rt.NewConn()
	go func() {
		for i := 0; i < 800; i++ {
			_ = c.Do(opSet, true, args(fmt.Sprintf("k%d", i), "v"))
		}
	}()

	const jobs = 128
	got := make(chan struct{}, jobs)
	for i := 0; i < jobs; i++ {
		i := i
		rt.PostOwner(0, func(cx *Ctx) {
			w.io.submit(ioJob{buf: []byte{byte(i)}, off: int64(i), onDone: func(cx *Ctx, res ioResult) {
				w.sink += uint64(res.n) // owner-only, shared with executeOne's stage one
				got <- struct{}{}
			}})
		})
	}
	for i := 0; i < jobs; i++ {
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d completions landed", i, jobs)
		}
	}
}

// TestStagePool pins the admission bound and the capacity reuse: get refuses
// past max outstanding, put readmits one, and a grown buffer returns with its
// capacity intact for the next drain.
func TestStagePool(t *testing.T) {
	var p stagePool
	p.init(64, 2)

	a := p.get()
	b := p.get()
	if a == nil || b == nil {
		t.Fatal("get refused a buffer under the bound")
	}
	if cap(a) < 64 || cap(b) < 64 {
		t.Fatalf("fresh buffer under bufcap: %d, %d", cap(a), cap(b))
	}
	if p.get() != nil {
		t.Fatal("get past the in-flight bound handed a buffer out")
	}

	// A grown buffer returns and keeps its capacity for reuse.
	grown := append(a[:0], make([]byte, 4096)...)
	p.put(grown)
	reused := p.get()
	if cap(reused) < 4096 {
		t.Fatalf("reused buffer cap %d, want the grown capacity >= 4096", cap(reused))
	}
	if p.get() != nil {
		t.Fatal("bound not re-enforced after a reuse")
	}

	p.put(reused)
	p.put(b)
	if p.out != 0 {
		t.Fatalf("out = %d after returning every buffer, want 0", p.out)
	}
}
