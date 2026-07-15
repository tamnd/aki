package sqlo1b

import (
	"bytes"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// countFile counts syscall-level calls so tests can pin coalescing.
type countFile struct {
	*os.File
	reads, writes atomic.Int32
}

func (c *countFile) ReadAt(b []byte, off int64) (int, error) {
	c.reads.Add(1)
	return c.File.ReadAt(b, off)
}

func (c *countFile) WriteAt(b []byte, off int64) (int, error) {
	c.writes.Add(1)
	return c.File.WriteAt(b, off)
}

// gatedSyncFile blocks Sync until released, to prove fsync lives off
// the submission path.
type gatedSyncFile struct {
	*countFile
	gate chan struct{}
}

func (g *gatedSyncFile) Sync() error {
	<-g.gate
	return g.countFile.Sync()
}

func newCountFile(t *testing.T) *countFile {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "b.aki"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return &countFile{File: f}
}

// collect drains n completions or fails on timeout, returning
// tag -> err.
func collect(t *testing.T, comp <-chan IOResult, n int) map[uint64]error {
	t.Helper()
	got := make(map[uint64]error, n)
	for range n {
		select {
		case r := <-comp:
			if _, dup := got[r.Tag]; dup {
				t.Fatalf("tag %d completed twice", r.Tag)
			}
			got[r.Tag] = r.Err
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out with %d of %d completions", len(got), n)
		}
	}
	return got
}

const testExtent = 4 * GroupSize

func groupBuf(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, GroupSize)
}

func TestIOPoolRoundtrip(t *testing.T) {
	f := newCountFile(t)
	comp := make(chan IOResult, 64)
	p := NewIOPool(f, testExtent, 4, comp)
	defer p.Close()

	// Two extents, mixed lanes, non-adjacent tags.
	writes := []IOReq{
		{Op: OpWrite, Prio: PrioFG, Ext: 0, Off: 0, Buf: groupBuf(0xA1), Tag: 1},
		{Op: OpWrite, Prio: PrioBG, Ext: 1, Off: GroupSize, Buf: groupBuf(0xB2), Tag: 2},
		{Op: OpWrite, Prio: PrioFG, Ext: 1, Off: 3 * GroupSize, Buf: groupBuf(0xC3), Tag: 3},
	}
	p.Submit(writes)
	for tag, err := range collect(t, comp, 3) {
		if err != nil {
			t.Fatalf("write tag %d: %v", tag, err)
		}
	}

	reads := []IOReq{
		{Op: OpRead, Prio: PrioFG, Ext: 0, Off: 0, Buf: make([]byte, GroupSize), Tag: 11},
		{Op: OpRead, Prio: PrioBG, Ext: 1, Off: GroupSize, Buf: make([]byte, GroupSize), Tag: 12},
		{Op: OpRead, Prio: PrioFG, Ext: 1, Off: 3 * GroupSize, Buf: make([]byte, GroupSize), Tag: 13},
	}
	p.Submit(reads)
	for tag, err := range collect(t, comp, 3) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	for i, want := range []byte{0xA1, 0xB2, 0xC3} {
		if !bytes.Equal(reads[i].Buf, groupBuf(want)) {
			t.Fatalf("read %d came back wrong", i)
		}
	}
}

// TestIOPoolCoalescing pins that file-adjacent same-op same-prio
// requests become one syscall, and that gaps, op flips, lane flips,
// and the 16 group cap all split runs.
func TestIOPoolCoalescing(t *testing.T) {
	f := newCountFile(t)
	comp := make(chan IOResult, 64)
	p := NewIOPool(f, testExtent, 1, comp)
	defer p.Close()

	// Four adjacent groups, the last two in extent 1: extents sit
	// contiguously in the file, so this is one range and one pwrite.
	locs := []struct {
		ext uint64
		off uint32
	}{{0, 2 * GroupSize}, {0, 3 * GroupSize}, {1, 0}, {1, GroupSize}}
	adj := make([]IOReq, 4)
	for i, l := range locs {
		adj[i] = IOReq{Op: OpWrite, Ext: l.ext, Off: l.off,
			Buf: groupBuf(byte(i)), Tag: uint64(i)}
	}
	p.Submit(adj)
	collect(t, comp, 4)
	if got := f.writes.Load(); got != 1 {
		t.Fatalf("4 adjacent writes took %d syscalls, want 1", got)
	}

	// Scatter reads back through one pread and check the payloads.
	reads := make([]IOReq, 4)
	for i, l := range locs {
		reads[i] = IOReq{Op: OpRead, Ext: l.ext, Off: l.off,
			Buf: make([]byte, GroupSize), Tag: uint64(10 + i)}
	}
	p.Submit(reads)
	for tag, err := range collect(t, comp, 4) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	if got := f.reads.Load(); got != 1 {
		t.Fatalf("4 adjacent reads took %d syscalls, want 1", got)
	}
	for i := range reads {
		if !bytes.Equal(reads[i].Buf, groupBuf(byte(i))) {
			t.Fatalf("scattered read %d came back wrong", i)
		}
	}

	// A gap, an op flip, and a lane flip each split the run.
	f.writes.Store(0)
	split := []IOReq{
		{Op: OpWrite, Ext: 0, Off: 0, Buf: groupBuf(1), Tag: 20},
		{Op: OpWrite, Ext: 0, Off: 2 * GroupSize, Buf: groupBuf(2), Tag: 21}, // gap
		{Op: OpWrite, Ext: 0, Off: 3 * GroupSize, Buf: groupBuf(3), Tag: 22},
		{Op: OpWrite, Prio: PrioBG, Ext: 1, Off: 0, Buf: groupBuf(4), Tag: 23}, // lane flip
	}
	p.Submit(split)
	collect(t, comp, 4)
	if got := f.writes.Load(); got != 3 {
		t.Fatalf("split batch took %d write syscalls, want 3", got)
	}
}

func TestIOPoolCoalesceCap(t *testing.T) {
	f := newCountFile(t)
	comp := make(chan IOResult, 64)
	// Large extent so 20 groups sit adjacent inside extent 0.
	p := NewIOPool(f, 32*GroupSize, 1, comp)
	defer p.Close()

	reqs := make([]IOReq, 20)
	for i := range reqs {
		reqs[i] = IOReq{Op: OpWrite, Ext: 0, Off: uint32(i) * GroupSize,
			Buf: groupBuf(byte(i)), Tag: uint64(i)}
	}
	p.Submit(reqs)
	collect(t, comp, 20)
	if got := f.writes.Load(); got != 2 {
		t.Fatalf("20 adjacent groups took %d syscalls, want 2 at the 16 group cap", got)
	}
}

// TestIOPoolSyncOffPath proves a blocked fsync stalls neither
// submission nor writes: with one worker and the sync gate held,
// writes submitted after the Sync still complete.
func TestIOPoolSyncOffPath(t *testing.T) {
	g := &gatedSyncFile{countFile: newCountFile(t), gate: make(chan struct{})}
	comp := make(chan IOResult, 64)
	p := NewIOPool(g, testExtent, 1, comp)

	p.Sync(100)
	p.Submit([]IOReq{{Op: OpWrite, Ext: 0, Off: 0, Buf: groupBuf(0xEE), Tag: 1}})

	select {
	case r := <-comp:
		if r.Tag != 1 || r.Err != nil {
			t.Fatalf("first completion tag %d err %v, want the write", r.Tag, r.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("write stuck behind a blocked fsync")
	}

	close(g.gate)
	if err := collect(t, comp, 1)[100]; err != nil {
		t.Fatalf("sync: %v", err)
	}
	p.Close()
}

func TestIOPoolValidation(t *testing.T) {
	f := newCountFile(t)
	comp := make(chan IOResult, 64)
	p := NewIOPool(f, testExtent, 2, comp)
	defer p.Close()

	p.Submit([]IOReq{
		{Op: 9, Ext: 0, Off: 0, Buf: groupBuf(0), Tag: 1},
		{Op: OpWrite, Ext: 0, Off: 0, Tag: 2},                                  // empty buf
		{Op: OpWrite, Ext: 0, Off: testExtent - 100, Buf: groupBuf(0), Tag: 3}, // crosses extent
		{Op: OpRead, Ext: 5, Off: 0, Buf: make([]byte, GroupSize), Tag: 4},     // past EOF
	})
	got := collect(t, comp, 4)
	for tag := uint64(1); tag <= 4; tag++ {
		if got[tag] == nil {
			t.Fatalf("tag %d completed clean, want an error", tag)
		}
	}
}

func TestIOPoolCloseDrains(t *testing.T) {
	f := newCountFile(t)
	comp := make(chan IOResult, 256)
	p := NewIOPool(f, testExtent, 2, comp)

	var reqs []IOReq
	for i := range uint64(100) {
		reqs = append(reqs, IOReq{Op: OpWrite, Prio: uint8(i % 2), Ext: i % 8,
			Off: uint32(i%4) * GroupSize, Buf: groupBuf(byte(i)), Tag: i})
	}
	p.Submit(reqs)
	p.Sync(1000)
	p.Close()

	got := collect(t, comp, 101)
	for tag, err := range got {
		if err != nil {
			t.Fatalf("tag %d after close: %v", tag, err)
		}
	}
}
