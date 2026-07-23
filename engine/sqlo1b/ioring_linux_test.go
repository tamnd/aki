package sqlo1b

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The ring tests skip, not fail, where io_uring is denied (Docker's
// default seccomp, ancient kernels): the fallback contract makes that
// a legal environment, and the CI container arm relies on the skip.

func ringT(t *testing.T, extentSize uint32, depth, compCap int) (*IORing, *os.File, chan IOResult) {
	t.Helper()
	if err := RingProbe(); err != nil {
		if errors.Is(err, ErrRingUnsupported) {
			t.Skipf("io_uring unavailable here: %v", err)
		}
		t.Fatalf("RingProbe: %v", err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "ring.dat"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.Truncate(4 * int64(extentSize)); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	comp := make(chan IOResult, compCap)
	r, err := NewIORing(f, extentSize, depth, comp)
	if err != nil {
		t.Fatalf("NewIORing: %v", err)
	}
	t.Cleanup(func() { r.Close(); f.Close() })
	return r, f, comp
}

func collectRing(t *testing.T, comp chan IOResult, n int) map[uint64]error {
	t.Helper()
	got := make(map[uint64]error, n)
	for range n {
		res := <-comp
		if _, dup := got[res.Tag]; dup {
			t.Fatalf("tag %d completed twice", res.Tag)
		}
		got[res.Tag] = res.Err
	}
	return got
}

func TestRingProbeReports(t *testing.T) {
	err := RingProbe()
	if err != nil && !errors.Is(err, ErrRingUnsupported) {
		t.Fatalf("probe failed outside the unsupported contract: %v", err)
	}
}

func TestRingWriteReadBack(t *testing.T) {
	const ext = uint32(1 << 16)
	r, _, comp := ringT(t, ext, 8, 64)

	// Writes across two extents, including a group the file has never
	// touched, then reads of the same ranges into fresh buffers.
	writes := []IOReq{
		{Op: OpWrite, Prio: PrioFG, Ext: 0, Off: 0, Buf: bytes.Repeat([]byte{0xA1}, GroupSize), Tag: 1},
		{Op: OpWrite, Prio: PrioBG, Ext: 0, Off: GroupSize, Buf: bytes.Repeat([]byte{0xB2}, GroupSize), Tag: 2},
		{Op: OpWrite, Prio: PrioFG, Ext: 3, Off: ext - GroupSize, Buf: bytes.Repeat([]byte{0xC3}, GroupSize), Tag: 3},
	}
	r.Submit(writes)
	for tag, err := range collectRing(t, comp, len(writes)) {
		if err != nil {
			t.Fatalf("write tag %d: %v", tag, err)
		}
	}

	reads := []IOReq{
		{Op: OpRead, Prio: PrioFG, Ext: 0, Off: 0, Buf: make([]byte, GroupSize), Tag: 11},
		{Op: OpRead, Prio: PrioFG, Ext: 0, Off: GroupSize, Buf: make([]byte, GroupSize), Tag: 12},
		{Op: OpRead, Prio: PrioBG, Ext: 3, Off: ext - GroupSize, Buf: make([]byte, GroupSize), Tag: 13},
	}
	r.Submit(reads)
	for tag, err := range collectRing(t, comp, len(reads)) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	for i, want := range []byte{0xA1, 0xB2, 0xC3} {
		if !bytes.Equal(reads[i].Buf, bytes.Repeat([]byte{want}, GroupSize)) {
			t.Fatalf("read %d came back wrong (first byte %#x, want %#x)", i, reads[i].Buf[0], want)
		}
	}
}

func TestRingSyncRoundTrip(t *testing.T) {
	r, _, comp := ringT(t, 1<<16, 8, 16)
	r.Submit([]IOReq{{Op: OpWrite, Ext: 0, Off: 0, Buf: []byte{1, 2, 3, 4}, Tag: 5}})
	if res := <-comp; res.Tag != 5 || res.Err != nil {
		t.Fatalf("write completion: %+v", res)
	}
	r.Sync(6)
	if res := <-comp; res.Tag != 6 || res.Err != nil {
		t.Fatalf("sync completion: %+v", res)
	}
}

func TestRingManyInflight(t *testing.T) {
	// Far more requests than the ring depth: chunking, the CQ-bound
	// reservation, and slot recycling all have to hold.
	const ext = uint32(1 << 16)
	const n = 512
	r, _, comp := ringT(t, ext, 4, n)

	pattern := func(i int) []byte {
		b := make([]byte, GroupSize)
		for j := range b {
			b[j] = byte(i * 31)
		}
		return b
	}
	writes := make([]IOReq, n)
	for i := range writes {
		writes[i] = IOReq{
			Op: OpWrite, Ext: uint64(i % 4), Off: uint32(i/4) * GroupSize,
			Buf: pattern(i), Tag: uint64(i),
		}
	}
	r.Submit(writes)
	for tag, err := range collectRing(t, comp, n) {
		if err != nil {
			t.Fatalf("write tag %d: %v", tag, err)
		}
	}

	reads := make([]IOReq, n)
	for i := range reads {
		reads[i] = IOReq{
			Op: OpRead, Ext: uint64(i % 4), Off: uint32(i/4) * GroupSize,
			Buf: make([]byte, GroupSize), Tag: uint64(1000 + i),
		}
	}
	r.Submit(reads)
	for tag, err := range collectRing(t, comp, n) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	for i := range reads {
		if !bytes.Equal(reads[i].Buf, pattern(i)) {
			t.Fatalf("read %d came back wrong", i)
		}
	}
}

func TestRingValidation(t *testing.T) {
	const ext = uint32(1 << 16)
	r, _, comp := ringT(t, ext, 8, 16)
	bad := []IOReq{
		{Op: OpRead, Ext: 0, Off: ext - 8, Buf: make([]byte, 16), Tag: 21}, // crosses the extent
		{Op: OpRead, Ext: 0, Off: 0, Buf: make([]byte, 16), Tag: 22},       // fails with its batch
	}
	r.Submit(bad)
	for tag, err := range collectRing(t, comp, len(bad)) {
		if err == nil {
			t.Fatalf("tag %d passed validation and should not have", tag)
		}
	}
}

func TestRingCloseDrainsQueued(t *testing.T) {
	const ext = uint32(1 << 16)
	f, err := os.Create(filepath.Join(t.TempDir(), "ring.dat"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	if probeErr := RingProbe(); probeErr != nil {
		t.Skipf("io_uring unavailable here: %v", probeErr)
	}
	if err := f.Truncate(int64(ext)); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	const n = 64
	comp := make(chan IOResult, n+1)
	r, err := NewIORing(f, ext, 8, comp)
	if err != nil {
		t.Fatalf("NewIORing: %v", err)
	}
	reqs := make([]IOReq, n)
	for i := range reqs {
		reqs[i] = IOReq{Op: OpWrite, Ext: 0, Off: uint32(i) * GroupSize, Buf: make([]byte, GroupSize), Tag: uint64(i)}
	}
	r.Submit(reqs)
	r.Sync(999)
	r.Close()
	got := collectRing(t, comp, n+1)
	for tag, err := range got {
		if err != nil {
			t.Fatalf("tag %d: %v", tag, err)
		}
	}
	if _, ok := got[999]; !ok {
		t.Fatal("sync completion lost across Close")
	}
}
