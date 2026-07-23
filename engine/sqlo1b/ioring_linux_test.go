package sqlo1b

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
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
	r, err := NewIORing(f, extentSize, depth, 0, comp)
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
	// reservation, and slot recycling all have to hold. 512 groups
	// over 4 extents needs 128 groups of room per extent.
	const ext = uint32(1 << 19)
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

// ringRegT is ringT with a registered pool, skipping where the box
// refuses registration (RLIMIT_MEMLOCK) like it skips where it
// refuses the ring.
func ringRegT(t *testing.T, extentSize uint32, depth, regBufs, compCap int) (*IORing, chan IOResult) {
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
	r, err := NewIORing(f, extentSize, depth, regBufs, comp)
	if err != nil {
		f.Close()
		if errors.Is(err, ErrRingUnsupported) {
			t.Skipf("buffer registration unavailable here: %v", err)
		}
		t.Fatalf("NewIORing: %v", err)
	}
	t.Cleanup(func() { r.Close(); f.Close() })
	return r, comp
}

func TestRingRegisteredRoundTrip(t *testing.T) {
	const ext = uint32(1 << 16)
	const half = 4
	r, comp := ringRegT(t, ext, 8, 2*half, 16)
	if r.RegBufs() != 2*half {
		t.Fatalf("RegBufs = %d, want %d", r.RegBufs(), 2*half)
	}

	// Writes from the pool's first half, reads into the second, both
	// on the fixed opcodes end to end.
	writes := make([]IOReq, half)
	for i := range writes {
		buf := r.RegBuf(i)
		for j := range buf {
			buf[j] = byte(i*37 + 1)
		}
		writes[i] = IOReq{Op: OpWrite, Ext: uint64(i % 2), Off: uint32(i/2) * GroupSize, Buf: buf, Tag: uint64(i)}
	}
	r.Submit(writes)
	for tag, err := range collectRing(t, comp, half) {
		if err != nil {
			t.Fatalf("write tag %d: %v", tag, err)
		}
	}
	reads := make([]IOReq, half)
	for i := range reads {
		reads[i] = IOReq{Op: OpRead, Ext: uint64(i % 2), Off: uint32(i/2) * GroupSize, Buf: r.RegBuf(half + i), Tag: uint64(100 + i)}
	}
	r.Submit(reads)
	for tag, err := range collectRing(t, comp, half) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	for i := range reads {
		if !bytes.Equal(r.RegBuf(half+i), r.RegBuf(i)) {
			t.Fatalf("registered read %d came back wrong", i)
		}
	}
	if got := r.fixedOps.Load(); got != 2*half {
		t.Fatalf("fixedOps = %d, want %d (every op should have ridden the fixed path)", got, 2*half)
	}
}

func TestRingMixedFixedAndPlain(t *testing.T) {
	const ext = uint32(1 << 16)
	r, comp := ringRegT(t, ext, 8, 2, 8)
	pool := r.RegBuf(0)
	for j := range pool {
		pool[j] = 0xEE
	}
	heap := bytes.Repeat([]byte{0xDD}, GroupSize)
	r.Submit([]IOReq{
		{Op: OpWrite, Ext: 0, Off: 0, Buf: pool, Tag: 1},
		{Op: OpWrite, Ext: 0, Off: GroupSize, Buf: heap, Tag: 2},
	})
	for tag, err := range collectRing(t, comp, 2) {
		if err != nil {
			t.Fatalf("write tag %d: %v", tag, err)
		}
	}
	back := make([]byte, 2*GroupSize)
	r.Submit([]IOReq{{Op: OpRead, Ext: 0, Off: 0, Buf: back, Tag: 3}})
	if res := <-comp; res.Err != nil {
		t.Fatalf("read back: %v", res.Err)
	}
	if back[0] != 0xEE || back[GroupSize] != 0xDD {
		t.Fatalf("mixed batch landed wrong: %#x %#x", back[0], back[GroupSize])
	}
	if got := r.fixedOps.Load(); got != 1 {
		t.Fatalf("fixedOps = %d, want 1 (pool write fixed, heap write and oversized read plain)", got)
	}
}

func TestRingRegBufAlignment(t *testing.T) {
	r, _ := ringRegT(t, 1<<16, 4, 4, 4)
	for i := range r.RegBufs() {
		buf := r.RegBuf(i)
		if len(buf) != GroupSize {
			t.Fatalf("RegBuf(%d) len %d", i, len(buf))
		}
		if addr := uintptr(unsafe.Pointer(&buf[0])); addr%GroupSize != 0 {
			t.Fatalf("RegBuf(%d) at %#x not %d-aligned for O_DIRECT", i, addr, GroupSize)
		}
	}
}

func TestRingODirect(t *testing.T) {
	const ext = uint32(1 << 16)
	if err := RingProbe(); err != nil {
		t.Skipf("io_uring unavailable here: %v", err)
	}
	name := filepath.Join(t.TempDir(), "direct.dat")
	plain, err := os.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := plain.Truncate(int64(ext)); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	plain.Close()
	f, err := OpenDirect(name)
	if err != nil {
		// tmpfs and some filesystems refuse O_DIRECT; that is the
		// documented fallback condition, not a failure.
		t.Skipf("O_DIRECT unavailable here: %v", err)
	}
	defer f.Close()
	comp := make(chan IOResult, 4)
	r, err := NewIORing(f, ext, 4, 2, comp)
	if err != nil {
		if errors.Is(err, ErrRingUnsupported) {
			t.Skipf("ring with registration unavailable here: %v", err)
		}
		t.Fatalf("NewIORing: %v", err)
	}
	defer r.Close()
	out := r.RegBuf(0)
	for j := range out {
		out[j] = 0x5A
	}
	r.Submit([]IOReq{{Op: OpWrite, Ext: 0, Off: 0, Buf: out, Tag: 1}})
	if res := <-comp; res.Err != nil {
		t.Fatalf("O_DIRECT write: %v", res.Err)
	}
	r.Submit([]IOReq{{Op: OpRead, Ext: 0, Off: 0, Buf: r.RegBuf(1), Tag: 2}})
	if res := <-comp; res.Err != nil {
		t.Fatalf("O_DIRECT read: %v", res.Err)
	}
	if !bytes.Equal(r.RegBuf(1), out) {
		t.Fatal("O_DIRECT round trip came back wrong")
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
	const ext = uint32(1 << 18) // 64 groups fit one extent
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
	r, err := NewIORing(f, ext, 8, 0, comp)
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

// Three deterministic batching shapes: the counters expose how many
// submit-side enters carried how many SQEs, and each shape below has
// exactly one legal outcome regardless of completion timing.

// A single Submit above the batch target enters exactly once: the
// amortization the ring exists for.
func TestRingBatchAmortize(t *testing.T) {
	const ext = uint32(1 << 16)
	const n = 24
	r, _, comp := ringT(t, ext, 32, n)
	reqs := make([]IOReq, n)
	for i := range reqs {
		reqs[i] = IOReq{
			Op:  OpRead,
			Ext: uint64(i / (int(ext) / GroupSize)),
			Off: uint32(i%(int(ext)/GroupSize)) * GroupSize,
			Buf: make([]byte, GroupSize),
			Tag: uint64(i),
		}
	}
	r.Submit(reqs)
	for tag, err := range collectRing(t, comp, n) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	if enters, entered := r.EnterStats(); enters != 1 || entered != n {
		t.Fatalf("EnterStats = %d enters, %d entered; want 1, %d", enters, entered, n)
	}
}

// A batch below every threshold still leaves on the drain-window
// tick, in one enter, because nothing was queued behind it.
func TestRingBatchDrainTick(t *testing.T) {
	const n = 4
	r, _, comp := ringT(t, 1<<16, 32, n)
	reqs := make([]IOReq, n)
	for i := range reqs {
		reqs[i] = IOReq{Op: OpRead, Ext: 0, Off: uint32(i) * GroupSize, Buf: make([]byte, GroupSize), Tag: uint64(i)}
	}
	r.Submit(reqs)
	for tag, err := range collectRing(t, comp, n) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	if enters, entered := r.EnterStats(); enters != 1 || entered != n {
		t.Fatalf("EnterStats = %d enters, %d entered; want 1, %d", enters, entered, n)
	}
}

// A Submit wider than the SQ splits into exactly two enters: the
// first flush fires on the full SQ, the remainder leaves either on
// the pressure-lowered target or the drain tick, but never as more
// than one enter.
func TestRingBatchFullSQThenRemainder(t *testing.T) {
	const ext = uint32(1 << 16)
	const n = 40
	r, _, comp := ringT(t, ext, 32, n)
	reqs := make([]IOReq, n)
	for i := range reqs {
		reqs[i] = IOReq{
			Op:  OpRead,
			Ext: uint64(i / (int(ext) / GroupSize)),
			Off: uint32(i%(int(ext)/GroupSize)) * GroupSize,
			Buf: make([]byte, GroupSize),
			Tag: uint64(i),
		}
	}
	r.Submit(reqs)
	for tag, err := range collectRing(t, comp, n) {
		if err != nil {
			t.Fatalf("read tag %d: %v", tag, err)
		}
	}
	if enters, entered := r.EnterStats(); enters != 2 || entered != n {
		t.Fatalf("EnterStats = %d enters, %d entered; want 2, %d", enters, entered, n)
	}
}
