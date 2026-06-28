//go:build linux

package uring

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
)

// newRing builds a ring or skips the test when the kernel has no io_uring, which
// keeps the suite green on old CI kernels instead of failing for the wrong reason.
func newRing(t *testing.T, entries uint32) *Ring {
	t.Helper()
	r, err := New(entries)
	if err != nil {
		if errors.Is(err, ErrUnsupported) {
			t.Skipf("io_uring unavailable: %v", err)
		}
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// TestNopBatch submits a batch of no-ops in one enter and checks every one comes
// back, carrying its user_data, with a zero result. This exercises the SQE claim,
// publish, single io_uring_enter, and CQ reap mechanics with no fd involved.
func TestNopBatch(t *testing.T) {
	r := newRing(t, 64)
	const n = 16
	for i := range n {
		if !r.PrepNop(uint64(i + 1)) {
			t.Fatalf("PrepNop %d: ring unexpectedly full", i)
		}
	}
	submitted, err := r.SubmitAndWait(n)
	if err != nil {
		t.Fatalf("SubmitAndWait: %v", err)
	}
	if submitted != n {
		t.Fatalf("submitted %d, want %d", submitted, n)
	}

	seen := make(map[uint64]bool, n)
	into := make([]Completion, n)
	for len(seen) < n {
		got := r.Reap(into)
		if got == 0 {
			t.Fatalf("reaped 0 with %d/%d seen", len(seen), n)
		}
		for _, c := range into[:got] {
			if c.Res != 0 {
				t.Errorf("nop user_data=%d res=%d, want 0", c.UserData, c.Res)
			}
			seen[c.UserData] = true
		}
	}
	for i := range n {
		if !seen[uint64(i+1)] {
			t.Errorf("missing completion for user_data=%d", i+1)
		}
	}
}

// TestSendRecvSocketpair drives a real send and recv over a socketpair through the
// ring: one enter carries both the recv (posted first) and the send, and the bytes
// must arrive intact. This is the exact pattern the reactor will use, both ends'
// I/O submitted together in one syscall.
func TestSendRecvSocketpair(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("Socketpair: %v", err)
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])

	r := newRing(t, 64)
	payload := []byte("the quick brown fox jumps over io_uring")
	recvBuf := make([]byte, len(payload))

	const recvData, sendData = 1, 2
	if !r.PrepRecv(recvData, fds[0], recvBuf) {
		t.Fatal("PrepRecv: ring full")
	}
	if !r.PrepSend(sendData, fds[1], payload) {
		t.Fatal("PrepSend: ring full")
	}
	if _, err := r.SubmitAndWait(2); err != nil {
		t.Fatalf("SubmitAndWait: %v", err)
	}

	into := make([]Completion, 2)
	var sentN, recvN int32 = -1, -1
	seen := 0
	for seen < 2 {
		got := r.Reap(into)
		if got == 0 {
			// recv may complete only after send is delivered; nudge the kernel.
			if _, err := r.SubmitAndWait(uint32(2 - seen)); err != nil {
				t.Fatalf("SubmitAndWait drain: %v", err)
			}
			continue
		}
		for _, c := range into[:got] {
			switch c.UserData {
			case sendData:
				sentN = c.Res
			case recvData:
				recvN = c.Res
			default:
				t.Errorf("unexpected user_data=%d", c.UserData)
			}
			seen++
		}
	}

	if sentN < 0 {
		t.Fatalf("send failed: res=%d (errno %d)", sentN, -sentN)
	}
	if recvN < 0 {
		t.Fatalf("recv failed: res=%d (errno %d)", recvN, -recvN)
	}
	if int(recvN) != len(payload) {
		t.Fatalf("recv %d bytes, want %d", recvN, len(payload))
	}
	if !bytes.Equal(recvBuf, payload) {
		t.Fatalf("recv payload mismatch: %q != %q", recvBuf, payload)
	}
}

// TestRingFull confirms nextSQE refuses past the ring's capacity rather than
// overrunning, so the caller's submit-and-retry loop has a true signal.
func TestRingFull(t *testing.T) {
	r := newRing(t, 8) // kernel rounds to a power of two; capacity is r.sqEntries
	capacity := int(r.sqEntries)
	for i := range capacity {
		if !r.PrepNop(uint64(i)) {
			t.Fatalf("PrepNop %d refused below capacity %d", i, capacity)
		}
	}
	if r.PrepNop(9999) {
		t.Fatalf("PrepNop accepted past capacity %d", capacity)
	}
	if _, err := r.SubmitAndWait(uint32(capacity)); err != nil {
		t.Fatalf("SubmitAndWait: %v", err)
	}
	into := make([]Completion, capacity)
	drained := 0
	for drained < capacity {
		got := r.Reap(into)
		if got == 0 {
			t.Fatalf("reaped 0 with %d/%d drained", drained, capacity)
		}
		drained += got
	}
	// After draining, the ring takes submissions again.
	if !r.PrepNop(1) {
		t.Fatal("PrepNop refused after drain")
	}
}
