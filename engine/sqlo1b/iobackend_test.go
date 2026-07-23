package sqlo1b

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// TestIOBackendAuto checks the startup selection end to end: a store
// on a real file picks the ring exactly when the probe says the box
// supports it, and the pool everywhere else. Either way the store
// must serve reads, which is the R-I8 identical-behavior contract in
// miniature.
func TestIOBackendAuto(t *testing.T) {
	r := newStoreRig(t)
	got := r.s.Stats().IOBackend
	want := "iopool"
	if runtime.GOOS == "linux" && RingProbe() == nil {
		want = "ioring"
	}
	if got != want {
		t.Fatalf("IOBackend = %q, want %q", got, want)
	}
	r.apply(t, putOp("k", []byte("v"), 0))
	r.verify(t)
	r.reopen(t)
	if got := r.s.Stats().IOBackend; got != want {
		t.Fatalf("IOBackend after reopen = %q, want %q", got, want)
	}
	r.verify(t)
}

// TestIOBackendForcePool checks the gate's off arm: with the force
// knob set, even a ring-capable box runs the pool.
func TestIOBackendForcePool(t *testing.T) {
	ForceIOPool = true
	defer func() { ForceIOPool = false }()
	r := newStoreRig(t)
	if got := r.s.Stats().IOBackend; got != "iopool" {
		t.Fatalf("IOBackend = %q, want %q under ForceIOPool", got, "iopool")
	}
	r.apply(t, putOp("k", []byte("v"), 0))
	r.verify(t)
}

// wrappedStoreFile hides the *os.File behind a plain StoreFile, the
// shape the crash harness FaultFile presents.
type wrappedStoreFile struct {
	StoreFile
}

// TestIOBackendWrappedFile checks that a store on a wrapped file (no
// fd to submit against) lands on the pool without complaint, so the
// crash harness always runs deterministic IO.
func TestIOBackendWrappedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.aki")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := CreateStoreOn(wrappedStoreFile{f}, sqlo1.WALPath(path), 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Stats().IOBackend; got != "iopool" {
		t.Fatalf("IOBackend = %q, want %q on a wrapped file", got, "iopool")
	}
}
