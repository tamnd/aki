package vfs

import "sync"

// Fault wraps a VFS and injects failures to drive crash-recovery tests (spec
// 2064 doc 04 §7, doc 23). It can crash after a configured number of writes or
// syncs, and it can simulate a torn write that persists only a prefix of a
// page. The wrapped VFS is normally a *Mem so a "crash" can be modelled by
// discarding the handle and reopening the same in-memory bytes.
type Fault struct {
	inner VFS

	mu sync.Mutex
	// crashAfterWrites, when > 0, makes the (crashAfterWrites+1)-th WriteAt
	// across all files return ErrInjectedCrash without persisting.
	crashAfterWrites int
	// crashAfterSyncs, when > 0, makes the (crashAfterSyncs+1)-th Sync return
	// ErrInjectedCrash. Writes before that sync are not guaranteed durable.
	crashAfterSyncs int
	// tornWriteAt, when >= 0, truncates the next WriteAt to this many bytes
	// (a torn page), then resets to -1.
	tornWriteAt int
	// failSyncEIO, when true, makes every Sync return ErrInjectedCrash to model
	// the fsyncgate EIO-after-write failure (spec 2064 doc 04 §6).
	failSyncEIO bool

	writes int
	syncs  int
}

// NewFault wraps inner with a fault injector in its default pass-through state.
func NewFault(inner VFS) *Fault {
	return &Fault{inner: inner, tornWriteAt: -1}
}

// CrashAfterWrites arms a crash on the n-th subsequent write (1-based).
func (fl *Fault) CrashAfterWrites(n int) {
	fl.mu.Lock()
	fl.crashAfterWrites = n
	fl.writes = 0
	fl.mu.Unlock()
}

// CrashAfterSyncs arms a crash on the n-th subsequent sync (1-based).
func (fl *Fault) CrashAfterSyncs(n int) {
	fl.mu.Lock()
	fl.crashAfterSyncs = n
	fl.syncs = 0
	fl.mu.Unlock()
}

// TornNextWrite makes the next write persist only prefix bytes.
func (fl *Fault) TornNextWrite(prefix int) {
	fl.mu.Lock()
	fl.tornWriteAt = prefix
	fl.mu.Unlock()
}

// FailSyncEIO toggles permanent sync failure.
func (fl *Fault) FailSyncEIO(on bool) {
	fl.mu.Lock()
	fl.failSyncEIO = on
	fl.mu.Unlock()
}

// Disarm clears all injection state.
func (fl *Fault) Disarm() {
	fl.mu.Lock()
	fl.crashAfterWrites = 0
	fl.crashAfterSyncs = 0
	fl.tornWriteAt = -1
	fl.failSyncEIO = false
	fl.mu.Unlock()
}

func (fl *Fault) Open(name string, create bool) (File, error) {
	f, err := fl.inner.Open(name, create)
	if err != nil {
		return nil, err
	}
	return &faultFile{fl: fl, inner: f}, nil
}

func (fl *Fault) Remove(name string) error { return fl.inner.Remove(name) }
func (fl *Fault) Exists(name string) bool  { return fl.inner.Exists(name) }

type faultFile struct {
	fl    *Fault
	inner File
}

func (f *faultFile) WriteAt(p []byte, off int64) (int, error) {
	f.fl.mu.Lock()
	f.fl.writes++
	crash := f.fl.crashAfterWrites > 0 && f.fl.writes > f.fl.crashAfterWrites
	torn := f.fl.tornWriteAt
	f.fl.tornWriteAt = -1
	f.fl.mu.Unlock()

	if crash {
		return 0, ErrInjectedCrash
	}
	if torn >= 0 && torn < len(p) {
		// Persist only a prefix, then report the crash so the caller learns the
		// write did not complete; recovery must cope with the torn tail.
		_, _ = f.inner.WriteAt(p[:torn], off)
		return torn, ErrInjectedCrash
	}
	return f.inner.WriteAt(p, off)
}

func (f *faultFile) Sync() error {
	f.fl.mu.Lock()
	f.fl.syncs++
	crash := f.fl.failSyncEIO || (f.fl.crashAfterSyncs > 0 && f.fl.syncs > f.fl.crashAfterSyncs)
	f.fl.mu.Unlock()
	if crash {
		return ErrInjectedCrash
	}
	return f.inner.Sync()
}

func (f *faultFile) ReadAt(p []byte, off int64) (int, error) { return f.inner.ReadAt(p, off) }
func (f *faultFile) Truncate(n int64) error                  { return f.inner.Truncate(n) }
func (f *faultFile) Size() (int64, error)                    { return f.inner.Size() }
func (f *faultFile) Close() error                            { return f.inner.Close() }
