// Package vfs is the virtual-filesystem seam (spec 2064 doc 03, doc 23). The
// pager and WAL run over a VFS rather than the os package directly, so tests can
// substitute a fault-injecting backend that crashes the engine at any write or
// fsync boundary and simulates torn writes and fsync failures. This seam is the
// spine of all crash-recovery testing.
//
// The interface is deliberately small: open/create/remove/exists a named file,
// and per-file positioned read/write, truncate, sync, size, and close. Every
// durability guarantee in aki is expressed in terms of these operations, so a
// faithful in-memory model of them is a faithful model of the engine's
// durability.
package vfs

import (
	"errors"
	"io/fs"
)

// ErrNotExist is returned by Open when the file is absent and create is false.
var ErrNotExist = fs.ErrNotExist

// File is an open file in a VFS. All offsets are absolute byte positions.
//
// The contract mirrors POSIX: WriteAt/ReadAt are positioned and do not move an
// implicit cursor; Sync flushes durably (or reports the failure); a write that
// extends past the current size grows the file.
type File interface {
	// ReadAt reads len(p) bytes at off, like io.ReaderAt.
	ReadAt(p []byte, off int64) (int, error)
	// WriteAt writes p at off, like io.WriterAt.
	WriteAt(p []byte, off int64) (int, error)
	// Truncate sets the file size to n bytes.
	Truncate(n int64) error
	// Sync flushes buffered writes durably. A non-nil return is fatal to the
	// engine per the fsync-fatal policy (spec 2064 doc 04 §6).
	Sync() error
	// Size returns the current file size in bytes.
	Size() (int64, error)
	// Close releases the handle.
	Close() error
}

// VFS is a namespace of files.
type VFS interface {
	// Open opens name. If it is absent and create is true, an empty file is
	// created; if absent and create is false, ErrNotExist is returned.
	Open(name string, create bool) (File, error)
	// Remove deletes name. Removing an absent file is not an error.
	Remove(name string) error
	// Exists reports whether name is present.
	Exists(name string) bool
}

// ErrInjectedCrash is the error a fault-injecting VFS returns at an injected
// fault point. Callers treat it like any I/O failure; the test harness uses it
// to drive the crash to the recovery path.
var ErrInjectedCrash = errors.New("aki/vfs: injected crash")
