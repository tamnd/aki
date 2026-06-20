package vfs

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
)

// runFileContract exercises the File contract against any VFS implementation.
func runFileContract(t *testing.T, fs VFS, name string) {
	t.Helper()
	f, err := fs.Open(name, true)
	if err != nil {
		t.Fatalf("Open create: %v", err)
	}
	defer f.Close()

	if n, err := f.WriteAt([]byte("hello"), 0); err != nil || n != 5 {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	if sz, err := f.Size(); err != nil || sz != 5 {
		t.Fatalf("Size: %d err=%v", sz, err)
	}
	// Positioned write past EOF grows the file.
	if _, err := f.WriteAt([]byte("world"), 10); err != nil {
		t.Fatalf("WriteAt grow: %v", err)
	}
	if sz, _ := f.Size(); sz != 15 {
		t.Fatalf("Size after grow: %d want 15", sz)
	}
	buf := make([]byte, 5)
	if n, err := f.ReadAt(buf, 0); err != nil || n != 5 || string(buf) != "hello" {
		t.Fatalf("ReadAt: n=%d err=%v buf=%q", n, err, buf)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := f.Truncate(5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if sz, _ := f.Size(); sz != 5 {
		t.Fatalf("Size after truncate: %d want 5", sz)
	}
	// Reading at EOF yields io.EOF.
	if _, err := f.ReadAt(buf, 100); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt at EOF: err=%v want io.EOF", err)
	}
}

func TestMemContract(t *testing.T) {
	runFileContract(t, NewMem(), "test.aki")
}

func TestOSContract(t *testing.T) {
	dir := t.TempDir()
	runFileContract(t, NewOS(), filepath.Join(dir, "test.aki"))
}

func TestMemOpenMissing(t *testing.T) {
	m := NewMem()
	if _, err := m.Open("nope", false); !errors.Is(err, ErrNotExist) {
		t.Errorf("got %v want ErrNotExist", err)
	}
}

func TestMemExistsRemove(t *testing.T) {
	m := NewMem()
	f, _ := m.Open("a", true)
	f.Close()
	if !m.Exists("a") {
		t.Error("Exists=false after create")
	}
	if err := m.Remove("a"); err != nil {
		t.Fatal(err)
	}
	if m.Exists("a") {
		t.Error("Exists=true after remove")
	}
	// Removing a missing file is not an error.
	if err := m.Remove("gone"); err != nil {
		t.Errorf("Remove missing: %v", err)
	}
}

func TestMemSharedBacking(t *testing.T) {
	// Two handles to the same name share bytes (models reopen after crash).
	m := NewMem()
	f1, _ := m.Open("s", true)
	f1.WriteAt([]byte("abc"), 0)
	f1.Close()
	f2, _ := m.Open("s", false)
	defer f2.Close()
	buf := make([]byte, 3)
	if _, err := f2.ReadAt(buf, 0); err != nil || string(buf) != "abc" {
		t.Errorf("reopen read: %q err=%v", buf, err)
	}
}

func TestFaultCrashAfterWrites(t *testing.T) {
	fl := NewFault(NewMem())
	f, _ := fl.Open("c", true)
	defer f.Close()
	fl.CrashAfterWrites(2)
	if _, err := f.WriteAt([]byte("1"), 0); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if _, err := f.WriteAt([]byte("2"), 1); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if _, err := f.WriteAt([]byte("3"), 2); !errors.Is(err, ErrInjectedCrash) {
		t.Errorf("write 3: got %v want ErrInjectedCrash", err)
	}
}

func TestFaultCrashAfterSyncs(t *testing.T) {
	fl := NewFault(NewMem())
	f, _ := fl.Open("c", true)
	defer f.Close()
	fl.CrashAfterSyncs(1)
	if err := f.Sync(); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if err := f.Sync(); !errors.Is(err, ErrInjectedCrash) {
		t.Errorf("sync 2: got %v want ErrInjectedCrash", err)
	}
}

func TestFaultTornWrite(t *testing.T) {
	mem := NewMem()
	fl := NewFault(mem)
	f, _ := fl.Open("t", true)
	defer f.Close()
	fl.TornNextWrite(2)
	if _, err := f.WriteAt([]byte("abcde"), 0); !errors.Is(err, ErrInjectedCrash) {
		t.Errorf("torn write: got %v want ErrInjectedCrash", err)
	}
	// Only the 2-byte prefix persisted.
	raw, _ := mem.Open("t", false)
	buf := make([]byte, 5)
	n, _ := raw.ReadAt(buf, 0)
	if n != 2 || string(buf[:2]) != "ab" {
		t.Errorf("torn prefix: n=%d buf=%q", n, buf[:n])
	}
}

func TestFaultSyncEIO(t *testing.T) {
	fl := NewFault(NewMem())
	f, _ := fl.Open("e", true)
	defer f.Close()
	fl.FailSyncEIO(true)
	if err := f.Sync(); !errors.Is(err, ErrInjectedCrash) {
		t.Errorf("got %v want ErrInjectedCrash", err)
	}
	fl.Disarm()
	if err := f.Sync(); err != nil {
		t.Errorf("after disarm: %v", err)
	}
}
