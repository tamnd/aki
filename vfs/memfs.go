package vfs

import (
	"io"
	"sort"
	"sync"
)

// Mem is an in-memory VFS. It backs unit tests and the engine's :memory: mode
// (spec 2064 doc 02 §13). Files are byte slices guarded by a mutex; Sync is a
// no-op because there is no durable medium to flush to.
type Mem struct {
	mu    sync.Mutex
	files map[string]*memData
}

// NewMem returns an empty in-memory VFS.
func NewMem() *Mem {
	return &Mem{files: make(map[string]*memData)}
}

// memData is the shared backing store for a named file. Multiple memFile
// handles may reference the same memData.
type memData struct {
	mu   sync.Mutex
	data []byte
}

// Open opens or, when create is true, creates name.
func (m *Mem) Open(name string, create bool) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.files[name]
	if !ok {
		if !create {
			return nil, ErrNotExist
		}
		d = &memData{}
		m.files[name] = d
	}
	return &memFile{d: d}, nil
}

// Remove deletes name; an absent file is not an error.
func (m *Mem) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)
	return nil
}

// Exists reports whether name is present.
func (m *Mem) Exists(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.files[name]
	return ok
}

// Names returns the sorted list of files, for test inspection.
func (m *Mem) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.files))
	for n := range m.files {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

type memFile struct{ d *memData }

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if off >= int64(len(f.d.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.d.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	end := off + int64(len(p))
	if end > int64(len(f.d.data)) {
		grown := make([]byte, end)
		copy(grown, f.d.data)
		f.d.data = grown
	}
	copy(f.d.data[off:], p)
	return len(p), nil
}

func (f *memFile) Truncate(n int64) error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if n <= int64(len(f.d.data)) {
		f.d.data = f.d.data[:n]
		return nil
	}
	grown := make([]byte, n)
	copy(grown, f.d.data)
	f.d.data = grown
	return nil
}

func (f *memFile) Sync() error { return nil }

func (f *memFile) Size() (int64, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return int64(len(f.d.data)), nil
}

func (f *memFile) Close() error { return nil }
