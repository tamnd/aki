package vfs

import "os"

// OS is the production VFS backed by the operating system's filesystem. It is
// the default backend the engine opens a real .aki file through.
type OS struct{}

// NewOS returns an OS-backed VFS.
func NewOS() *OS { return &OS{} }

// Open opens or creates name on the real filesystem.
func (o *OS) Open(name string, create bool) (File, error) {
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(name, flag, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotExist
		}
		return nil, err
	}
	return &osFile{f: f}, nil
}

// Remove deletes name; an absent file is not an error.
func (o *OS) Remove(name string) error {
	err := os.Remove(name)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

// Exists reports whether name is present.
func (o *OS) Exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// osFile wraps an *os.File to satisfy File.
type osFile struct{ f *os.File }

func (o *osFile) ReadAt(p []byte, off int64) (int, error)  { return o.f.ReadAt(p, off) }
func (o *osFile) WriteAt(p []byte, off int64) (int, error) { return o.f.WriteAt(p, off) }
func (o *osFile) Truncate(n int64) error                   { return o.f.Truncate(n) }
func (o *osFile) Sync() error                              { return o.f.Sync() }
func (o *osFile) Close() error                             { return o.f.Close() }

func (o *osFile) Size() (int64, error) {
	st, err := o.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
