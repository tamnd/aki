package pager

import (
	"testing"

	"github.com/tamnd/aki/vfs"
)

// validImage builds a real .aki file in memory and returns its bytes, the seed
// the file-format fuzzer mutates.
func validImage(t *testing.F) []byte {
	t.Helper()
	fs := vfs.NewMem()
	p, err := Create(fs, "seed.aki", Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = p.Close()
	f, err := fs.Open("seed.aki", false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = f.Close() }()
	size, err := f.Size()
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read: %v", err)
	}
	return buf
}

// FuzzOpen mutates a valid .aki image and opens it. Open must reject a corrupt
// file with an error, never panic (doc 23 §7.5). A file that opens is closed.
func FuzzOpen(f *testing.F) {
	img := validImage(f)
	f.Add(img)
	if len(img) > 32 {
		f.Add(img[:32]) // truncated header
	}
	f.Add([]byte{})
	f.Add([]byte("tamndaki fmt001"))

	f.Fuzz(func(t *testing.T, data []byte) {
		fs := vfs.NewMem()
		file, err := fs.Open("fuzz.aki", true)
		if err != nil {
			t.Fatalf("open mem file: %v", err)
		}
		if _, err := file.WriteAt(data, 0); err != nil {
			_ = file.Close()
			t.Fatalf("write: %v", err)
		}
		_ = file.Close()

		p, err := Open(fs, "fuzz.aki", Options{})
		if err != nil {
			return
		}
		_ = p.Close()
	})
}
