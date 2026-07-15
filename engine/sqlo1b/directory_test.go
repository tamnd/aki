package sqlo1b

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestDirPagesArithmetic(t *testing.T) {
	cases := []struct{ len, pages uint64 }{
		{1, 1}, {255, 1}, {256, 1}, {257, 2}, {512, 2}, {513, 3}, {600, 3},
	}
	for _, tc := range cases {
		if got := DirPages(tc.len); got != tc.pages {
			t.Errorf("DirPages(%d) = %d, want %d", tc.len, got, tc.pages)
		}
	}
	if DirPageEntries != 256 {
		t.Fatalf("DirPageEntries = %d, the doc 8.4 page is 256 entries", DirPageEntries)
	}
}

func dirPtr(i uint64) FullPtr {
	return FullPtr{Pos: 0x1000_0000 + i, Sum: 0xAAAA_0000_0000_0000 | i}
}

func TestDirectoryOps(t *testing.T) {
	d := NewDirectory(dirPtr(0))
	if d.Len() != 1 {
		t.Fatalf("fresh directory has %d buckets", d.Len())
	}
	if got := d.Append(dirPtr(1)); got != 1 {
		t.Fatalf("append returned bucket %d, want 1", got)
	}
	if err := d.Set(0, dirPtr(7)); err != nil {
		t.Fatal(err)
	}
	p, err := d.Get(0)
	if err != nil || p != dirPtr(7) {
		t.Fatalf("Get(0) = %v, %v", p, err)
	}
	if _, err := d.Get(2); err == nil {
		t.Error("Get past the last bucket")
	}
	if err := d.Set(2, dirPtr(9)); err == nil {
		t.Error("Set past the last bucket")
	}
}

func TestDirPageLayoutGolden(t *testing.T) {
	d := NewDirectory(FullPtr{Pos: 0x1122334455667788, Sum: 0x99AABBCCDDEEFF00})
	d.Append(FullPtr{Pos: 2, Sum: 3})
	pages := d.Pages()
	if len(pages) != 1 || len(pages[0]) != GroupSize {
		t.Fatalf("2 buckets encoded as %d pages", len(pages))
	}
	b := pages[0]
	if binary.LittleEndian.Uint64(b[0:8]) != 0x1122334455667788 {
		t.Errorf("entry 0 pos bytes %x", b[0:8])
	}
	if binary.LittleEndian.Uint64(b[8:16]) != 0x99AABBCCDDEEFF00 {
		t.Errorf("entry 0 sum bytes %x", b[8:16])
	}
	if binary.LittleEndian.Uint64(b[16:24]) != 2 || binary.LittleEndian.Uint64(b[24:32]) != 3 {
		t.Error("entry 1 not at offset 16")
	}
	for i := 32; i < GroupSize; i++ {
		if b[i] != 0 {
			t.Fatalf("nonzero byte at %d past the live entries", i)
		}
	}
}

// TestDirectoryRoundTrip walks the full checkpoint shape: 600
// buckets over three pages, page pointers minted like the store
// will, a root image, and a load through a fetch that serves raw
// groups.
func TestDirectoryRoundTrip(t *testing.T) {
	const n = 600
	d := NewDirectory(dirPtr(0))
	for i := uint64(1); i < n; i++ {
		d.Append(dirPtr(i))
	}
	pages := d.Pages()
	if len(pages) != 3 {
		t.Fatalf("600 buckets encoded as %d pages, want 3", len(pages))
	}
	byPos := map[uint64][]byte{}
	pagePtrs := make([]FullPtr, len(pages))
	for i, pg := range pages {
		pos, err := NewPos(9, uint16(i), 0)
		if err != nil {
			t.Fatal(err)
		}
		pagePtrs[i] = MakeFullPtr(pos, pg)
		byPos[uint64(pos)] = pg
	}
	root := EncodeDirRoot(pagePtrs)
	if len(root) != 3*16 {
		t.Fatalf("root image is %d bytes, want 48", len(root))
	}
	fetches := 0
	fetch := func(p FullPtr) ([]byte, error) {
		fetches++
		pg, ok := byPos[p.Pos]
		if !ok {
			return nil, fmt.Errorf("no page at pos %#x", p.Pos)
		}
		return pg, nil
	}
	got, err := LoadDirectory(root, n, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 3 {
		t.Fatalf("load fetched %d pages, want 3", fetches)
	}
	if got.Len() != n {
		t.Fatalf("loaded %d buckets, want %d", got.Len(), n)
	}
	for i := range uint64(n) {
		p, err := got.Get(i)
		if err != nil || p != dirPtr(i) {
			t.Fatalf("bucket %d came back %v, %v", i, p, err)
		}
	}
}

func TestDirectoryLoadRejects(t *testing.T) {
	const n = 300
	d := NewDirectory(dirPtr(0))
	for i := uint64(1); i < n; i++ {
		d.Append(dirPtr(i))
	}
	pages := d.Pages()
	pagePtrs := make([]FullPtr, len(pages))
	for i, pg := range pages {
		pos, err := NewPos(9, uint16(i), 0)
		if err != nil {
			t.Fatal(err)
		}
		pagePtrs[i] = MakeFullPtr(pos, pg)
	}
	root := EncodeDirRoot(pagePtrs)
	serve := func(mutate func(pageNo int, pg []byte)) func(FullPtr) ([]byte, error) {
		return func(p FullPtr) ([]byte, error) {
			for i, pp := range pagePtrs {
				if pp.Pos == p.Pos {
					pg := append([]byte(nil), pages[i]...)
					mutate(i, pg)
					return pg, nil
				}
			}
			return nil, errors.New("unknown page")
		}
	}

	if _, err := LoadDirectory(root[:len(root)-1], n, serve(func(int, []byte) {})); err == nil {
		t.Error("truncated root loaded")
	}
	if _, err := LoadDirectory(root, n+256, serve(func(int, []byte) {})); err == nil {
		t.Error("root missing a page loaded")
	}
	// An off-by-one inside the last page is invisible here: the tail
	// is zeros, so bucket 300 decodes as a zero pointer. dirLen comes
	// from the hash_epoch committed beside dir_root, so this is a
	// format-core bug, not corruption, and the zero pointer fails
	// Verify on its first use anyway. Pin the behavior.
	offByOne, err := LoadDirectory(root, n+1, serve(func(int, []byte) {}))
	if err != nil {
		t.Fatalf("off-by-one inside the zero tail should load: %v", err)
	}
	if p, err := offByOne.Get(n); err != nil || p != (FullPtr{}) {
		t.Fatalf("phantom bucket came back %v, %v, want the zero pointer", p, err)
	}
	if _, err := LoadDirectory(root, n, serve(func(pageNo int, pg []byte) { pg[100] ^= 1 })); err == nil {
		t.Error("flipped page passed the checksum")
	}
	if _, err := LoadDirectory(root, n, func(FullPtr) ([]byte, error) { return nil, errors.New("disk") }); err == nil {
		t.Error("fetch error swallowed")
	}
}

func TestDecodeDirPageRejects(t *testing.T) {
	page := make([]byte, GroupSize)
	if _, err := DecodeDirPage(page[:100], 0, 1); err == nil {
		t.Error("short page decoded")
	}
	if _, err := DecodeDirPage(page, 1, 256); err == nil {
		t.Error("page past the directory decoded")
	}
	bad := make([]byte, GroupSize)
	bad[dirEntrySize*2] = 1 // entry 2 of a 2-entry page
	if _, err := DecodeDirPage(bad, 0, 2); err == nil {
		t.Error("garbage past the live entries decoded")
	}
	// The same byte is a live entry when the page holds 3.
	if _, err := DecodeDirPage(bad, 0, 3); err != nil {
		t.Errorf("live third entry rejected: %v", err)
	}
	// A full page's last entry is live at exactly 256 buckets.
	full := make([]byte, GroupSize)
	full[GroupSize-1] = 0xFF
	if _, err := DecodeDirPage(full, 0, 256); err != nil {
		t.Errorf("full page rejected: %v", err)
	}
	if _, err := DecodeDirPage(full, 0, 255); err == nil {
		t.Error("256th entry accepted in a 255-bucket directory")
	}
}

// TestDirectoryTracksSplits drives the directory the way the store
// will: every SplitBucket lands as one Set and one Append, and the
// directory length tracks NumBuckets through a few levels.
func TestDirectoryTracksSplits(t *testing.T) {
	d := NewDirectory(dirPtr(0))
	level, split := uint8(0), uint64(0)
	for i := range uint64(20) {
		s := split
		if err := d.Set(s, dirPtr(1000+i)); err != nil {
			t.Fatal(err)
		}
		newBucket := d.Append(dirPtr(2000 + i))
		if newBucket != s+uint64(1)<<level {
			t.Fatalf("split %d appended bucket %d, want %d", i, newBucket, s+uint64(1)<<level)
		}
		level, split = AdvanceSplit(level, split)
		if d.Len() != NumBuckets(level, split) {
			t.Fatalf("after split %d directory holds %d buckets, NumBuckets says %d", i, d.Len(), NumBuckets(level, split))
		}
	}
}
