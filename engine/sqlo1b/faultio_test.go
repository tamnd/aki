package sqlo1b

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func faultBase(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "fault.aki"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func readBack(t *testing.T, r *FaultFile, off int64, n int) []byte {
	t.Helper()
	p := make([]byte, n)
	if _, err := r.ReadAt(p, off); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return p
}

func TestFaultFileCacheView(t *testing.T) {
	base := faultBase(t)
	f := NewFaultFile(base)
	want := bytes.Repeat([]byte{0xAB}, 100)
	if _, err := f.WriteAt(want, 50); err != nil {
		t.Fatal(err)
	}

	// Live code sees the write through the cache, even past base EOF.
	if got := readBack(t, f, 50, 100); !bytes.Equal(got, want) {
		t.Fatal("cache view missing the unsynced write")
	}
	// Durable media has nothing yet.
	if st, err := base.Stat(); err != nil || st.Size() != 0 {
		t.Fatalf("unsynced write reached the base: size %d, err %v", st.Size(), err)
	}
	if f.Pending() != 1 {
		t.Fatalf("pending %d, want 1", f.Pending())
	}

	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if f.Pending() != 0 {
		t.Fatal("sync left the cache dirty")
	}
	direct := make([]byte, 100)
	if _, err := base.ReadAt(direct, 50); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(direct, want) {
		t.Fatal("sync did not land the write")
	}
}

func TestFaultFileCrashLosesUnsynced(t *testing.T) {
	base := faultBase(t)
	f := NewFaultFile(base)
	if _, err := f.WriteAt([]byte("durable"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("VOLATILE"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Crash(KeepNone); err != nil {
		t.Fatal(err)
	}
	if got := readBack(t, f, 0, 7); string(got) != "durable" {
		t.Fatalf("after crash: %q", got)
	}
}

func TestFaultFileSectorTear(t *testing.T) {
	base := faultBase(t)
	f := NewFaultFile(base)
	if err := base.Truncate(4 * FaultSectorSize); err != nil {
		t.Fatal(err)
	}
	// One write covering sectors 0..3; only sector 2 lands.
	buf := make([]byte, 4*FaultSectorSize)
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	if _, err := f.WriteAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Crash(func(_ int, sectors int64) []bool {
		if sectors != 4 {
			t.Fatalf("write spans %d sectors, want 4", sectors)
		}
		return []bool{false, false, true, false}
	}); err != nil {
		t.Fatal(err)
	}

	got := readBack(t, f, 0, 4*FaultSectorSize)
	for s := range 4 {
		lo, hi := s*FaultSectorSize, (s+1)*FaultSectorSize
		if s == 2 {
			if !bytes.Equal(got[lo:hi], buf[lo:hi]) {
				t.Fatalf("kept sector %d did not land", s)
			}
			continue
		}
		if !bytes.Equal(got[lo:hi], make([]byte, FaultSectorSize)) {
			t.Fatalf("lost sector %d landed anyway", s)
		}
	}
}

// TestFaultFileReorderedFlush models the disk applying an earlier
// cached write but not the later one that overlapped it: the old
// bytes rule, which is exactly the reordering recovery must survive.
func TestFaultFileReorderedFlush(t *testing.T) {
	base := faultBase(t)
	f := NewFaultFile(base)
	oldb := bytes.Repeat([]byte{0x11}, FaultSectorSize)
	newb := bytes.Repeat([]byte{0x22}, FaultSectorSize)
	if _, err := f.WriteAt(oldb, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(newb, 0); err != nil {
		t.Fatal(err)
	}
	// Before the crash the cache view shows the newest write.
	if got := readBack(t, f, 0, FaultSectorSize); !bytes.Equal(got, newb) {
		t.Fatal("cache view lost the newer write")
	}
	if err := f.Crash(func(w int, sectors int64) []bool {
		m := make([]bool, sectors)
		for i := range m {
			m[i] = w == 0
		}
		return m
	}); err != nil {
		t.Fatal(err)
	}
	if got := readBack(t, f, 0, FaultSectorSize); !bytes.Equal(got, oldb) {
		t.Fatal("reordered flush did not leave the older bytes")
	}
}

func TestFaultFileKeepRandomDeterminism(t *testing.T) {
	image := func(seed uint64) []byte {
		base := faultBase(t)
		f := NewFaultFile(base)
		for i := range 8 {
			buf := bytes.Repeat([]byte{byte(i + 1)}, 3*FaultSectorSize/2)
			if _, err := f.WriteAt(buf, int64(i)*FaultSectorSize); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Crash(KeepRandom(seed)); err != nil {
			t.Fatal(err)
		}
		return readBack(t, f, 0, 10*FaultSectorSize)
	}
	a, b, c := image(7), image(7), image(8)
	if !bytes.Equal(a, b) {
		t.Fatal("the same seed produced different crashes")
	}
	if bytes.Equal(a, c) {
		t.Fatal("different seeds produced the same crash; suspicious flips")
	}
}

// TestFaultFileUnderIOPool proves the harness composes at the iopool
// layer: writes submitted through a real pool are volatile until the
// pool's Sync path runs, and a crash before it loses them.
func TestFaultFileUnderIOPool(t *testing.T) {
	run := func(t *testing.T, syncFirst bool) []byte {
		t.Helper()
		base := faultBase(t)
		if err := base.Truncate(2 * testExtent); err != nil {
			t.Fatal(err)
		}
		if err := NewFaultFile(base).Sync(); err != nil {
			t.Fatal(err)
		}
		f := NewFaultFile(base)
		comp := make(chan IOResult, 8)
		pool := NewIOPool(f, testExtent, 2, comp)
		buf := bytes.Repeat([]byte{0xCD}, GroupSize)
		pool.Submit([]IOReq{
			{Op: OpWrite, Ext: 0, Off: 0, Buf: buf, Tag: 1},
			{Op: OpWrite, Ext: 1, Off: GroupSize, Buf: buf, Tag: 2},
		})
		collect(t, comp, 2)
		if syncFirst {
			pool.Sync(3)
			collect(t, comp, 1)
		}
		pool.Close()
		if err := f.Crash(KeepNone); err != nil {
			t.Fatal(err)
		}
		out := make([]byte, GroupSize)
		if _, err := base.ReadAt(out, 0); err != nil {
			t.Fatal(err)
		}
		return out
	}

	if got := run(t, false); !bytes.Equal(got, make([]byte, GroupSize)) {
		t.Fatal("unsynced pool write survived the crash")
	}
	if got := run(t, true); !bytes.Equal(got, bytes.Repeat([]byte{0xCD}, GroupSize)) {
		t.Fatal("synced pool write lost in the crash")
	}
}

// TestFaultFileTornSuperblock is the format-level demonstration the
// harness exists for: a torn superblock commit destroys only the
// slot being written, and pick-highest-valid falls back to the old
// root.
func TestFaultFileTornSuperblock(t *testing.T) {
	base := faultBase(t)
	f := NewFaultFile(base)
	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}

	next := *sb
	next.Seq = 2
	next.WALTrimSeq = 40
	if _, err := f.WriteAt(next.Encode(), slotAOff); err != nil {
		t.Fatal(err)
	}
	// The commit tears: only the first sector of the 8-sector
	// superblock lands.
	if err := f.Crash(func(_ int, sectors int64) []bool {
		m := make([]bool, sectors)
		m[0] = true
		return m
	}); err != nil {
		t.Fatal(err)
	}
	got, slot, err := ReadSuperblock(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 1 {
		t.Fatalf("torn commit read back seq %d slot %d, want the old root", got.Seq, slot)
	}

	// The retried commit with nothing torn is picked up.
	if _, err := f.WriteAt(next.Encode(), slotAOff); err != nil {
		t.Fatal(err)
	}
	if err := f.Crash(KeepAll); err != nil {
		t.Fatal(err)
	}
	got, _, err = ReadSuperblock(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 2 || got.WALTrimSeq != 40 {
		t.Fatalf("full commit read back seq %d trim %d", got.Seq, got.WALTrimSeq)
	}
}

// TestFaultFileTornGroup shows tears surface as checksum failures:
// a full pointer over a group that lost a sector refuses to verify.
func TestFaultFileTornGroup(t *testing.T) {
	base := faultBase(t)
	f := NewFaultFile(base)
	if err := base.Truncate(GroupSize); err != nil {
		t.Fatal(err)
	}
	b := NewGroupBuilder(GroupSize)
	for range 8 {
		if _, err := b.Append(bytes.Repeat([]byte{0x5A}, 400)); err != nil {
			t.Fatal(err)
		}
	}
	group := b.Close()
	ptr := MakeFullPtr(0, group)
	if _, err := f.WriteAt(group, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Crash(func(_ int, sectors int64) []bool {
		m := KeepAll(0, sectors)
		m[len(m)-1] = false // the slot table sector never lands
		return m
	}); err != nil {
		t.Fatal(err)
	}
	got := readBack(t, f, 0, len(group))
	if err := ptr.Verify(got); err == nil {
		t.Fatal("torn group verified")
	}
}
