package sqlo1b

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
)

// fixture returns a superblock with every field nonzero, so zeroing
// any covered field in the encoding is a real corruption.
func fixture(seq uint64) *Superblock {
	sb := &Superblock{
		Version:      FormatVersion,
		IOUnit:       DefaultIOUnit,
		ExtentSize:   DefaultExtentSize,
		Flags:        0xA5A50001,
		Seq:          seq,
		ExtentCount:  9,
		WALTrimSeq:   77,
		HashEpoch:    PackHashEpoch(1234, 5),
		DirRoot:      FullPtr{Pos: 0x1111, Sum: 0x2222},
		AllocmapRoot: FullPtr{Pos: 0x3333, Sum: 0x4444},
		DictRoot:     FullPtr{Pos: 0x5555, Sum: 0x6666},
		StatsRoot:    FullPtr{Pos: 0x7777, Sum: 0x8888},
		RecordCount:    123456,
		GarbageBytes:   654321,
		HighWater:      987654,
		KeyRecordCount: 424242,
	}
	for i := range sb.DBID {
		sb.DBID[i] = byte(i + 1)
	}
	return sb
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	want := fixture(42)
	got, err := DecodeSuperblock(want.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("roundtrip mismatch:\ngot  %+v\nwant %+v", got, want)
	}
}

// TestEncodeOffsets pins the on-disk table: this is the format, so
// the bytes are asserted, not just the roundtrip.
func TestEncodeOffsets(t *testing.T) {
	b := fixture(42).Encode()
	if got := string(b[0:15]); got != "tamndaki sqlob1" {
		t.Fatalf("magic %q", got)
	}
	if b[15] != 0 {
		t.Fatal("magic missing trailing NUL")
	}
	if got := binary.LittleEndian.Uint32(b[16:]); got != FormatVersion {
		t.Fatalf("version at 16 = %d", got)
	}
	if got := binary.LittleEndian.Uint32(b[20:]); got != DefaultIOUnit {
		t.Fatalf("io_unit at 20 = %d", got)
	}
	if got := binary.LittleEndian.Uint64(b[32:]); got != 42 {
		t.Fatalf("seq at 32 = %d", got)
	}
	if got := binary.LittleEndian.Uint64(b[80:]); got != 0x1111 {
		t.Fatalf("dir_root pos at 80 = %#x", got)
	}
	if got := int64(binary.LittleEndian.Uint64(b[160:])); got != 987654 {
		t.Fatalf("high_water at 160 = %d", got)
	}
	if got := binary.LittleEndian.Uint64(b[168:]); got != 424242 {
		t.Fatalf("key_record_count at 168 = %d", got)
	}
	for i := 176; i < 4080; i++ {
		if b[i] != 0 {
			t.Fatalf("reserved byte %d not zero", i)
		}
	}
	if got := binary.LittleEndian.Uint64(b[4080:]); got != 42 {
		t.Fatalf("commit_seq_echo at 4080 = %d", got)
	}
	if got := binary.LittleEndian.Uint64(b[4088:]); got != xxhash.Sum64(b[:4088]) {
		t.Fatal("checksum at 4088 does not seal bytes 0..4087")
	}
}

// superFields is the doc 03 section 3 table, used by the corruption
// matrix so every field is attacked by name.
var superFields = []struct {
	name string
	off  int
	size int
}{
	{"magic", 0, 16},
	{"version", 16, 4},
	{"io_unit", 20, 4},
	{"extent_size", 24, 4},
	{"flags", 28, 4},
	{"seq", 32, 8},
	{"extent_count", 40, 8},
	{"db_id", 48, 16},
	{"wal_trim_seq", 64, 8},
	{"hash_epoch", 72, 8},
	{"dir_root", 80, 16},
	{"allocmap_root", 96, 16},
	{"dict_root", 112, 16},
	{"stats_root", 128, 16},
	{"record_count", 144, 8},
	{"garbage_bytes", 152, 8},
	{"high_water", 160, 8},
	{"key_record_count", 168, 8},
	{"reserved", 176, 3904},
	{"commit_seq_echo", 4080, 8},
	{"checksum", 4088, 8},
}

// TestCorruptionMatrix zeroes each field of slot A in turn and checks
// two things: the standalone decode verdict on the corrupted copy,
// and that open still lands on the surviving slot B root. Zeroing
// reserved is the one no-op, because zero is its encoded form.
func TestCorruptionMatrix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.aki")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	slotA := fixture(5).Encode()
	if _, err := f.WriteAt(slotA, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(fixture(4).Encode(), SuperblockSize); err != nil {
		t.Fatal(err)
	}

	for _, fld := range superFields {
		corrupted := make([]byte, SuperblockSize)
		copy(corrupted, slotA)
		for i := range fld.size {
			corrupted[fld.off+i] = 0
		}
		_, decErr := DecodeSuperblock(corrupted)
		if fld.name == "reserved" {
			if decErr != nil {
				t.Fatalf("zeroed %s must stay valid, got %v", fld.name, decErr)
			}
		} else if decErr == nil {
			t.Fatalf("zeroed %s decoded without error", fld.name)
		}

		if _, err := f.WriteAt(corrupted, 0); err != nil {
			t.Fatal(err)
		}
		sb, slot, err := ReadSuperblock(f)
		if err != nil {
			t.Fatalf("zeroed %s: open failed outright: %v", fld.name, err)
		}
		if fld.name == "reserved" {
			if sb.Seq != 5 || slot != 0 {
				t.Fatalf("zeroed reserved: picked seq %d slot %d, want 5/0", sb.Seq, slot)
			}
		} else if sb.Seq != 4 || slot != 1 {
			t.Fatalf("zeroed %s: picked seq %d slot %d, want the survivor 4/1", fld.name, sb.Seq, slot)
		}
		if _, err := f.WriteAt(slotA, 0); err != nil {
			t.Fatal(err)
		}
	}
}

// TestTornSlots tears slot A at several prefix lengths, then both
// slots, pinning that a torn write can only cost the copy it hit.
func TestTornSlots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.aki")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(fixture(8).Encode(), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(fixture(7).Encode(), SuperblockSize); err != nil {
		t.Fatal(err)
	}

	incoming := fixture(9).Encode()
	for _, tear := range []int{512, 2048, 4087} {
		if _, err := f.WriteAt(incoming[:tear], 0); err != nil {
			t.Fatal(err)
		}
		sb, slot, err := ReadSuperblock(f)
		if err != nil {
			t.Fatalf("tear at %d: %v", tear, err)
		}
		if sb.Seq != 7 || slot != 1 {
			t.Fatalf("tear at %d: picked seq %d slot %d, want 7/1", tear, sb.Seq, slot)
		}
	}

	if _, err := f.WriteAt(incoming[:100], SuperblockSize); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadSuperblock(f); !errors.Is(err, ErrNoSuperblock) {
		t.Fatalf("both torn: got %v, want ErrNoSuperblock", err)
	}
}

// TestEchoGuard rewrites seq and reseals the checksum, so only the
// 4080 echo can catch the mismatch; this is the odd-device partial
// 4 KiB write the field exists for.
func TestEchoGuard(t *testing.T) {
	b := fixture(7).Encode()
	binary.LittleEndian.PutUint64(b[32:], 8)
	binary.LittleEndian.PutUint64(b[4088:], xxhash.Sum64(b[:4088]))
	_, err := DecodeSuperblock(b)
	if err == nil || !strings.Contains(err.Error(), "echo") {
		t.Fatalf("got %v, want the echo guard", err)
	}
}

// TestVersionGuard reseals a version-2 superblock; the checksum is
// fine, the version is not ours, decode must refuse.
func TestVersionGuard(t *testing.T) {
	b := fixture(7).Encode()
	binary.LittleEndian.PutUint32(b[16:], 2)
	binary.LittleEndian.PutUint64(b[4088:], xxhash.Sum64(b[:4088]))
	if _, err := DecodeSuperblock(b); err == nil {
		t.Fatal("version 2 decoded without error")
	}
}

// TestCommitAlternation walks seq 2..5 and checks each commit lands
// on seq%2's slot while the other slot keeps the previous root.
func TestCommitAlternation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.aki")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb := fixture(1)
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}

	rawSeq := func(slot int) uint64 {
		b := make([]byte, SuperblockSize)
		if _, err := f.ReadAt(b, int64(slot)*SuperblockSize); err != nil {
			t.Fatal(err)
		}
		return binary.LittleEndian.Uint64(b[32:])
	}
	if rawSeq(0) != 1 || rawSeq(1) != 1 {
		t.Fatalf("init slots hold %d/%d, want 1/1", rawSeq(0), rawSeq(1))
	}

	for seq := uint64(2); seq <= 5; seq++ {
		sb.Seq = seq
		sb.ExtentCount = seq * 10
		if err := CommitSuperblock(f, sb); err != nil {
			t.Fatal(err)
		}
		wantOther := seq - 1
		gotThis, gotOther := rawSeq(int(seq%2)), rawSeq(int((seq+1)%2))
		if gotThis != seq || gotOther != wantOther {
			t.Fatalf("after commit %d: slots hold %d/%d, want %d and %d",
				seq, gotThis, gotOther, seq, wantOther)
		}
		picked, slot, err := ReadSuperblock(f)
		if err != nil {
			t.Fatal(err)
		}
		if picked.Seq != seq || slot != int(seq%2) {
			t.Fatalf("after commit %d: picked seq %d slot %d", seq, picked.Seq, slot)
		}
	}
}

func TestPackHashEpoch(t *testing.T) {
	for _, tc := range []struct {
		split uint64
		level uint8
	}{{0, 0}, {1, 1}, {1 << 55, 255}, {0xFFFFFFFFFFFFFF, 7}} {
		s, l := UnpackHashEpoch(PackHashEpoch(tc.split, tc.level))
		if s != tc.split || l != tc.level {
			t.Fatalf("roundtrip (%d,%d) -> (%d,%d)", tc.split, tc.level, s, l)
		}
	}
}

func TestNewSuperblock(t *testing.T) {
	a, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if a.Seq != 1 || a.Version != FormatVersion || a.IOUnit != DefaultIOUnit || a.ExtentSize != DefaultExtentSize {
		t.Fatalf("creation defaults wrong: %+v", a)
	}
	if a.DBID == ([16]byte{}) || a.DBID == b.DBID {
		t.Fatal("db_id must be random and unique")
	}
	if _, err := DecodeSuperblock(a.Encode()); err != nil {
		t.Fatalf("fresh superblock does not verify: %v", err)
	}
}
