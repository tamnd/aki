package sqlo1b

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// subkey builds a well-formed 16-byte synthetic key for tests.
func subkey(rooth uint64, kind uint8, segid uint64) []byte {
	b := make([]byte, SubkeySize)
	binary.LittleEndian.PutUint64(b, rooth)
	b[8] = kind
	b[9] = byte(segid)
	b[10] = byte(segid >> 8)
	return b
}

// TestRecordLayoutGolden pins the doc 03 section 6.1 byte layout by
// hand: any offset drift in the encoder fails here first.
func TestRecordLayoutGolden(t *testing.T) {
	r := &Record{
		RType:    RecString,
		RFlags:   RFlagExpiry,
		Key:      []byte("k1"),
		Value:    []byte("hello"),
		ExpireMS: 0x0123456789ABCDEF,
	}
	got, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 12+8+2+5+4)
	binary.LittleEndian.PutUint32(want[0:], uint32(len(want)))
	want[4] = RecString
	want[5] = RFlagExpiry
	binary.LittleEndian.PutUint16(want[6:], 2)
	binary.LittleEndian.PutUint32(want[8:], 5)
	binary.LittleEndian.PutUint64(want[12:], 0x0123456789ABCDEF)
	copy(want[20:], "k1")
	copy(want[22:], "hello")
	binary.LittleEndian.PutUint32(want[27:], crc32.Checksum(want[:27], crcTable))
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded record\n got %x\nwant %x", got, want)
	}
	if r.EncodedLen() != len(want) {
		t.Fatalf("EncodedLen %d, want %d", r.EncodedLen(), len(want))
	}
}

// legalRecords covers every rtype at a legal shape, optional fields
// on and off.
func legalRecords() []*Record {
	return []*Record{
		{RType: RecString, Key: []byte("plain"), Value: []byte("v")},
		{RType: RecString, RFlags: RFlagExpiry | RFlagDict, Key: []byte("x"), Value: bytes.Repeat([]byte{7}, 300), ExpireMS: 99},
		{RType: RecRoot, RFlags: RFlagExpiry, Key: []byte("mylist"), Value: []byte{1, 2, 3}, ExpireMS: 1 << 41},
		{RType: RecRoot, Key: []byte("s"), Value: nil},
		{RType: RecSeg, RFlags: RFlagRootgen, Key: subkey(42, 1, 7), Value: []byte("elems"), Rootgen: 3},
		{RType: RecSeg, RFlags: RFlagRootgen | RFlagDict, Key: subkey(1, 2, 0), Value: []byte("z"), Rootgen: 1 << 30},
		{RType: RecTomb, Key: []byte("gone")},
		{RType: RecTomb, RFlags: RFlagExpiry, Key: []byte("gone2"), ExpireMS: 5},
		{RType: RecFence, RFlags: RFlagRootgen, Key: subkey(9, 3, 1), Value: []byte("fence page"), Rootgen: 2},
		{RType: RecMeta, Key: []byte{0xFF}, Value: []byte("internal")},
	}
}

func TestRecordRoundtripVariants(t *testing.T) {
	for i, r := range legalRecords() {
		enc, err := r.Encode()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		got, err := DecodeRecord(enc)
		if err != nil {
			t.Fatalf("record %d decode: %v", i, err)
		}
		if got.RType != r.RType || got.RFlags != r.RFlags ||
			!bytes.Equal(got.Key, r.Key) || !bytes.Equal(got.Value, r.Value) {
			t.Fatalf("record %d roundtrip diverged: %+v vs %+v", i, got, r)
		}
		if got.HasExpiry() && got.ExpireMS != r.ExpireMS {
			t.Fatalf("record %d expiry %d, want %d", i, got.ExpireMS, r.ExpireMS)
		}
		if got.HasRootgen() && got.Rootgen != r.Rootgen {
			t.Fatalf("record %d rootgen %d, want %d", i, got.Rootgen, r.Rootgen)
		}
	}
}

// TestRecordTailTrim decodes a record with trailing pad bytes, the
// shape GroupView.Record hands over for a group's last slot.
func TestRecordTailTrim(t *testing.T) {
	enc, err := (&Record{RType: RecString, Key: []byte("k"), Value: []byte("v")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	padded := append(append([]byte{}, enc...), make([]byte, 40)...)
	binary.LittleEndian.PutUint32(padded[len(enc):], PadMarker)
	got, err := DecodeRecord(padded)
	if err != nil {
		t.Fatal(err)
	}
	reenc, err := got.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reenc, enc) {
		t.Fatalf("re-encode with pad tail diverged:\n got %x\nwant %x", reenc, enc)
	}
}

func TestRecordEncodeRejects(t *testing.T) {
	cases := []struct {
		name string
		rec  *Record
	}{
		{"rtype zero", &Record{RType: 0, Key: []byte("k")}},
		{"rtype past meta", &Record{RType: RecMeta + 1, Key: []byte("k")}},
		{"reserved flag bit", &Record{RType: RecString, RFlags: 1 << 3, Key: []byte("k")}},
		{"empty key", &Record{RType: RecString, Key: nil}},
		{"oversized key", &Record{RType: RecString, Key: make([]byte, 65536)}},
		{"tomb with value", &Record{RType: RecTomb, Key: []byte("k"), Value: []byte("v")}},
		{"tomb with dict bit", &Record{RType: RecTomb, RFlags: RFlagDict, Key: []byte("k")}},
		{"seg without rootgen", &Record{RType: RecSeg, Key: subkey(1, 1, 1)}},
		{"seg with short subkey", &Record{RType: RecSeg, RFlags: RFlagRootgen, Key: []byte("short")}},
		{"fence with long subkey", &Record{RType: RecFence, RFlags: RFlagRootgen, Key: make([]byte, SubkeySize+1), Rootgen: 1}},
		{"fence without rootgen", &Record{RType: RecFence, Key: subkey(9, 3, 1)}},
		{"rootgen on string", &Record{RType: RecString, RFlags: RFlagRootgen, Key: []byte("k")}},
		{"rootgen on root", &Record{RType: RecRoot, RFlags: RFlagRootgen, Key: []byte("k")}},
	}
	for _, c := range cases {
		if _, err := c.rec.Encode(); err == nil {
			t.Errorf("%s: encoded without error", c.name)
		}
	}
}

// reseal recomputes rcrc after a deliberate structural edit, so the
// decode failure under test is the structural check, not the crc.
func reseal(b []byte) {
	binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], crcTable))
}

func TestRecordDecodeRejects(t *testing.T) {
	base, err := (&Record{RType: RecString, Key: []byte("key"), Value: []byte("value")}).Encode()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := DecodeRecord(base[:15]); err == nil {
		t.Error("decoded a truncated header")
	}
	short := append([]byte{}, base...)
	if _, err := DecodeRecord(short[:len(short)-1]); err == nil {
		t.Error("decoded with rlen past the buffer")
	}

	tiny := append([]byte{}, base...)
	binary.LittleEndian.PutUint32(tiny, recHdrSize+recTailSize-1)
	if _, err := DecodeRecord(tiny); err == nil {
		t.Error("decoded with rlen below the envelope minimum")
	}

	// Structural damage under a valid crc must still fail: the crc
	// proves the bytes, validation proves the shape.
	badType := append([]byte{}, base...)
	badType[4] = 0
	reseal(badType)
	if _, err := DecodeRecord(badType); err == nil {
		t.Error("decoded rtype 0 under a valid crc")
	}
	badFlags := append([]byte{}, base...)
	badFlags[5] = 1 << 6
	reseal(badFlags)
	if _, err := DecodeRecord(badFlags); err == nil {
		t.Error("decoded reserved rflags under a valid crc")
	}
	badMath := append([]byte{}, base...)
	binary.LittleEndian.PutUint32(badMath[8:], 6)
	reseal(badMath)
	if _, err := DecodeRecord(badMath); err == nil {
		t.Error("decoded with vlen disagreeing with rlen")
	}
	segFlags := append([]byte{}, base...)
	segFlags[4] = RecSeg
	reseal(segFlags)
	if _, err := DecodeRecord(segFlags); err == nil {
		t.Error("decoded a seg record without rootgen or subkey shape")
	}
}

// TestRecordCorruptionSweep is F6's blunt half: every single-byte
// corruption inside the record must fail decode, and bytes past rlen
// must not matter.
func TestRecordCorruptionSweep(t *testing.T) {
	for i, r := range legalRecords() {
		enc, err := r.Encode()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		for off := range enc {
			mut := append([]byte{}, enc...)
			mut[off] ^= 0xFF
			if _, err := DecodeRecord(mut); err == nil {
				t.Fatalf("record %d decoded with byte %d flipped", i, off)
			}
		}
		tail := append(append([]byte{}, enc...), 0xDE, 0xAD, 0xBE, 0xEF)
		if _, err := DecodeRecord(tail); err != nil {
			t.Fatalf("record %d failed on trailing garbage: %v", i, err)
		}
	}
}

// FuzzRecordDecode drives raw bytes at the decoder: it must never
// panic, and anything it accepts must re-encode to the identical
// prefix, which is the canonical-form property the corpus leans on.
func FuzzRecordDecode(f *testing.F) {
	for _, r := range legalRecords() {
		enc, err := r.Encode()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(enc)
	}
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xFF}, 64))
	f.Fuzz(func(t *testing.T, b []byte) {
		rec, err := DecodeRecord(b)
		if err != nil {
			return
		}
		reenc, err := rec.Encode()
		if err != nil {
			t.Fatalf("accepted record failed to re-encode: %v", err)
		}
		if !bytes.Equal(reenc, b[:len(reenc)]) {
			t.Fatalf("re-encode diverged:\n got %x\nwant %x", reenc, b[:len(reenc)])
		}
	})
}

// FuzzRecordRoundtrip drives structured inputs at the encoder: any
// record Encode accepts must decode back field for field.
func FuzzRecordRoundtrip(f *testing.F) {
	f.Add(uint8(RecString), uint8(0), []byte("k"), []byte("v"), uint64(0), uint32(0))
	f.Add(uint8(RecSeg), RFlagRootgen, subkey(3, 1, 2), []byte("seg"), uint64(0), uint32(9))
	f.Add(uint8(RecTomb), RFlagExpiry, []byte("dead"), []byte{}, uint64(123), uint32(0))
	f.Fuzz(func(t *testing.T, rtype, rflags uint8, key, value []byte, exp uint64, gen uint32) {
		r := &Record{RType: rtype, RFlags: rflags, Key: key, Value: value, ExpireMS: exp, Rootgen: gen}
		enc, err := r.Encode()
		if err != nil {
			return
		}
		if len(enc) != r.EncodedLen() {
			t.Fatalf("Encode emitted %d bytes, EncodedLen says %d", len(enc), r.EncodedLen())
		}
		got, err := DecodeRecord(enc)
		if err != nil {
			t.Fatalf("encoded record failed decode: %v", err)
		}
		if got.RType != rtype || got.RFlags != rflags ||
			!bytes.Equal(got.Key, key) || !bytes.Equal(got.Value, value) {
			t.Fatalf("roundtrip diverged: %+v", got)
		}
		if got.HasExpiry() && got.ExpireMS != exp {
			t.Fatalf("expiry %d, want %d", got.ExpireMS, exp)
		}
		if got.HasRootgen() && got.Rootgen != gen {
			t.Fatalf("rootgen %d, want %d", got.Rootgen, gen)
		}
	})
}
