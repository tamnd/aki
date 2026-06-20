package format

import (
	"bytes"
	"testing"
)

func TestMagicBytes(t *testing.T) {
	want := []byte("tamndaki fmt001\n")
	if !bytes.Equal(Magic[:], want) {
		t.Errorf("Magic=%q want %q", Magic[:], want)
	}
	if len(Magic) != 16 {
		t.Errorf("Magic length %d want 16", len(Magic))
	}
}

func TestValidPageSize(t *testing.T) {
	good := []uint32{4096, 8192, 16384, 32768, 65536}
	for _, n := range good {
		if !ValidPageSize(n) {
			t.Errorf("ValidPageSize(%d)=false want true", n)
		}
	}
	bad := []uint32{0, 1, 2048, 4095, 4097, 24576, 65537, 131072}
	for _, n := range bad {
		if ValidPageSize(n) {
			t.Errorf("ValidPageSize(%d)=true want false", n)
		}
	}
}

func TestFileHeaderRoundTrip(t *testing.T) {
	h := NewFileHeader(DefaultPageSize, DefaultDBCount, 1719000000000000)
	h.UserVersion = 42
	h.ChangeCounter = 7
	b := make([]byte, DefaultPageSize)
	if err := h.MarshalTo(b); err != nil {
		t.Fatal(err)
	}
	got, err := ParseFileHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.PageSize != DefaultPageSize || got.DBCount != DefaultDBCount {
		t.Errorf("page/db mismatch: %+v", got)
	}
	if got.UserVersion != 42 || got.ChangeCounter != 7 {
		t.Errorf("mutable fields lost: %+v", got)
	}
	if got.FreelistHead != NullPage || got.CatalogRoot != NullPage {
		t.Errorf("sentinels wrong: head=%#x root=%#x", got.FreelistHead, got.CatalogRoot)
	}
	if got.MetaPageA != 1 || got.MetaPageB != 2 {
		t.Errorf("meta page numbers wrong: %d %d", got.MetaPageA, got.MetaPageB)
	}
}

func TestFileHeaderBadMagic(t *testing.T) {
	b := make([]byte, DefaultPageSize)
	NewFileHeader(DefaultPageSize, DefaultDBCount, 0).MarshalTo(b)
	b[0] ^= 0xFF
	if _, err := ParseFileHeader(b); err != ErrBadMagic {
		t.Errorf("got %v want ErrBadMagic", err)
	}
}

func TestFileHeaderBadChecksum(t *testing.T) {
	b := make([]byte, DefaultPageSize)
	NewFileHeader(DefaultPageSize, DefaultDBCount, 0).MarshalTo(b)
	b[50] ^= 0xFF // corrupt a covered byte, leave magic intact
	if _, err := ParseFileHeader(b); err != ErrBadChecksum {
		t.Errorf("got %v want ErrBadChecksum", err)
	}
}

func TestPageHeaderRoundTrip(t *testing.T) {
	h := PageHeader{
		Type:      PageTypeBTreeLeaf,
		Flags:     0,
		CellCount: 12,
		FreeStart: 200,
		FreeEnd:   16000,
		PageLSN:   0xCAFEBABE,
	}
	b := make([]byte, CommonHeaderSize)
	if err := h.MarshalTo(b); err != nil {
		t.Fatal(err)
	}
	got, err := ParsePageHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Errorf("round-trip: got %+v want %+v", got, h)
	}
	if got.FreeSpace() != 16000-200 {
		t.Errorf("FreeSpace=%d", got.FreeSpace())
	}
}

func TestMetaPageRoundTrip(t *testing.T) {
	h := NewFileHeader(DefaultPageSize, DefaultDBCount, 0)
	m := NewMetaPage(h, 1)
	m.TxnID = 99
	m.CatalogRoot = 5
	m.DBRootPages[0] = 7
	b := make([]byte, DefaultPageSize)
	if err := m.MarshalTo(b, DefaultPageSize); err != nil {
		t.Fatal(err)
	}
	got, err := ParseMetaPage(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.MetaSeq != 1 || got.TxnID != 99 || got.CatalogRoot != 5 {
		t.Errorf("meta fields lost: %+v", got)
	}
	if got.DBRootPages[0] != 7 || got.DBRootPages[1] != NullPage {
		t.Errorf("db roots wrong: %v", got.DBRootPages)
	}
	if got.Header.Type != PageTypeMeta {
		t.Errorf("page type=%#x want meta", got.Header.Type)
	}
}

func TestMetaPageBadChecksum(t *testing.T) {
	h := NewFileHeader(DefaultPageSize, DefaultDBCount, 0)
	b := make([]byte, DefaultPageSize)
	NewMetaPage(h, 1).MarshalTo(b, DefaultPageSize)
	b[40] ^= 0xFF
	if _, err := ParseMetaPage(b); err != ErrBadChecksum {
		t.Errorf("got %v want ErrBadChecksum", err)
	}
}

func TestLiveMeta(t *testing.T) {
	a := MetaPage{MetaSeq: 5}
	b := MetaPage{MetaSeq: 6}
	if live, ok := LiveMeta(a, b, true, true); !ok || live.MetaSeq != 6 {
		t.Errorf("higher seq should win: %+v ok=%v", live, ok)
	}
	if live, ok := LiveMeta(a, b, true, false); !ok || live.MetaSeq != 5 {
		t.Errorf("only-a-valid should pick a: %+v ok=%v", live, ok)
	}
	if _, ok := LiveMeta(a, b, false, false); ok {
		t.Error("neither valid should be ok=false")
	}
}
