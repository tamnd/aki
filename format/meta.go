package format

import "github.com/tamnd/aki/encoding"

// MetaPage is one of the two double-buffered meta pages on pages 1 and 2 (doc
// 02 §9). A commit writes the updated snapshot to the non-live page and fsyncs;
// the page with the higher valid MetaSeq is the live snapshot, giving atomic
// visibility of the root-pointer set without a separate journal.
type MetaPage struct {
	Header PageHeader // page_type = PageTypeMeta

	MetaSeq       uint64    // offset 16
	TxnID         uint64    // offset 24
	ChangeCounter uint64    // offset 32
	PageCount     uint32    // offset 40
	FreelistHead  uint32    // offset 44
	FreelistCount uint32    // offset 48
	CatalogRoot   uint32    // offset 52
	WALCommitLSN  uint64    // offset 56
	DBCount       uint32    // offset 64
	SchemaVersion uint32    // offset 68
	DBRootPages   [8]uint32 // offset 72 (32 bytes)
	// MetaChecksum at offset 120 is CRC-32C of bytes 0..119; computed by
	// MarshalTo and verified by ParseMetaPage.
	MetaChecksum uint32
}

// MarshalTo writes the meta page into b, which must be at least one page. It
// fills the common header, the meta fields, and the trailing CRC-32C over bytes
// 0..119, then zero-pads the rest of the page.
func (m MetaPage) MarshalTo(b []byte, pageSize uint32) error {
	if uint32(len(b)) < pageSize {
		return ErrShortBuffer
	}
	for i := range pageSize {
		b[i] = 0
	}
	h := m.Header
	h.Type = PageTypeMeta
	h.FreeStart = MetaHeaderSize
	h.FreeEnd = uint16(pageSize)
	if err := h.MarshalTo(b); err != nil {
		return err
	}
	encoding.PutU64(b[16:], m.MetaSeq)
	encoding.PutU64(b[24:], m.TxnID)
	encoding.PutU64(b[32:], m.ChangeCounter)
	encoding.PutU32(b[40:], m.PageCount)
	encoding.PutU32(b[44:], m.FreelistHead)
	encoding.PutU32(b[48:], m.FreelistCount)
	encoding.PutU32(b[52:], m.CatalogRoot)
	encoding.PutU64(b[56:], m.WALCommitLSN)
	encoding.PutU32(b[64:], m.DBCount)
	encoding.PutU32(b[68:], m.SchemaVersion)
	for i := range 8 {
		encoding.PutU32(b[72+i*4:], m.DBRootPages[i])
	}
	// bytes 104..119 reserved, already zeroed.
	sum := crc32c(b[0:120])
	encoding.PutU32(b[120:], sum)
	return nil
}

// ParseMetaPage reads and validates a meta page from b. A CRC-32C mismatch
// returns ErrBadChecksum; callers treat a bad meta page as not-live and fall
// back to the other one.
func ParseMetaPage(b []byte) (MetaPage, error) {
	if len(b) < MetaHeaderSize {
		return MetaPage{}, ErrShortBuffer
	}
	stored := encoding.U32(b[120:])
	if crc32c(b[0:120]) != stored {
		return MetaPage{}, ErrBadChecksum
	}
	h, err := ParsePageHeader(b)
	if err != nil {
		return MetaPage{}, err
	}
	m := MetaPage{Header: h}
	m.MetaSeq = encoding.U64(b[16:])
	m.TxnID = encoding.U64(b[24:])
	m.ChangeCounter = encoding.U64(b[32:])
	m.PageCount = encoding.U32(b[40:])
	m.FreelistHead = encoding.U32(b[44:])
	m.FreelistCount = encoding.U32(b[48:])
	m.CatalogRoot = encoding.U32(b[52:])
	m.WALCommitLSN = encoding.U64(b[56:])
	m.DBCount = encoding.U32(b[64:])
	m.SchemaVersion = encoding.U32(b[68:])
	for i := range 8 {
		m.DBRootPages[i] = encoding.U32(b[72+i*4:])
	}
	m.MetaChecksum = stored
	return m, nil
}

// NewMetaPage returns the initial meta page for a freshly created file, derived
// from its header. Both meta pages start identical except MetaSeq: A gets seq 1
// and B gets seq 0 so A is live.
func NewMetaPage(h FileHeader, metaSeq uint64) MetaPage {
	var roots [8]uint32
	for i := range roots {
		roots[i] = NullPage
	}
	return MetaPage{
		MetaSeq:       metaSeq,
		TxnID:         0,
		ChangeCounter: h.ChangeCounter,
		PageCount:     h.PageCount,
		FreelistHead:  h.FreelistHead,
		FreelistCount: h.FreelistCount,
		CatalogRoot:   h.CatalogRoot,
		WALCommitLSN:  0,
		DBCount:       h.DBCount,
		SchemaVersion: h.SchemaVersion,
		DBRootPages:   roots,
	}
}

// LiveMeta returns whichever of a or b is the live snapshot: the valid page
// with the higher MetaSeq. The bools report whether each parsed cleanly. If
// neither is valid, ok is false.
func LiveMeta(a, b MetaPage, aok, bok bool) (live MetaPage, ok bool) {
	switch {
	case aok && bok:
		if a.MetaSeq >= b.MetaSeq {
			return a, true
		}
		return b, true
	case aok:
		return a, true
	case bok:
		return b, true
	default:
		return MetaPage{}, false
	}
}
