package sqlo1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// The doc 07 list model, slice 1: the inline tier. A short list lives
// whole in its root value, elements in list order, and answers
// OBJECT ENCODING listpack the way Redis does at the same thresholds.
// The noded ladder rung (positional fence, node segments, sub 3) lands
// with the next slice; every path that would need it returns
// errListNoded, so each seam the noded slice replaces is explicit.
const (
	// listSubNoded is the doc 07 noded list root layout, the next
	// doc-assigned sub after the segmented hash. Defined here for the
	// sniffer; the layout itself lands with the noded slice, amended
	// to carry the shared planed prefix (rootgen at 4, rooth at 8)
	// that planedRootInfo reads.
	listSubNoded = 3

	// listSubInline is the inline list root, planeless like every
	// 0x10-block sub.
	listSubInline = inlineSubBase | TagList

	// The inline rung's thresholds, doc 07 section 1: a list stays
	// inline while the whole encoded root payload fits listInlineMax
	// and the element count fits listInlineMaxCount, the same figures
	// Redis uses for its listpack-to-quicklist conversion.
	listInlineMax      = 2048
	listInlineMaxCount = 128
)

// Inline root payload layout:
//
//	u8   sub     // listSubInline
//	u8   lflags  // reserved, zero inline (bit0 fence-paged is noded-only)
//	u16  count
//	elements, list order
//
// Each element is a u32 length then the bytes, byte-identical to the
// doc 07 section 2 node entry, so the noded upgrade copies the element
// region wholesale instead of re-encoding.
const (
	listInlineHdrLen = 4
	listElemHdrLen   = 4
)

// errListNoded marks a path the noded slice owns: a write past the
// inline thresholds, or any op on a noded root. Nothing creates a
// noded root yet, so outside hand-built tests only the threshold
// crossings can reach it.
var errListNoded = errors.New("sqlo1: list is past the inline tier; the noded slice owns this path")

// putListInlineHdr fills the header slot; the buffer already holds
// listInlineHdrLen bytes with contents unspecified (grow's contract),
// so every header byte is written.
func putListInlineHdr(b []byte, count int) {
	b[0] = listSubInline
	b[1] = 0
	binary.LittleEndian.PutUint16(b[2:], uint16(count))
}

// appendListElem encodes one element onto dst. The caller bounds the
// element structurally: an oversized element cannot fit the inline
// payload, and the noded slice owns its own guard.
func appendListElem(dst, e []byte) []byte {
	var h [listElemHdrLen]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(e)))
	return append(append(dst, h[:]...), e...)
}

// listInline is the decoded inline root: the count and the raw element
// region. The region aliases the decoded payload and dies with it.
type listInline struct {
	count int
	elems []byte
}

// decodeListInline validates everything: a corrupt inline root fails
// at the first read that meets it, never at a later write.
func decodeListInline(v []byte) (listInline, error) {
	if len(v) < listInlineHdrLen {
		return listInline{}, fmt.Errorf("sqlo1: inline list root of %d bytes has no header", len(v))
	}
	if v[0] != listSubInline {
		return listInline{}, fmt.Errorf("sqlo1: inline list root has sub %d", v[0])
	}
	if v[1] != 0 {
		return listInline{}, fmt.Errorf("sqlo1: inline list root has reserved lflags %#x", v[1])
	}
	if len(v) > listInlineMax {
		return listInline{}, fmt.Errorf("sqlo1: inline list root of %d bytes is over the cap", len(v))
	}
	count := int(binary.LittleEndian.Uint16(v[2:]))
	if count == 0 || count > listInlineMaxCount {
		return listInline{}, fmt.Errorf("sqlo1: inline list root count %d out of range", count)
	}
	n := 0
	for p := v[listInlineHdrLen:]; len(p) > 0; n++ {
		if len(p) < listElemHdrLen {
			return listInline{}, fmt.Errorf("sqlo1: inline list element %d torn at the header", n)
		}
		el := int(binary.LittleEndian.Uint32(p))
		if len(p) < listElemHdrLen+el {
			return listInline{}, fmt.Errorf("sqlo1: inline list element %d torn at %d of %d bytes", n, len(p)-listElemHdrLen, el)
		}
		p = p[listElemHdrLen+el:]
	}
	if n != count {
		return listInline{}, fmt.Errorf("sqlo1: inline list walks %d elements, header says %d", n, count)
	}
	return listInline{count: count, elems: v[listInlineHdrLen:]}, nil
}

// listElemIter walks a decoded element region. The decode already
// validated the walk, so next never fails.
type listElemIter struct{ p []byte }

func (it *listElemIter) next() ([]byte, bool) {
	if len(it.p) == 0 {
		return nil, false
	}
	el := int(binary.LittleEndian.Uint32(it.p))
	e := it.p[listElemHdrLen : listElemHdrLen+el]
	it.p = it.p[listElemHdrLen+el:]
	return e, true
}

// List is the list type layer over the shard runtime. Not safe for
// concurrent use, like the other type layers: the caller serializes
// (R1).
type List struct {
	t *Tiered

	// rootBuf stages a rebuilt root payload; safe to fill from spans
	// aliasing a read because Tiered copies on Set and nothing else
	// touches it between.
	rootBuf []byte

	// spans holds one op's element spans into the read payload; valBuf
	// and vals carry a pop's reply, copied out before the write that
	// would recycle the read's arena bytes. Reply values stay valid
	// until the next call on this List.
	spans  [][]byte
	valBuf []byte
	vals   [][]byte
}

// NewList builds the list layer over t. No minter yet: the inline
// tier is planeless, and the noded slice brings the lease plumbing
// with the layout that needs it.
func NewList(t *Tiered) *List {
	return &List{t: t}
}

// restamp mirrors Str.restamp: puts a key's expiry back after a write
// that may have gone through a fresh hot header.
func (l *List) restamp(ctx context.Context, key []byte, expMs int64) error {
	if expMs == 0 {
		return nil
	}
	_, err := l.t.ExpireAt(ctx, key, expMs)
	return err
}

// listState classifies a key for the list ops.
type listState int

const (
	listAbsent listState = iota
	listInlineState
	listNodedState
)

// stateOf reads key and classifies it. The decoded inline view aliases
// the read; it dies on the next Tiered call.
func (l *List) stateOf(ctx context.Context, key []byte) (listState, listInline, int64, error) {
	v, root, expMs, ok, err := l.t.LookupEntry(ctx, key)
	if err != nil || !ok {
		return listAbsent, listInline{}, 0, err
	}
	if !root {
		return listAbsent, listInline{}, 0, ErrWrongType
	}
	tag, _, err := sniffRoot(v)
	if err != nil {
		return listAbsent, listInline{}, 0, err
	}
	if tag != TagList {
		return listAbsent, listInline{}, 0, ErrWrongType
	}
	if v[0] == listSubNoded {
		return listNodedState, listInline{}, expMs, nil
	}
	li, err := decodeListInline(v)
	if err != nil {
		return listAbsent, listInline{}, 0, err
	}
	return listInlineState, li, expMs, nil
}

// Push appends elems at the left or right end and reports the new
// length. Elements land one at a time in argument order, Redis's rule,
// so a multi-element left push reads back reversed. xOnly is the
// LPUSHX/RPUSHX gate: a missing key stays missing and answers 0. A
// push past the inline thresholds returns errListNoded until the
// noded slice replaces that seam with the upgrade.
func (l *List) Push(ctx context.Context, key []byte, left, xOnly bool, elems ...[]byte) (int64, error) {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	switch st {
	case listNodedState:
		return 0, errListNoded
	case listAbsent:
		if xOnly {
			return 0, nil
		}
	}
	count := li.count + len(elems)
	l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
	if left {
		for i := len(elems) - 1; i >= 0; i-- {
			l.rootBuf = appendListElem(l.rootBuf, elems[i])
		}
		l.rootBuf = append(l.rootBuf, li.elems...)
	} else {
		l.rootBuf = append(l.rootBuf, li.elems...)
		for _, e := range elems {
			l.rootBuf = appendListElem(l.rootBuf, e)
		}
	}
	if count > listInlineMaxCount || len(l.rootBuf) > listInlineMax {
		return 0, errListNoded
	}
	putListInlineHdr(l.rootBuf, count)
	if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
		return 0, err
	}
	return int64(count), l.restamp(ctx, key, expMs)
}

// Pop removes up to count elements from the left or right end, in pop
// order (a right pop reads back tail first, Redis's rule). ok reports
// whether the key existed; a missing key answers nil, false. Popping
// the last element deletes the key. The returned values stay valid
// until the next call on this List.
func (l *List) Pop(ctx context.Context, key []byte, left bool, count int) ([][]byte, bool, error) {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil {
		return nil, false, err
	}
	switch st {
	case listNodedState:
		return nil, false, errListNoded
	case listAbsent:
		return nil, false, nil
	}
	l.vals = l.vals[:0]
	if count <= 0 {
		return l.vals, true, nil
	}
	k := min(count, li.count)

	// The spans alias the read and stay valid until the write below,
	// the first Tiered call after it. The reply copies out first.
	l.spans = l.spans[:0]
	it := listElemIter{p: li.elems}
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		l.spans = append(l.spans, e)
	}
	total := 0
	for i := range k {
		if left {
			total += len(l.spans[i])
		} else {
			total += len(l.spans[len(l.spans)-1-i])
		}
	}
	l.valBuf = grow(l.valBuf, total)
	off := 0
	for i := range k {
		e := l.spans[i]
		if !left {
			e = l.spans[len(l.spans)-1-i]
		}
		copy(l.valBuf[off:], e)
		l.vals = append(l.vals, l.valBuf[off:off+len(e)])
		off += len(e)
	}

	if k == li.count {
		if _, err := l.t.Del(ctx, key); err != nil {
			return nil, false, err
		}
		return l.vals, true, nil
	}
	l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
	rest := l.spans[k:]
	if !left {
		rest = l.spans[:li.count-k]
	}
	for _, e := range rest {
		l.rootBuf = appendListElem(l.rootBuf, e)
	}
	putListInlineHdr(l.rootBuf, li.count-k)
	if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
		return nil, false, err
	}
	return l.vals, true, l.restamp(ctx, key, expMs)
}

// Len reports the list length; a missing key answers 0, Redis's LLEN.
// Inline it is the root header count, the doc 07 L-I2 exactness bet in
// miniature; the noded root's count field lands with the noded slice.
func (l *List) Len(ctx context.Context, key []byte) (int64, error) {
	st, li, _, err := l.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	switch st {
	case listAbsent:
		return 0, nil
	case listNodedState:
		return 0, errListNoded
	}
	return int64(li.count), nil
}

// Encoding answers OBJECT ENCODING for a list key: listpack inline,
// quicklist noded, matching Redis's names at the same thresholds.
func (l *List) Encoding(ctx context.Context, key []byte) (string, bool, error) {
	st, _, _, err := l.stateOf(ctx, key)
	if err != nil {
		return "", false, err
	}
	switch st {
	case listAbsent:
		return "", false, nil
	case listNodedState:
		return "quicklist", true, nil
	}
	return "listpack", true, nil
}
