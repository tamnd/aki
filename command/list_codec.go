package command

import (
	"errors"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// errCorruptList is returned when a stored list body cannot be decoded, which
// means the value record is damaged.
var errCorruptList = errors.New("corrupt list value")

// List OBJECT ENCODING thresholds live in encLimits (enc_limits.go), read from
// list-max-listpack-size. aki stores its own physical list form (a
// length-prefixed element sequence), so they only decide which Redis encoding
// name the key reports, matching the t_list.c rule.

// listDecode unpacks a stored list body into its elements. The body is a
// uvarint element count followed by each element as a uvarint length and bytes.
func listDecode(body []byte) ([][]byte, error) {
	if len(body) == 0 {
		return nil, nil
	}
	n, off, err := encoding.Uvarint(body)
	if err != nil {
		return nil, err
	}
	var elems [][]byte
	for range n {
		l, m, err := encoding.Uvarint(body[off:])
		if err != nil {
			return nil, err
		}
		off += m
		if off+int(l) > len(body) {
			return nil, errCorruptList
		}
		elem := make([]byte, l)
		copy(elem, body[off:off+int(l)])
		elems = append(elems, elem)
		off += int(l)
	}
	return elems, nil
}

// listBlobRangeReply writes the inclusive [start, stop] window of the blob-form
// list stored in body straight to enc, walking the encoded body in place. It
// never materializes the whole [][]byte the way listDecode does, and that
// materialization (one allocation and copy per element) is the bulk of an LRANGE's
// cost on a hot list. The body is uvarint(count) then each element as
// uvarint(len)+bytes, the shape listDecode reads. start and stop carry the LRANGE
// index rules, resolved by listRangeBounds.
//
// It walks the body twice. The first pass validates every length and bound up to
// the end of the window; the second writes the window. Validating first means a
// corrupt body returns false with nothing written, so the caller falls back to the
// cold path exactly as a decode error would there, instead of emitting a
// half-formed array. Two index-only walks of a small body cost far less than the
// per-element allocation listDecode pays, and the element bytes are written as
// sub-slices of body with no copy of their own.
func listBlobRangeReply(enc *resp.Encoder, body []byte, start, stop int64) bool {
	if len(body) == 0 {
		enc.WriteArrayLen(0)
		return true
	}
	n, off0, err := encoding.Uvarint(body)
	if err != nil {
		return false
	}
	count := int64(n)
	lo, hi := listRangeBounds(start, stop, count)
	if lo > hi || count == 0 {
		enc.WriteArrayLen(0)
		return true
	}
	// Pass 1: validate lengths and bounds through element hi, writing nothing.
	off := off0
	for i := int64(0); i <= hi; i++ {
		l, m, err := encoding.Uvarint(body[off:])
		if err != nil {
			return false
		}
		off += m + int(l)
		if off > len(body) {
			return false
		}
	}
	// Pass 2: the window is known good, so write [lo, hi] straight off the body.
	enc.WriteArrayLen(int(hi - lo + 1))
	off = off0
	for i := int64(0); i <= hi; i++ {
		l, m, _ := encoding.Uvarint(body[off:])
		off += m
		if i >= lo {
			enc.WriteBulkString(body[off : off+int(l)])
		}
		off += int(l)
	}
	return true
}

// listEncode packs elements back into the stored body form.
func listEncode(elems [][]byte) []byte {
	body := encoding.AppendUvarint(nil, uint64(len(elems)))
	for _, e := range elems {
		body = encoding.AppendUvarint(body, uint64(len(e)))
		body = append(body, e...)
	}
	return body
}

// listBlobPush splices vals onto the head or tail of a stored list body without
// decoding the existing elements. The body is uvarint(count) followed by each
// element as uvarint(len)+bytes, so the existing element bytes can be copied as
// one block: a head push writes the pushed run (arguments reversed, the order
// LPUSH leaves) in front of that block, a tail push appends it after. Only the
// count prefix and the pushed elements are encoded, so a push allocates once
// instead of decoding and re-allocating every element. A nil or empty body is a
// fresh list. It returns the new body and element count.
func listBlobPush(body []byte, vals [][]byte, head bool) ([]byte, int, error) {
	var oldCount uint64
	elems := body
	if len(body) > 0 {
		n, off, err := encoding.Uvarint(body)
		if err != nil {
			return nil, 0, err
		}
		oldCount = n
		elems = body[off:]
	}
	newCount := oldCount + uint64(len(vals))

	pushedLen := 0
	for _, v := range vals {
		pushedLen += encoding.UvarintLen(uint64(len(v))) + len(v)
	}
	out := make([]byte, 0, encoding.UvarintLen(newCount)+pushedLen+len(elems))
	out = encoding.AppendUvarint(out, newCount)
	if head {
		// LPUSH k a b c leaves [c, b, a, ...]: the pushed run is reversed so the
		// last argument ends up nearest the head.
		for i := len(vals) - 1; i >= 0; i-- {
			out = encoding.AppendUvarint(out, uint64(len(vals[i])))
			out = append(out, vals[i]...)
		}
		out = append(out, elems...)
	} else {
		out = append(out, elems...)
		for _, v := range vals {
			out = encoding.AppendUvarint(out, uint64(len(v)))
			out = append(out, v...)
		}
	}
	return out, int(newCount), nil
}

// listBlobReportedEnc returns the OBJECT ENCODING a spliced list body should
// report, applying the Redis rule (quicklistNodeExceedsLimit) to the new body
// without allocating a decoded element slice. The previous encoding pins the
// floor (a quicklist never demotes). listBodyMetrics walks the body once for the
// element count and the exact listpack byte size, which the entry cap (positive
// fill only) and the byte cap are then checked against.
func listBlobReportedEnc(lim encLimits, prevEnc uint8, body []byte) (uint8, error) {
	if prevEnc == keyspace.EncQuicklist {
		return keyspace.EncQuicklist, nil
	}
	maxEntries, maxBytes := lim.listLimits()
	count, lpBytes, err := listBodyMetrics(body)
	if err != nil {
		return 0, err
	}
	if maxEntries > 0 && count > maxEntries {
		return keyspace.EncQuicklist, nil
	}
	if lpBytes > maxBytes {
		return keyspace.EncQuicklist, nil
	}
	return keyspace.EncListpack, nil
}

// listCollReportedEnc returns the OBJECT ENCODING a coll-form list should report,
// computed from its maintained metadata so a push never walks the rows. It mirrors
// listEncoding: prevEnc pins the floor (a quicklist never demotes), the count caps
// the entry limit only when the fill is a positive entry count, and lpByteSum is
// the running sum of each element's lpEntrySize, so adding the fixed listpack
// header gives the same total listpackBytes would compute from the elements.
func listCollReportedEnc(lim encLimits, prevEnc uint8, count int, lpByteSum uint64) uint8 {
	if prevEnc == keyspace.EncQuicklist {
		return keyspace.EncQuicklist
	}
	maxEntries, maxBytes := lim.listLimits()
	if maxEntries > 0 && count > maxEntries {
		return keyspace.EncQuicklist
	}
	if int64(lpHeaderBytes)+int64(lpByteSum) > int64(maxBytes) {
		return keyspace.EncQuicklist
	}
	return keyspace.EncListpack
}

// listEncoding picks the reported encoding for a list. Once a key is a
// quicklist it never goes back to listpack, so prev pins the floor. The entry
// cap (positive fill only) and the listpack byte cap come from
// list-max-listpack-size via lim, matching quicklistNodeExceedsLimit.
func listEncoding(lim encLimits, elems [][]byte, prev uint8) uint8 {
	if prev == keyspace.EncQuicklist {
		return keyspace.EncQuicklist
	}
	maxEntries, maxBytes := lim.listLimits()
	if maxEntries > 0 && len(elems) > maxEntries {
		return keyspace.EncQuicklist
	}
	if listpackBytes(elems) > maxBytes {
		return keyspace.EncQuicklist
	}
	return keyspace.EncListpack
}

// getList reads the list at key and returns its elements head to tail. The header
// carries the type and encoding so callers can check for WRONGTYPE and keep the
// encoding floor. A missing key returns found false with no error. A large list
// lives in the btree-backed sub-tree form (list_tree.go); getList materializes it
// in list order so every read caller and the bulk rewrite commands (LINSERT,
// LREM, LTRIM, LPOS, LMOVE, LMPOP, the blocking variants, DUMP/RDB) work on either
// form unchanged. The push, pop, length, range, LINDEX and LSET commands branch on
// hdr.IsColl() before getList so they never rewrite a whole blob for a btree-backed
// list.
func getList(db *keyspace.DB, key []byte) ([][]byte, keyspace.ValueHeader, bool, error) {
	body, hdr, found, err := db.Get(key)
	if err != nil || !found {
		return nil, hdr, found, err
	}
	if hdr.Type != keyspace.TypeList {
		return nil, hdr, true, nil
	}
	if hdr.IsColl() {
		elems, e := collectListElems(db, key)
		return elems, hdr, true, e
	}
	elems, err := listDecode(body)
	if err != nil {
		return nil, hdr, true, err
	}
	return elems, hdr, true, nil
}

// hotGetListBody returns the raw stored body of a blob-form list at key from the
// lock-free hot cache, for callers that walk the body in place rather than
// decoding it (LRANGE streams the requested window straight off it). It returns a
// miss for a coll-form list, whose elements live in a sub-tree rather than the
// value body, so the caller takes the cold path that reads the window from the
// tree. The returned body aliases the cache entry and is only valid for the
// duration of the call, which is all the streaming reply needs since the encoder
// copies each element into the reply buffer as it goes.
func hotGetListBody(ctx *Ctx, key []byte) ([]byte, bool) {
	e := ctx.d.engine
	if e == nil {
		return nil, false
	}
	body, hdr, ok := e.viewHotGet(ctx.Conn.DB(), key)
	if !ok || hdr.Type != keyspace.TypeList || hdr.IsColl() {
		return nil, false
	}
	return body, true
}

// hotGetList tries to decode the list at key from the lock-free hot cache.
// Returns (elems, true) on a hit and (nil, false) on a miss.
func hotGetList(ctx *Ctx, key []byte) ([][]byte, bool) {
	e := ctx.d.engine
	if e == nil {
		return nil, false
	}
	body, hdr, ok := e.viewHotGet(ctx.Conn.DB(), key)
	if !ok {
		return nil, false
	}
	if hdr.Type != keyspace.TypeList {
		return nil, false
	}
	elems, err := listDecode(body)
	if err != nil {
		return nil, false
	}
	return elems, true
}
