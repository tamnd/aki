package command

import (
	"errors"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
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

// listEncode packs elements back into the stored body form.
func listEncode(elems [][]byte) []byte {
	body := encoding.AppendUvarint(nil, uint64(len(elems)))
	for _, e := range elems {
		body = encoding.AppendUvarint(body, uint64(len(e)))
		body = append(body, e...)
	}
	return body
}

// listEncoding picks the reported encoding for a list. Once a key is a
// quicklist it never goes back to listpack, so prev pins the floor. The entry
// and byte caps come from list-max-listpack-size via lim.
func listEncoding(lim encLimits, elems [][]byte, prev uint8) uint8 {
	if prev == keyspace.EncQuicklist {
		return keyspace.EncQuicklist
	}
	maxEntries, maxBytes := lim.listLimits()
	if len(elems) > maxEntries {
		return keyspace.EncQuicklist
	}
	total := listEntryOverhead
	for _, e := range elems {
		if len(e) > listMaxListpackElemBytes {
			return keyspace.EncQuicklist
		}
		total += len(e) + listEntryOverhead
		if total > maxBytes {
			return keyspace.EncQuicklist
		}
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
