package command

import (
	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// A large list is stored element-per-row in the key's btree-backed collection
// sub-tree (keyspace.CollUpdate / CollRead). Unlike hash/set/zset, a list is
// ordered by position rather than by element bytes, so the row key is the
// element's absolute position and the row value is the element:
//
//	pos -> element
//
// The collMeta head/tail window tracks the live positions: head is the lowest
// live position and tail is one past the highest, so the element count is
// tail-head, a left push decrements head, and a right push increments tail. The
// position is a signed int64 encoded big-endian with the sign bit flipped
// (listPosRow) so the btree's bytewise row order matches list order even after
// head runs negative from repeated left pushes. A straight cursor walk of the rows
// reproduces the list head to tail.
//
// A small list keeps the single-blob form in list_codec.go. It promotes to the
// sub-tree exactly when its reported encoding becomes quicklist, so OBJECT
// ENCODING flips at the same threshold as Redis and never demotes on its own.
//
// getList (list_codec.go) is coll-aware, so every read caller and the bulk
// rewrite commands (LINSERT, LREM, LTRIM, LPOS, LMOVE, LMPOP, the blocking
// variants, DUMP/RDB) work on either form. The push, pop, length, range, LINDEX
// and LSET commands branch on hdr.IsColl() so they do point or window sub-tree
// ops and never rewrite a whole blob. A bulk command that writes a blob over a
// promoted list demotes it, and the next push past the threshold re-promotes it.

// listPosRow encodes a signed list position as an order-preserving row key.
// Flipping the sign bit maps int64 order onto uint64 order, and big-endian bytes
// then sort in numeric position order, so a negative head still orders below a
// positive tail.
func listPosRow(pos int64) []byte {
	return encoding.AppendU64BE(make([]byte, 0, 8), uint64(pos)^(1<<63))
}

// listWantsTree reports whether a list with these elements should live in the
// btree-backed form. The rule is the encoding rule: a list is tree-backed exactly
// when it reports quicklist, so promotion happens at the listpack threshold and
// the encoding name stays correct for free.
func listWantsTree(lim encLimits, elems [][]byte, prevEnc uint8) bool {
	return listEncoding(lim, elems, prevEnc) == keyspace.EncQuicklist
}

// listPromote moves a list from the blob form to the btree-backed form. It writes
// one row per element at positions 0..n-1, records the byte total, and sets the
// head/tail window through CollUpdate, which creates the fresh sub-tree, frees the
// old blob, and carries over the key's TTL. enc is the OBJECT ENCODING the moved
// list reports: with early-coll a list moves to the sub-tree once its blob would
// spill to overflow, well before the 128-entry threshold, so it can still report
// listpack while stored in coll form. Callers compute enc from the post-push state.
func listPromote(db *keyspace.DB, key []byte, elems [][]byte, enc uint8) error {
	return db.CollUpdate(key, keyspace.TypeList, enc, func(w *keyspace.CollWriter) error {
		var byteSum uint64
		for i, e := range elems {
			if _, err := w.Put(listPosRow(int64(i)), e); err != nil {
				return err
			}
			byteSum += uint64(len(e))
		}
		w.SetHead(0)
		w.SetTail(int64(len(elems)))
		w.SetCount(uint64(len(elems)))
		w.SetBytes(byteSum)
		return nil
	})
}

// listHeader probes the value header at key without decoding the body, so a write
// command can route to the blob path or the sub-tree path. found is false for a
// missing key.
func listHeader(db *keyspace.DB, key []byte) (keyspace.ValueHeader, bool, error) {
	return db.CollMetaHeader(key)
}

// collectListElems walks a btree-backed list's rows in position order and returns
// every element. The row order is the position order, which is the list order, so
// a straight cursor walk reproduces the list head to tail. The caller has already
// confirmed the key is a coll list.
func collectListElems(db *keyspace.DB, key []byte) ([][]byte, error) {
	var out [][]byte
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		out = make([][]byte, 0, r.Count())
		c := r.Cursor()
		if e := c.First(); e != nil {
			return e
		}
		for c.Valid() {
			out = append(out, append([]byte(nil), c.Value()...))
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return out, err
}

// listLen returns the element count for key in whichever form the list is stored.
// For a btree-backed list it reads the metadata count in O(1) rather than walking.
func listLen(db *keyspace.DB, key []byte) (n int64, hdr keyspace.ValueHeader, keyFound bool, err error) {
	hdr, keyFound, err = listHeader(db, key)
	if err != nil || !keyFound {
		return 0, hdr, keyFound, err
	}
	if hdr.Type != keyspace.TypeList {
		return 0, hdr, true, nil
	}
	if hdr.IsColl() {
		_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
			n = int64(r.Count())
			return nil
		})
		return n, hdr, true, err
	}
	elems, _, _, e := getList(db, key)
	if e != nil {
		return 0, hdr, true, e
	}
	return int64(len(elems)), hdr, true, nil
}

// listTreePush appends vals onto the head or tail end of a btree-backed list
// inside a write callback, moving the position window and keeping the count in
// step. For a left push each value lands at a new lowest position, so the pushed
// run ends up reversed, exactly as LPUSH leaves it. It returns the new length.
func listTreePush(w *keyspace.CollWriter, vals [][]byte, head bool) (int64, error) {
	var added uint64
	if head {
		h := w.Head()
		for _, v := range vals {
			h--
			if _, e := w.Put(listPosRow(h), v); e != nil {
				return 0, e
			}
			added += uint64(len(v))
		}
		w.SetHead(h)
	} else {
		t := w.Tail()
		for _, v := range vals {
			if _, e := w.Put(listPosRow(t), v); e != nil {
				return 0, e
			}
			added += uint64(len(v))
			t++
		}
		w.SetTail(t)
	}
	w.SetBytes(w.Bytes() + added)
	n := w.Tail() - w.Head()
	w.SetCount(uint64(n))
	return n, nil
}

// listTreePop removes up to n elements from the head or tail end of a
// btree-backed list inside a write callback and returns them in reply order: a
// head pop returns elements head first, a tail pop returns them tail first. It
// moves the window and keeps the count in step. When the count reaches zero,
// CollUpdate tears the key and its sub-tree down.
func listTreePop(w *keyspace.CollWriter, n int, head bool) ([][]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	popped := make([][]byte, 0, n)
	var removed uint64
	if head {
		h := w.Head()
		for i := 0; i < n; i++ {
			row := listPosRow(h)
			v, ok, e := w.Get(row)
			if e != nil {
				return nil, e
			}
			if !ok {
				break
			}
			popped = append(popped, append([]byte(nil), v...))
			removed += uint64(len(v))
			if _, e := w.Delete(row); e != nil {
				return nil, e
			}
			h++
		}
		w.SetHead(h)
	} else {
		t := w.Tail()
		for i := 0; i < n; i++ {
			t--
			row := listPosRow(t)
			v, ok, e := w.Get(row)
			if e != nil {
				return nil, e
			}
			if !ok {
				t++
				break
			}
			popped = append(popped, append([]byte(nil), v...))
			removed += uint64(len(v))
			if _, e := w.Delete(row); e != nil {
				return nil, e
			}
		}
		w.SetTail(t)
	}
	if removed > w.Bytes() {
		w.SetBytes(0)
	} else {
		w.SetBytes(w.Bytes() - removed)
	}
	w.SetCount(uint64(w.Tail() - w.Head()))
	return popped, nil
}

// listTreeIndex returns the element at logical index i in a btree-backed list, a
// point sub-tree lookup. A negative index counts from the tail. found is false
// when the index is out of range. The caller has confirmed the key is a coll list.
func listTreeIndex(db *keyspace.DB, key []byte, i int64) (elem []byte, found bool, err error) {
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		n := r.Tail() - r.Head()
		pos := i
		if pos < 0 {
			pos += n
		}
		if pos < 0 || pos >= n {
			return nil
		}
		v, ok, e := r.Get(listPosRow(r.Head() + pos))
		if e != nil {
			return e
		}
		if ok {
			elem = append([]byte(nil), v...)
			found = true
		}
		return nil
	})
	return elem, found, err
}

// listTreeSet replaces the element at logical index i in a btree-backed list, a
// point sub-tree write. A negative index counts from the tail. oob is true when
// the index is out of range, in which case nothing is written. It reads the old
// element to keep the byte total accurate and re-derives the reported encoding,
// since replacing a small element with one past the per-element or byte cap flips
// listpack to quicklist exactly as Redis does. prevEnc pins the encoding floor.
func listTreeSet(db *keyspace.DB, lim encLimits, key, val []byte, i int64, prevEnc uint8) (oob bool, err error) {
	err = db.CollUpdate(key, keyspace.TypeList, prevEnc, func(w *keyspace.CollWriter) error {
		n := w.Tail() - w.Head()
		pos := i
		if pos < 0 {
			pos += n
		}
		if pos < 0 || pos >= n {
			oob = true
			return nil
		}
		row := listPosRow(w.Head() + pos)
		old, _, e := w.Get(row)
		if e != nil {
			return e
		}
		oldLen := uint64(len(old))
		if _, e := w.Put(row, val); e != nil {
			return e
		}
		nb := w.Bytes()
		if oldLen > nb {
			nb = 0
		} else {
			nb -= oldLen
		}
		nb += uint64(len(val))
		w.SetBytes(nb)
		w.SetEnc(listCollReportedEnc(lim, prevEnc, int(w.Count()), nb, [][]byte{val}))
		return nil
	})
	return oob, err
}

// listTreeRange returns the elements in the inclusive logical range [start, stop]
// of a btree-backed list, applying the Redis negative-index and clamp rules, by
// seeking to the first position and walking forward. It reads only the requested
// window rather than materializing the whole list.
func listTreeRange(db *keyspace.DB, key []byte, start, stop int64) ([][]byte, error) {
	var out [][]byte
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		n := r.Tail() - r.Head()
		lo, hi := listRangeBounds(start, stop, n)
		if lo > hi || n == 0 {
			return nil
		}
		out = make([][]byte, 0, hi-lo+1)
		c := r.Cursor()
		if e := c.Seek(listPosRow(r.Head() + lo)); e != nil {
			return e
		}
		for i := lo; i <= hi && c.Valid(); i++ {
			out = append(out, append([]byte(nil), c.Value()...))
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return out, err
}
