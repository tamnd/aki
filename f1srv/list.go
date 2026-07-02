package f1srv

import (
	"bytes"
	"encoding/binary"
	"strconv"
)

// List is the fourth collection type on f1raw, and unlike the hash, set, and zset it has
// no stable per-element key: a list is a deque whose element identity is its relative
// position, not any bytes it carries (spec 2064/f1_rewrite_ltm/08 section 1). So the model
// manufactures an order-preserving position key for every element and stores each element
// element-per-row under it, exactly the element-per-row shape the other collections use, so
// a list rides the same lock-free point path with no second structure.
//
// The position is an int64 index into an ever-growing window [head, tail): RPUSH writes at
// tail and advances it, LPUSH decrements head and writes there, and the window stays
// perfectly contiguous under end-only edits (push and pop). Contiguity is what makes this
// model beat a general order-statistic index for a deque: positional access (LINDEX,
// LRANGE) is direct index arithmetic plus one point lookup, an O(1) descent-free seek, where
// a quicklist walks nodes and a plain ordered index pays an O(log n) rank descent. The
// window is stored in a per-list header row (kindListMeta under the bare key) as head, tail,
// the running listpack byte size, and a sticky "has ever been large" bit; count is tail-head,
// so LLEN is one header read with no scan.
//
// Element sub-key layout: uvarint(len(listKey)) | listKey | orderKey, where orderKey is the
// 8-byte order-preserving big-endian encoding of the int64 position. The length prefix makes
// (listKey, position) injective so two lists never share a row, and the order-preserving
// encoding means a byte comparison of two element keys equals their position order, so the
// rows of one list sort head-to-tail. A list has a single element family, so the record kind
// byte alone (kindListElem) keeps element rows disjoint from every other type's rows; no
// in-key family tag is needed the way the zset's dual member/score families require one.
//
// Write serialization: the push and pop commands take the per-key stripe lock (shared with
// the INCR family and the other collections) so a list's element rows and its header window
// stay consistent under concurrent writers. LINDEX and LRANGE take the same lock so the
// window they walk cannot shift out from under them mid-read; LLEN is a single lock-free
// header read like SCARD.
const (
	kindListElem byte = 0x05 // a single list element row, value is the element bytes verbatim
	kindListMeta byte = 0x0B // the per-list header row (coll_header): head, tail, lpBytes, everLarge
)

// listHeaderBytes is the fixed listpack overhead Redis counts in lpBytes: the 6-byte header
// (4-byte total length plus 2-byte element count) and the 1-byte 0xFF terminator. An empty
// list's running byte count starts here, and each element adds its listEntrySize on top, so
// the total mirrors the lpBytes t_list.c compares against list-max-listpack-size.
const listHeaderBytes = 7

// listListpackMaxBytes is the byte budget for the default list-max-listpack-size of -2: a
// list reports "listpack" for OBJECT ENCODING until its elements would fill more than 8192
// bytes inside a real listpack, then "quicklist" and never back (Redis never downgrades). The
// default carries no element-count cap and no per-element value cap, only this byte budget,
// which the running Redis 8.8 and Valkey 9.1 defaults both confirm (200 tiny integers and a
// 100-byte value both stay listpack). CONFIG is a no-op on f1srv, so this is the threshold
// every stock server a client compares against uses.
const listListpackMaxBytes = 8192

// listMetaSize is the fixed width of the header row: head int64 | tail int64 | lpBytes uint64,
// each 8 bytes little-endian, then a 1-byte sticky "ever large" flag.
const listMetaSize = 25

// listElemKey builds the composite element key for (lkey, pos) into the reused kbuf, so a
// list command allocates nothing for its key. The 8-byte order key is uint64(pos) with the
// sign bit flipped and stored big-endian, so negative positions (produced by LPUSH pushing
// below zero) sort before positive ones and a plain byte comparison of two element keys
// equals their position order.
func (c *connState) listElemKey(lkey []byte, pos int64) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(lkey)))
	b = append(b, tmp[:n]...)
	b = append(b, lkey...)
	var ord [8]byte
	binary.BigEndian.PutUint64(ord[:], uint64(pos)^(1<<63))
	b = append(b, ord[:]...)
	c.kbuf = b
	return b
}

// listHeader reads a list's header window: the head and tail positions, the running listpack
// byte size, and the sticky large flag. ok is false when the list has no header (empty or
// missing key), in which case head and tail are 0 (an empty window) and lpBytes is the empty
// listpack overhead, so a first push starts counting from the right base.
func (c *connState) listHeader(lkey []byte) (head, tail int64, lpBytes uint64, everLarge, ok bool) {
	var hb [listMetaSize]byte
	v, got := c.srv.store.GetKind(lkey, hb[:0], kindListMeta)
	if !got || len(v) < listMetaSize {
		return 0, 0, listHeaderBytes, false, false
	}
	head = int64(binary.LittleEndian.Uint64(v[0:8]))
	tail = int64(binary.LittleEndian.Uint64(v[8:16]))
	lpBytes = binary.LittleEndian.Uint64(v[16:24])
	everLarge = v[24] != 0
	return head, tail, lpBytes, everLarge, true
}

// listHeaderAt is listHeader that also returns the header row's arena offset, so a push or
// pop that will write the window straight back can rewrite it in place with listPutHeaderAt
// and skip the second index probe a plain PutHeader would repeat under the stripe lock. off
// is meaningful only when ok is true; it stays valid across the caller's element edits
// because a fixed-width header row is never outgrown-republished and element writes append
// under different keys, so the header record does not move.
func (c *connState) listHeaderAt(lkey []byte) (head, tail int64, lpBytes uint64, everLarge bool, off uint64, ok bool) {
	var hb [listMetaSize]byte
	v, o, got := c.srv.store.GetKindAt(lkey, hb[:0], kindListMeta)
	if !got || len(v) < listMetaSize {
		return 0, 0, listHeaderBytes, false, 0, false
	}
	head = int64(binary.LittleEndian.Uint64(v[0:8]))
	tail = int64(binary.LittleEndian.Uint64(v[8:16]))
	lpBytes = binary.LittleEndian.Uint64(v[16:24])
	everLarge = v[24] != 0
	return head, tail, lpBytes, everLarge, o, true
}

// listPackHeader lays the 25-byte header window into ob so both the create path (PutKind) and
// the in-place update path (InPlaceAt) share one encoding.
func listPackHeader(ob *[listMetaSize]byte, head, tail int64, lpBytes uint64, everLarge bool) {
	binary.LittleEndian.PutUint64(ob[0:8], uint64(head))
	binary.LittleEndian.PutUint64(ob[8:16], uint64(tail))
	binary.LittleEndian.PutUint64(ob[16:24], lpBytes)
	ob[24] = 0
	if everLarge {
		ob[24] = 1
	}
}

// listPutHeader writes a list's header window, or deletes the header when the window is empty
// (head == tail) so the list key stops existing: an empty list is no list, exactly as Redis
// deletes a list whose last element is popped.
func (c *connState) listPutHeader(lkey []byte, head, tail int64, lpBytes uint64, everLarge bool) error {
	if head == tail {
		c.srv.store.DeleteKind(lkey, kindListMeta)
		return nil
	}
	var ob [listMetaSize]byte
	listPackHeader(&ob, head, tail, lpBytes, everLarge)
	_, err := c.srv.store.PutKind(lkey, ob[:], kindListMeta)
	return err
}

// listPutHeaderAt writes a non-empty header window in place at a known offset, the fused
// write-back that pairs with listHeaderAt: the header is a fixed 25 bytes so it always fits
// the room the first PutKind reserved, and rewriting it at off skips the index probe
// listPutHeader would spend. It is only for a still-live window (head != tail); a pop that
// drains the list to empty must delete the header through listPutHeader instead, which is the
// rare once-per-lifetime path where the extra probe does not matter.
func (c *connState) listPutHeaderAt(off uint64, head, tail int64, lpBytes uint64, everLarge bool) {
	var ob [listMetaSize]byte
	listPackHeader(&ob, head, tail, lpBytes, everLarge)
	c.srv.store.InPlaceAt(off, ob[:])
}

// listWriteBackHeader is the pop write-back: it rewrites the surviving window in place at the
// offset the header read returned (the common case, one fewer index probe), or deletes the
// header when the pop drained the list to empty so the key stops existing. off must be the
// offset from the listHeaderAt read taken under this same stripe lock; the take of the popped
// element rows does not move the header record, so off still points at it.
func (c *connState) listWriteBackHeader(lkey []byte, head, tail int64, lpBytes uint64, everLarge bool, off uint64) {
	if head == tail {
		c.srv.store.DeleteKind(lkey, kindListMeta)
		return
	}
	c.listPutHeaderAt(off, head, tail, lpBytes, everLarge)
}

// listEncodingName reports a list's OBJECT ENCODING: "quicklist" once the list has ever grown
// past the listpack byte budget (the sticky flag latched by a push), else "listpack". It reads
// the flag straight from the header with no scan, and the stickiness mirrors Redis, which never
// converts a quicklist back to a listpack when it shrinks. A missing header reads as the empty
// default, "listpack".
func (c *connState) listEncodingName(lkey []byte) string {
	_, _, _, everLarge, _ := c.listHeader(lkey)
	if everLarge {
		return "quicklist"
	}
	return "listpack"
}

func (c *connState) cmdLPush(argv [][]byte)  { c.cmdPush(argv, true, false) }
func (c *connState) cmdRPush(argv [][]byte)  { c.cmdPush(argv, false, false) }
func (c *connState) cmdLPushX(argv [][]byte) { c.cmdPush(argv, true, true) }
func (c *connState) cmdRPushX(argv [][]byte) { c.cmdPush(argv, false, true) }

// cmdPush is the shared body for LPUSH/LPUSHX (atHead) and RPUSH/RPUSHX (atTail). It appends
// every element to the correct end of the window one at a time, so LPUSH a b c leaves the list
// [c b a] and RPUSH a b c leaves [a b c], matching Redis's per-element prepend/append order.
// The running listpack byte size grows by each element's entry size, and the sticky large flag
// latches on the first time the total crosses the byte budget, which is what OBJECT ENCODING
// reads back. When requireExisting is set (the LPUSHX/RPUSHX forms), a missing list is left
// untouched and the reply is 0 rather than a freshly created list.
func (c *connState) cmdPush(argv [][]byte, atHead, requireExisting bool) {
	if len(argv) < 3 {
		name := "rpush"
		switch {
		case atHead && requireExisting:
			name = "lpushx"
		case requireExisting:
			name = "rpushx"
		case atHead:
			name = "lpush"
		}
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	lkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, lpBytes, everLarge, hoff, existed := c.listHeaderAt(lkey)
	if requireExisting && !existed {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	for _, elem := range argv[2:] {
		var pos int64
		if atHead {
			head--
			pos = head
		} else {
			pos = tail
			tail++
		}
		ek := c.listElemKey(lkey, pos)
		if _, err := c.srv.store.PutKind(ek, elem, kindListElem); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		lpBytes += uint64(listEntrySize(elem))
		if !everLarge && lpBytes > listListpackMaxBytes {
			everLarge = true
		}
	}
	// A push never empties the window, so the header is always written, not deleted. When the
	// list already had a header, rewrite it in place at its known offset (the fused write-back,
	// one fewer index probe); when this push created the list, create the header row.
	if existed {
		c.listPutHeaderAt(hoff, head, tail, lpBytes, everLarge)
	} else if err := c.listPutHeader(lkey, head, tail, lpBytes, everLarge); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()
	// A push made the list non-empty, so wake the longest-waiting BLPOP/BRPOP/BLMOVE/BLMPOP
	// blocked on this key, if any. The call is an atomic load and a return when nobody is
	// blocked, so the common push pays nothing for the registry.
	c.srv.signalListKey(lkey)
	c.writeInt(tail - head)
}

// pushVerb classifies argv[0] as one of the four list-append verbs and reports how it appends:
// atHead for LPUSH/LPUSHX, requireExisting for the X forms. ok is false for every other command
// and for a push carrying no element (fewer than three args), so the caller keeps that on the
// ordinary single-command dispatch, where the arity error text is produced. The leading-byte
// switch keeps the classification off the GET/SET hot path: only a command whose verb starts
// with L or R pays the eqFold comparisons.
func pushVerb(argv [][]byte) (atHead, requireExisting, ok bool) {
	if len(argv) < 3 {
		return false, false, false
	}
	cmd := argv[0]
	if len(cmd) == 0 {
		return false, false, false
	}
	switch cmd[0] {
	case 'L', 'l', 'R', 'r':
	default:
		return false, false, false
	}
	switch {
	case eqFold(cmd, "RPUSH"):
		return false, false, true
	case eqFold(cmd, "LPUSH"):
		return true, false, true
	case eqFold(cmd, "RPUSHX"):
		return false, true, true
	case eqFold(cmd, "LPUSHX"):
		return true, true, true
	}
	return false, false, false
}

// drainPush handles a push command that the drain loop has classified, then greedily peeks ahead
// in the same pipeline for more pushes to the same key with the same verb from this one
// connection and folds them all into one cmdPushCoalesced call. It returns the buffer offset past
// every command it consumed. first is the already-parsed leading push; pos points just past it.
//
// Every element slice points into rbuf, which drain does not compact until the whole batch is
// parsed, so a captured element stays valid across the look-ahead even though each parse reuses
// the shared argv backing. That reuse is exactly why first must not be read after the first
// peek: its element headers live in that backing and the peek overwrites them, so this collects
// first's elements up front and never touches first again.
func (c *connState) drainPush(first [][]byte, atHead, requireExisting bool, pos int) int {
	lkey := first[1]
	coll := c.pushColl[:0]
	bnd := c.pushBnd[:0]
	for _, e := range first[2:] {
		coll = append(coll, e)
	}
	bnd = append(bnd, len(coll))
	for {
		argv, consumed, status := c.parse(c.rbuf[pos:])
		if status != parseOK {
			break
		}
		ah, re, ok := pushVerb(argv)
		if !ok || ah != atHead || re != requireExisting || !bytes.Equal(argv[1], lkey) {
			break
		}
		pos += consumed
		for _, e := range argv[2:] {
			coll = append(coll, e)
		}
		bnd = append(bnd, len(coll))
	}
	c.pushColl = coll
	c.pushBnd = bnd
	c.cmdPushCoalesced(lkey, atHead, requireExisting, coll, bnd)
	return pos
}

// cmdPushCoalesced applies a run of same-key, same-verb pushes captured from one connection's
// pipeline under a single stripe-lock acquisition, then writes one integer reply per original
// command. It is exactly equivalent to running the commands one after another: they arrived from
// one connection in program order, which the run preserves, so folding them touches the key's
// lock once rather than once per command. The only thing another client can observe is that its
// own ops on the key land before or after the whole run rather than between two of these pushes,
// and the wire protocol never ordered one client's commands against another's. elems holds every
// element in arrival order; bnd[k] is the cumulative element count through command k. Because each
// element lengthens the window by exactly one whichever end it joins, the reply for command k is
// the pre-run length plus bnd[k], so the replies need no per-element bookkeeping.
func (c *connState) cmdPushCoalesced(lkey []byte, atHead, requireExisting bool, elems [][]byte, bnd []int) {
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		for range bnd {
			c.writeErr(wrongType)
		}
		return
	}
	head, tail, lpBytes, everLarge, hoff, existed := c.listHeaderAt(lkey)
	if requireExisting && !existed {
		// An X-form push onto a missing list creates nothing and replies 0, and every command in
		// the run sees the same missing list, so each replies 0.
		mu.Unlock()
		for range bnd {
			c.writeInt(0)
		}
		return
	}
	baseLen := tail - head
	for i, elem := range elems {
		var pos int64
		if atHead {
			head--
			pos = head
		} else {
			pos = tail
			tail++
		}
		ek := c.listElemKey(lkey, pos)
		if _, err := c.srv.store.PutKind(ek, elem, kindListElem); err != nil {
			// Undo the reservation for the element that did not land, persist the header for the
			// elements that did so the window stays consistent, then reply the running length for
			// every command that completed and an error for the one the failure fell in and all
			// still queued behind it, the way a mid-pipeline store error would surface one at a
			// time. A store PutKind error is effectively unreachable in the in-memory regime; this
			// path exists so a coalesced run degrades exactly like the per-command path.
			if atHead {
				head++
			} else {
				tail--
			}
			if existed {
				c.listPutHeaderAt(hoff, head, tail, lpBytes, everLarge)
			} else if head != tail {
				_ = c.listPutHeader(lkey, head, tail, lpBytes, everLarge)
			}
			mu.Unlock()
			if head != tail {
				c.srv.signalListKey(lkey)
			}
			emsg := "ERR " + err.Error()
			for _, b := range bnd {
				if b <= i {
					c.writeInt(baseLen + int64(b))
				} else {
					c.writeErr(emsg)
				}
			}
			return
		}
		lpBytes += uint64(listEntrySize(elem))
		if !everLarge && lpBytes > listListpackMaxBytes {
			everLarge = true
		}
	}
	if existed {
		c.listPutHeaderAt(hoff, head, tail, lpBytes, everLarge)
	} else if err := c.listPutHeader(lkey, head, tail, lpBytes, everLarge); err != nil {
		mu.Unlock()
		emsg := "ERR " + err.Error()
		for range bnd {
			c.writeErr(emsg)
		}
		return
	}
	mu.Unlock()
	c.srv.signalListKey(lkey)
	for _, b := range bnd {
		c.writeInt(baseLen + int64(b))
	}
}

func (c *connState) cmdLPop(argv [][]byte) { c.cmdPop(argv, true) }
func (c *connState) cmdRPop(argv [][]byte) { c.cmdPop(argv, false) }

// cmdPop is the shared body for LPOP (atHead) and RPOP (atTail). Without a count it returns
// one element as a bulk string (nil on a missing key); with a count it returns an array of up
// to count elements (a null array on a missing key), drawn from the head outward for LPOP or
// the tail inward for RPOP, so RPOP key 2 over [a b c d] yields [d c]. Popping the last
// element deletes the list. Each element's row is read then point-deleted and the window bound
// advanced, so a pop is O(1) per element with no scan and no rewrite of the surviving rows.
func (c *connState) cmdPop(argv [][]byte, atHead bool) {
	if len(argv) < 2 || len(argv) > 3 {
		name := "rpop"
		if atHead {
			name = "lpop"
		}
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	hasCount := len(argv) == 3
	var count int64 = 1
	if hasCount {
		n, err := atoi64(argv[2])
		if err != nil {
			c.writeErr("ERR value is out of range, must be positive")
			return
		}
		if n < 0 {
			c.writeErr("ERR value is out of range, must be positive")
			return
		}
		count = n
	}

	lkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, lpBytes, everLarge, hoff, ok := c.listHeaderAt(lkey)
	if !ok {
		mu.Unlock()
		if hasCount {
			c.writeNilArray()
		} else {
			c.writeNil()
		}
		return
	}
	n := tail - head
	want := count
	if want > n {
		want = n
	}

	// No-count form: one element as a bulk string. The window is non-empty here (ok is true
	// only for a live header), so the single pop always yields an element.
	if !hasCount {
		var pos int64
		if atHead {
			pos = head
			head++
		} else {
			pos = tail - 1
			tail--
		}
		ek := c.listElemKey(lkey, pos)
		v, _ := c.srv.store.TakeKind(ek, c.vbuf[:0], kindListElem)
		c.vbuf = v
		lpBytes -= uint64(listEntrySize(v))
		c.listWriteBackHeader(lkey, head, tail, lpBytes, everLarge, hoff)
		mu.Unlock()
		c.writeBulk(v)
		return
	}

	// Count form: stream up to want elements as an array. Each element is copied straight into
	// the reply buffer by writeBulk before the next read reuses vbuf, so no per-element buffer
	// is held. The array header is exact because want is clamped to the live window size.
	c.writeArrayHeader(int(want))
	for i := int64(0); i < want; i++ {
		var pos int64
		if atHead {
			pos = head
			head++
		} else {
			pos = tail - 1
			tail--
		}
		ek := c.listElemKey(lkey, pos)
		v, _ := c.srv.store.TakeKind(ek, c.vbuf[:0], kindListElem)
		c.vbuf = v
		lpBytes -= uint64(listEntrySize(v))
		c.writeBulk(v)
	}
	c.listWriteBackHeader(lkey, head, tail, lpBytes, everLarge, hoff)
	mu.Unlock()
}

// cmdLLen implements LLEN: the list length is tail-head from the header, read lock-free with
// no scan, and 0 for a missing key. A plain string under the key is WRONGTYPE.
func (c *connState) cmdLLen(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'llen' command")
		return
	}
	lkey := argv[1]
	if c.stringConflict(lkey) {
		c.writeErr(wrongType)
		return
	}
	head, tail, _, _, _ := c.listHeader(lkey)
	c.writeInt(tail - head)
}

// cmdLIndex implements LINDEX: it maps the signed index onto the contiguous window (a
// non-negative index counts from head, a negative one from tail) and reads that one element's
// row directly, an O(1) point lookup with no descent. An out-of-range index or a missing key
// replies nil; a plain string under the key is WRONGTYPE. It takes the stripe lock so the
// window cannot shift under a concurrent pop between the header read and the element read.
func (c *connState) cmdLIndex(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'lindex' command")
		return
	}
	lkey := argv[1]
	idx, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, _, _, ok := c.listHeader(lkey)
	if !ok {
		mu.Unlock()
		c.writeNil()
		return
	}
	var pos int64
	if idx < 0 {
		pos = tail + idx
	} else {
		pos = head + idx
	}
	if pos < head || pos >= tail {
		mu.Unlock()
		c.writeNil()
		return
	}
	ek := c.listElemKey(lkey, pos)
	v, found := c.srv.store.GetKind(ek, c.vbuf[:0], kindListElem)
	c.vbuf = v
	mu.Unlock()
	if !found {
		c.writeNil()
		return
	}
	c.writeBulk(v)
}

// cmdLRange implements LRANGE: it normalizes the inclusive [start, stop] range against the
// list length the way Redis does (negatives count from the end, start clamps up to 0, stop
// clamps down to the last index) and streams each element in the window directly by position,
// one point lookup apiece. An empty or inverted range replies with an empty array; a plain
// string under the key is WRONGTYPE. It takes the stripe lock so the window is stable across
// the walk.
func (c *connState) cmdLRange(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'lrange' command")
		return
	}
	lkey := argv[1]
	start, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	stop, err := atoi64(argv[3])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	// LRANGE only excludes concurrent list writers, not other readers, so it takes the shared
	// lock and lets many LRANGE of one hot list run on many cores at once, a win a
	// single-threaded server cannot match. A list has no per-element TTL and the read never
	// mutates under the lock, so the shared path is always safe.
	mu.RLock()
	if c.stringConflict(lkey) {
		mu.RUnlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, _, _, ok := c.listHeader(lkey)
	if !ok {
		mu.RUnlock()
		c.writeArrayHeader(0)
		return
	}
	n := tail - head
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		mu.RUnlock()
		c.writeArrayHeader(0)
		return
	}
	c.writeArrayHeader(int(stop - start + 1))
	for i := start; i <= stop; i++ {
		ek := c.listElemKey(lkey, head+i)
		v, _ := c.srv.store.GetKind(ek, c.vbuf[:0], kindListElem)
		c.vbuf = v
		c.writeBulk(v)
	}
	mu.RUnlock()
}

// cmdLSet implements LSET: it overwrites the element at a signed index with a new value, an
// O(1) positional point write that never touches any other row. It maps the index onto the
// window exactly as LINDEX does, replaces that one element row, and adjusts the running
// listpack byte size by the difference between the new and old encoded sizes so OBJECT
// ENCODING stays exact; the sticky large flag can only latch on, never clear. A missing key is
// "ERR no such key", an out-of-range index is "ERR index out of range", and a plain string
// under the key is WRONGTYPE. It takes the stripe lock so the window is stable between the
// header read and the element write.
func (c *connState) cmdLSet(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'lset' command")
		return
	}
	lkey := argv[1]
	idx, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	val := argv[3]
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, lpBytes, everLarge, hoff, ok := c.listHeaderAt(lkey)
	if !ok {
		mu.Unlock()
		c.writeErr("ERR no such key")
		return
	}
	pos := head + idx
	if idx < 0 {
		pos = tail + idx
	}
	if pos < head || pos >= tail {
		mu.Unlock()
		c.writeErr("ERR index out of range")
		return
	}
	ek := c.listElemKey(lkey, pos)
	old, found := c.srv.store.GetKind(ek, c.vbuf[:0], kindListElem)
	oldSize := 0
	if found {
		oldSize = listEntrySize(old)
	}
	if _, err := c.srv.store.PutKind(ek, val, kindListElem); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	lpBytes = lpBytes - uint64(oldSize) + uint64(listEntrySize(val))
	if !everLarge && lpBytes > listListpackMaxBytes {
		everLarge = true
	}
	c.listPutHeaderAt(hoff, head, tail, lpBytes, everLarge)
	mu.Unlock()
	c.writeSimple("OK")
}

// cmdLPos implements LPOS: it finds the position of an element in list order, scanning by
// value with the RANK, COUNT, and MAXLEN options. RANK picks which match to start from (1 is
// the first from the head, a negative rank scans from the tail), COUNT bounds how many matches
// to return (0 means all, and its presence switches the reply from a single integer or nil to
// an array), and MAXLEN caps the number of elements compared (0 is unlimited). Every scanned
// element is one direct point read of its position row; the reported position is the dense
// external index (offset from the head). A missing key is a nil reply (or an empty array with
// COUNT), and a plain string under the key is WRONGTYPE. It takes the stripe lock so the
// window is stable across the scan.
func (c *connState) cmdLPos(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'lpos' command")
		return
	}
	lkey := argv[1]
	target := argv[2]
	var rank int64 = 1
	rankGiven := false
	var count int64 = -1 // -1 means COUNT not given (single-match reply)
	var maxlen int64 = 0 // 0 means no comparison cap
	for i := 3; i < len(argv); i += 2 {
		if i+1 >= len(argv) {
			c.writeErr("ERR syntax error")
			return
		}
		opt := argv[i]
		n, err := atoi64(argv[i+1])
		if err != nil {
			c.writeErr("ERR value is not an integer or out of range")
			return
		}
		switch {
		case eqFold(opt, "RANK"):
			if n == 0 {
				c.writeErr("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
				return
			}
			rank = n
			rankGiven = true
		case eqFold(opt, "COUNT"):
			if n < 0 {
				c.writeErr("ERR COUNT can't be negative")
				return
			}
			count = n
		case eqFold(opt, "MAXLEN"):
			if n < 0 {
				c.writeErr("ERR MAXLEN can't be negative")
				return
			}
			maxlen = n
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	_ = rankGiven

	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, _, _, _, ok := c.listHeaderAt(lkey)
	if !ok {
		mu.Unlock()
		if count >= 0 {
			c.writeArrayHeader(0)
		} else {
			c.writeNil()
		}
		return
	}

	// Direction and how many leading matches to skip come from the sign and magnitude of rank.
	backward := rank < 0
	skip := rank - 1
	if backward {
		skip = -rank - 1
	}
	matches := c.lposScan(lkey, target, head, tail, backward, skip, count, maxlen)
	mu.Unlock()

	if count >= 0 {
		c.writeArrayHeader(len(matches))
		for _, p := range matches {
			c.writeInt(p)
		}
		return
	}
	if len(matches) == 0 {
		c.writeNil()
		return
	}
	c.writeInt(matches[0])
}

// lposScan walks the window in the search direction, comparing each element to target and
// collecting the dense positions of the matches after skipping the first skip of them. It
// stops when it has enough (want, where want <= 0 with a given COUNT means all) or when it has
// compared maxlen elements (maxlen 0 disables the cap). The returned positions are external
// indexes (offset from head), so they are already in the form the reply wants.
func (c *connState) lposScan(lkey, target []byte, head, tail int64, backward bool, skip, want, maxlen int64) []int64 {
	var out []int64
	var compared int64
	step := int64(1)
	pos := head
	if backward {
		step = -1
		pos = tail - 1
	}
	for pos >= head && pos < tail {
		if maxlen > 0 && compared >= maxlen {
			break
		}
		ek := c.listElemKey(lkey, pos)
		v, found := c.srv.store.GetKind(ek, c.vbuf[:0], kindListElem)
		c.vbuf = v
		compared++
		if found && string(v) == string(target) {
			if skip > 0 {
				skip--
			} else {
				out = append(out, pos-head)
				// want < 0 means single-match mode: one is enough. want == 0 means all matches.
				if want < 0 || (want > 0 && int64(len(out)) >= want) {
					break
				}
			}
		}
		pos += step
	}
	return out
}

// cmdLTrim implements LTRIM: it keeps only the positional window [start, stop] and discards
// the rest. Because a trim removes only from the two ends, the surviving elements stay a
// contiguous run, so this moves the head and tail bounds inward and point-deletes the rows that
// fall outside, no renumbering of survivors. It normalizes the inclusive range the way LRANGE
// does; an empty or inverted range removes everything and deletes the key. A missing key is a
// no-op that still replies OK, and a plain string under the key is WRONGTYPE. It takes the
// stripe lock so the window cannot shift under the trim.
func (c *connState) cmdLTrim(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'ltrim' command")
		return
	}
	lkey := argv[1]
	start, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	stop, err := atoi64(argv[3])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, lpBytes, everLarge, hoff, ok := c.listHeaderAt(lkey)
	if !ok {
		mu.Unlock()
		c.writeSimple("OK")
		return
	}
	n := tail - head
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if stop >= n {
		stop = n - 1
	}
	// An empty or inverted window trims the whole list away: delete every row and the header.
	if start > stop || start >= n {
		for p := head; p < tail; p++ {
			c.srv.store.DeleteKind(c.listElemKey(lkey, p), kindListElem)
		}
		c.srv.store.DeleteKind(lkey, kindListMeta)
		mu.Unlock()
		c.writeSimple("OK")
		return
	}
	newHead := head + start
	newTail := head + stop + 1
	// Point-delete the rows that fall outside the surviving window, subtracting each from the
	// running byte size so OBJECT ENCODING stays exact. The survivors keep their position keys.
	for p := head; p < newHead; p++ {
		ek := c.listElemKey(lkey, p)
		v, took := c.srv.store.TakeKind(ek, c.vbuf[:0], kindListElem)
		if took {
			lpBytes -= uint64(listEntrySize(v))
		}
	}
	for p := newTail; p < tail; p++ {
		ek := c.listElemKey(lkey, p)
		v, took := c.srv.store.TakeKind(ek, c.vbuf[:0], kindListElem)
		if took {
			lpBytes -= uint64(listEntrySize(v))
		}
	}
	c.listPutHeaderAt(hoff, newHead, newTail, lpBytes, everLarge)
	mu.Unlock()
	c.writeSimple("OK")
}

// cmdLInsert implements LINSERT key BEFORE|AFTER pivot value. It is the first list command
// that edits the interior of the window rather than an end, so it is where the dense-window
// model has to answer what the spec's sparse fractional order key (2064/f1_rewrite_ltm/08)
// answers with an O(1) key-between-neighbors insert. This engine keeps the dense window on
// purpose: LINDEX, LRANGE, and the push/pop ends are all O(1) direct index arithmetic here,
// where a fractional key would push positional access onto an O(log n) order-statistic
// select. A list is a deque whose reads and end edits dominate and whose interior inserts are
// rare, so the trade runs the other way from a general ordered index: pay the interior insert,
// keep the common path free. So this opens the slot by shifting the shorter side of the pivot
// by one position (the side with fewer elements to move), an O(min(i, n-i)) rewrite that leaves
// the window contiguous, then writes the new element into the freed slot. A missing key replies
// 0, a pivot that is not present replies -1, and a plain string under the key is WRONGTYPE.
func (c *connState) cmdLInsert(argv [][]byte) {
	if len(argv) != 5 {
		c.writeErr("ERR wrong number of arguments for 'linsert' command")
		return
	}
	lkey := argv[1]
	var before bool
	switch {
	case eqFold(argv[2], "BEFORE"):
		before = true
	case eqFold(argv[2], "AFTER"):
		before = false
	default:
		c.writeErr("ERR syntax error")
		return
	}
	pivot := argv[3]
	val := argv[4]

	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, lpBytes, everLarge, hoff, ok := c.listHeaderAt(lkey)
	if !ok {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	// Find the first pivot occurrence in list order. Each element is one direct point read. A
	// separate found flag carries the result rather than a sentinel position, because the window
	// runs through negative positions (LPUSH pushes below zero) so no int64 is a safe "absent"
	// marker.
	var pivotPos int64
	pivotFound := false
	for p := head; p < tail; p++ {
		v, got := c.srv.store.GetKind(c.listElemKey(lkey, p), c.vbuf[:0], kindListElem)
		c.vbuf = v
		if got && string(v) == string(pivot) {
			pivotPos = p
			pivotFound = true
			break
		}
	}
	if !pivotFound {
		mu.Unlock()
		c.writeInt(-1)
		return
	}

	// Insertion index i within the window: BEFORE lands the new element at the pivot's index,
	// AFTER at the next one. left is how many elements sit before the slot, right how many after.
	i := pivotPos - head
	if !before {
		i++
	}
	n := tail - head
	left := i
	right := n - i

	var newElemPos int64
	if left <= right {
		// Shift the left run [head, head+i) down by one, lowest source first so each target slot
		// is freshly vacated (head-1 is empty to begin with). The freed slot at head+i-1 takes the
		// new element and the window grows on the left.
		for p := head; p < head+i; p++ {
			v, _ := c.srv.store.TakeKind(c.listElemKey(lkey, p), c.vbuf[:0], kindListElem)
			c.vbuf = v
			_, _ = c.srv.store.PutKind(c.listElemKey(lkey, p-1), v, kindListElem)
		}
		newElemPos = head + i - 1
		head--
	} else {
		// Shift the right run [head+i, tail) up by one, highest source first so each target slot is
		// freshly vacated (tail is empty to begin with). The freed slot at head+i takes the new
		// element and the window grows on the right.
		for p := tail - 1; p >= head+i; p-- {
			v, _ := c.srv.store.TakeKind(c.listElemKey(lkey, p), c.vbuf[:0], kindListElem)
			c.vbuf = v
			_, _ = c.srv.store.PutKind(c.listElemKey(lkey, p+1), v, kindListElem)
		}
		newElemPos = head + i
		tail++
	}
	_, _ = c.srv.store.PutKind(c.listElemKey(lkey, newElemPos), val, kindListElem)
	lpBytes += uint64(listEntrySize(val))
	if !everLarge && lpBytes > listListpackMaxBytes {
		everLarge = true
	}
	c.listPutHeaderAt(hoff, head, tail, lpBytes, everLarge)
	mu.Unlock()
	// LINSERT grew the list by one, so it can satisfy a client blocked on this key.
	c.srv.signalListKey(lkey)
	c.writeInt(tail - head)
}

// cmdLRem implements LREM key count value. It removes matching elements from the interior of
// the window and then compacts the survivors back into a contiguous run so the dense-window
// invariant holds for the next positional read. count > 0 removes the first count matches
// scanning from the head, count < 0 the last |count| scanning from the tail, and count 0 all
// of them. A first pass collects the matching positions (positions only, not values, so a huge
// list does not buffer its contents), the sign of count selects which of those to drop, and a
// second pass walks the window with a write cursor, point-deleting the dropped rows and sliding
// each survivor down to close the gaps. Removing the last element deletes the key, exactly as
// Redis drops an emptied list. A missing key replies 0 and a plain string under the key is
// WRONGTYPE.
func (c *connState) cmdLRem(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'lrem' command")
		return
	}
	lkey := argv[1]
	count, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	target := argv[3]

	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	if c.stringConflict(lkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	head, tail, lpBytes, everLarge, hoff, ok := c.listHeaderAt(lkey)
	if !ok {
		mu.Unlock()
		c.writeInt(0)
		return
	}

	// Pass one: collect the positions of every match in list order.
	var matches []int64
	for p := head; p < tail; p++ {
		v, found := c.srv.store.GetKind(c.listElemKey(lkey, p), c.vbuf[:0], kindListElem)
		c.vbuf = v
		if found && string(v) == string(target) {
			matches = append(matches, p)
		}
	}
	if len(matches) == 0 {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	// Select which matches to drop from the sign and magnitude of count: a positive count keeps
	// the leading matches, a negative count the trailing ones, and zero drops them all.
	del := make(map[int64]struct{}, len(matches))
	switch {
	case count > 0:
		k := count
		if k > int64(len(matches)) {
			k = int64(len(matches))
		}
		for _, p := range matches[:k] {
			del[p] = struct{}{}
		}
	case count < 0:
		k := -count
		if k > int64(len(matches)) {
			k = int64(len(matches))
		}
		for _, p := range matches[int64(len(matches))-k:] {
			del[p] = struct{}{}
		}
	default:
		for _, p := range matches {
			del[p] = struct{}{}
		}
	}

	// Pass two: walk the window with a write cursor. A dropped element's row is point-deleted and
	// its bytes come off the running size; a survivor slides down to the cursor only when a gap has
	// opened before it, so an untouched prefix is never rewritten.
	removed := int64(0)
	w := head
	for p := head; p < tail; p++ {
		if _, drop := del[p]; drop {
			v, took := c.srv.store.TakeKind(c.listElemKey(lkey, p), c.vbuf[:0], kindListElem)
			if took {
				lpBytes -= uint64(listEntrySize(v))
			}
			removed++
			continue
		}
		if w != p {
			v, _ := c.srv.store.TakeKind(c.listElemKey(lkey, p), c.vbuf[:0], kindListElem)
			c.vbuf = v
			_, _ = c.srv.store.PutKind(c.listElemKey(lkey, w), v, kindListElem)
		}
		w++
	}
	if w == head {
		// Every element was removed: the list is empty, so the key stops existing.
		c.srv.store.DeleteKind(lkey, kindListMeta)
	} else {
		c.listPutHeaderAt(hoff, head, w, lpBytes, everLarge)
	}
	mu.Unlock()
	c.writeInt(removed)
}

// listEntrySize returns the bytes one element occupies inside a Redis listpack: its encoding
// plus the backlen field, matching lpEncodeGetType followed by lpEncodeBacklen. This is what
// the running lpBytes sums, so OBJECT ENCODING flips to quicklist at the exact element the
// real listpack would overflow the byte budget. A value that parses as a canonical int64 takes
// the compact integer encoding; anything else a string encoding sized by its length.
func listEntrySize(e []byte) int {
	enc := listEncodingSize(e)
	return enc + listBacklenSize(enc)
}

// listEncodingSize returns the size of an element's listpack encoding: the type byte or bytes
// plus the payload, before the backlen.
func listEncodingSize(e []byte) int {
	if v, ok := listTryInteger(e); ok {
		switch {
		case v >= 0 && v <= 127:
			return 1 // 7-bit unsigned
		case v >= -4096 && v <= 4095:
			return 2 // 13-bit
		case v >= -32768 && v <= 32767:
			return 3 // 16-bit
		case v >= -8388608 && v <= 8388607:
			return 4 // 24-bit
		case v >= -2147483648 && v <= 2147483647:
			return 5 // 32-bit
		default:
			return 9 // 64-bit
		}
	}
	n := len(e)
	switch {
	case n < 64:
		return 1 + n // 6-bit string length
	case n < 4096:
		return 2 + n // 12-bit string length
	default:
		return 5 + n // 32-bit string length
	}
}

// listBacklenSize returns the number of bytes lpEncodeBacklen uses for an entry whose encoding
// is encLen bytes long; the backlen lets a listpack be walked backwards and grows one byte per
// 7 bits of entry length.
func listBacklenSize(encLen int) int {
	switch {
	case encLen <= 127:
		return 1
	case encLen < 16384:
		return 2
	case encLen < 2097152:
		return 3
	case encLen < 268435456:
		return 4
	default:
		return 5
	}
}

// listTryInteger reports whether e is the canonical decimal form of an int64, the test
// lpStringToInt64 makes before storing an element as an integer. The round-trip check rejects
// leading zeros, a leading plus, "-0", surrounding spaces, and any other non-canonical
// spelling, so "10" is an integer but "010", "+10", "-0", and "10\n" are strings, exactly as
// Redis's listpack decides.
func listTryInteger(e []byte) (int64, bool) {
	if len(e) == 0 || len(e) > 20 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(e), 10, 64)
	if err != nil {
		return 0, false
	}
	if strconv.FormatInt(v, 10) != string(e) {
		return 0, false
	}
	return v, true
}

// listPopEnd removes one element from the head (atHead) or the tail of lkey and returns it,
// assuming the caller already holds lkey's stripe lock and has ruled out a string conflict. ok
// is false when the list is empty or missing. It maintains the header exactly like cmdPop: the
// window bound advances at the chosen end, the running listpack size drops by the element, and
// the header is rewritten in place or deleted when the pop drains the list to empty. The value
// is returned aliasing c.vbuf (a copy out of the arena), so it stays valid after the row is
// gone and can be pushed straight onto another list.
func (c *connState) listPopEnd(lkey []byte, atHead bool) (val []byte, ok bool) {
	head, tail, lpBytes, everLarge, hoff, have := c.listHeaderAt(lkey)
	if !have {
		return c.vbuf[:0], false
	}
	var pos int64
	if atHead {
		pos = head
		head++
	} else {
		pos = tail - 1
		tail--
	}
	ek := c.listElemKey(lkey, pos)
	v, _ := c.srv.store.TakeKind(ek, c.vbuf[:0], kindListElem)
	c.vbuf = v
	lpBytes -= uint64(listEntrySize(v))
	c.listWriteBackHeader(lkey, head, tail, lpBytes, everLarge, hoff)
	return v, true
}

// listPushEnd prepends (atHead) or appends one element to lkey, assuming the caller already
// holds lkey's stripe lock and has ruled out a string conflict. It mirrors cmdPush for a single
// element: it extends the window at the chosen end, grows the running listpack size, latches the
// sticky large flag, and rewrites the header in place or creates it for a new list. elem may
// alias c.vbuf (the value a listPopEnd just returned) because it is copied into the arena by
// PutKind before the header read that would reuse the buffer, and the header read lands in its
// own scratch, not vbuf.
func (c *connState) listPushEnd(lkey, elem []byte, atHead bool) error {
	head, tail, lpBytes, everLarge, hoff, existed := c.listHeaderAt(lkey)
	var pos int64
	if atHead {
		head--
		pos = head
	} else {
		pos = tail
		tail++
	}
	ek := c.listElemKey(lkey, pos)
	if _, err := c.srv.store.PutKind(ek, elem, kindListElem); err != nil {
		return err
	}
	lpBytes += uint64(listEntrySize(elem))
	if !everLarge && lpBytes > listListpackMaxBytes {
		everLarge = true
	}
	if existed {
		c.listPutHeaderAt(hoff, head, tail, lpBytes, everLarge)
		return nil
	}
	return c.listPutHeader(lkey, head, tail, lpBytes, everLarge)
}

// parseLR decodes a LEFT/RIGHT direction token (case-insensitive), reporting atHead for LEFT.
// The list move and multi-pop commands take one or two of these to say which end they act on.
func parseLR(tok []byte) (atHead, ok bool) {
	switch {
	case eqFold(tok, "LEFT"):
		return true, true
	case eqFold(tok, "RIGHT"):
		return false, true
	}
	return false, false
}

// cmdLMove implements LMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT>: it pops one element
// from the chosen end of the source and pushes it onto the chosen end of the destination,
// atomically under both keys' stripe locks, and returns the moved element (a nil bulk when the
// source is empty or missing). Source equal to destination is a rotation on one list, which the
// pop-then-push handles directly: the pop rewrites (or deletes) the header and the push reads it
// back, so rotating a one-element list is the no-op Redis makes it. Either key holding a string
// is WRONGTYPE.
func (c *connState) cmdLMove(argv [][]byte) {
	if len(argv) != 5 {
		c.writeErr("ERR wrong number of arguments for 'lmove' command")
		return
	}
	c.lmove(argv[1], argv[2], argv[3], argv[4])
}

// cmdRPopLPush implements RPOPLPUSH source destination, the classic form that predates LMOVE and
// is exactly LMOVE source destination RIGHT LEFT: pop the source's tail, push it to the
// destination's head.
func (c *connState) cmdRPopLPush(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'rpoplpush' command")
		return
	}
	c.lmove(argv[1], argv[2], []byte("RIGHT"), []byte("LEFT"))
}

// lmove is the shared body for LMOVE and RPOPLPUSH once the direction tokens are known. It
// validates the two directions, takes both stripe locks in a fixed order (deadlock-safe against
// a concurrent move of the same pair the other way), then pops one element from the source and
// pushes it to the destination. A missing or empty source moves nothing and replies with a nil
// bulk.
func (c *connState) lmove(source, destination, fromTok, toTok []byte) {
	fromHead, ok1 := parseLR(fromTok)
	toHead, ok2 := parseLR(toTok)
	if !ok1 || !ok2 {
		c.writeErr("ERR syntax error")
		return
	}
	unlock := c.lockTwoStripes(source, destination)
	if c.stringConflict(source) || c.stringConflict(destination) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	v, ok := c.listPopEnd(source, fromHead)
	if !ok {
		unlock()
		c.writeNil()
		return
	}
	if err := c.listPushEnd(destination, v, toHead); err != nil {
		unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	unlock()
	// The moved element landed on the destination, so wake a client blocked on it.
	c.srv.signalListKey(destination)
	c.writeBulk(v)
}

// cmdLMPop implements LMPOP numkeys key [key ...] <LEFT|RIGHT> [COUNT count]: it pops up to count
// elements (default 1) from the chosen end of the first key in the list that is a non-empty list,
// and replies with a two-element array of that key's name and the popped elements, or a nil array
// when every listed key is empty or missing. It scans the keys left to right under each key's own
// stripe lock, so it never holds more than one lock and stops at the first key that yields
// anything. A listed key holding a plain string is WRONGTYPE.
func (c *connState) cmdLMPop(argv [][]byte) {
	// LMPOP numkeys key [key ...] <LEFT|RIGHT> [COUNT count]
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'lmpop' command")
		return
	}
	numkeys, err := atoi64(argv[1])
	if err != nil {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	if numkeys <= 0 {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	// argv layout after numkeys: numkeys keys, then the direction, then an optional COUNT count.
	if int64(len(argv)) < 2+numkeys+1 {
		c.writeErr("ERR syntax error")
		return
	}
	keys := argv[2 : 2+numkeys]
	rest := argv[2+numkeys:]
	if len(rest) == 0 {
		c.writeErr("ERR syntax error")
		return
	}
	atHead, ok := parseLR(rest[0])
	if !ok {
		c.writeErr("ERR syntax error")
		return
	}
	count := int64(1)
	switch len(rest) {
	case 1:
		// direction only, count defaults to 1
	case 3:
		if !eqFold(rest[1], "COUNT") {
			c.writeErr("ERR syntax error")
			return
		}
		n, err := atoi64(rest[2])
		if err != nil || n <= 0 {
			c.writeErr("ERR count should be greater than 0")
			return
		}
		count = n
	default:
		c.writeErr("ERR syntax error")
		return
	}

	for _, key := range keys {
		mu := &c.srv.incrMu[c.srv.stripe(key)]
		mu.Lock()
		if c.stringConflict(key) {
			mu.Unlock()
			c.writeErr(wrongType)
			return
		}
		head, tail, lpBytes, everLarge, hoff, have := c.listHeaderAt(key)
		if !have {
			mu.Unlock()
			continue
		}
		n := tail - head
		want := count
		if want > n {
			want = n
		}
		// Two-element reply: the key name, then the array of popped elements.
		c.writeArrayHeader(2)
		c.writeBulk(key)
		c.writeArrayHeader(int(want))
		for i := int64(0); i < want; i++ {
			var pos int64
			if atHead {
				pos = head
				head++
			} else {
				pos = tail - 1
				tail--
			}
			ek := c.listElemKey(key, pos)
			v, _ := c.srv.store.TakeKind(ek, c.vbuf[:0], kindListElem)
			c.vbuf = v
			lpBytes -= uint64(listEntrySize(v))
			c.writeBulk(v)
		}
		c.listWriteBackHeader(key, head, tail, lpBytes, everLarge, hoff)
		mu.Unlock()
		return
	}
	// Every listed key was empty or missing.
	c.writeNilArray()
}
