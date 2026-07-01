package f1srv

import (
	"bytes"
	"encoding/binary"
)

// The STORE forms (SINTERSTORE/SUNIONSTORE/SDIFFSTORE) compute the same k-way merge as
// their read cousins (spec 2064/f1_rewrite_ltm/06 section 5) and write the result into a
// destination set as element-per-row rows plus the maintained header, returning the stored
// cardinality. Two things the reads never had to handle show up here:
//
//   - Aliasing. The destination may also be a source (SINTERSTORE dst dst other). Clearing
//     the destination up front would pull the ground out from under a cursor still reading
//     it, so an aliased store buffers the arena-stable result first, then clears, then
//     writes. The buffered members are subslices of the immutable arena, and a delete frees
//     only index slots, never arena bytes, so the buffered members stay valid across the
//     clear. A non-aliased store streams the result straight in, O(k) memory for a result
//     of k members even against billion-member sources.
//   - Destination overwrite. The destination is replaced regardless of its prior type, so a
//     plain string there is dropped, not a WRONGTYPE. The WRONGTYPE check covers the sources
//     only, exactly as the reads do. An empty result deletes the destination (empty set is
//     no set), matching Redis.
//
// All sources and the destination take their stripe locks for the whole operation through
// lockStripes, so nothing changes under the merge and the destination write is atomic with
// respect to concurrent readers of any key involved.

// clearSetRows removes every member row of skey and its header row in bounded batches, so
// clearing a huge destination never materializes the whole set. It leaves any string at the
// key alone; the STORE handlers drop that separately with store.Delete before calling this.
// A caller may hold arena-stable subslices of skey's own members across this clear (the
// aliased-store case): a delete frees only the ordered-index slot, never the arena bytes the
// subslices point at, so the buffered result survives.
func (c *connState) clearSetRows(skey []byte) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	prefix := make([]byte, 0, n+len(skey))
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, skey...)
	batch := make([][]byte, 0, hashScanBatch)
	for {
		keys, _ := c.srv.store.CollScan(prefix, nil, hashScanBatch, batch[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			if c.srv.store.DeleteKind(k, kindSetMember) {
				c.srv.store.CollRemove(k)
			}
		}
	}
	c.srv.store.DeleteKind(skey, kindSetMeta)
}

// storeAlgebra is the shared body of the three STORE forms: it locks the destination and
// every source, rejects a source held by a plain string, computes the result with the given
// iterator, writes it into the destination as a fresh set, and replies with the stored
// cardinality. each is sinterEach/sunionEach/sdiffEach; only the iterator differs.
func (c *connState) storeAlgebra(argv [][]byte, cmdName string, each func([][]byte, func([]byte) bool)) {
	// <CMD> destination key [key ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for '" + cmdName + "' command")
		return
	}
	dest := argv[1]
	keys := argv[2:]
	all := make([][]byte, 0, len(keys)+1)
	all = append(all, dest)
	all = append(all, keys...)
	unlock := c.lockStripes(all)
	// WRONGTYPE covers the sources only: the destination is overwritten whatever it held.
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	aliased := false
	for _, k := range keys {
		if bytes.Equal(k, dest) {
			aliased = true
			break
		}
	}

	count := 0
	var writeErr error
	insert := func(m []byte) bool {
		mk := c.memberKey(dest, m)
		isNew, err := c.srv.store.PutKind(mk, nil, kindSetMember)
		if err != nil {
			writeErr = err
			return false
		}
		if isNew {
			c.srv.store.CollInsert(mk, kindSetMember)
			count++
		}
		return true
	}

	if aliased {
		// Buffer the arena-stable result before touching the destination: the destination is
		// one of the sources, so clearing it first would corrupt the cursor reading it. The
		// buffered members survive the clear because a delete frees only index slots.
		out := make([][]byte, 0)
		each(keys, func(m []byte) bool {
			out = append(out, m)
			return true
		})
		c.srv.store.Delete(dest)
		c.clearSetRows(dest)
		for _, m := range out {
			if !insert(m) {
				break
			}
		}
	} else {
		// The destination is not a source, so stream the result straight in: peak memory is k
		// cursors plus one member in hand even for a result of millions of members.
		c.srv.store.Delete(dest)
		c.clearSetRows(dest)
		each(keys, insert)
	}
	if writeErr != nil {
		unlock()
		c.writeErr("ERR " + writeErr.Error())
		return
	}
	if err := c.setSetCard(dest, uint64(count)); err != nil {
		unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	unlock()
	c.writeInt(int64(count))
}

// cmdSInterStore stores the intersection of the sources into the destination and replies
// with its cardinality.
func (c *connState) cmdSInterStore(argv [][]byte) {
	c.storeAlgebra(argv, "sinterstore", c.sinterEach)
}

// cmdSUnionStore stores the union of the sources into the destination and replies with its
// cardinality.
func (c *connState) cmdSUnionStore(argv [][]byte) {
	c.storeAlgebra(argv, "sunionstore", c.sunionEach)
}

// cmdSDiffStore stores the difference of the first source minus the rest into the
// destination and replies with its cardinality.
func (c *connState) cmdSDiffStore(argv [][]byte) {
	c.storeAlgebra(argv, "sdiffstore", c.sdiffEach)
}
