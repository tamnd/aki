package f1srv

import (
	"bytes"

	"github.com/tamnd/aki/engine/f1raw"
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
//
// It enumerates the members to delete from the dense member vector rather than the ordered
// index (spec 2064/f1_rewrite_ltm/20): the vector is the authoritative membership structure
// for the set type, so the clear walks it exactly as SMEMBERS does and never descends the
// skip-list. The caller holds skey's stripe lock across the STORE, so the layout is frozen and
// one drained downward walk per partition yields every live member once. Each member row is
// dropped from the hash index (DeleteKind); the set type no longer indexes members in the
// ordered index. SetVecScanDown and SetPartVecScanDown build the vector on first use, so a set
// cleared before it was ever enumerated or drawn from still resolves its members. Walking the
// frozen snapshot while deleting is safe: DeleteKind touches the hash index, not the vector, so
// the snapshot length stays stable and the downward walk covers every member once; the vector
// itself is torn down wholesale at the end.
func (c *connState) clearSetRows(skey []byte) {
	p := c.partitionsFor(skey)
	scan := make([][]byte, 0, hashScanBatch)
	if p > 1 {
		base := c.partScanBase(skey)
		for part := 0; part < p; part++ {
			hi := -1
			for {
				keys, next := c.srv.store.SetPartVecScanDown(base, p, part, hi, hashScanBatch, scan[:0])
				for _, k := range keys {
					c.srv.store.DeleteKind(k, kindSetMember)
				}
				if next == 0 {
					break
				}
				hi = next
			}
		}
	} else {
		prefix := c.setPrefix(skey)
		hi := -1
		for {
			keys, next := c.srv.store.SetVecScanDown(prefix, hi, hashScanBatch, scan[:0])
			for _, k := range keys {
				c.srv.store.DeleteKind(k, kindSetMember)
			}
			if next == 0 {
				break
			}
			hi = next
		}
	}
	c.srv.store.DeleteKind(skey, kindSetMeta)
	// Drop skey's dense member vector(s) and partition descriptor wholesale (spec 2064/18 5.3):
	// the set is gone, so the vectors are stale. CollRandDrop drops the whole-set vector and,
	// through the descriptor, every partition vector. A later STORE into this same key rebuilds a
	// fresh vector as it publishes members, and the per-member CollRandInsert/CollPartRandInsert
	// calls in storeAlgebra build-or-append onto that, so nothing points at the just-deleted rows.
	c.srv.store.CollRandDrop(c.setPrefix(skey))
}

// storeAlgebra is the shared body of the three STORE forms: it locks the destination and
// every source, rejects a source held by a plain string, computes the result with the given
// iterator, writes it into the destination as a fresh set, and replies with the stored
// cardinality. each is sinterEach/sunionEach/sdiffEach; only the iterator differs.
//
// merge is the sorted-hash merge form of the same operation (setMergeIntersect/setMergeDiff/
// setMergeUnion): when it engages it returns the whole result as arena-stable subslices before any
// destination mutation, which the store then clears the destination and writes, exactly as the aliased
// buffered path does. It engages only for the eligible two-source same-P shapes and returns false
// otherwise, so the each fallback still handles every other shape. The store holds all sources' stripe
// locks, so the merge's synchronous fold and read are current.
func (c *connState) storeAlgebra(argv [][]byte, cmdName string, each func([][]byte, func([]byte) bool), merge func([][]byte) ([][]byte, bool)) {
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

	// The destination is partitioned exactly when the sources are (partitionsFor is a whole-server
	// hook until the adaptive engage of slice 6), so a routed store writes each result member under
	// its partition-routed key and the same reader (streamSetPart) that framed the sources reads the
	// result back byte-for-byte. A partitioned set reports the hashtable encoding (section 6.11), so
	// the routed write skips the encoding fold and stamps the header with P via setHeaderEncodeP.
	destP := c.partitionsFor(dest)
	count := 0
	enc := encNone
	var writeErr error
	// Collect each stored member as a (hash, offset) entry per destination partition so the sorted-hash
	// order is folded once in bulk after the write (SortedHashBuild below), not one incremental fold per
	// member. The per-member fold regrows the destination's flat sorted array O(k) times for an O(k^2)
	// build, the cliff labs/setstorebuild isolates and the reason a STORE that spilled to the coll form
	// collapsed against the rivals; the bulk build is one O(k log k) sort. buckets has one slot per
	// destination partition, and an unpartitioned destination uses slot 0. The member hash is computed
	// while m is in hand (it aliases arena or cursor scratch that a later member reuses), so only the
	// uint64 hash and the offset are kept, never the member bytes.
	buckets := make([][]f1raw.SortedHashEntry, destP)
	insert := func(m []byte) bool {
		var mk []byte
		if destP > 1 {
			part := f1raw.PartitionOf(m, destP)
			mk = c.partMemberKey(dest, m, part, destP)
			isNew, err := c.srv.store.PutKind(mk, nil, kindSetMember)
			if err != nil {
				writeErr = err
				return false
			}
			if isNew {
				// Eagerly build-or-splice the partition's dense vector through the descriptor so
				// CollRandDrop can tear it down (doc 20 section 6.1). The vector is the authoritative
				// membership structure for the set type, so the store no longer touches the ordered
				// index. After clearSetRows dropped the old vector the first stored member rebuilds it;
				// each subsequent member appends. base is built into ppbuf, distinct from mk's kbuf;
				// CollPartRandInsertOff sets its final byte and returns the member's arena offset for
				// the bulk sorted-hash build.
				base := c.partScanBase(dest)
				off, ok := c.srv.store.CollPartRandInsertOff(base, destP, part, mk, kindSetMember)
				if ok {
					buckets[part] = append(buckets[part], f1raw.SortedHashEntry{Hash: f1raw.MemberHash(m), Off: off})
				}
				count++
			}
			return true
		}
		mk = c.memberKey(dest, m)
		isNew, err := c.srv.store.PutKind(mk, nil, kindSetMember)
		if err != nil {
			writeErr = err
			return false
		}
		if isNew {
			// Collect the freshly-stored member's offset for the two one-shot builds run after the write:
			// the dense member vector (CollRandBulkBuild) and the sorted-hash order (SortedHashBuild). The
			// store no longer inserts each member into the vector as it goes (CollRandInsertOff): rebuilding
			// the reused destination's vector one add at a time paid a back-index map insert, a snapshot
			// allocation, and an occasional slots doubling per member, and labs/setvecbuild shows folding the
			// same offsets into one bulk build is 3-4x cheaper for a fraction of the allocations. MemberOff
			// resolves the offset PutKind just wrote without touching the vector.
			off, ok := c.srv.store.MemberOff(mk, kindSetMember)
			if ok {
				buckets[0] = append(buckets[0], f1raw.SortedHashEntry{Hash: f1raw.MemberHash(m), Off: off})
			}
			count++
			enc = foldSetEnc(enc, m, uint64(count))
		}
		return true
	}

	handled := false
	if merge != nil {
		// The sorted-hash merge yields the whole result as arena-stable subslices before any
		// destination write, so it subsumes the aliased case: clear the destination (the buffered
		// members survive a delete that frees only index slots) and insert the buffer. It engages
		// only for the eligible two-source same-P shapes; otherwise ok is false and the each
		// fallback below runs.
		if merged, ok := merge(keys); ok {
			c.srv.store.Delete(dest)
			c.clearSetRows(dest)
			for _, m := range merged {
				if !insert(m) {
					break
				}
			}
			handled = true
		}
	}
	if !handled {
		if aliased {
			// Buffer the arena-stable result before touching the destination: the destination is
			// one of the sources, so clearing it first would corrupt the cursor reading it. The
			// buffered members survive the clear because a delete frees only index slots. Size the
			// buffer to the summed source cardinalities, the upper bound for any of the three set
			// operations, so the append does not double and re-copy as members land.
			out := make([][]byte, 0, algebraBufCap(c.summedCard(keys)))
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
	}
	if writeErr != nil {
		unlock()
		c.writeErr("ERR " + writeErr.Error())
		return
	}
	// Build the unpartitioned destination's dense member vector in one pass from the offsets collected
	// during the write, rather than the per-member vector insert the write dropped (see insert above).
	// clearSetRows dropped the old vector, so this installs the fresh one; a lock-free draw sees no
	// vector or the whole vector, never a partial. An empty result installs no vector, matching the
	// header delete storePutHeader does for a zero count. Only the unpartitioned destination takes this
	// path: a partitioned destination's per-partition vectors are still built through the descriptor by
	// CollPartRandInsertOff in insert, which the bulk build does not replace.
	if destP == 1 && len(buckets[0]) > 0 {
		offs := make([]uint64, len(buckets[0]))
		for i, e := range buckets[0] {
			offs[i] = e.Off
		}
		c.srv.store.CollRandBulkBuild(c.setPrefix(dest), offs)
	}
	// Fold each destination partition's sorted-hash array in one bulk pass from the entries collected
	// during the write, instead of the O(k^2) per-member incremental fold the write would otherwise
	// have journaled. A partition that took members is built from them; a partition left empty (a STORE
	// that emptied a previously-populated destination, or every member routing elsewhere) is reset so a
	// destination reused across STOREs never folds a new result on top of the previous one's stale
	// offsets. Both are no-ops when the fold facility is off. This runs under the destination's stripe
	// lock, still held, so no concurrent reader sees a half-built order.
	for part := 0; part < destP; part++ {
		var prefix []byte
		if destP > 1 {
			base := c.partScanBase(dest)
			base[len(base)-1] = byte(part)
			prefix = base
		} else {
			prefix = c.setPrefix(dest)
		}
		if len(buckets[part]) == 0 {
			c.srv.store.SortedHashReset(prefix)
		} else {
			c.srv.store.SortedHashBuild(prefix, buckets[part])
		}
	}
	if err := c.storePutHeader(dest, count, enc, destP); err != nil {
		unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	unlock()
	// A STORE can materialize a set large enough to warrant partitioning in one shot, so the engage
	// trigger runs on the freshly-built destination the same way it does after a SADD. It is a no-op
	// when the feature is off or the result is below threshold, and runs off the stripe locks the
	// store held so it can take the migration's exclusive stripe set without self-deadlock.
	if count > 0 {
		c.maybeEngageSet(dest, count)
	}
	c.writeInt(int64(count))
}

// storePutHeader writes the destination set's header after a STORE, routing on the destination's
// partition count. An unpartitioned destination keeps the existing 9-byte header (count plus the
// folded encoding), so a P=1 store is byte-for-byte what it was before partitioning. A partitioned
// destination stamps the partition count into the header via setHeaderEncodeP and records the
// hashtable encoding a partitioned set always reports (section 6.11), matching what setBumpCard
// writes on a routed SADD so a STORE-built and an SADD-built partitioned set are indistinguishable.
// A zero count deletes the header either way, so an empty result leaves no set.
func (c *connState) storePutHeader(dest []byte, count int, enc byte, p int) error {
	if p <= 1 {
		return c.setPutHeader(dest, uint64(count), enc)
	}
	if count == 0 {
		c.srv.store.DeleteKind(dest, kindSetMeta)
		return nil
	}
	hdr := setHeaderEncodeP(nil, uint64(count), encHashtable, p)
	_, err := c.srv.store.PutKind(dest, hdr, kindSetMeta)
	return err
}

// cmdSInterStore stores the intersection of the sources into the destination and replies
// with its cardinality.
func (c *connState) cmdSInterStore(argv [][]byte) {
	c.storeAlgebra(argv, "sinterstore", c.sinterEach, c.setMergeIntersect)
}

// cmdSUnionStore stores the union of the sources into the destination and replies with its
// cardinality.
func (c *connState) cmdSUnionStore(argv [][]byte) {
	// The store path dedups through the destination index, so it walks the sources raw (sunionEachRaw)
	// rather than the read form's seen-set (sunionEach): the map was re-deduplicating what insert already
	// deduplicates, an O(union) allocation that made SUNIONSTORE the worst-scaling algebra store.
	c.storeAlgebra(argv, "sunionstore", c.sunionEachRaw, c.setMergeUnion)
}

// cmdSDiffStore stores the difference of the first source minus the rest into the
// destination and replies with its cardinality.
func (c *connState) cmdSDiffStore(argv [][]byte) {
	c.storeAlgebra(argv, "sdiffstore", c.sdiffEach, c.setMergeDiff)
}
