package f1srv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math/bits"
	"math/rand/v2"

	"github.com/tamnd/aki/engine/f1raw"
)

// Set is the second collection type on f1raw, and like the hash it is element-per-row:
// every member is its own record under a composite key, and a per-set header row carries
// the maintained cardinality so SCARD is O(1) without a scan. The set is the hash with
// the value stripped out (spec 2064/f1_rewrite_ltm/06 section 1.1): a member row is
// member-plus-nothing, so its value field is zero bytes and membership is a single
// index probe, the same shape as HEXISTS.
//
// Namespaces stay disjoint by the record kind byte, exactly as the hash does. A member
// row is kindSetMember under the composite key, a header row is kindSetMeta under the
// bare set key. The set meta kind is distinct from the hash meta kind so SCARD on a hash
// key and HLEN on a set key never cross-read one another's header count.
//
// Member sub-key layout: uvarint(len(setKey)) | setKey | member, the same length-prefixed
// composite the hash uses. The length prefix makes (setKey, member) injective, so two
// different sets can never share a member row and a prefix scan bounded by
// uvarint(len(setKey))|setKey enumerates precisely one set in member-byte order. That
// member order is what makes the set algebra a k-way merge (section 5), and it is a
// superset of the API's no-order promise: SMEMBERS returns a valid unspecified order that
// happens to be sorted.
//
// Write serialization: SADD/SREM take the per-key stripe lock (shared with the INCR
// family and the hash) so a set's member rows and its header count stay consistent under
// concurrent writers. Reads (SISMEMBER/SMISMEMBER/SCARD) are lock-free.
const (
	kindSetMember byte = 0x02 // a single set member row, empty value
	kindSetMeta   byte = 0x09 // the per-set header row (coll_header)
)

// memberKey builds the composite element key for (setKey, member) into the reused
// scratch buffer, so a set command allocates nothing for its key.
func (c *connState) memberKey(skey, member []byte) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	b = append(b, member...)
	c.kbuf = b
	return b
}

// setPrefix builds the bounding prefix uvarint(len(skey)) | skey for a set's member rows
// into the reusable pbuf, distinct from the memberKey scratch kbuf so the prefix stays
// stable across an enumeration. Every member row's composite key starts with this exact
// prefix and no other set's does, so a scan bounded by it enumerates precisely one set.
func (c *connState) setPrefix(skey []byte) []byte {
	b := c.pbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	c.pbuf = b
	return b
}

// setHeader reads a set's header row: the maintained cardinality and the encoding tag folded
// forward by its writers (object.go). ok is false when the set has no header row (no members),
// in which case the count is 0 and the encoding is encNone. The encoding byte is the 9th header
// byte; a header written before the encoding tag existed is read as encNone.
func (c *connState) setHeader(skey []byte) (count uint64, enc byte, ok bool) {
	var cb [9]byte
	v, got := c.srv.store.GetKind(skey, cb[:0], kindSetMeta)
	if !got || len(v) < 8 {
		return 0, encNone, false
	}
	enc = encNone
	if len(v) >= 9 {
		enc = v[8]
	}
	return binary.LittleEndian.Uint64(v), enc, true
}

// setCard reads a set's maintained cardinality from its header row lock-free, returning 0
// when the set has no members (no header row). It loads the count word with one atomic read
// (CountInt64) rather than decoding the whole header value, so SCARD never takes the stripe
// lock and never tears against a concurrent in-place count decrement.
func (c *connState) setCard(skey []byte) uint64 {
	n, ok := c.srv.store.CountInt64(skey, kindSetMeta)
	if !ok || n < 0 {
		return 0
	}
	return uint64(n)
}

// setPutHeader writes a set's cardinality and encoding tag to its header row, or deletes the
// header when the count reaches zero so the set key stops existing (empty set is no set). It is
// the write used by the paths that grow a set (SADD, SMOVE into a destination, the STORE forms)
// and know the fresh encoding.
func (c *connState) setPutHeader(skey []byte, count uint64, enc byte) error {
	if count == 0 {
		c.srv.store.DeleteKind(skey, kindSetMeta)
		return nil
	}
	var ob [9]byte
	binary.LittleEndian.PutUint64(ob[:8], count)
	ob[8] = enc
	_, err := c.srv.store.PutKind(skey, ob[:], kindSetMeta)
	return err
}

// setSetCard writes a set's cardinality while preserving its recorded encoding, or deletes the
// header when the count reaches zero. It is the write used by the paths that only shrink a set
// (SREM, SPOP, SMOVE out of a source): a removal never changes the encoding (Redis never
// downgrades), so it keeps the tag the set already carries.
func (c *connState) setSetCard(skey []byte, count uint64) error {
	if count == 0 {
		c.srv.store.DeleteKind(skey, kindSetMeta)
		return nil
	}
	_, enc, _ := c.setHeader(skey)
	return c.setPutHeader(skey, count, enc)
}

// setHeaderEncodeP builds a set header value carrying the cardinality, the encoding tag, and the
// partition count P (spec 2064/f1_rewrite_ltm/19 section 5). For P==1, the unpartitioned set, it
// writes the existing 9-byte header (8 LittleEndian count + 1 encoding byte) byte-for-byte, so a
// set that never engages partitioning keeps exactly the header a stock reader already understands
// and a header written before this field existed reads back as P=1. For P>1 it appends the one
// partition byte after the encoding, holding P as its base-2 exponent (P=2 stores 1, P=256 stores
// 8) so the whole range fits a single byte and only ever decodes to a power of two. That records
// how many partitions a recovering reader must expect so it derives each partition's vector from
// the right prefix range. Slice 2 defines and tests this codec; the write paths keep calling
// setPutHeader (P=1) until the adaptive engage in slice 6 grows a hot set to P>1 and starts
// writing the partition byte. P must be a power of two in [1, 256].
func setHeaderEncodeP(dst []byte, count uint64, enc byte, p int) []byte {
	var hb [8]byte
	binary.LittleEndian.PutUint64(hb[:], count)
	dst = append(dst, hb[:]...)
	dst = append(dst, enc)
	if p > 1 {
		dst = append(dst, byte(bits.TrailingZeros(uint(p))))
	}
	return dst
}

// setHeaderDecodeP reads a set header value back into its cardinality, encoding tag, and partition
// count. A value shorter than 8 bytes is not a header and reports ok=false. The encoding is the 9th
// byte, encNone when a pre-encoding header omitted it. The partition count is the 10th byte when
// present, read as the base-2 exponent setHeaderEncodeP wrote, and defaults to 1 (unpartitioned)
// for every header without it, which is every header written before partitioning and every header
// of a set that never engaged it. An exponent above 8 would decode to more than 256 partitions, so
// it is rejected back to P=1 rather than trusted, keeping a corrupt or foreign tenth byte from
// mis-routing a scan.
func setHeaderDecodeP(v []byte) (count uint64, enc byte, p int, ok bool) {
	if len(v) < 8 {
		return 0, encNone, 1, false
	}
	count = binary.LittleEndian.Uint64(v)
	enc = encNone
	if len(v) >= 9 {
		enc = v[8]
	}
	p = 1
	if len(v) >= 10 {
		if exp := v[9]; exp >= 1 && exp <= 8 {
			p = 1 << exp
		}
	}
	return count, enc, p, true
}

// partitionP returns the engaged partition count for skey from the per-key registry, or 1 when the
// set is unpartitioned (spec 2064/f1_rewrite_ltm/19 slice 6). It is the lock-free read partitionsFor
// takes on every set command: one atomic load of the published registry pointer, a nil check that
// returns 1 for the common empty-registry case (no set has engaged, which is every keyspace until
// the adaptive transition grows a hot set), and otherwise a single map lookup that does not allocate
// because Go elides the []byte-to-string conversion for a map index. A set appears here only once it
// has engaged partitioning, so the overwhelming majority of keys miss and take the P=1 path.
func (s *Server) partitionP(skey []byte) int {
	m := s.setPartP.Load()
	if m == nil {
		return 1
	}
	if p, ok := (*m)[string(skey)]; ok && p > 1 {
		return p
	}
	return 1
}

// engageP records that skey is now partitioned into p partitions, installed by copy-on-write so a
// concurrent lock-free partitionP walks either the old or the new map, never a half-updated one. The
// caller holds the set's whole-key exclusive lock across the migration this publishes, so two
// engagements of one key cannot race; setPartMu serializes the map swap against engagements and
// drops of other keys. p must be greater than 1: P=1 is the absence of an entry, not an entry of 1,
// so a set never engaged records nothing and reads back as unpartitioned.
func (s *Server) engageP(skey []byte, p int) {
	s.setPartMu.Lock()
	old := s.setPartP.Load()
	n := 1
	if old != nil {
		n += len(*old)
	}
	nm := make(map[string]int, n)
	if old != nil {
		for k, v := range *old {
			nm[k] = v
		}
	}
	nm[string(skey)] = p
	s.setPartP.Store(&nm)
	s.setPartMu.Unlock()
}

// unengageP removes skey from the partition registry, the reset a DEL/UNLINK/expiry drop or a RENAME
// source drop performs so a key recreated under the same name starts fresh at P=1 (section 3.1). It
// checks the published map lock-free first and returns at once when the key is absent, which is the
// common case (an unpartitioned key was never registered), so a drop of an ordinary set never takes
// setPartMu. Only a drop of a genuinely engaged set swaps the map by copy-on-write under the mutex.
func (s *Server) unengageP(skey []byte) {
	if m := s.setPartP.Load(); m == nil {
		return
	} else if _, ok := (*m)[string(skey)]; !ok {
		return
	}
	s.setPartMu.Lock()
	old := s.setPartP.Load()
	if old == nil {
		s.setPartMu.Unlock()
		return
	}
	if _, ok := (*old)[string(skey)]; !ok {
		s.setPartMu.Unlock()
		return
	}
	nm := make(map[string]int, len(*old))
	for k, v := range *old {
		if k == string(skey) {
			continue
		}
		nm[k] = v
	}
	s.setPartP.Store(&nm)
	s.setPartMu.Unlock()
}

// partitionsFor reports the partition count P every set command routes key skey through
// (spec 2064/f1_rewrite_ltm/19 slices 3 and 6). It reads the server's forceP hook with one atomic
// load first: forceP is 0 in production so the common path falls through to the per-key registry,
// and the slice-3 correctness tests and the contention microbenchmark set forceP to drive every set
// through one P regardless of the registry. With forceP unset, the per-key registry answers: a set
// that has engaged the adaptive transition (slice 6) carries its P there and routes through it, and
// every other set misses the registry and takes the unpartitioned P=1 body byte-for-byte.
func (c *connState) partitionsFor(skey []byte) int {
	if p := int(c.srv.forceP.Load()); p > 1 {
		return p
	}
	return c.srv.partitionP(skey)
}

// partScanBase builds the partition-scan prefix uvarint(len(skey))|skey|<byte> into the reusable
// ppbuf, distinct from the memberKey scratch (kbuf) and the enumeration prefix (pbuf) so it stays
// stable while a member key is built alongside it in the routed write loop. The final byte is a
// placeholder the caller rewrites to a partition id (the routed writes) or the store's weighted
// draw rewrites per partition (the routed reads); because its length equals a partition prefix,
// the member of a draw key returned against it sits at k[len(base):]. It is built only on the P>1
// path, so the partition byte is always present (an unpartitioned set never calls this).
func (c *connState) partScanBase(skey []byte) []byte {
	b := c.ppbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	b = append(b, 0)
	c.ppbuf = b
	return b
}

// partMemberKey builds the partition-routed composite element key for (skey, member) into the
// reused scratch buffer, mirroring memberKey but inserting the one partition byte between the
// length-prefixed set key and the member when p>1. For p==1 it is byte-identical to memberKey, so
// the routed path and the unpartitioned path address the same row for an unpartitioned set. The
// bytes match f1raw.appendPartMemberKey exactly, so a key this builds under partition part is the
// key a later derivePartVec scan of part's prefix range rebuilds and a routed probe resolves. The
// caller supplies part (already computed once via f1raw.PartitionOf) so a write and its later
// lookup never recompute a possibly-different partition.
func (c *connState) partMemberKey(skey, member []byte, part, p int) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	if p > 1 {
		b = append(b, byte(part))
	}
	b = append(b, member...)
	c.kbuf = b
	return b
}

// setBumpCard adjusts a set's maintained cardinality by delta under the partitioned write paths.
// The count stays a single whole-key header word (slice 3 keeps one cardinality per set; the
// per-partition counts of partSet arrive with slice 4's draw), so a routed write bumps it with one
// in-place atomic (CountAddInt64) that never takes a whole-key lock in the common case: the header
// already exists once a set has any member. Only the first add to an empty set finds no header, and
// only then does it take the whole-key stripe lock to create one, recording P via setHeaderEncodeP
// with the hashtable encoding a partitioned set always reports (section 6.11). A drain to zero
// deletes the header so an empty set stops existing. The whole-key stripe is distinct from every
// partition stripe (stripePart always folds one extra byte), so this create never contends with a
// concurrent routed member write on a partition lock.
func (c *connState) setBumpCard(skey []byte, delta, p int) {
	if n, ok := c.srv.store.CountAddInt64(skey, kindSetMeta, int64(delta)); ok {
		if n <= 0 {
			c.srv.store.DeleteKind(skey, kindSetMeta)
		}
		return
	}
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if n, ok := c.srv.store.CountAddInt64(skey, kindSetMeta, int64(delta)); ok {
		if n <= 0 {
			c.srv.store.DeleteKind(skey, kindSetMeta)
		}
	} else if delta > 0 {
		hdr := setHeaderEncodeP(nil, uint64(delta), encHashtable, p)
		_, _ = c.srv.store.PutKind(skey, hdr, kindSetMeta)
	}
	mu.Unlock()
}

// cmdSAddPart is the P>1 routing of SADD (spec 2064/f1_rewrite_ltm/19 slice 3): each member routes
// to partition f1raw.PartitionOf(member, p) and the add takes only that partition's stripe lock,
// held across just this one member, so two members in different partitions of one hot key add on
// two cores at once instead of serializing on the set's single stripe. The members process
// sequentially with no two partition locks held at once (section 7 lock ordering), and the shared
// cardinality is bumped once after the loop off any partition lock. When a partition's draw vector
// has already been built (a draw has run against that partition), the new member is spliced into it
// under the partition lock via CollRandInsert so a later draw sees it; a partition never yet drawn
// from has no vector and stays lazy, deriving on its first draw. The base prefix is built once and
// its final byte rewritten per member, so the whole add allocates no per-member prefix.
func (c *connState) cmdSAddPart(skey []byte, members [][]byte, p int) {
	added := 0
	base := c.partScanBase(skey)
	last := len(base) - 1
	for _, member := range members {
		part := f1raw.PartitionOf(member, p)
		mu := &c.srv.incrMu[c.srv.stripePart(skey, part)]
		mu.Lock()
		mk := c.partMemberKey(skey, member, part, p)
		isNew, err := c.srv.store.PutKind(mk, nil, kindSetMember)
		if err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		if isNew {
			c.srv.store.CollInsert(mk, kindSetMember)
			base[last] = byte(part)
			c.srv.store.CollRandInsert(base, mk, kindSetMember)
			added++
		}
		mu.Unlock()
	}
	if added > 0 {
		c.setBumpCard(skey, added, p)
	}
	c.writeInt(int64(added))
}

// cmdSRemPart is the P>1 routing of SREM: each member routes to its partition and the remove takes
// only that partition's lock, held across just this member. It mirrors cmdSRem's deferred index
// splice, batching removed keys packed end to end, but the splice is issued per partition so the
// batch never mixes partitions. When the partition's draw vector is built, the member is swap-
// removed from it via CollRandRemove BEFORE DeleteKind clears the hash record, so the vector slot's
// arena offset is still resolvable; a partition with no vector is a no-op. The shared cardinality is
// decremented once after the loop.
func (c *connState) cmdSRemPart(skey []byte, members [][]byte, p int) {
	removed := 0
	base := c.partScanBase(skey)
	last := len(base) - 1
	for _, member := range members {
		part := f1raw.PartitionOf(member, p)
		mu := &c.srv.incrMu[c.srv.stripePart(skey, part)]
		mu.Lock()
		mk := c.partMemberKey(skey, member, part, p)
		base[last] = byte(part)
		c.srv.store.CollRandRemove(base, mk, kindSetMember)
		if c.srv.store.DeleteKind(mk, kindSetMember) {
			c.srv.store.CollRemove(mk)
			removed++
		}
		mu.Unlock()
	}
	if removed > 0 {
		c.setBumpCard(skey, -removed, p)
	}
	c.writeInt(int64(removed))
}

// cmdSIsMemberPart is the P>1 routing of SISMEMBER: the member routes to its partition and the
// probe is a single lock-free index lookup on the partition-routed key, exactly as the
// unpartitioned probe is, since a read never contends with a partition write.
func (c *connState) cmdSIsMemberPart(skey, member []byte, p int) {
	part := f1raw.PartitionOf(member, p)
	mk := c.partMemberKey(skey, member, part, p)
	if c.srv.store.ExistsKind(mk, kindSetMember) {
		c.writeInt(1)
		return
	}
	c.writeInt(0)
}

// cmdSMIsMemberPart is the P>1 routing of SMISMEMBER: each member routes to its partition and gets
// one lock-free probe, framed by the same array header the unpartitioned path writes.
func (c *connState) cmdSMIsMemberPart(skey []byte, members [][]byte, p int) {
	c.writeArrayHeader(len(members))
	for _, member := range members {
		part := f1raw.PartitionOf(member, p)
		mk := c.partMemberKey(skey, member, part, p)
		if c.srv.store.ExistsKind(mk, kindSetMember) {
			c.writeInt(1)
			continue
		}
		c.writeInt(0)
	}
}

func (c *connState) cmdSAdd(argv [][]byte) {
	// SADD key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'sadd' command")
		return
	}
	skey := argv[1]
	if p := c.partitionsFor(skey); p > 1 {
		if c.stringConflict(skey) {
			c.writeErr(wrongType)
			return
		}
		c.cmdSAddPart(skey, argv[2:], p)
		return
	}
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	// Track the running cardinality and encoding so the header is written once with the
	// count bumped and the encoding folded forward over every member actually added.
	count, enc, _ := c.setHeader(skey)
	// The member vector's bounding prefix is stable across the loop: setPrefix uses pbuf while
	// memberKey uses the distinct kbuf, so building each member key does not disturb it.
	prefix := c.setPrefix(skey)
	added := 0
	for _, member := range argv[2:] {
		mk := c.memberKey(skey, member)
		isNew, err := c.srv.store.PutKind(mk, nil, kindSetMember)
		if err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		if isNew {
			c.srv.store.CollInsert(mk, kindSetMember)
			// Keep the dense member vector in step with the ordered index: append the new member's
			// offset if a vector exists, a no-op otherwise (the lazy contract, spec 2064/18 section
			// 5.1). Only on a genuine insert, so a re-add of an existing member appends nothing.
			c.srv.store.CollRandInsert(prefix, mk, kindSetMember)
			added++
			count++
			enc = foldSetEnc(enc, member, count)
		}
	}
	if added > 0 {
		if err := c.setPutHeader(skey, count, enc); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(int64(added))
}

func (c *connState) cmdSRem(argv [][]byte) {
	// SREM key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'srem' command")
		return
	}
	skey := argv[1]
	if p := c.partitionsFor(skey); p > 1 {
		if c.stringConflict(skey) {
			c.writeErr(wrongType)
			return
		}
		c.cmdSRemPart(skey, argv[2:], p)
		return
	}
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	removed := 0
	// Accumulate the removed member composite keys packed end to end so the ordered-index
	// splice is deferred off this stripe-locked reply path in one batch (spec 2064/16 slice 2)
	// instead of one synchronous O(log n) splice per member here. memberKey reuses its scratch
	// on the next call, so each removed key is copied into the packed buffer before the next.
	prefix := c.setPrefix(skey)
	buf := c.delKeyBuf[:0]
	ends := c.delKeyEnd[:0]
	for _, member := range argv[2:] {
		mk := c.memberKey(skey, member)
		// Swap-remove the member from the dense vector before the hash record is deleted, so its
		// offset is still resolvable (spec 2064/18 section 5.2). A no-op when no vector exists or
		// the member was not a vector slot, so a miss costs one shard-mutex acquire and nothing more.
		c.srv.store.CollRandRemove(prefix, mk, kindSetMember)
		if c.srv.store.DeleteKind(mk, kindSetMember) {
			buf = append(buf, mk...)
			ends = append(ends, len(buf))
			removed++
		}
	}
	c.delKeyBuf = buf
	c.delKeyEnd = ends
	c.srv.store.CollRemovePacked(buf, ends, kindSetMember)
	if removed > 0 {
		// Decrement the maintained cardinality with one in-place atomic instead of a
		// GetKind + PutKind read-modify-write of the whole header value. The stripe lock
		// still serializes this set's writers, so the decrement cannot interleave with a
		// concurrent SADD's header write, and it stays consistent with a lock-free SCARD
		// that reads the same word atomically. When the count reaches zero the set is
		// empty, so the header row is deleted under the same lock (empty set is no set),
		// the retire-to-zero boundary the design keeps serialized.
		n, ok := c.srv.store.CountAddInt64(skey, kindSetMeta, -int64(removed))
		if !ok || n <= 0 {
			c.srv.store.DeleteKind(skey, kindSetMeta)
		}
	}
	mu.Unlock()
	c.writeInt(int64(removed))
}

func (c *connState) cmdSIsMember(argv [][]byte) {
	// SISMEMBER key member
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'sismember' command")
		return
	}
	if p := c.partitionsFor(argv[1]); p > 1 {
		c.cmdSIsMemberPart(argv[1], argv[2], p)
		return
	}
	mk := c.memberKey(argv[1], argv[2])
	if c.srv.store.ExistsKind(mk, kindSetMember) {
		c.writeInt(1)
		return
	}
	c.writeInt(0)
}

func (c *connState) cmdSMIsMember(argv [][]byte) {
	// SMISMEMBER key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'smismember' command")
		return
	}
	if p := c.partitionsFor(argv[1]); p > 1 {
		c.cmdSMIsMemberPart(argv[1], argv[2:], p)
		return
	}
	c.writeArrayHeader(len(argv) - 2)
	for _, member := range argv[2:] {
		mk := c.memberKey(argv[1], member)
		if c.srv.store.ExistsKind(mk, kindSetMember) {
			c.writeInt(1)
			continue
		}
		c.writeInt(0)
	}
}

func (c *connState) cmdSCard(argv [][]byte) {
	// SCARD key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'scard' command")
		return
	}
	c.writeInt(int64(c.setCard(argv[1])))
}

// streamSet is the enumeration body for SMEMBERS. It takes the set's stripe lock so the
// header count it frames the RESP array with cannot drift against the member rows it then
// streams, rejects a string of the same key as WRONGTYPE, and walks the ordered element
// index in bounded batches, emitting each member (the composite key past the prefix) in
// member-byte order. The header count and the live member-row count stay exactly equal
// because every SADD pairs CollInsert with a count bump and every SREM pairs CollRemove
// with a decrement, so the framed length always matches what is streamed.
func (c *connState) streamSet(skey []byte) {
	if p := c.partitionsFor(skey); p > 1 {
		c.streamSetPart(skey, p)
		return
	}
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	// A whole-set read only excludes concurrent SADD/SREM writers, not other readers, so it
	// takes the shared lock and lets many SMEMBERS of one hot set run on many cores at once, a
	// win a single-threaded server cannot match. A set has no per-member TTL, so there is
	// nothing to reap, and the read never mutates under the lock; the shared path is always safe.
	mu.RLock()
	if c.stringConflict(skey) {
		mu.RUnlock()
		c.writeErr(wrongType)
		return
	}
	count := c.setCard(skey)
	c.writeArrayHeader(int(count))

	prefix := c.setPrefix(skey)
	plen := len(prefix)
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			c.writeBulk(k[plen:])
		}
		if last == nil {
			break
		}
		after = last
	}
	mu.RUnlock()
}

// streamSetPart is the SMEMBERS body for a partitioned set (spec 2064/f1_rewrite_ltm/19 section
// 6.7). One large logical set is stored as P partitions, and because the partition byte sits
// between the length-prefixed set key and the member (appendPartMemberKey), the whole-set prefix
// bounds every partition's rows in one contiguous ordered run in (partition, member) order. So a
// single walk of that prefix sweeps all P partitions with no per-partition scan, the same total
// work as the unpartitioned walk, just crossing partition boundaries transparently.
//
// A whole-set read must exclude the partitioned member writers so a row cannot be removed and its
// arena offset reused while this streams it, exactly the protection the unpartitioned path gets
// from the whole-key stripe RLock. The partitioned writers take per-partition stripe write locks
// (cmdSAddPart/cmdSRemPart), so this read-locks all P partition stripes, deduplicated because two
// partitions can hash to one stripe and recursive read-locking one RWMutex can deadlock a waiting
// writer. Under those held read locks the member rows are frozen.
//
// The array is framed from a counting pass, not the header count: a partitioned write bumps the
// shared cardinality with a lock-free CountAddInt64 only after it releases its partition lock
// (setBumpCard), so the header count can momentarily lag the actual rows even with the writers
// excluded. Framing from a first pass that counts the frozen rows and streaming them in a second
// pass guarantees the framed length equals the number of members emitted. The member starts one
// byte past the whole-set prefix because the partition byte precedes it, so it strips plen+1.
func (c *connState) streamSetPart(skey []byte, p int) {
	if c.stringConflict(skey) {
		c.writeErr(wrongType)
		return
	}
	stripes := c.lockSetPartitionsShared(skey, p)
	defer c.unlockSetPartitionsShared(stripes)

	prefix := c.setPrefix(skey)
	moff := len(prefix) + 1
	scan := make([][]byte, 0, hashScanBatch)

	// Pass 1: count the frozen member rows so the frame matches exactly what pass 2 streams.
	var n int
	var after []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		n += len(keys)
		if last == nil {
			break
		}
		after = last
	}
	c.writeArrayHeader(n)

	// Pass 2: stream the same rows, stripping the partition byte to recover each member.
	after = nil
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			c.writeBulk(k[moff:])
		}
		if last == nil {
			break
		}
		after = last
	}
}

// lockSetPartitionsShared read-locks every distinct stripe the P partitions of skey route to and
// returns them so the caller unlocks in reverse (spec 2064/f1_rewrite_ltm/19 section 7). A whole-set
// read of a partitioned set takes the shared side so many readers of one hot set still run at once,
// while excluding the partitioned member writers that hold the partition stripe write lock. The
// stripe set is deduplicated because stripePart can map two partitions to one stripe and recursively
// read-locking a single RWMutex can deadlock against a waiting writer. The stripes are taken in
// ascending index order, the same global order lockStripes uses for its exclusive algebra locks, so
// a shared SMEMBERS read and an exclusive SINTER/SINTERSTORE over overlapping partition stripes of
// one set acquire in one order and can never form a lock-order cycle. A partitioned member writer
// holds just one stripe write lock at a time with nothing else held while it waits, so it never
// closes a cycle either.
func (c *connState) lockSetPartitionsShared(skey []byte, p int) []uint32 {
	stripes := make([]uint32, 0, p)
	for part := 0; part < p; part++ {
		s := c.srv.stripePart(skey, part)
		dup := false
		for _, e := range stripes {
			if e == s {
				dup = true
				break
			}
		}
		if !dup {
			stripes = append(stripes, s)
		}
	}
	for i := 1; i < len(stripes); i++ {
		for j := i; j > 0 && stripes[j] < stripes[j-1]; j-- {
			stripes[j], stripes[j-1] = stripes[j-1], stripes[j]
		}
	}
	for _, s := range stripes {
		c.srv.incrMu[s].RLock()
	}
	return stripes
}

// unlockSetPartitionsShared releases the shared partition-stripe locks lockSetPartitionsShared took,
// in reverse acquisition order.
func (c *connState) unlockSetPartitionsShared(stripes []uint32) {
	for i := len(stripes) - 1; i >= 0; i-- {
		c.srv.incrMu[stripes[i]].RUnlock()
	}
}

func (c *connState) cmdSMembers(argv [][]byte) {
	// SMEMBERS key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'smembers' command")
		return
	}
	c.streamSet(argv[1])
}

// cmdSScan is the LTM-safe incremental set enumeration (spec 2064/f1_rewrite_ltm/06
// section 8): each call scans a bounded window of member rows and returns an opaque
// cursor to resume, so a client walks a billion-member set without the server ever
// materializing it. The set has no per-member value, so SSCAN returns a flat member array
// with no NOVALUES option, unlike HSCAN.
//
// Cursor encoding mirrors HSCAN: "0" starts a fresh iteration and "0" is returned when it
// completes, and any live position is the hex of the last composite key returned. A
// composite key always carries the uvarint length prefix, so it is never empty and its
// hex is never the single byte "0", which keeps a live cursor from ever colliding with the
// done sentinel.
//
// Cursor stability: a member present for the whole scan and never removed is returned
// exactly once (the ordered index walks each key once and the cursor resumes strictly
// after the last one), and a member added or removed mid-scan may or may not appear. The
// scan is lock-free like the other set reads.
func (c *connState) cmdSScan(argv [][]byte) {
	// SSCAN key cursor [MATCH pattern] [COUNT count]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'sscan' command")
		return
	}
	skey := argv[1]

	var after []byte
	if len(argv[2]) != 1 || argv[2][0] != '0' {
		dec, err := hex.DecodeString(string(argv[2]))
		if err != nil {
			c.writeErr("ERR invalid cursor")
			return
		}
		after = dec
	}

	count := 10
	var pattern []byte
	for i := 3; i < len(argv); i++ {
		switch {
		case eqFold(argv[i], "MATCH"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			pattern = argv[i+1]
			i++
		case eqFold(argv[i], "COUNT"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, err := atoi64(argv[i+1])
			if err != nil || n <= 0 {
				c.writeErr("ERR syntax error")
				return
			}
			count = int(n)
			i++
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	if c.stringConflict(skey) {
		c.writeErr(wrongType)
		return
	}

	prefix := c.setPrefix(skey)
	initCap := count
	if initCap > hashScanBatch {
		initCap = hashScanBatch
	}
	scan := make([][]byte, 0, initCap)
	keys, last := c.srv.store.CollScan(prefix, after, count, scan)

	// The whole-set prefix bounds every partition's rows in (partition, member) order, so one
	// bounded window naturally crosses partition boundaries and the opaque cursor (the hex of the
	// last composite key, which carries the partition byte) resumes into the next partition with no
	// special cursor layout. For a partitioned set the member sits one byte past the prefix because
	// the partition byte precedes it, so both the MATCH filter and the reply strip plen+1 (spec
	// 2064/f1_rewrite_ltm/19 section 6.8).
	moff := len(prefix)
	if c.partitionsFor(skey) > 1 {
		moff++
	}
	matched := keys[:0]
	for _, k := range keys {
		if pattern != nil && !globMatch(pattern, k[moff:]) {
			continue
		}
		matched = append(matched, k)
	}

	var cursor []byte
	if len(keys) < count || last == nil {
		cursor = []byte{'0'}
	} else {
		cursor = []byte(hex.EncodeToString(last))
	}

	c.writeArrayHeader(2)
	c.writeBulk(cursor)
	c.writeArrayHeader(len(matched))
	for _, k := range matched {
		c.writeBulk(k[moff:])
	}
}

// setWalkAll appends every member of a set, in member order, to dst as arena-stable
// subslices (the composite key past the prefix). It is the whole-set sequential walk the
// large-count random-selection path falls back to (spec 2064/f1_rewrite_ltm/06 section
// 10.1): when the requested count is a big fraction of the cardinality, walking the set
// once and dropping the surplus is cheaper and steadier than random-seek-and-dedup, whose
// collision retries blow up as the count approaches the cardinality.
func (c *connState) setWalkAll(prefix []byte, dst [][]byte) [][]byte {
	plen := len(prefix)
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			dst = append(dst, k[plen:])
		}
		if last == nil {
			break
		}
		after = last
	}
	return dst
}

// setSampleDistinct returns count distinct members of a set of cardinality card (count is
// assumed already clamped to at most card), as arena-stable member subslices. It is the
// uniform-without-replacement sampler SPOP and positive-count SRANDMEMBER share.
//
// Below half the cardinality it draws a uniform random index into the order-statistic
// ordered index and selects that member (spec section 10.1), deduping on the index so
// each member appears at most once; the O(log n) selection means a random member is a
// descent, never an O(n) count, and the true-uniform draw avoids the byte-space clumping
// a raw random seek would suffer. At or above half the cardinality it crosses over to a
// single sequential walk and a partial shuffle, which is O(card) but avoids the retry
// storm the dedup path hits as count nears card. The caller serializes the set's writers
// so card and the index agree for the span of the sample.
func (c *connState) setSampleDistinct(prefix []byte, card, count int) [][]byte {
	if count >= card {
		return c.setWalkAll(prefix, make([][]byte, 0, card))
	}
	if count*2 >= card {
		all := c.setWalkAll(prefix, make([][]byte, 0, card))
		// Partial Fisher-Yates: shuffle only the count positions we return.
		for i := 0; i < count; i++ {
			j := i + rand.IntN(len(all)-i)
			all[i], all[j] = all[j], all[i]
		}
		return all[:count]
	}
	seen := make(map[int]struct{}, count)
	out := make([][]byte, 0, count)
	plen := len(prefix)
	for len(out) < count {
		idx := rand.IntN(card)
		if _, dup := seen[idx]; dup {
			continue
		}
		seen[idx] = struct{}{}
		k, ok := c.srv.store.CollSelectAt(prefix, idx)
		if !ok {
			continue
		}
		out = append(out, k[plen:])
	}
	return out
}

// cmdSRandMember is the non-destructive random member read (spec section 8.8). The
// no-count form returns one uniform random member (nil on a missing key); the count form
// follows Redis's sign convention exactly, a known compatibility trap: a positive count
// returns up to that many distinct members (capped at the cardinality, no duplicates),
// while a negative count returns exactly abs(count) members with replacement, so
// duplicates are possible and the result is never capped by the cardinality.
func (c *connState) cmdSRandMember(argv [][]byte) {
	// SRANDMEMBER key [count]
	if len(argv) < 2 || len(argv) > 3 {
		c.writeErr("ERR wrong number of arguments for 'srandmember' command")
		return
	}
	skey := argv[1]

	if p := c.partitionsFor(skey); p > 1 {
		c.cmdSRandMemberPart(argv, p)
		return
	}

	if len(argv) == 2 {
		// No-count form: one member, or nil for a missing (or wrong-type) key.
		if c.stringConflict(skey) {
			c.writeErr(wrongType)
			return
		}
		// Draw from the set's dense member vector (spec 2064/18): an O(1) array index instead of
		// the O(log n) order-statistic skip-list descent CollSelectAt walked. The vector builds
		// itself on this first draw by scanning the live ordered run, so it needs no
		// SyncPendingRemovals reconcile (the scan already skips a tombstoned-but-not-yet-spliced
		// node) and no separate cardinality probe (an empty or missing set draws ok=false).
		prefix := c.setPrefix(skey)
		k, ok := c.srv.store.CollRandSelect(prefix, c.nextRand())
		if !ok {
			c.writeNil()
			return
		}
		c.writeBulk(k[len(prefix):])
		return
	}

	count, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	// The stripe lock keeps the cardinality and the ordered index consistent across a
	// multi-pick sample, the same serialization the set's writers take.
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	// Reconcile any deferred SREM splices before rank-based sampling: the stripe lock keeps this
	// key's cardinality and ordered index consistent across the multi-pick sample, and draining
	// under it means no new tombstone for this key can appear mid-sample (spec 2064/16 slice 2).
	c.srv.store.SyncPendingRemovals()
	card := int(c.setCard(skey))
	if count == 0 || card == 0 {
		mu.Unlock()
		c.writeArrayHeader(0)
		return
	}
	prefix := c.setPrefix(skey)
	if count < 0 {
		// With replacement: exactly abs(count) members, duplicates allowed.
		n := int(-count)
		c.writeArrayHeader(n)
		for i := 0; i < n; i++ {
			k, ok := c.srv.store.CollSelectAt(prefix, rand.IntN(card))
			if !ok {
				c.writeNil()
				continue
			}
			c.writeBulk(k[len(prefix):])
		}
		mu.Unlock()
		return
	}
	want := int(count)
	if want > card {
		want = card
	}
	members := c.setSampleDistinct(prefix, card, want)
	c.writeArrayHeader(len(members))
	for _, m := range members {
		c.writeBulk(m)
	}
	mu.Unlock()
}

// cmdSPop is the destructive random member draw (spec section 8.7): it selects like
// SRANDMEMBER's positive form (uniform, distinct) but removes what it draws and returns
// it. The no-count form returns one member as a bulk string (nil on a missing key); the
// count form returns an array and, unlike SRANDMEMBER, rejects a negative count. Removing
// the last member deletes the set (empty set is no set), and popping a count at or past
// the cardinality returns the whole set and deletes it.
func (c *connState) cmdSPop(argv [][]byte) {
	// SPOP key [count]
	if len(argv) < 2 || len(argv) > 3 {
		c.writeErr("ERR wrong number of arguments for 'spop' command")
		return
	}
	skey := argv[1]

	var count int64 = 1
	hasCount := len(argv) == 3
	if hasCount {
		n, err := atoi64(argv[2])
		if err != nil {
			c.writeErr("ERR value is not an integer or out of range")
			return
		}
		if n < 0 {
			c.writeErr("ERR value is out of range, must be positive")
			return
		}
		count = n
	}

	if p := c.partitionsFor(skey); p > 1 {
		c.cmdSPopPart(skey, count, hasCount, p)
		return
	}

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	if !hasCount {
		// No-count form: draw one uniform victim from the dense member vector and swap-remove it
		// in O(1) (spec 2064/18 section 5), instead of the O(log n) rank descent the count form
		// still runs. The vector tracks live membership on its own, so this path needs neither the
		// header read the shrink used nor the SyncPendingRemovals reconcile a rank draw requires.
		prefix := c.setPrefix(skey)
		k, ok := c.srv.store.CollRandSelectRemove(prefix)
		if !ok {
			// Empty or missing set: the vector built from a live scan has no slot to draw.
			mu.Unlock()
			c.writeNil()
			return
		}
		member := k[len(prefix):]
		// The vector already dropped the victim's slot; delete its hash record, then defer the
		// ordered-index splice off this reply path in one batched tombstone, the same handoff SREM
		// makes (spec 2064/16 slice 2). k is a stable arena subslice, so packing it and returning
		// its member tail after the delete is safe.
		c.srv.store.DeleteKind(k, kindSetMember)
		buf := append(c.delKeyBuf[:0], k...)
		c.delKeyBuf = buf
		ends := append(c.delKeyEnd[:0], len(buf))
		c.delKeyEnd = ends
		c.srv.store.CollRemovePacked(buf, ends, kindSetMember)
		// Decrement the maintained cardinality in place; at zero the set is gone and its header row
		// is dropped under the same lock (empty set is no set), exactly as SREM retires to zero.
		if n, ok := c.srv.store.CountAddInt64(skey, kindSetMeta, -1); !ok || n <= 0 {
			c.srv.store.DeleteKind(skey, kindSetMeta)
		}
		mu.Unlock()
		c.writeBulk(member)
		return
	}

	// Count form: reconcile any deferred splices so the rank-based sample sees exact ordered-index
	// widths (a not-yet-spliced dead node would skew the uniform pick, spec 2064/16 slice 2), then
	// read the header once under the lock for both the sample bound and the shrink write.
	c.srv.store.SyncPendingRemovals()
	card64, enc, _ := c.setHeader(skey)
	card := int(card64)
	if count == 0 || card == 0 {
		mu.Unlock()
		c.writeArrayHeader(0)
		return
	}
	want := int(count)
	if want > card {
		want = card
	}
	// Sample all the members to pop first (indices stable, nothing removed yet), then
	// remove them, so a whole-set pop and a partial pop share one path.
	prefix := c.setPrefix(skey)
	members := c.setSampleDistinct(prefix, card, want)
	for _, m := range members {
		mk := c.memberKey(skey, m)
		// Drop the popped member from the dense vector before its record goes (spec 2064/18 5.2).
		c.srv.store.CollRandRemove(prefix, mk, kindSetMember)
		if c.srv.store.DeleteKind(mk, kindSetMember) {
			c.srv.store.CollRemove(mk)
		}
	}
	if err := c.setPutHeader(skey, uint64(card-len(members)), enc); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()
	c.writeArrayHeader(len(members))
	for _, m := range members {
		c.writeBulk(m)
	}
}

// cmdSRandMemberPart is the P>1 routing of SRANDMEMBER (spec 2064/f1_rewrite_ltm/19 sections 6.5
// and 6.6): every form composes the P per-partition draw vectors into one exactly-uniform draw
// through the weighted-partition scheme, non-destructively and lock-free on the common path. The
// no-count form draws one member; the negative-count form draws abs(count) with replacement by
// looping the single draw; the positive-count form draws up to count distinct members without
// replacement. A missing or empty set yields nil for the no-count form and an empty array for the
// count form, matching the unpartitioned path.
func (c *connState) cmdSRandMemberPart(argv [][]byte, p int) {
	skey := argv[1]
	if c.stringConflict(skey) {
		c.writeErr(wrongType)
		return
	}
	base := c.partScanBase(skey)

	if len(argv) == 2 {
		// No-count form: one uniform member across the P partitions, or nil for an empty set.
		k, ok := c.srv.store.CollPartRandOne(base, p, c.nextRand())
		if !ok {
			c.writeNil()
			return
		}
		c.writeBulk(k[len(base):])
		return
	}

	count, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	if count == 0 || c.srv.store.CollPartTotal(base, p) == 0 {
		c.writeArrayHeader(0)
		return
	}
	if count < 0 {
		// With replacement: exactly abs(count) draws, each an independent uniform member, so
		// duplicates are allowed and the result is never capped by the cardinality.
		n := int(-count)
		c.writeArrayHeader(n)
		for i := 0; i < n; i++ {
			k, ok := c.srv.store.CollPartRandOne(base, p, c.nextRand())
			if !ok {
				c.writeNil()
				continue
			}
			c.writeBulk(k[len(base):])
		}
		return
	}
	// Positive count: up to count distinct members, without replacement, capped at the cardinality
	// inside CollPartSampleDistinct.
	members := c.srv.store.CollPartSampleDistinct(base, p, int(count), c.nextRand(), nil)
	c.writeArrayHeader(len(members))
	for _, m := range members {
		c.writeBulk(m[len(base):])
	}
}

// cmdSPopPart is the P>1 routing of SPOP (spec 2064/f1_rewrite_ltm/19 section 6.6): it draws a
// uniform member by the weighted-partition scheme and removes it. Because each pop shrinks the set,
// looping the single draw is exactly sampling without replacement. The pick of a partition is
// lock-free; the pop then takes only that partition's stripe lock, held across the vector swap-
// remove, the hash-record delete, and the ordered-index splice, so two pops routing to two
// partitions of one hot key run on two cores instead of serializing on the set's single stripe.
// Holding the partition stripe lock across the whole pop serializes it with that partition's SADD
// and SREM, so a concurrent SREM of the same member cannot double-count the cardinality. When the
// picked partition drains under a concurrent pop between the lock-free pick and the lock, the pop
// re-picks with fresh counts (section 6.6.1). The cardinality is decremented once after the loop.
func (c *connState) cmdSPopPart(skey []byte, count int64, hasCount bool, p int) {
	if c.stringConflict(skey) {
		c.writeErr(wrongType)
		return
	}
	base := c.partScanBase(skey)
	last := len(base) - 1

	if !hasCount {
		// No-count form: pop one uniform member, or nil for an empty set. The retry budget bounds a
		// pathological concurrent drain that keeps emptying the partition this pick lands on.
		for attempt := 0; attempt < 16; attempt++ {
			part := c.srv.store.CollPartPick(base, p, c.nextRand())
			if part < 0 {
				c.writeNil()
				return
			}
			mu := &c.srv.incrMu[c.srv.stripePart(skey, part)]
			mu.Lock()
			base[last] = byte(part)
			k, ok := c.srv.store.CollPartPopLocked(base)
			if !ok {
				mu.Unlock()
				continue
			}
			member := k[len(base):]
			if c.srv.store.DeleteKind(k, kindSetMember) {
				c.srv.store.CollRemove(k)
			}
			mu.Unlock()
			c.setBumpCard(skey, -1, p)
			c.writeBulk(member)
			return
		}
		c.writeNil()
		return
	}

	// Count form: pop up to count distinct members, each a fresh weighted draw+remove, until count is
	// reached or the set empties. The member keys are stable arena subslices, so they stay valid to
	// write after every removal.
	if count == 0 {
		c.writeArrayHeader(0)
		return
	}
	want := int(count)
	initCap := want
	if initCap > 256 {
		initCap = 256
	}
	out := make([][]byte, 0, initCap)
	for len(out) < want {
		part := c.srv.store.CollPartPick(base, p, c.nextRand())
		if part < 0 {
			break
		}
		mu := &c.srv.incrMu[c.srv.stripePart(skey, part)]
		mu.Lock()
		base[last] = byte(part)
		k, ok := c.srv.store.CollPartPopLocked(base)
		if !ok {
			mu.Unlock()
			continue
		}
		if c.srv.store.DeleteKind(k, kindSetMember) {
			c.srv.store.CollRemove(k)
		}
		mu.Unlock()
		out = append(out, k[len(base):])
	}
	if len(out) > 0 {
		c.setBumpCard(skey, -len(out), p)
	}
	c.writeArrayHeader(len(out))
	for _, m := range out {
		c.writeBulk(m)
	}
}

// lockTwoStripes takes the stripe locks for two keys in a fixed order (lower stripe
// index first) so two SMOVEs touching the same pair of keys from opposite directions
// can never deadlock, and returns an unlock closure. When both keys map to the same
// stripe it locks that one mutex once and unlocks it once, since a sync.Mutex is not
// reentrant. This is the first two-key write on f1raw; every prior collection write
// took exactly one stripe lock.
func (c *connState) lockTwoStripes(a, b []byte) func() {
	return c.lockTwoStripeIdx(c.srv.stripe(a), c.srv.stripe(b))
}

// lockTwoStripeIdx takes two stripe mutexes by index in ascending index order (lower first),
// deduplicating when both indices are the same, and returns an unlock closure. Ascending index
// order is the one global lock order lockStripes and lockSetPartitionsShared also follow, so a
// two-stripe write can never deadlock against a concurrent multi-stripe algebra or shared read that
// holds a superset of stripes: every acquirer takes its stripes in the same total order. When both
// indices coincide it locks the one mutex once, since a sync.Mutex is not reentrant.
func (c *connState) lockTwoStripeIdx(sa, sb uint32) func() {
	if sa == sb {
		mu := &c.srv.incrMu[sa]
		mu.Lock()
		return mu.Unlock
	}
	lo, hi := sa, sb
	if lo > hi {
		lo, hi = hi, lo
	}
	mlo := &c.srv.incrMu[lo]
	mhi := &c.srv.incrMu[hi]
	mlo.Lock()
	mhi.Lock()
	return func() {
		mhi.Unlock()
		mlo.Unlock()
	}
}

// cmdSMove atomically moves one member from a source set to a destination set (spec
// 2064/f1_rewrite_ltm/06 section 8.9): it removes the member row from the source and
// adds it to the destination under both sets' stripe locks, keeping both header counts
// in step, and returns 1 when the member moved or 0 when the member was not in the
// source (in which case the destination is untouched). If the member already lives in
// the destination it is only removed from the source, never duplicated. A source that
// equals the destination is a no-op that reports whether the member is present. Either
// key holding a plain string is WRONGTYPE.
func (c *connState) cmdSMove(argv [][]byte) {
	// SMOVE source destination member
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'smove' command")
		return
	}
	source, destination, member := argv[1], argv[2], argv[3]

	// A partitioned set routes the member to one partition and locks only that partition's stripe,
	// so the move goes through the partition-aware body (spec 2064/f1_rewrite_ltm/19 section 6.10).
	// partitionsFor is a whole-server hook until slice 6, so source and destination share P and both
	// are partitioned together; the per-key P values differing is a slice-6 concern.
	if pSrc, pDst := c.partitionsFor(source), c.partitionsFor(destination); pSrc > 1 || pDst > 1 {
		c.smovePart(source, destination, member, pSrc, pDst)
		return
	}

	unlock := c.lockTwoStripes(source, destination)
	if c.stringConflict(source) || c.stringConflict(destination) {
		unlock()
		c.writeErr(wrongType)
		return
	}

	// Source and destination the same set: the move is a no-op, so just report whether
	// the member is present without touching any row or count.
	if bytes.Equal(source, destination) {
		present := c.srv.store.ExistsKind(c.memberKey(source, member), kindSetMember)
		unlock()
		if present {
			c.writeInt(1)
			return
		}
		c.writeInt(0)
		return
	}

	// Not in the source: nothing moves and the destination stays untouched.
	srcMK := c.memberKey(source, member)
	if !c.srv.store.ExistsKind(srcMK, kindSetMember) {
		unlock()
		c.writeInt(0)
		return
	}

	// Remove from the source and decrement its header, deleting the header at zero. Drop the
	// member from the source's dense vector first, while its record is still resolvable (spec
	// 2064/18 section 5.2).
	c.srv.store.CollRandRemove(c.setPrefix(source), srcMK, kindSetMember)
	if c.srv.store.DeleteKind(srcMK, kindSetMember) {
		c.srv.store.CollRemove(srcMK)
	}
	if sc := c.setCard(source); sc > 0 {
		if err := c.setSetCard(source, sc-1); err != nil {
			unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}

	// Add to the destination only if absent, so a member already there is not
	// duplicated and the header count only rises on a genuine insert.
	dstMK := c.memberKey(destination, member)
	isNew, err := c.srv.store.PutKind(dstMK, nil, kindSetMember)
	if err != nil {
		unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	if isNew {
		c.srv.store.CollInsert(dstMK, kindSetMember)
		// Append the moved member to the destination's dense vector if it exists (spec 2064/18 5.1).
		c.srv.store.CollRandInsert(c.setPrefix(destination), dstMK, kindSetMember)
		// A genuine insert can raise the destination's encoding (a non-integer member arriving
		// at an intset, or a growth past a threshold), so fold it forward like SADD does.
		count, enc, _ := c.setHeader(destination)
		count++
		enc = foldSetEnc(enc, member, count)
		if err := c.setPutHeader(destination, count, enc); err != nil {
			unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	unlock()
	c.writeInt(1)
}

// smovePart is the P>1 routing of SMOVE (spec 2064/f1_rewrite_ltm/19 section 6.10). The member
// routes to the source partition PartitionOf(m, pSrc) and, independently, the destination partition
// PartitionOf(m, pDst); the two partition ids are unrelated because they are computed against two
// possibly-different partition counts. The command locks exactly those two partition stripes, taken
// in ascending stripe-index order through lockTwoStripeIdx, the same global order lockStripes and
// lockSetPartitionsShared follow, so two SMOVEs touching the same pair of partitions from opposite
// argument order agree on the order and cannot deadlock, and neither can an SMOVE deadlock against a
// concurrent algebra or SMEMBERS holding a superset of those stripes. Under both locks it removes
// the member from the source partition and adds it to the destination partition. The shared per-key
// counts are bumped after the locks release, exactly as cmdSAddPart and cmdSRemPart bump them: the
// header count is eventually consistent and every partitioned reader reframes from the live rows, so
// the momentary lag is invisible.
func (c *connState) smovePart(source, destination, member []byte, pSrc, pDst int) {
	srcStripe := c.srv.stripe(source)
	srcPart := 0
	if pSrc > 1 {
		srcPart = f1raw.PartitionOf(member, pSrc)
		srcStripe = c.srv.stripePart(source, srcPart)
	}
	dstStripe := c.srv.stripe(destination)
	dstPart := 0
	if pDst > 1 {
		dstPart = f1raw.PartitionOf(member, pDst)
		dstStripe = c.srv.stripePart(destination, dstPart)
	}
	unlock := c.lockTwoStripeIdx(srcStripe, dstStripe)

	if c.stringConflict(source) || c.stringConflict(destination) {
		unlock()
		c.writeErr(wrongType)
		return
	}

	// Source equals destination: the member routes to one partition under one key, so a move cannot
	// change which partition holds it. Report presence without touching a row or a count, exactly as
	// the unpartitioned same-key case does.
	if bytes.Equal(source, destination) {
		present := c.srv.store.ExistsKind(c.partMemberKey(source, member, srcPart, pSrc), kindSetMember)
		unlock()
		if present {
			c.writeInt(1)
		} else {
			c.writeInt(0)
		}
		return
	}

	// Not in the source partition: nothing moves and the destination stays untouched.
	srcMK := c.partMemberKey(source, member, srcPart, pSrc)
	if !c.srv.store.ExistsKind(srcMK, kindSetMember) {
		unlock()
		c.writeInt(0)
		return
	}

	// Remove from the source partition. Drop from that partition's dense vector first, while the
	// record is still resolvable (spec 2064/18 section 5.2). The vector base is built into ppbuf with
	// its final byte set to the source partition; srcMK lives in the distinct kbuf, so both stay live
	// at once without colliding. An unpartitioned source (pSrc==1) uses its whole-set prefix instead.
	if pSrc > 1 {
		base := c.partScanBase(source)
		base[len(base)-1] = byte(srcPart)
		c.srv.store.CollRandRemove(base, srcMK, kindSetMember)
	} else {
		c.srv.store.CollRandRemove(c.setPrefix(source), srcMK, kindSetMember)
	}
	if c.srv.store.DeleteKind(srcMK, kindSetMember) {
		c.srv.store.CollRemove(srcMK)
	}

	// Add to the destination partition only if absent, so a member already there is not duplicated.
	// partMemberKey reuses kbuf, overwriting srcMK, which is already consumed; the vector base reuses
	// ppbuf, overwriting the source base, also already consumed.
	dstMK := c.partMemberKey(destination, member, dstPart, pDst)
	isNew, err := c.srv.store.PutKind(dstMK, nil, kindSetMember)
	if err != nil {
		unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	if isNew {
		c.srv.store.CollInsert(dstMK, kindSetMember)
		if pDst > 1 {
			base := c.partScanBase(destination)
			base[len(base)-1] = byte(dstPart)
			c.srv.store.CollRandInsert(base, dstMK, kindSetMember)
		} else {
			c.srv.store.CollRandInsert(c.setPrefix(destination), dstMK, kindSetMember)
		}
	}
	unlock()

	// Adjust the shared per-key counts off the partition locks. The source always lost a member (it
	// was present under the lock), the destination gained one only on a genuine insert. A partitioned
	// set carries its count in the whole-key header word bumped lock-free; setBumpCard stamps the
	// hashtable encoding a partitioned set always reports, so no encoding fold is needed here.
	c.setBumpCard(source, -1, pSrc)
	if isNew {
		c.setBumpCard(destination, 1, pDst)
	}
	c.writeInt(1)
}
