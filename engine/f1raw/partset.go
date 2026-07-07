package f1raw

import (
	"encoding/binary"
	"sync/atomic"
)

// Intra-key set partitioning (spec 2064/f1_rewrite_ltm/19) splits one large logical set
// into P independent partitions so a workload that pounds a single key spreads across P
// locks and up to P cores instead of serializing on one. The dense member vector (doc 18)
// gave the set an O(1) random draw, but that draw and every single-member write still funnel
// through one shard mutex and one stripe lock chosen by the set's key, so a single hot key
// runs on one core no matter how many the box has. A set with key K and partition count P is
// stored as P partitions, each a self-contained dense vector with its own cardinality, and a
// member m lives in partition hash(m) & (P-1). Point operations touch only their member's
// partition; whole-set operations sweep all P.
//
// This file is the partition data structure and the partition-aware key layout, built behind
// the feature flag with nothing routed through it yet (spec 2064/19 slice 2). It is standalone
// and self-tested: the command paths keep taking the unpartitioned path until slice 3 routes
// the point writes and probes and slice 4 routes the weighted draw. With P forced to 1 the key
// layout is byte-identical to the unpartitioned composite key, so a set that never engages
// partitioning stores exactly the bytes it does today and an existing store reads back unchanged.

// maxPartitions is the ceiling on P. The partition id rides in one composite-key byte and one
// header byte, so it spans 0..255 and P tops out at 256. P is always a power of two so a member
// routes to its partition with a mask instead of a modulo, which section 2.2 of the spec relies
// on for an exactly-uniform split.
const maxPartitions = 256

// validPartCount reports whether p is a legal partition count: a power of two in [1, 256]. P=1
// is the unpartitioned set (one partition, no partition byte), the default every set starts at.
func validPartCount(p int) bool {
	return p >= 1 && p <= maxPartitions && p&(p-1) == 0
}

// partOf routes a member to its partition, hash(member) & (p-1). p must be a power of two so
// the mask is an exact modulo, giving each partition an equal share of a well-distributed hash
// (the same store hash the index uses), which is what keeps the P partitions close to equal in
// size and the draw close to uniform. For p==1 the mask is zero and every member routes to
// partition 0, which is the unpartitioned set.
func partOf(member []byte, p int) int {
	return int(hash(member) & uint64(p-1))
}

// PartitionOf is the exported entry the server's command layer calls to route a member to its
// partition under partition count p, hash(member) & (p-1). It uses the same store hash the ordered
// index uses, so a member the server routes to a partition-prefixed key here lands in the same
// partition a later derivePartVec scan of that prefix range rebuilds, and a routed probe finds the
// row a routed write left. p must be a power of two in [1, 256]; p==1 routes every member to
// partition 0, the unpartitioned set. It is the only piece of partition routing the server cannot
// compute itself, because the store hash is unexported; the server builds the partition key and
// takes the partition lock on its own side.
func PartitionOf(member []byte, p int) int {
	return partOf(member, p)
}

// appendPartPrefix appends the bounding prefix for one partition of set skey under partition
// count p. For p==1 it is uvarint(len(skey)) | skey, byte-identical to the unpartitioned set
// prefix, so a P=1 set scans exactly as it does today. For p>1 it is uvarint(len(skey)) | skey |
// byte(part), so a scan bounded by it enumerates precisely one partition's member rows, and a
// scan bounded by the shorter whole-set prefix enumerates all P partitions in (part, member)
// order. part must be in [0, p).
func appendPartPrefix(dst, skey []byte, part, p int) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	dst = append(dst, tmp[:n]...)
	dst = append(dst, skey...)
	if p > 1 {
		dst = append(dst, byte(part))
	}
	return dst
}

// appendPartKey appends the composite member key for (skey, member) in its routed partition
// under partition count p. For p==1 it is uvarint(len(skey)) | skey | member, the exact
// unpartitioned member key, so nothing about a set's on-disk bytes changes until it engages
// partitioning. For p>1 it inserts the one partition byte between the length-prefixed set key
// and the member, so the member's partition is recorded in the key and a partition scan finds
// exactly its members. The partition is derived from the member here, so a caller never has to
// route separately: the key and the partition it lands in are computed in one place.
func appendPartKey(dst, skey, member []byte, p int) []byte {
	part := partOf(member, p)
	return appendPartMemberKey(dst, skey, member, part, p)
}

// appendPartMemberKey is appendPartKey with the partition supplied rather than derived, for the
// paths that already know a member's partition (a routed write that computed it once) and must
// not recompute a possibly-different value. part must equal partOf(member, p) for the key to be
// findable by a later routed lookup; callers that pass a mismatched part get a key no draw will
// resolve, which is why the deriving appendPartKey is the default entry point.
func appendPartMemberKey(dst, skey, member []byte, part, p int) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	dst = append(dst, tmp[:n]...)
	dst = append(dst, skey...)
	if p > 1 {
		dst = append(dst, byte(part))
	}
	dst = append(dst, member...)
	return dst
}

// splitPartKey splits a composite member key back into its set key, partition, and member,
// given the partition count p the set carries. It reads the uvarint length prefix, slices the
// set key, and then for p>1 peels the one partition byte before the member; for p==1 there is no
// partition byte and the member is everything after the set key, with the partition reported as
// 0. It reports ok=false on a key too short to hold its declared set key plus (for p>1) the
// partition byte, so a truncated or foreign key is rejected rather than mis-split. The returned
// slices alias key; a caller that keeps them past key's lifetime copies.
func splitPartKey(key []byte, p int) (skey []byte, part int, member []byte, ok bool) {
	klen, n := binary.Uvarint(key)
	if n <= 0 {
		return nil, 0, nil, false
	}
	rest := key[n:]
	if uint64(len(rest)) < klen {
		return nil, 0, nil, false
	}
	skey = rest[:klen]
	rest = rest[klen:]
	if p > 1 {
		if len(rest) < 1 {
			return nil, 0, nil, false
		}
		part = int(rest[0])
		rest = rest[1:]
	}
	return skey, part, rest, true
}

// partSet is one large logical set stored as P independent partitions. p is the partition count,
// a power of two in [1, 256]. vecs holds one dense member vector per partition, each built lazily
// on the first draw against that partition exactly as the unpartitioned vector is, so a partition
// never drawn from allocates no vector. count holds the maintained per-partition cardinality, the
// sum of which is the set's SCARD; keeping the count per partition is what lets a point write bump
// only its partition's counter under only its partition's lock, with no shared cardinality word to
// serialize on. A nil vecs[i] means partition i has no resident vector yet; count[i] is authoritative
// regardless, maintained by the routed writes.
//
// partSet is the resident side of a partitioned set. The durable side is the member rows under the
// partition-prefixed keys plus the header P field (section 5); partSet is derived from those rows the
// same way the unpartitioned vector is, never persisted, and rebuilt per partition on demand. Slice 2
// builds and tests it in isolation; slice 3 and slice 4 wire the commands through it.
type partSet struct {
	p     int
	vecs  []*memberVec
	count []atomic.Int64
}

// newPartSet allocates a partitioned set with p partitions and no resident vectors. It panics on
// an invalid p because p is always computed internally from validPartCount-checked values, so an
// invalid p is a programming error, not input to validate. The per-partition counts start at zero
// and the routed writes maintain them; the vectors stay nil until a draw against a partition builds
// one.
func newPartSet(p int) *partSet {
	if !validPartCount(p) {
		panic("f1raw: invalid partition count")
	}
	return &partSet{
		p:     p,
		vecs:  make([]*memberVec, p),
		count: make([]atomic.Int64, p),
	}
}

// total is the set's cardinality, the sum of the per-partition counts. It reads each count with one
// atomic load, so it never tears against a concurrent routed write bumping one partition, and it is
// the value SCARD reports. It is O(P), which is at most 256 loads and negligible next to a scan.
func (ps *partSet) total() int64 {
	var n int64
	for i := range ps.count {
		n += ps.count[i].Load()
	}
	return n
}

// partVec returns partition i's resident vector, or nil if it has not been built yet. The caller
// holds whatever lock guards the partition; slice 2 exposes it for the per-partition rebuild and
// the tests, and the routed draw in slice 4 loads it under the partition's lock.
func (ps *partSet) partVec(i int) *memberVec {
	return ps.vecs[i]
}

// setPartVec installs partition i's vector, the result of a per-partition derive. The caller holds
// the partition's lock so two first-draws against one partition do not both build.
func (ps *partSet) setPartVec(i int, v *memberVec) {
	ps.vecs[i] = v
}

// derivePartVec rebuilds partition part's dense vector by scanning that partition's member rows
// through the ordered index and appending each live offset, exactly as deriveOnFirstDraw rebuilds
// the unpartitioned vector but bounded to one partition's prefix range. The scan reads the same
// ordered run the enumeration commands walk, so it captures precisely the partition's live members
// (its liveness filter skips a tombstoned-but-not-yet-spliced node), and because a partition's keys
// all share the partition-prefixed bound, the scan touches only that partition and never bleeds into
// a sibling. The build is O(partition card) once; every draw after is O(1). It does not install the
// vector; the caller installs it under the partition lock (setPartVec) so the build and the publish
// are one critical section.
func (s *Store) derivePartVec(skey []byte, part, p int) *memberVec {
	v := newMemberVec(64)
	prefix := appendPartPrefix(nil, skey, part, p)
	var after []byte
	buf := make([]uint64, 0, 512)
	for {
		buf = buf[:0]
		offs, last := s.oidx.Load().scanBatch(prefix, after, 512, buf)
		for _, off := range offs {
			v.add(off)
		}
		if last == nil || len(offs) == 0 {
			break
		}
		after = append(after[:0], last...)
	}
	return v
}
