package command

import (
	"bytes"

	"github.com/tamnd/aki/keyspace"
)

// The STORE forms (SINTERSTORE, SUNIONSTORE, SDIFFSTORE) used to go through the
// same loadSets/computeSetOp materialize as the read forms did, then write the
// whole result back with one db.Set. The result lived in RAM in full before a
// single byte reached the destination, so storing the intersection or union of
// sets far larger than RAM OOM-killed under a tight cap even when the destination
// would have spilled to the btree-backed coll form anyway.
//
// streamSetOpStore computes the result with the same bounded machinery the read
// forms use (setDriveMembers/setProbeColl: one driver walked in batches, every
// other coll source point-probed) and writes it into the destination as it goes.
// Small results buffer up to the listpack threshold and write as one blob, exactly
// the intset/listpack form Redis would pick, so the common case is byte-for-byte
// what it was. A result that crosses the hashtable threshold spills into a fresh
// coll-form destination and every later member is written straight into that
// sub-tree, so the destination is built element-by-element and never held whole.
// For SUNIONSTORE the spilled sub-tree is also the dedup structure: each member is
// Put into it and the tree reports whether it was new, so the distinct union of
// huge sources is computed without an in-memory dedup set, the sharpest
// larger-than-memory win in this family.
//
// One case keeps the materialize path: when the destination key is also one of the
// sources (SINTERSTORE dst dst a). Writing into the destination while reading it
// would mutate a source mid-walk, so the aliased case falls back to the buffered
// compute, the same O(result) memory Redis itself uses to build a STORE result.

// storeSink accumulates a streamed set-algebra result into a destination key. It
// buffers members while the result still fits a blob (intset or listpack), and the
// moment the buffer crosses the hashtable threshold it deletes the destination,
// writes the buffered members into a fresh coll sub-tree, and switches to writing
// each later member straight into that sub-tree in batches. Memory is bounded by
// the blob threshold plus one flush batch, never the result.
type storeSink struct {
	db    *keyspace.DB
	dst   []byte
	lim   encLimits
	dedup bool // SUNIONSTORE: drop members already emitted

	// Pre-spill blob buffer and the running aggregates that decide when the set
	// must promote to the coll form (the setEncoding rule, evaluated in O(1) per
	// member from these instead of rescanning the whole buffer).
	buf     [][]byte
	seen    map[string]struct{} // dedup set while buffering (union only)
	allInt  bool
	maxLen  int
	spilled bool

	// Post-spill flush batch written into the coll sub-tree once it fills.
	batch [][]byte

	count int64 // distinct members stored so far
}

// storeFlushBatch is the number of members buffered between coll-form writes after
// a spill, so the sub-tree descent and metadata rewrite are amortized over a batch
// rather than paid per member.
const storeFlushBatch = 256

func newStoreSink(db *keyspace.DB, dst []byte, lim encLimits, dedup bool) *storeSink {
	s := &storeSink{db: db, dst: dst, lim: lim, dedup: dedup, allInt: true}
	if dedup {
		s.seen = map[string]struct{}{}
	}
	return s
}

// add records one result member. m aliases a driver batch the caller still owns,
// so add copies it before keeping any reference.
func (s *storeSink) add(m []byte) error {
	if s.spilled {
		s.batch = append(s.batch, append([]byte(nil), m...))
		if len(s.batch) >= storeFlushBatch {
			return s.flushBatch()
		}
		return nil
	}
	if s.dedup {
		if _, ok := s.seen[string(m)]; ok {
			return nil
		}
		s.seen[string(m)] = struct{}{}
	}
	s.buf = append(s.buf, append([]byte(nil), m...))
	s.count++
	if _, ok := parseInteger(m); !ok {
		s.allInt = false
	}
	if len(m) > s.maxLen {
		s.maxLen = len(m)
	}
	if s.treeWanted() {
		return s.spill()
	}
	return nil
}

// treeWanted reports whether the buffered members have crossed the threshold where
// the set reports hashtable, which is exactly where it must become coll form. It
// mirrors setEncoding (with the intset floor) from the running aggregates so the
// check stays O(1) per member instead of rescanning the buffer.
func (s *storeSink) treeWanted() bool {
	n := int64(len(s.buf))
	if s.allInt && n <= s.lim.setIntset {
		return false
	}
	if n <= s.lim.setEntries && int64(s.maxLen) <= s.lim.setValue {
		return false
	}
	return true
}

// spill deletes the destination and writes the buffered members into a fresh
// coll-form sub-tree, then drops the buffer so later members write straight into
// the tree. The count is already the buffer length, so the sub-tree count is set
// to it directly.
func (s *storeSink) spill() error {
	if _, err := s.db.Delete(s.dst); err != nil {
		return err
	}
	buffered := s.buf
	err := s.db.CollUpdate(s.dst, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
		for _, m := range buffered {
			if _, e := w.Put(m, nil); e != nil {
				return e
			}
		}
		w.SetCount(uint64(len(buffered)))
		return nil
	})
	if err != nil {
		return err
	}
	s.spilled = true
	s.buf = nil
	s.seen = nil
	return nil
}

// flushBatch writes the pending post-spill members into the coll sub-tree. Put
// reports whether each member was new, so the count and the dedup (union) stay
// correct against members already in the tree.
func (s *storeSink) flushBatch() error {
	pending := s.batch
	if len(pending) == 0 {
		return nil
	}
	err := s.db.CollUpdate(s.dst, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
		for _, m := range pending {
			created, e := w.Put(m, nil)
			if e != nil {
				return e
			}
			if created {
				w.SetCount(w.Count() + 1)
				s.count++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.batch = s.batch[:0]
	return nil
}

// finish writes the result that is still buffered (the small, common case) and
// reports how many distinct members the destination holds and whether a delete
// removed a prior value (so the caller can fire the right keyspace event). A spilled
// result is already in the destination; finish only flushes its tail.
func (s *storeSink) finish() (n int64, deletedExisting bool, err error) {
	if s.spilled {
		if e := s.flushBatch(); e != nil {
			return 0, false, e
		}
		return s.count, false, nil
	}
	if len(s.buf) == 0 {
		existed, e := s.db.Delete(s.dst)
		return 0, existed, e
	}
	enc := setEncoding(s.lim, s.buf, keyspace.EncIntset)
	if e := s.db.Set(s.dst, setEncode(s.buf), keyspace.TypeSet, enc, -1); e != nil {
		return 0, false, e
	}
	return s.count, false, nil
}

// handleSetOpStore implements SINTERSTORE, SUNIONSTORE and SDIFFSTORE. When the
// destination is independent of the sources it streams the result into the
// destination without materializing any whole source (streamSetOpStore). When the
// destination aliases a source it falls back to the buffered compute, which reads
// every source before touching the destination (storeSetOpMaterialize).
func handleSetOpStore(ctx *Ctx, op setOp) {
	dst := ctx.Argv[1]
	keys := ctx.Argv[2:]
	for _, k := range keys {
		if bytes.Equal(dst, k) {
			storeSetOpMaterialize(ctx, op, dst, keys)
			return
		}
	}
	streamSetOpStore(ctx, op, dst, keys)
}

// streamSetOpStore computes the set operation with the bounded read-form machinery
// and writes the result into dst through a storeSink, so the destination is built
// element-by-element and neither the sources nor the result are ever held whole.
func streamSetOpStore(ctx *Ctx, op setOp, dst []byte, keys [][]byte) {
	var (
		wrongTyp        bool
		n               int64
		deletedExisting bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		hdrs := make([]keyspace.ValueHeader, len(keys))
		cards := make([]int64, len(keys))
		found := make([]bool, len(keys))
		for i, k := range keys {
			c, hdr, f, err := setCard(db, k)
			if err != nil {
				return err
			}
			if f && hdr.Type != keyspace.TypeSet {
				wrongTyp = true
				return nil
			}
			cards[i], hdrs[i], found[i] = c, hdr, f
		}

		sink := newStoreSink(db, dst, ctx.encLimits(), op == opUnion)

		switch op {
		case opUnion:
			if err := driveUnion(db, keys, hdrs, found, sink); err != nil {
				return err
			}
		default:
			empty, err := driveInterDiff(db, op, keys, cards, hdrs, sink)
			if err != nil {
				return err
			}
			if empty {
				// A short-circuit empty result still overwrites the destination.
				existed, e := db.Delete(dst)
				deletedExisting = existed
				return e
			}
		}

		nn, del, err := sink.finish()
		n, deletedExisting = nn, del
		return err
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if n > 0 {
		ctx.notify(notifySet, setStoreEvent(op), dst)
	} else if deletedExisting {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}

// driveInterDiff streams SINTERSTORE (opInter) or SDIFFSTORE (opDiff) survivors into
// the sink. It returns empty true when a short-circuit makes the result empty (a
// missing or empty source for an intersection, an empty first source for a
// difference) so the caller can overwrite the destination with a delete.
func driveInterDiff(db *keyspace.DB, op setOp, keys [][]byte, cards []int64, hdrs []keyspace.ValueHeader, sink *storeSink) (empty bool, err error) {
	driver := 0
	var others []int
	if op == opInter {
		driver = -1
		for i := range keys {
			if cards[i] == 0 {
				return true, nil
			}
			if driver < 0 || cards[i] < cards[driver] {
				driver = i
			}
		}
		for i := range keys {
			if i != driver {
				others = append(others, i)
			}
		}
	} else {
		if cards[0] == 0 {
			return true, nil
		}
		for i := 1; i < len(keys); i++ {
			others = append(others, i)
		}
	}
	wantPresent := op == opInter

	// Map the small blob others once; point-probe the coll others per batch so they
	// are never cloned whole. An empty other contributes nothing.
	blobMaps := make(map[int]map[string]struct{})
	var collOthers []int
	for _, j := range others {
		if cards[j] == 0 {
			continue
		}
		if hdrs[j].IsColl() {
			collOthers = append(collOthers, j)
			continue
		}
		members, _, _, e := getSet(db, keys[j])
		if e != nil {
			return false, e
		}
		blobMaps[j] = toMembership(members)
	}

	survivors := func(batch [][]byte) ([][]byte, error) {
		s := batch
		for _, m := range blobMaps {
			if wantPresent {
				s = keepInMembership(s, m)
			} else {
				s = keepNotInMembership(s, m)
			}
			if len(s) == 0 {
				return s, nil
			}
		}
		for _, j := range collOthers {
			present, e := setProbeColl(db, keys[j], s)
			if e != nil {
				return nil, e
			}
			if wantPresent {
				s = keepPresent(s, present)
			} else {
				s = keepAbsent(s, present)
			}
			if len(s) == 0 {
				return s, nil
			}
		}
		return s, nil
	}

	err = setDriveMembers(db, keys[driver], hdrs[driver], func(batch [][]byte) (bool, error) {
		s, e := survivors(batch)
		if e != nil {
			return false, e
		}
		for _, m := range s {
			if e := sink.add(m); e != nil {
				return false, e
			}
		}
		return false, nil
	})
	return false, err
}

// driveUnion streams SUNIONSTORE: every found source contributes its members to the
// sink, which deduplicates them (in the buffer while small, then against the coll
// sub-tree once spilled).
func driveUnion(db *keyspace.DB, keys [][]byte, hdrs []keyspace.ValueHeader, found []bool, sink *storeSink) error {
	for i, k := range keys {
		if !found[i] {
			continue
		}
		if err := setDriveMembers(db, k, hdrs[i], func(batch [][]byte) (bool, error) {
			for _, m := range batch {
				if e := sink.add(m); e != nil {
					return false, e
				}
			}
			return false, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// storeSetOpMaterialize is the fallback for SINTERSTORE/SUNIONSTORE/SDIFFSTORE when
// the destination aliases a source. It reads every source in full before writing
// the destination, so the destination read stays consistent while it is rewritten.
// This is the same O(result) memory Redis uses to build any STORE result.
func storeSetOpMaterialize(ctx *Ctx, op setOp, dst []byte, keys [][]byte) {
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		sets, wt, err := loadSets(db, keys)
		if err != nil {
			return err
		}
		if wt {
			wrongTyp = true
			return nil
		}
		result := computeSetOp(op, sets)
		n = int64(len(result))
		if len(result) == 0 {
			existed, err := db.Delete(dst)
			dstDeleted = existed
			return err
		}
		return db.Set(dst, setEncode(result), keyspace.TypeSet, setEncoding(ctx.encLimits(), result, keyspace.EncIntset), -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if n > 0 {
		ctx.notify(notifySet, setStoreEvent(op), dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}
