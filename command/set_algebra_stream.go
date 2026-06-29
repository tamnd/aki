package command

import (
	"github.com/tamnd/aki/keyspace"
)

// The materialize trap the read-form set algebra used to fall into: loadSets cloned
// every member of every source set onto the heap (getSet), then intersect/union/
// difference walked those in-memory slices. SINTER of a tiny set with a set far
// larger than RAM still dragged the huge set through memory in full, an OOM under a
// tight cap even though the result can be no larger than the tiny set.
//
// streamSInterDiff and streamSUnion compute the result without materializing any
// whole coll source. They reuse the SINTERCARD machinery (setDriveMembers,
// setProbeColl): one source is the driver, walked through an arena-backed cursor in
// copied batches with its reader closed before any probe runs, and every other coll
// source is answered with point lookups. Only small blob sources (bounded by the
// listpack threshold) are mapped in memory.
//
// SINTER and SDIFF stream straight into the RESP writer in two passes: pass one
// counts the survivors so the set-length header is exact, pass two re-walks and
// re-probes to emit them. Memory is one driver batch plus the running count, never
// the result or any source, so the intersection or difference of huge coll sets
// holds O(batch) regardless of how large the sources or the reply are. The second
// pass repeats the probes, so it is slower than a buffered compute, but slow is the
// trade the directive asks for over an OOM.
//
// SUNION cannot be counted without deduplicating, and a deduplicated union is the
// distinct result, so its floor is one dedup set, the same dict Redis builds. It
// holds that set (members as keys), contributing each source through batches rather
// than a full clone, so peak memory is the distinct result plus one driver batch,
// not the sum of the sources.

// handleSetOp implements SINTER, SUNION and SDIFF over the keys, streaming the result
// as a set reply without materializing any whole coll source.
func handleSetOp(ctx *Ctx, op setOp, keys [][]byte) {
	if op == opUnion {
		streamSUnion(ctx, keys)
		return
	}
	streamSInterDiff(ctx, op, keys)
}

// streamSInterDiff streams SINTER (op == opInter) or SDIFF (op == opDiff). The driver
// is the smallest source for an intersection (the result cannot exceed it) and the
// first source for a difference (the result is a subset of it). A surviving member is
// present in every other source for an intersection and absent from all of them for a
// difference.
func streamSInterDiff(ctx *Ctx, op setOp, keys [][]byte) {
	enc := ctx.enc()
	wrongTyp := false
	ok := ctx.view(func(db *keyspace.DB) error {
		cards := make([]int64, len(keys))
		hdrs := make([]keyspace.ValueHeader, len(keys))
		for i, k := range keys {
			c, hdr, found, err := setCard(db, k)
			if err != nil {
				return err
			}
			if found && hdr.Type != keyspace.TypeSet {
				wrongTyp = true
				return nil
			}
			if found {
				cards[i] = c
				hdrs[i] = hdr
			}
		}

		// Pick the driver and the others, and short-circuit the empty results.
		driver := 0
		var others []int
		if op == opInter {
			driver = -1
			for i := range keys {
				if cards[i] == 0 {
					// A missing or empty source makes the whole intersection empty.
					enc.WriteSetLen(0)
					return nil
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
				// An empty first source makes the whole difference empty.
				enc.WriteSetLen(0)
				return nil
			}
			for i := 1; i < len(keys); i++ {
				others = append(others, i)
			}
		}
		wantPresent := op == opInter

		// Map the small blob others once (bounded by the listpack threshold); the coll
		// others are point-probed per batch so they are never cloned whole. An empty
		// other contributes nothing: it removes no member from a difference and cannot
		// appear in an intersection (already short-circuited above), so skip it.
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
			members, _, _, err := getSet(db, keys[j])
			if err != nil {
				return err
			}
			blobMaps[j] = toMembership(members)
		}

		// survivors filters a driver batch down to the members that belong in the
		// result. The cheap in-memory blob filters run first to shrink the batch before
		// the per-member point probes against the coll others.
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
				present, err := setProbeColl(db, keys[j], s)
				if err != nil {
					return nil, err
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

		// Pass one: count the survivors so the set-length header is exact.
		count := 0
		if err := setDriveMembers(db, keys[driver], hdrs[driver], func(batch [][]byte) (bool, error) {
			s, err := survivors(batch)
			if err != nil {
				return false, err
			}
			count += len(s)
			return false, nil
		}); err != nil {
			return err
		}

		// Pass two: re-walk and re-probe to emit the survivors, spilling the buffer
		// periodically so the encoded reply is never held whole either.
		enc.WriteSetLen(count)
		i := 0
		return setDriveMembers(db, keys[driver], hdrs[driver], func(batch [][]byte) (bool, error) {
			s, err := survivors(batch)
			if err != nil {
				return false, err
			}
			for _, m := range s {
				enc.WriteBulkStreaming(m)
				if i&streamFlushEvery == streamFlushEvery {
					if fe := ctx.Conn.StreamFlush(); fe != nil {
						return false, fe
					}
				}
				i++
			}
			return false, nil
		})
	})
	if !ok {
		return
	}
	if wrongTyp {
		enc.WriteError(wrongTypeError)
	}
}

// streamSUnion streams SUNION. It deduplicates into one membership set, the inherent
// floor for a union, contributing each source through cursor batches rather than a
// full clone, then streams the distinct members from that set. Union order is
// unspecified, so emitting in the set's iteration order is conformant.
func streamSUnion(ctx *Ctx, keys [][]byte) {
	enc := ctx.enc()
	wrongTyp := false
	ok := ctx.view(func(db *keyspace.DB) error {
		hdrs := make([]keyspace.ValueHeader, len(keys))
		found := make([]bool, len(keys))
		for i, k := range keys {
			_, hdr, f, err := setCard(db, k)
			if err != nil {
				return err
			}
			if f && hdr.Type != keyspace.TypeSet {
				wrongTyp = true
				return nil
			}
			hdrs[i] = hdr
			found[i] = f
		}

		seen := map[string]struct{}{}
		for i, k := range keys {
			if !found[i] {
				continue
			}
			if err := setDriveMembers(db, k, hdrs[i], func(batch [][]byte) (bool, error) {
				for _, m := range batch {
					seen[string(m)] = struct{}{}
				}
				return false, nil
			}); err != nil {
				return err
			}
		}

		enc.WriteSetLen(len(seen))
		i := 0
		for m := range seen {
			enc.WriteBulkStreaming([]byte(m))
			if i&streamFlushEvery == streamFlushEvery {
				if e := ctx.Conn.StreamFlush(); e != nil {
					return e
				}
			}
			i++
		}
		return nil
	})
	if !ok {
		return
	}
	if wrongTyp {
		enc.WriteError(wrongTypeError)
	}
}

// keepNotInMembership filters members down to those absent from set, in place over
// the backing array (the caller passes a batch it no longer needs intact).
func keepNotInMembership(members [][]byte, set map[string]struct{}) [][]byte {
	kept := members[:0]
	for _, m := range members {
		if _, ok := set[string(m)]; !ok {
			kept = append(kept, m)
		}
	}
	return kept
}

// keepAbsent filters members down to those whose present flag is clear, in place.
func keepAbsent(members [][]byte, present []bool) [][]byte {
	kept := members[:0]
	for i, m := range members {
		if !present[i] {
			kept = append(kept, m)
		}
	}
	return kept
}
