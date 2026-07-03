package f1srv

import (
	"bytes"
	"encoding/binary"
	"math"
)

// delFamily names the three named-element delete commands the drain loop coalesces:
// HDEL, SREM, and ZREM. They share the shape "delete named members from one key", the
// destructive counterpart to the coalesced push, so a run of them from one connection to
// the same key folds into a single stripe-lock acquisition and a single count-header
// rewrite instead of one lock cycle and one header write per command.
type delFamily int

const (
	delNone delFamily = iota
	delHash
	delSet
	delZset
)

// delVerb classifies a command as one of the coalescable named-element deletes and reports
// which family it is. The gate mirrors pushVerb: at least a key and one member, and a first
// letter that can start one of the three verbs, so a non-matching command pays only a byte
// compare. LPOP/RPOP/SPOP are positional or random pops, not named-member deletes, so they
// are not folded here.
func delVerb(argv [][]byte) (delFamily, bool) {
	if len(argv) < 3 {
		return delNone, false
	}
	cmd := argv[0]
	if len(cmd) == 0 {
		return delNone, false
	}
	switch cmd[0] {
	case 'H', 'h', 'S', 's', 'Z', 'z':
	default:
		return delNone, false
	}
	switch {
	case eqFold(cmd, "HDEL"):
		return delHash, true
	case eqFold(cmd, "SREM"):
		return delSet, true
	case eqFold(cmd, "ZREM"):
		return delZset, true
	}
	return delNone, false
}

// drainDelete handles a named-element delete the drain loop has classified, then greedily
// peeks ahead in the same pipeline for more deletes of the same family to the same key from
// this one connection and folds them all into one coalesced applier call. It returns the
// buffer offset past every command it consumed. first is the already-parsed leading delete;
// pos points just past it.
//
// Every member slice points into rbuf, which drain does not compact until the whole batch is
// parsed, so a captured member stays valid across the look-ahead even though each parse reuses
// the shared argv backing. That reuse is why first must not be read after the first peek: this
// collects first's members up front and never touches first again, exactly as drainPush does.
func (c *connState) drainDelete(first [][]byte, fam delFamily, pos int) int {
	key := first[1]
	elems := c.delColl[:0]
	bnd := c.delBnd[:0]
	elems = append(elems, first[2:]...)
	bnd = append(bnd, len(elems))
	for {
		argv, consumed, status := c.parse(c.rbuf[pos:])
		if status != parseOK {
			break
		}
		f, ok := delVerb(argv)
		if !ok || f != fam || !bytes.Equal(argv[1], key) {
			break
		}
		pos += consumed
		elems = append(elems, argv[2:]...)
		bnd = append(bnd, len(elems))
	}
	c.delColl = elems
	c.delBnd = bnd
	switch fam {
	case delHash:
		c.cmdHDelCoalesced(key, elems, bnd)
	case delSet:
		c.cmdSRemCoalesced(key, elems, bnd)
	case delZset:
		c.cmdZRemCoalesced(key, elems, bnd)
	}
	return pos
}

// cmdHDelCoalesced applies a run of same-key HDELs captured from one connection's pipeline
// under a single stripe-lock acquisition and a single hash-count rewrite, then writes one
// integer reply per original command. It is exactly equivalent to running the commands one
// after another: they arrive from one connection in program order, which the run preserves by
// deleting members in arrival order against the running store state, so a field already
// dropped by an earlier command in the run counts zero for a later one just as it would
// unfolded. elems holds every field in arrival order; bnd[k] is the cumulative field count
// through command k, so command k owns elems[bnd[k-1]:bnd[k]].
func (c *connState) cmdHDelCoalesced(hkey []byte, elems [][]byte, bnd []int) {
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	if c.stringConflict(hkey) {
		mu.Unlock()
		for range bnd {
			c.writeErr(wrongType)
		}
		return
	}
	counts := c.delCnt[:0]
	buf := c.delKeyBuf[:0]
	ends := c.delKeyEnd[:0]
	total := 0
	i := 0
	// One coarse gate for the whole run: when no field of this hash carries a TTL (the common
	// case, answered by a single atomic load while the keyspace has no field TTL at all) the
	// per-field TTL-row delete below is a guaranteed no-op, so skip it and save a hash probe
	// per deleted field.
	hadTTL := c.hashHasFieldTTL(hkey)
	for _, end := range bnd {
		deleted := 0
		for ; i < end; i++ {
			fk := c.fieldKey(hkey, elems[i])
			if c.srv.store.DeleteKindNoCount(fk, kindHashField) {
				// Copy the composite key into the packed arena for one batched oindex remove
				// at the end of the run; kbuf is reused on the next fieldKey call.
				buf = append(buf, fk...)
				ends = append(ends, len(buf))
				// Drop any TTL sibling the field carried so the global hfe gate and the per-hash
				// hint stay exact when a TTL'd field is deleted outright.
				if hadTTL {
					c.clearFieldTTLLocked(hkey, fk)
				}
				deleted++
			}
		}
		counts = append(counts, deleted)
		total += deleted
	}
	c.delKeyBuf = buf
	c.delKeyEnd = ends
	c.delCnt = counts
	// Fold the whole run's global record-count decrement into one atomic (spec 2064/16 slice 3).
	// The per-field deletes above went through DeleteKindNoCount to keep the contended s.count line
	// off the hot path, so charge it once here for every field actually removed.
	if total > 0 {
		c.srv.store.AddCount(-int64(total))
	}
	// Defer the ordered-index splice off the stripe-locked reply path (spec 2064/16 slice 2).
	// CollRemovePacked copies the packed keys onto the tombstone queue when deferred removal is
	// on, so buf and ends are free to reuse the moment it returns.
	c.srv.store.CollRemovePacked(buf, ends, kindHashField)
	if total > 0 {
		n, ok := c.srv.store.CountAddInt64(hkey, kindHashMeta, -int64(total))
		if !ok || n <= 0 {
			c.srv.store.DeleteKind(hkey, kindHashMeta)
		}
	}
	mu.Unlock()
	for _, d := range counts {
		c.writeInt(int64(d))
	}
}

// cmdSRemCoalesced is the set counterpart to cmdHDelCoalesced: a run of same-key SREMs under
// one stripe lock and one cardinality rewrite, replying the removed count per command.
func (c *connState) cmdSRemCoalesced(skey []byte, elems [][]byte, bnd []int) {
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		for range bnd {
			c.writeErr(wrongType)
		}
		return
	}
	counts := c.delCnt[:0]
	buf := c.delKeyBuf[:0]
	ends := c.delKeyEnd[:0]
	total := 0
	i := 0
	for _, end := range bnd {
		removed := 0
		for ; i < end; i++ {
			mk := c.memberKey(skey, elems[i])
			if c.srv.store.DeleteKindNoCount(mk, kindSetMember) {
				buf = append(buf, mk...)
				ends = append(ends, len(buf))
				removed++
			}
		}
		counts = append(counts, removed)
		total += removed
	}
	c.delKeyBuf = buf
	c.delKeyEnd = ends
	c.delCnt = counts
	// Fold the run's global record-count decrement into one atomic (spec 2064/16 slice 3): the
	// members went through DeleteKindNoCount to keep the contended s.count line off the hot path.
	if total > 0 {
		c.srv.store.AddCount(-int64(total))
	}
	// Defer the ordered-index splice off the stripe-locked reply path (spec 2064/16 slice 2).
	c.srv.store.CollRemovePacked(buf, ends, kindSetMember)
	if total > 0 {
		n, ok := c.srv.store.CountAddInt64(skey, kindSetMeta, -int64(total))
		if !ok || n <= 0 {
			c.srv.store.DeleteKind(skey, kindSetMeta)
		}
	}
	mu.Unlock()
	for _, r := range counts {
		c.writeInt(int64(r))
	}
}

// cmdZRemCoalesced is the sorted-set counterpart: a run of same-key ZREMs under one stripe
// lock and one cardinality rewrite. Each member removal drops both index rows (the member row
// and the score row rebuilt from the score read out of the member row), matching cmdZRem.
func (c *connState) cmdZRemCoalesced(zkey []byte, elems [][]byte, bnd []int) {
	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		for range bnd {
			c.writeErr(wrongType)
		}
		return
	}
	counts := c.delCnt[:0]
	// A ZREM unlinks two rows of two different kinds per member: the member row (kindZsetMember)
	// and its score sidecar (kindZsetScore). Pack them into two kind-separated arenas so each kind
	// defers its ordered-index splice in one CollRemovePacked call off the reply path, the same
	// deferral SREM/HDEL get. Interleaving both kinds in one buffer would force the synchronous
	// splice, since CollRemovePacked takes a single kind, which is what held ZREM at the global
	// oindex lock and a hair under 2x at P16 (spec 2064/16 section 8.2).
	mbuf := c.delKeyBuf[:0]
	mends := c.delKeyEnd[:0]
	sbuf := c.delScrBuf[:0]
	sends := c.delScrEnd[:0]
	total := 0
	recs := 0
	i := 0
	for _, end := range bnd {
		removed := 0
		for ; i < end; i++ {
			member := elems[i]
			mk := c.zmemberKey(zkey, member)
			// Take reads the score out of the member row and deletes that row in one index
			// probe, where a GetKind then DeleteKind would find the same record twice. The
			// score is copied into vbuf before the slot clears, so it stays readable below to
			// address the score row directly, the point of storing the score in the member row
			// (spec section 2.5). NoCount keeps the contended global s.count line off the hot
			// path; the run charges it once through AddCount below (spec 2064/16 slice 3).
			v, ok := c.srv.store.TakeKindNoCount(mk, c.vbuf[:0], kindZsetMember)
			c.vbuf = v
			if !ok {
				continue
			}
			recs++
			score := math.Float64frombits(binary.LittleEndian.Uint64(v))
			// The member row is gone; record its composite key in the member arena for the
			// deferred oindex remove. Copy it out of kbuf before zscoreKey rebuilds kbuf below.
			mbuf = append(mbuf, mk...)
			mends = append(mends, len(mbuf))
			sk := c.zscoreKey(zkey, score, member)
			if c.srv.store.DeleteKindNoCount(sk, kindZsetScore) {
				sbuf = append(sbuf, sk...)
				sends = append(sends, len(sbuf))
				recs++
			}
			removed++
		}
		counts = append(counts, removed)
		total += removed
	}
	c.delKeyBuf = mbuf
	c.delKeyEnd = mends
	c.delScrBuf = sbuf
	c.delScrEnd = sends
	c.delCnt = counts
	// One global record-count decrement for every row the run actually unlinked (member rows
	// plus their score sidecars), the batched companion to TakeKindNoCount/DeleteKindNoCount.
	if recs > 0 {
		c.srv.store.AddCount(-int64(recs))
	}
	// Defer both ordered-index splices off the stripe-locked reply path (spec 2064/16 slice 2),
	// one CollRemovePacked per kind. Both copy their packed keys onto the tombstone queue when
	// deferred removal is on, so both arenas are free to reuse the moment these return.
	c.srv.store.CollRemovePacked(mbuf, mends, kindZsetMember)
	c.srv.store.CollRemovePacked(sbuf, sends, kindZsetScore)
	if total > 0 {
		n, ok := c.srv.store.CountAddInt64(zkey, kindZsetMeta, -int64(total))
		if !ok || n <= 0 {
			c.srv.store.DeleteKind(zkey, kindZsetMeta)
		}
	}
	mu.Unlock()
	for _, r := range counts {
		c.writeInt(int64(r))
	}
}
