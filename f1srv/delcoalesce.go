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

// collRemoveBatch drops every composite key an applier deleted from the hash index out of
// the ordered element index under one index-lock acquisition. buf holds the removed keys
// packed end to end and ends[k] is the cumulative byte length through the k-th key, so the
// keys reslice out of buf without copying again. The reslice runs only after buf is fully
// built, so a mid-build append that reallocated buf cannot leave an earlier entry pointing
// at freed backing. An empty run touches nothing and never takes the lock.
func (c *connState) collRemoveBatch(buf []byte, ends []int) {
	if len(ends) == 0 {
		return
	}
	keys := c.delKeys[:0]
	prev := 0
	for _, e := range ends {
		keys = append(keys, buf[prev:e])
		prev = e
	}
	c.delKeys = keys
	c.srv.store.CollRemoveMany(keys)
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
	for _, end := range bnd {
		deleted := 0
		for ; i < end; i++ {
			fk := c.fieldKey(hkey, elems[i])
			if c.srv.store.DeleteKind(fk, kindHashField) {
				// Copy the composite key into the packed arena for one batched oindex remove
				// at the end of the run; kbuf is reused on the next fieldKey call.
				buf = append(buf, fk...)
				ends = append(ends, len(buf))
				// Drop any TTL sibling the field carried so the global hfe gate and the per-hash
				// hint stay exact when a TTL'd field is deleted outright.
				c.clearFieldTTLLocked(hkey, fk)
				deleted++
			}
		}
		counts = append(counts, deleted)
		total += deleted
	}
	c.delKeyBuf = buf
	c.delKeyEnd = ends
	c.delCnt = counts
	c.collRemoveBatch(buf, ends)
	if total > 0 {
		count := c.hashCount(hkey)
		if uint64(total) >= count {
			count = 0
		} else {
			count -= uint64(total)
		}
		if err := c.setHashCount(hkey, count); err != nil {
			mu.Unlock()
			emsg := "ERR " + err.Error()
			for range bnd {
				c.writeErr(emsg)
			}
			return
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
			if c.srv.store.DeleteKind(mk, kindSetMember) {
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
	c.collRemoveBatch(buf, ends)
	if total > 0 {
		count := c.setCard(skey)
		if uint64(total) >= count {
			count = 0
		} else {
			count -= uint64(total)
		}
		if err := c.setSetCard(skey, count); err != nil {
			mu.Unlock()
			emsg := "ERR " + err.Error()
			for range bnd {
				c.writeErr(emsg)
			}
			return
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
	buf := c.delKeyBuf[:0]
	ends := c.delKeyEnd[:0]
	total := 0
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
			// (spec section 2.5).
			v, ok := c.srv.store.TakeKind(mk, c.vbuf[:0], kindZsetMember)
			c.vbuf = v
			if !ok {
				continue
			}
			score := math.Float64frombits(binary.LittleEndian.Uint64(v))
			// The member row is gone; record its composite key for the batched oindex remove.
			// Copy it out of kbuf before zscoreKey rebuilds kbuf below.
			buf = append(buf, mk...)
			ends = append(ends, len(buf))
			sk := c.zscoreKey(zkey, score, member)
			if c.srv.store.DeleteKind(sk, kindZsetScore) {
				buf = append(buf, sk...)
				ends = append(ends, len(buf))
			}
			removed++
		}
		counts = append(counts, removed)
		total += removed
	}
	c.delKeyBuf = buf
	c.delKeyEnd = ends
	c.delCnt = counts
	c.collRemoveBatch(buf, ends)
	if total > 0 {
		count := c.zsetCard(zkey)
		if uint64(total) >= count {
			count = 0
		} else {
			count -= uint64(total)
		}
		if err := c.zsetSetCard(zkey, count); err != nil {
			mu.Unlock()
			emsg := "ERR " + err.Error()
			for range bnd {
				c.writeErr(emsg)
			}
			return
		}
	}
	mu.Unlock()
	for _, r := range counts {
		c.writeInt(int64(r))
	}
}
