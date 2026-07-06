package f1srv

import (
	"bytes"
	"sort"
)

// SORT and SORT_RO order the elements of a list, set, or sorted set and reply with the
// ordered elements, optionally dereferenced through GET patterns and optionally stored into a
// fresh list. This is the f1raw port of the v1 reference (command/sort.go): the Redis
// semantics are identical, but the sources are streamed off f1raw's element rows rather than a
// materialized keyspace object, and BY/GET dereferences are point reads on the string and
// hash-field rows.
//
// The element order a source contributes is the same one Redis uses: a list keeps its
// positional order and a sorted set comes in score order (its score-family rows are already in
// numeric order). A set has no inherent order, so a BY-nosort SORT over a set returns members in
// enumeration order without sorting, exactly as Redis leaves a set's order undefined; the one
// exception is a STORE, which forces an ALPHA sort so the stored list is deterministic. A BY
// pattern with no '*' disables sorting entirely.
//
// SORT can write through STORE and so carries FlagWrite in the command table; SORT_RO is the
// same command with STORE rejected at parse time, so a read-only client can run it.

// sortOpts holds the parsed SORT clauses.
type sortOpts struct {
	by     []byte   // BY pattern, nil when absent
	hasBy  bool     // BY was given, even with an empty pattern
	gets   [][]byte // GET patterns in order; "#" means the element itself
	store  []byte   // STORE destination, nil when absent
	offset int64    // LIMIT offset
	count  int64    // LIMIT count, -1 means all
	hasLim bool     // LIMIT was given
	desc   bool     // DESC sorts high to low
	alpha  bool     // ALPHA compares as bytes instead of numbers
}

// dontSort reports whether the BY clause disables sorting, which happens when the pattern has
// no '*' so every element would weigh the same and the source order is preserved.
func (o sortOpts) dontSort() bool {
	return o.hasBy && !bytes.Contains(o.by, []byte{'*'})
}

// sortItem pairs an element with the value it sorts on.
type sortItem struct {
	elem  []byte
	byStr []byte  // ALPHA weight bytes
	byNum float64 // numeric weight
}

// sortCell is one output value. A GET pattern that misses produces a nil cell, which is a null
// reply or, under STORE, an empty string.
type sortCell struct {
	val   []byte
	isNil bool
}

func (c *connState) cmdSort(argv [][]byte)   { c.runSort(argv, false) }
func (c *connState) cmdSortRO(argv [][]byte) { c.runSort(argv, true) }

// runSort parses the clauses, reads the source, sorts it, applies the LIMIT window, and either
// replies with the ordered rows or stores them into a fresh list. readonly forbids STORE.
func (c *connState) runSort(argv [][]byte, readonly bool) {
	if len(argv) < 2 {
		name := "sort"
		if readonly {
			name = "sort_ro"
		}
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	opts, errMsg := parseSortOpts(argv[2:], readonly)
	if errMsg != "" {
		c.writeErr(errMsg)
		return
	}
	key := argv[1]

	// Lazy expiry runs before the stripe locks are taken, since expireIfNeeded takes the stripe
	// lock itself and would deadlock under a lock this command already holds. This mirrors COPY.
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(key)
		if opts.store != nil {
			c.expireIfNeeded(opts.store)
		}
	}

	// SORT reads the source and every BY/GET key and may write STORE. The source and the
	// destination take their stripe locks for the whole command so the enumeration sees a stable
	// source and the store is atomic against concurrent readers. BY/GET dereferences are
	// lock-free point reads (store.Get/GetKind take no stripe lock), so they never re-enter a
	// lock this goroutine already holds even when they hash to the source stripe.
	lockKeys := [][]byte{key}
	if opts.store != nil {
		lockKeys = append(lockKeys, opts.store)
	}
	unlock := c.lockStripes(lockKeys)
	defer unlock()

	typ := c.resolveType(key)
	if typ != keyMissing && typ != keyList && typ != keySet && typ != keyZset {
		c.writeErr(wrongType)
		return
	}

	// Materialize the source elements as owned copies. Enumeration and the BY/GET lookups both
	// borrow the shared kbuf/pbuf scratch, so the elements are copied out before any lookup runs.
	elems := c.sortReadSource(key, typ)

	ordered, conv := c.sortElements(elems, typ, opts)
	if conv {
		c.writeErr("ERR One or more scores can't be converted into double")
		return
	}
	ordered = sortApplyLimit(ordered, opts)

	out, conv := c.sortBuildOutput(ordered, opts)
	if conv {
		c.writeErr("ERR One or more scores can't be converted into double")
		return
	}

	if opts.store != nil {
		c.sortStore(opts.store, out)
		return
	}

	c.writeArrayHeader(len(out))
	for _, cell := range out {
		if cell.isNil {
			c.writeNil()
		} else {
			c.writeBulk(cell.val)
		}
	}
}

// parseSortOpts reads the clauses after the key, returning a Redis-matching error string when a
// clause is malformed. STORE under SORT_RO is a syntax error, the same reply Redis gives.
func parseSortOpts(args [][]byte, readonly bool) (sortOpts, string) {
	opts := sortOpts{count: -1}
	for i := 0; i < len(args); i++ {
		switch {
		case eqFold(args[i], "ASC"):
			opts.desc = false
		case eqFold(args[i], "DESC"):
			opts.desc = true
		case eqFold(args[i], "ALPHA"):
			opts.alpha = true
		case eqFold(args[i], "LIMIT"):
			if i+2 >= len(args) {
				return opts, "ERR syntax error"
			}
			off, ok1 := parseInt64Strict(args[i+1])
			cnt, ok2 := parseInt64Strict(args[i+2])
			if !ok1 || !ok2 {
				return opts, "ERR value is not an integer or out of range"
			}
			opts.offset, opts.count, opts.hasLim = off, cnt, true
			i += 2
		case eqFold(args[i], "BY"):
			if i+1 >= len(args) {
				return opts, "ERR syntax error"
			}
			opts.by, opts.hasBy = args[i+1], true
			i++
		case eqFold(args[i], "GET"):
			if i+1 >= len(args) {
				return opts, "ERR syntax error"
			}
			opts.gets = append(opts.gets, args[i+1])
			i++
		case eqFold(args[i], "STORE"):
			if readonly {
				return opts, "ERR syntax error"
			}
			if i+1 >= len(args) {
				return opts, "ERR syntax error"
			}
			opts.store = args[i+1]
			i++
		default:
			return opts, "ERR syntax error"
		}
	}
	return opts, ""
}

// sortReadSource streams the source's elements into a slice of owned copies, in the natural
// order the type contributes: a list in positional order, a set in member-byte order, and a
// sorted set in score order. A missing key yields nothing. The copies outlive the shared scan
// scratch, so a following BY/GET lookup that reuses that scratch cannot corrupt them.
func (c *connState) sortReadSource(key []byte, typ keyKind) [][]byte {
	switch typ {
	case keyList:
		return c.sortReadList(key)
	case keySet:
		return c.sortReadSet(key)
	case keyZset:
		// A score-family row is keyed prefix | 8 sortable score bytes | member, so a byte-order
		// scan yields members in score order and the member starts 8 bytes past the prefix.
		return c.sortReadFamily(c.zscorePrefix(key), 8)
	default:
		return nil
	}
}

// sortReadList walks a list's header window [head, tail) and copies each element out in order.
// List element rows are keyed by position, not carried in the ordered index, so the window is
// read positionally exactly as LRANGE reads it.
func (c *connState) sortReadList(key []byte) [][]byte {
	// A resident push leaves element bytes only in the ring, not in f1raw rows, so retire the hot-list
	// window first to flush them back before this positional read (slice 3, impl/34). SORT holds the
	// source key's exclusive stripe lock through lockStripes, which is what drainEvict requires.
	c.listWinDrainEvict(key)
	head, tail, _, _, ok := c.listHeader(key)
	if !ok {
		return nil
	}
	out := make([][]byte, 0, tail-head)
	var vbuf []byte
	for p := head; p < tail; p++ {
		v, got := c.srv.store.GetKind(c.listElemKey(key, p), vbuf[:0], kindListElem)
		if !got {
			continue
		}
		out = append(out, append([]byte(nil), v...))
	}
	return out
}

// sortReadSet enumerates a set's members off the dense member vector rather than a raw ordered-run
// scan, mirroring streamSet, so the SORT set source no longer leans on the global order index (the
// oindex is being retired for the set type). SORT already holds the source key's exclusive stripe lock
// through lockStripes, which freezes the partition layout (a grow takes the whole-key stripe) and, for
// an unpartitioned set, the member writers as well, so the vector is read under a stable snapshot and
// there is no need to re-take rlockSet (which would self-deadlock on the key stripe this command already
// holds). partitionsFor is stable for the same reason. Each member is copied out because the shared scan
// scratch is reused by the following BY/GET lookups. For a partitioned set the member starts past the
// partition prefix (moff), which the old whole-prefix scan did not account for, so this also corrects
// the lab-only P>1 path to match streamSet.
func (c *connState) sortReadSet(skey []byte) [][]byte {
	var out [][]byte
	scan := make([][]byte, 0, hashScanBatch)
	if p := c.partitionsFor(skey); p > 1 {
		base := c.partScanBase(skey)
		moff := len(base)
		for part := 0; part < p; part++ {
			hi := -1
			for {
				keys, next := c.srv.store.SetPartVecScanDown(base, p, part, hi, hashScanBatch, scan[:0])
				for _, k := range keys {
					out = append(out, append([]byte(nil), k[moff:]...))
				}
				if next == 0 {
					break
				}
				hi = next
			}
		}
		return out
	}
	prefix := c.setPrefix(skey)
	plen := len(prefix)
	hi := -1
	for {
		keys, next := c.srv.store.SetVecScanDown(prefix, hi, hashScanBatch, scan[:0])
		for _, k := range keys {
			out = append(out, append([]byte(nil), k[plen:]...))
		}
		if next == 0 {
			break
		}
		hi = next
	}
	return out
}

// sortReadFamily scans one element family in bounded batches and copies each member out. skip is
// how many bytes of suffix precede the member in the row key: 0 for a set member (the member is
// the whole suffix), 8 for a sorted set score-family row (the member follows the score bytes).
func (c *connState) sortReadFamily(prefix []byte, skip int) [][]byte {
	plen := len(prefix)
	var out [][]byte
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			out = append(out, append([]byte(nil), k[plen+skip:]...))
		}
		if last == nil {
			break
		}
		after = last
	}
	return out
}

// sortElements orders the elements per the options. conv is true when a numeric sort meets a
// weight that is present but not a number, which rejects the whole command. A BY clause with no
// '*' skips sorting and the source's natural order is preserved, matching Redis: a set under
// BY-nosort has no defined order and is returned as it was enumerated, except that a STORE forces
// an ALPHA sort so the stored list is deterministic (this is Redis's storekey special case).
func (c *connState) sortElements(elems [][]byte, typ keyKind, opts sortOpts) (out [][]byte, conv bool) {
	eff := opts
	if eff.dontSort() && typ == keySet && eff.store != nil {
		eff.by, eff.hasBy, eff.alpha = nil, false, true
	}
	if eff.dontSort() {
		return elems, false
	}

	items := make([]sortItem, len(elems))
	for i, e := range elems {
		items[i].elem = e
		w := e
		present := true
		if eff.hasBy {
			lw, ok := c.lookupKeyByPattern(eff.by, e)
			if !ok {
				w, present = nil, false
			} else {
				w = lw
			}
		}
		if eff.alpha {
			items[i].byStr = w
			continue
		}
		n, ok := sortNumWeight(w, present)
		if !ok {
			return nil, true
		}
		items[i].byNum = n
	}

	sort.SliceStable(items, func(i, j int) bool {
		return sortLess(items[i], items[j], eff)
	})

	out = make([][]byte, len(items))
	for i, it := range items {
		out[i] = it.elem
	}
	return out, false
}

// sortLess reports whether a should come before b under the sort options. ALPHA compares the
// weight bytes then the element bytes; a numeric sort compares the parsed weights then the
// element bytes; DESC inverts the result.
func sortLess(a, b sortItem, opts sortOpts) bool {
	var less bool
	if opts.alpha {
		cmp := bytes.Compare(a.byStr, b.byStr)
		if cmp == 0 {
			cmp = bytes.Compare(a.elem, b.elem)
		}
		less = cmp < 0
	} else if a.byNum != b.byNum {
		less = a.byNum < b.byNum
	} else {
		less = bytes.Compare(a.elem, b.elem) < 0
	}
	if opts.desc {
		return !less
	}
	return less
}

// sortNumWeight parses a numeric sort weight the way Redis's SORT does. A weight that is missing
// entirely (a BY key or hash field that does not exist) is 0 with no error. A weight that is
// present is parsed as a whole float, and an empty, whitespace-only, or otherwise unparseable
// present value rejects the command, matching Redis: the element itself is always present, so an
// empty list element fails a numeric sort. ok is false only on a present value that is not a
// number.
func sortNumWeight(w []byte, present bool) (float64, bool) {
	if !present {
		return 0, true
	}
	f, err := parseScore(w)
	if err != nil {
		return 0, false
	}
	return f, true
}

// sortApplyLimit trims the ordered elements to the LIMIT window. An offset past the end yields
// nothing, and a negative count keeps everything from the offset.
func sortApplyLimit(elems [][]byte, opts sortOpts) [][]byte {
	if !opts.hasLim {
		return elems
	}
	off := opts.offset
	if off < 0 {
		off = 0
	}
	if off >= int64(len(elems)) {
		return nil
	}
	rest := elems[off:]
	if opts.count >= 0 && opts.count < int64(len(rest)) {
		rest = rest[:opts.count]
	}
	return rest
}

// sortBuildOutput turns the ordered elements into reply rows. With no GET clause each element is
// one row; with GET clauses each element expands to one row per pattern, where "#" is the
// element itself and any other pattern is dereferenced. A GET that misses yields a nil cell.
func (c *connState) sortBuildOutput(elems [][]byte, opts sortOpts) ([]sortCell, bool) {
	if len(opts.gets) == 0 {
		out := make([]sortCell, len(elems))
		for i, e := range elems {
			out[i] = sortCell{val: e}
		}
		return out, false
	}
	out := make([]sortCell, 0, len(elems)*len(opts.gets))
	for _, e := range elems {
		for _, g := range opts.gets {
			if len(g) == 1 && g[0] == '#' {
				out = append(out, sortCell{val: e})
				continue
			}
			v, ok := c.lookupKeyByPattern(g, e)
			if !ok {
				out = append(out, sortCell{isNil: true})
			} else {
				out = append(out, sortCell{val: v})
			}
		}
	}
	return out, false
}

// lookupKeyByPattern resolves a BY or GET pattern for one element. The first '*' in the pattern
// is replaced with the element. A pattern with no '*' resolves to nothing, matching Redis. A
// pattern of the form key->field reads a hash field; otherwise it reads a string key. The
// returned value is an owned copy, so it survives the shared read scratch being reused.
func (c *connState) lookupKeyByPattern(pattern, subst []byte) ([]byte, bool) {
	star := bytes.IndexByte(pattern, '*')
	if star < 0 {
		return nil, false
	}
	resolved := make([]byte, 0, len(pattern)+len(subst))
	resolved = append(resolved, pattern[:star]...)
	resolved = append(resolved, subst...)
	resolved = append(resolved, pattern[star+1:]...)

	if arrow := bytes.Index(resolved, []byte("->")); arrow >= 0 {
		hkey := resolved[:arrow]
		field := resolved[arrow+2:]
		if c.resolveType(hkey) != keyHash {
			return nil, false
		}
		if c.hfieldExpired(hkey, field) {
			return nil, false
		}
		fk := c.fieldKey(hkey, field)
		v, ok := c.srv.store.GetKind(fk, nil, kindHashField)
		if !ok {
			return nil, false
		}
		return append([]byte(nil), v...), true
	}

	if c.resolveType(resolved) != keyString {
		return nil, false
	}
	v, ok := c.srv.store.Get(resolved, nil)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// sortStore writes the SORT result into the destination as a fresh list and replies with the
// number of elements stored. An empty result deletes the destination (an empty list is no
// list), matching Redis, and replies 0. A nil GET cell stores as an empty string. The caller
// holds the destination stripe lock.
func (c *connState) sortStore(dst []byte, out []sortCell) {
	c.dropKeyLocked(dst)
	if len(out) == 0 {
		c.writeInt(0)
		return
	}
	var head, tail int64
	lpBytes := uint64(listHeaderBytes)
	everLarge := false
	for _, cell := range out {
		v := cell.val
		if cell.isNil {
			v = []byte{}
		}
		if _, err := c.srv.store.PutKind(c.listElemKey(dst, tail), v, kindListElem); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
		tail++
		lpBytes += uint64(listEntrySize(v))
		if !everLarge && lpBytes > listListpackMaxBytes {
			everLarge = true
		}
	}
	if err := c.listPutHeader(dst, head, tail, lpBytes, everLarge); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeInt(tail)
}
