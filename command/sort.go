package command

import (
	"bytes"
	"math"
	"sort"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// sortCommands returns SORT and its read-only sibling SORT_RO. SORT can write
// through STORE; SORT_RO is the same command with STORE rejected, so a replica or
// a read-only ACL can run it.
func sortCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "sort", Group: GroupGeneric, Since: "1.0.0",
			Arity: -2, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSort},
		{Name: "sort_ro", Group: GroupGeneric, Since: "7.0.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSortRO},
	}
}

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
	alpha  bool     // ALPHA compares as strings instead of numbers
}

// handleSort runs SORT, which may store its result.
func handleSort(ctx *Ctx) { runSort(ctx, false) }

// handleSortRO runs SORT_RO, which rejects STORE.
func handleSortRO(ctx *Ctx) { runSort(ctx, true) }

// runSort parses the clauses then sorts the source key. readonly forbids STORE.
func runSort(ctx *Ctx, readonly bool) {
	opts, errMsg := parseSortOpts(ctx.Argv[2:], readonly)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	key := ctx.Argv[1]

	var (
		out      []sortCell // result rows, one per output value
		wrongTyp bool
		failConv bool // a weight could not be read as a number
		stored   int  // number of elements written by STORE
		emptied  bool // STORE deleted the destination because the result was empty
	)

	run := func(db *keyspace.DB) error {
		_, hdr, found, err := db.Peek(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeList && hdr.Type != keyspace.TypeSet && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}

		var (
			ordered [][]byte
			conv    bool
		)
		if found && hdr.IsColl() && sortWindowEligible(opts) {
			// Bounded path: a coll-form source with a LIMIT is sorted by streaming the
			// elements through a top-(offset+count) heap, so memory tracks the window
			// the client asked for, not the size of the collection. The unbounded
			// full-sort and the BY-no-'*' no-sort cases keep the in-RAM path below.
			ordered, conv, err = sortCollWindow(db, key, hdr.Type, opts)
			if err != nil {
				return err
			}
		} else {
			elems, typ, _, serr := sortSource(db, key)
			if serr != nil {
				return serr
			}
			ordered, conv, err = sortElements(db, elems, typ, opts)
			if err != nil {
				return err
			}
			ordered = sortLimit(ordered, opts)
		}
		if conv {
			failConv = true
			return nil
		}
		out, err = buildOutput(db, ordered, opts)
		if err != nil {
			return err
		}
		if opts.store != nil {
			vals := make([][]byte, len(out))
			for i, c := range out {
				vals[i] = c.val // a nil GET result stores as an empty string below
				if c.isNil {
					vals[i] = []byte{}
				}
			}
			if len(vals) == 0 {
				had, derr := db.Delete(opts.store)
				emptied = had
				return derr
			}
			stored = len(vals)
			return db.Set(opts.store, listEncode(vals), keyspace.TypeList,
				listEncoding(ctx.encLimits(), vals, keyspace.EncListpack), -1)
		}
		return nil
	}

	var ok bool
	if opts.store != nil {
		ok = ctx.update(run)
	} else {
		ok = ctx.view(run)
	}
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if failConv {
		ctx.enc().WriteError("ERR One or more scores can't be converted into double")
		return
	}

	if opts.store != nil {
		if emptied {
			ctx.notify(notifyGeneric, "del", opts.store)
		} else if stored > 0 {
			ctx.notify(notifyList, "sortstore", opts.store)
		}
		ctx.enc().WriteInteger(int64(stored))
		return
	}

	enc := ctx.enc()
	enc.WriteArrayLen(len(out))
	for _, c := range out {
		if c.isNil {
			enc.WriteNull()
		} else {
			enc.WriteBulkString(c.val)
		}
	}
}

// sortCell is one output value. A GET pattern that misses produces a nil cell,
// which is a null reply or, under STORE, an empty string.
type sortCell struct {
	val   []byte
	isNil bool
}

// weighted pairs an element with the value it sorts on.
type weighted struct {
	elem  []byte
	byStr []byte  // ALPHA weight bytes
	byNum float64 // numeric weight
}

// sortLess reports whether a should come before b under the sort options, the one
// comparator the in-RAM sort and the bounded top-K window share. ALPHA compares
// the weight bytes then the element bytes; a numeric sort compares the parsed
// weights then the element bytes; DESC inverts the result. Two byte-identical
// elements are interchangeable in the output, so the unspecified order DESC gives
// them does not change the reply.
func sortLess(a, b weighted, opts sortOpts) bool {
	var less bool
	if opts.alpha {
		c := bytes.Compare(a.byStr, b.byStr)
		if c == 0 {
			c = bytes.Compare(a.elem, b.elem)
		}
		less = c < 0
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

// parseSortOpts reads the clauses after the key. It returns an error string when
// a clause is malformed, matching the Redis wording.
func parseSortOpts(args [][]byte, readonly bool) (sortOpts, string) {
	opts := sortOpts{count: -1}
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "ASC":
			opts.desc = false
		case "DESC":
			opts.desc = true
		case "ALPHA":
			opts.alpha = true
		case "LIMIT":
			if i+2 >= len(args) {
				return opts, "ERR syntax error"
			}
			off, ok1 := parseInteger(args[i+1])
			cnt, ok2 := parseInteger(args[i+2])
			if !ok1 || !ok2 {
				return opts, "ERR value is not an integer or out of range"
			}
			opts.offset, opts.count, opts.hasLim = off, cnt, true
			i += 2
		case "BY":
			if i+1 >= len(args) {
				return opts, "ERR syntax error"
			}
			opts.by, opts.hasBy = args[i+1], true
			i++
		case "GET":
			if i+1 >= len(args) {
				return opts, "ERR syntax error"
			}
			opts.gets = append(opts.gets, args[i+1])
			i++
		case "STORE":
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

// sortSource reads the key as a flat list of elements. A list keeps its order, a
// set comes in its stored order, and a sorted set comes in score order. found is
// false for a missing key, which sorts to an empty result.
func sortSource(db *keyspace.DB, key []byte) (elems [][]byte, typ uint8, found bool, err error) {
	_, hdr, ok, err := db.Peek(key)
	if err != nil || !ok {
		return nil, 0, false, err
	}
	switch hdr.Type {
	case keyspace.TypeList:
		// getList branches on the storage form: a blob-form list decodes its
		// listpack body, a coll-form list (past the 128-entry threshold) walks its
		// element sub-tree. Decoding the raw body unconditionally would read a
		// coll-form header as a listpack and fail with a corrupt-value error.
		elems, _, _, gerr := getList(db, key)
		return elems, hdr.Type, true, gerr
	case keyspace.TypeSet:
		ms, _, _, gerr := getSet(db, key)
		return ms, hdr.Type, true, gerr
	case keyspace.TypeZSet:
		zm, _, _, gerr := getZSet(db, key)
		if gerr != nil {
			return nil, hdr.Type, true, gerr
		}
		elems = make([][]byte, len(zm))
		for i, m := range zm {
			elems[i] = m.member
		}
		return elems, hdr.Type, true, nil
	default:
		return nil, hdr.Type, true, nil
	}
}

// dontSort reports whether the BY clause disables sorting, which happens when the
// pattern has no '*' so every element would weigh the same.
func (o sortOpts) dontSort() bool {
	return o.hasBy && !bytes.Contains(o.by, []byte{'*'})
}

// sortElements orders the elements per the options. conv is true when a numeric
// sort hit a value that is not a number. A BY clause with no '*' skips sorting.
func sortElements(db *keyspace.DB, elems [][]byte, typ uint8, opts sortOpts) (out [][]byte, conv bool, err error) {
	if opts.dontSort() {
		// A set has no inherent order, so a no-sort SORT over a set still sorts
		// lexically to stay deterministic, matching what Redis settled on.
		if typ == keyspace.TypeSet {
			out = append(out, elems...)
			sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
			return out, false, nil
		}
		return elems, false, nil
	}

	items := make([]weighted, len(elems))
	for i, e := range elems {
		items[i].elem = e
		w := e
		if opts.hasBy {
			lw, lok, lerr := lookupKeyByPattern(db, opts.by, e)
			if lerr != nil {
				return nil, false, lerr
			}
			if !lok {
				w = nil
			} else {
				w = lw
			}
		}
		if opts.alpha {
			items[i].byStr = w
		} else {
			if w == nil {
				items[i].byNum = 0
			} else {
				n, ok := parseFloat(w)
				if !ok {
					return nil, true, nil
				}
				items[i].byNum = n
			}
		}
	}

	sort.SliceStable(items, func(i, j int) bool {
		return sortLess(items[i], items[j], opts)
	})

	out = make([][]byte, len(items))
	for i, it := range items {
		out[i] = it.elem
	}
	return out, false, nil
}

// applyLimit trims the ordered elements to the LIMIT window. An offset past the
// end yields nothing, and a negative count keeps everything from the offset.
func sortLimit(elems [][]byte, opts sortOpts) [][]byte {
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

// buildOutput turns the ordered elements into the reply rows. With no GET clause
// each element is one row; with GET clauses each element expands to one row per
// pattern, where "#" is the element itself and any other pattern is dereferenced.
func buildOutput(db *keyspace.DB, elems [][]byte, opts sortOpts) ([]sortCell, error) {
	if len(opts.gets) == 0 {
		out := make([]sortCell, len(elems))
		for i, e := range elems {
			out[i] = sortCell{val: e}
		}
		return out, nil
	}
	out := make([]sortCell, 0, len(elems)*len(opts.gets))
	for _, e := range elems {
		for _, g := range opts.gets {
			if len(g) == 1 && g[0] == '#' {
				out = append(out, sortCell{val: e})
				continue
			}
			v, ok, err := lookupKeyByPattern(db, g, e)
			if err != nil {
				return nil, err
			}
			if !ok {
				out = append(out, sortCell{isNil: true})
			} else {
				out = append(out, sortCell{val: v})
			}
		}
	}
	return out, nil
}

// lookupKeyByPattern resolves a BY or GET pattern for one element. The first '*'
// in the pattern is replaced with the element. A pattern with no '*' resolves to
// nothing, matching Redis. A pattern of the form key->field reads a hash field,
// otherwise it reads a string key.
func lookupKeyByPattern(db *keyspace.DB, pattern, subst []byte) ([]byte, bool, error) {
	star := bytes.IndexByte(pattern, '*')
	if star < 0 {
		return nil, false, nil
	}
	resolved := make([]byte, 0, len(pattern)+len(subst))
	resolved = append(resolved, pattern[:star]...)
	resolved = append(resolved, subst...)
	resolved = append(resolved, pattern[star+1:]...)

	if arrow := bytes.Index(resolved, []byte("->")); arrow >= 0 {
		hkey := resolved[:arrow]
		field := resolved[arrow+2:]
		// A point sub-tree lookup on coll form, not a full materialize: a SORT
		// with BY weight_*->f over a list whose weight hashes are huge would
		// otherwise clone a whole hash per element just to read one field.
		val, fieldFound, hdr, keyFound, err := hashGetField(db, hkey, field)
		if err != nil || !keyFound || hdr.Type != keyspace.TypeHash || !fieldFound {
			return nil, false, err
		}
		return val, true, nil
	}

	body, hdr, found, err := db.Get(resolved)
	if err != nil || !found || hdr.Type != keyspace.TypeString {
		return nil, false, err
	}
	return body, true, nil
}

// sortScanPageCount is how many source elements one streaming page weighs before
// the next page. It bounds the per-page working set; the heap below bounds the
// retained set to the LIMIT window, so neither grows with the collection.
const sortScanPageCount = 1024

// sortWindowEligible reports whether SORT can take the bounded top-K path: a
// LIMIT with a non-negative count caps the result, and the BY clause does not
// disable sorting. A negative count means "everything from the offset", which is
// unbounded, so that case (and a missing LIMIT) keeps the in-RAM full sort. The
// BY-no-'*' no-sort case keeps the in-RAM path too, since it does not sort.
func sortWindowEligible(opts sortOpts) bool {
	return opts.hasLim && opts.count >= 0 && !opts.dontSort()
}

// sortCollWindow sorts a coll-form source under a LIMIT without materializing it.
// It streams the elements in COUNT-sized pages off the bounded scan cursor and
// keeps only the offset+count top-ranked elements in a heap, so memory tracks the
// window the client asked for, not the collection. conv is true when a numeric
// sort meets a weight that is not a number, which rejects the whole command, the
// same as the in-RAM path; that check runs on every streamed element, so the
// command is rejected even when the offending element falls outside the window.
//
// found must already be known true and the key in coll form. The BY and GET
// lookups read other keys, so they run here between pages, never while the source
// shard read lock is held inside a scan page, which keeps the shard lock from
// being taken twice on one goroutine.
func sortCollWindow(db *keyspace.DB, key []byte, typ uint8, opts sortOpts) (ordered [][]byte, conv bool, err error) {
	off := opts.offset
	if off < 0 {
		off = 0
	}
	// window is how many top elements answer the LIMIT. The caller guarantees
	// count >= 0; guard the add against int64 overflow on an absurd LIMIT.
	window := off
	if off > math.MaxInt64-opts.count {
		window = math.MaxInt64
	} else {
		window += opts.count
	}
	h := sortTopK{opts: opts, limit: window}

	if opts.hasBy {
		// A BY clause weighs each element by reading another key, which re-enters the
		// shard lock, so the element bytes must be copied out of the source page and
		// weighed with the lock released. sortCollWindowBy streams the source one
		// bounded page at a time: peak memory is one page plus the kept window, never
		// the whole collection.
		conv, err := sortCollWindowBy(db, key, typ, opts, &h)
		if err != nil || conv {
			return nil, conv, err
		}
	} else {
		// With no BY clause the weight is the element itself, so the whole source can
		// be walked in place under one read lock with an arena cursor: each element is
		// weighed without copying and only the elements that enter the kept window are
		// copied out. Allocation tracks the window, not the collection.
		conv, err := sortCollWindowNoBy(db, key, typ, opts, &h)
		if err != nil || conv {
			return nil, conv, err
		}
	}

	items := h.items
	sort.SliceStable(items, func(i, j int) bool {
		return sortLess(items[i], items[j], opts)
	})
	if off >= int64(len(items)) {
		return nil, false, nil
	}
	items = items[off:]
	if opts.count < int64(len(items)) {
		items = items[:opts.count]
	}
	ordered = make([][]byte, len(items))
	for i, it := range items {
		ordered[i] = it.elem
	}
	return ordered, false, nil
}

// collElemDecoder returns the row prefix and the per-type extractor that turns a
// coll-form sub-tree row into the element SORT orders by: a set member is the row
// key, a list element is the row value (the sub-tree is keyed by position), and a
// sorted set member is the member-index row key past its 'm' family byte. The
// prefix bounds a sorted set walk to the member family so the score-index rows are
// skipped.
func collElemDecoder(typ uint8) (prefix []byte, member func(k, v []byte) []byte, ok bool) {
	switch typ {
	case keyspace.TypeSet:
		return nil, func(k, _ []byte) []byte { return k }, true
	case keyspace.TypeList:
		return nil, func(_, v []byte) []byte { return v }, true
	case keyspace.TypeZSet:
		return []byte{zRowMember}, func(k, _ []byte) []byte { return k[1:] }, true
	default:
		return nil, nil, false
	}
}

// sortCollWindowNoBy walks the whole coll-form source in place under one read lock
// and feeds every element to the top-K heap, for a SORT with no BY clause whose
// weight is the element itself. An arena cursor decodes each row without allocating
// and the element bytes are copied out only when the element ranks into the kept
// window, so allocation tracks the window, not the collection. conv is true when a
// numeric sort meets an element that is not a number, which rejects the command.
func sortCollWindowNoBy(db *keyspace.DB, key []byte, typ uint8, opts sortOpts, h *sortTopK) (conv bool, err error) {
	prefix, member, ok := collElemDecoder(typ)
	if !ok {
		return false, nil
	}
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		c.UseArena()
		if e := c.Seek(prefix); e != nil {
			return e
		}
		for c.Valid() {
			k := c.Key()
			if len(prefix) > 0 && (len(k) < len(prefix) || !bytes.Equal(k[:len(prefix)], prefix)) {
				break // walked off the member family into the score index
			}
			elem := member(k, c.Value())
			var w weighted
			w.elem = elem
			if opts.alpha {
				w.byStr = elem
			} else {
				n, numOK := parseFloat(elem)
				if !numOK {
					conv = true
					return nil // a non-numeric element rejects the whole command
				}
				w.byNum = n
			}
			if h.wouldAccept(w) {
				// elem aliases the arena page and is only valid for this iteration, so
				// copy it now that it will be retained in the heap.
				ec := append([]byte(nil), elem...)
				kept := weighted{elem: ec, byNum: w.byNum}
				if opts.alpha {
					kept.byStr = ec
				}
				h.offer(kept)
			}
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return conv, err
}

// sortCollWindowBy streams the coll-form source one bounded page at a time, weighing
// each element by its BY pattern with the source lock released between pages, for a
// SORT with a BY clause. Peak memory is one page plus the kept window. conv is true
// when a numeric sort meets a weight that is not a number.
func sortCollWindowBy(db *keyspace.DB, key []byte, typ uint8, opts sortOpts, h *sortTopK) (conv bool, err error) {
	cursor := []byte("0")
	for {
		elems, next, perr := sortCollPage(db, key, typ, cursor, sortScanPageCount)
		if perr != nil {
			return false, perr
		}
		for _, elem := range elems {
			w, isConv, werr := weighElem(db, elem, opts)
			if werr != nil {
				return false, werr
			}
			if isConv {
				return true, nil
			}
			h.offer(w)
		}
		if next == "0" {
			break
		}
		cursor = []byte(next)
	}
	return false, nil
}

// sortCollPage reads one bounded page of a coll-form source's elements off the
// scan cursor, returning the raw element bytes (a set member, a list value, or a
// sorted set member) and the next cursor. The element bytes are copied out of the
// page, so they outlive the shard read lock the page held.
func sortCollPage(db *keyspace.DB, key []byte, typ uint8, cursor []byte, count int) (elems [][]byte, next string, err error) {
	prefix, member, ok := collElemDecoder(typ)
	if !ok {
		return nil, "0", nil
	}
	decode := func(k, v []byte) ([]byte, []byte, bool) { return member(k, v), nil, true }
	rows, nxt, e := collScanPage(db, key, prefix, cursor, count, nil, decode)
	if e != nil {
		return nil, "0", e
	}
	elems = make([][]byte, len(rows))
	for i, r := range rows {
		elems[i] = r.member
	}
	return elems, nxt, nil
}

// weighElem computes the sort weight for one element, the per-element half of
// sortElements. conv is true when a numeric sort meets a weight that is not a
// number. The BY lookup may read another key, so the caller must not hold a
// source shard lock when it calls this.
func weighElem(db *keyspace.DB, elem []byte, opts sortOpts) (w weighted, conv bool, err error) {
	w.elem = elem
	val := elem
	if opts.hasBy {
		lw, lok, lerr := lookupKeyByPattern(db, opts.by, elem)
		if lerr != nil {
			return w, false, lerr
		}
		if lok {
			val = lw
		} else {
			val = nil
		}
	}
	if opts.alpha {
		w.byStr = val
		return w, false, nil
	}
	if val == nil {
		w.byNum = 0
		return w, false, nil
	}
	n, ok := parseFloat(val)
	if !ok {
		return w, true, nil
	}
	w.byNum = n
	return w, false, nil
}

// sortTopK keeps the best limit elements under the sort order in a bounded
// max-heap whose root is the worst-ranked element kept, so a new element only
// displaces the root when it ranks ahead of it. Memory is O(limit), not O(n).
type sortTopK struct {
	items []weighted
	opts  sortOpts
	limit int64
}

// worse reports whether items[a] is ranked after items[b], so the heap parent is
// the worst element kept and sits at the root.
func (h *sortTopK) worse(a, b int) bool { return sortLess(h.items[b], h.items[a], h.opts) }

// wouldAccept reports whether w ranks well enough to enter the kept set. The caller
// uses it to copy an element's bytes out of a transient arena page only when the
// copy will be retained, so a walk over the whole collection allocates only for the
// elements that reach the kept window.
func (h *sortTopK) wouldAccept(w weighted) bool {
	if h.limit <= 0 {
		return false
	}
	if int64(len(h.items)) < h.limit {
		return true
	}
	return sortLess(w, h.items[0], h.opts)
}

// offer adds w to the kept set, dropping the worst element once the heap is full.
func (h *sortTopK) offer(w weighted) {
	if h.limit <= 0 {
		return
	}
	if int64(len(h.items)) < h.limit {
		h.items = append(h.items, w)
		h.up(len(h.items) - 1)
		return
	}
	if sortLess(w, h.items[0], h.opts) {
		h.items[0] = w
		h.down(0)
	}
}

func (h *sortTopK) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !h.worse(i, p) {
			break
		}
		h.items[i], h.items[p] = h.items[p], h.items[i]
		i = p
	}
}

func (h *sortTopK) down(i int) {
	n := len(h.items)
	for {
		worst := i
		if l := 2*i + 1; l < n && h.worse(l, worst) {
			worst = l
		}
		if r := 2*i + 2; r < n && h.worse(r, worst) {
			worst = r
		}
		if worst == i {
			break
		}
		h.items[i], h.items[worst] = h.items[worst], h.items[i]
		i = worst
	}
}
