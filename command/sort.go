package command

import (
	"bytes"
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
		elems, typ, found, err := sortSource(db, key)
		if err != nil {
			return err
		}
		if found && typ != keyspace.TypeList && typ != keyspace.TypeSet && typ != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}

		ordered, conv, err := sortElements(db, elems, typ, opts)
		if err != nil {
			return err
		}
		if conv {
			failConv = true
			return nil
		}
		ordered = sortLimit(ordered, opts)
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
		body, _, _, gerr := db.Get(key)
		if gerr != nil {
			return nil, hdr.Type, true, gerr
		}
		elems, gerr = listDecode(body)
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
		var less bool
		if opts.alpha {
			c := bytes.Compare(items[i].byStr, items[j].byStr)
			if c == 0 {
				c = bytes.Compare(items[i].elem, items[j].elem)
			}
			less = c < 0
		} else if items[i].byNum != items[j].byNum {
			less = items[i].byNum < items[j].byNum
		} else {
			less = bytes.Compare(items[i].elem, items[j].elem) < 0
		}
		if opts.desc {
			return !less
		}
		return less
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
		fields, hdr, found, err := getHash(db, hkey)
		if err != nil || !found || hdr.Type != keyspace.TypeHash {
			return nil, false, err
		}
		for _, f := range fields {
			if bytes.Equal(f.field, field) {
				return f.value, true, nil
			}
		}
		return nil, false, nil
	}

	body, hdr, found, err := db.Get(resolved)
	if err != nil || !found || hdr.Type != keyspace.TypeString {
		return nil, false, err
	}
	return body, true, nil
}
