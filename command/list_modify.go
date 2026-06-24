package command

import (
	"bytes"
	"slices"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// listModifyCommands returns the index and in-place modify commands: LINDEX,
// LSET, LINSERT, LREM and LTRIM (doc 09 §6.7 through §6.11).
func listModifyCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "lindex", Group: GroupList, Since: "1.0.0",
			Arity: 3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLIndex},
		{Name: "lset", Group: GroupList, Since: "1.0.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLSet},
		{Name: "linsert", Group: GroupList, Since: "2.2.0",
			Arity: 5, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLInsert},
		{Name: "lrem", Group: GroupList, Since: "1.0.0",
			Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLRem},
		{Name: "ltrim", Group: GroupList, Since: "1.0.0",
			Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLTrim},
	}
}

// handleLIndex implements LINDEX: the element at an index, or nil when the key
// is absent or the index is out of range. Negative indices count from the tail.
func handleLIndex(ctx *Ctx) {
	key := ctx.Argv[1]
	idx, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	var (
		wrongTyp bool
		found    bool
		elem     []byte
	)
	if !ctx.view(func(db *keyspace.DB) error {
		hdr, ok, err := listHeader(db, key)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		// A btree-backed list resolves the index to a position and does a point row
		// lookup rather than materializing the whole list.
		if hdr.IsColl() {
			elem, found, err = listTreeIndex(db, key, idx)
			return err
		}
		elems, _, _, err := getList(db, key)
		if err != nil {
			return err
		}
		i := idx
		if i < 0 {
			i += int64(len(elems))
		}
		if i >= 0 && i < int64(len(elems)) {
			elem = elems[i]
			found = true
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if !found {
		ctx.enc().WriteNull()
		return
	}
	ctx.enc().WriteBulkString(elem)
}

// handleLSet implements LSET: replace the element at an index. A missing key is
// ERR no such key, an out-of-range index is ERR index out of range.
func handleLSet(ctx *Ctx) {
	key := ctx.Argv[1]
	idx, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	val := ctx.Argv[3]
	var (
		wrongTyp bool
		noKey    bool
		oob      bool
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		hdr, found, err := listHeader(db, key)
		if err != nil {
			return err
		}
		if !found {
			noKey = true
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		// A btree-backed list resolves the index to a position and writes that one
		// row rather than rewriting the whole blob.
		if hdr.IsColl() {
			oob, err = listTreeSet(db, key, val, idx)
			return err
		}
		elems, _, _, err := getList(db, key)
		if err != nil {
			return err
		}
		i := idx
		if i < 0 {
			i += int64(len(elems))
		}
		if i < 0 || i >= int64(len(elems)) {
			oob = true
			return nil
		}
		elems[i] = val
		return db.Set(key, listEncode(elems), keyspace.TypeList, listEncoding(ctx.encLimits(), elems, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case noKey:
		ctx.enc().WriteError("ERR no such key")
	case oob:
		ctx.enc().WriteError("ERR index out of range")
	default:
		ctx.notify(notifyList, "lset", key)
		ctx.enc().WriteStatus("OK")
	}
}

// handleLInsert implements LINSERT key BEFORE|AFTER pivot element. It returns
// the new length, -1 when the pivot is missing, or 0 when the key is absent.
func handleLInsert(ctx *Ctx) {
	key := ctx.Argv[1]
	var after bool
	switch strings.ToUpper(string(ctx.Argv[2])) {
	case "BEFORE":
		after = false
	case "AFTER":
		after = true
	default:
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	pivot := ctx.Argv[3]
	val := ctx.Argv[4]
	var (
		wrongTyp bool
		result   int64
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		elems, hdr, found, err := getList(db, key)
		if err != nil {
			return err
		}
		if !found {
			result = 0
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		idx := -1
		for i, e := range elems {
			if bytes.Equal(e, pivot) {
				idx = i
				break
			}
		}
		if idx < 0 {
			result = -1
			return nil
		}
		pos := idx
		if after {
			pos = idx + 1
		}
		elems = slices.Insert(elems, pos, val)
		result = int64(len(elems))
		return db.Set(key, listEncode(elems), keyspace.TypeList, listEncoding(ctx.encLimits(), elems, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if result > 0 {
		ctx.notify(notifyList, "linsert", key)
	}
	ctx.enc().WriteInteger(result)
}

// handleLRem implements LREM key count element. A positive count scans head to
// tail, a negative count scans tail to head, and zero removes every match.
func handleLRem(ctx *Ctx) {
	key := ctx.Argv[1]
	count, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	target := ctx.Argv[3]
	var (
		wrongTyp bool
		emptied  bool
		removed  int64
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		elems, hdr, found, err := getList(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		rest, n := listRemove(elems, target, count)
		removed = int64(n)
		if n == 0 {
			return nil
		}
		if len(rest) == 0 {
			emptied = true
			_, err := db.Delete(key)
			return err
		}
		return db.Set(key, listEncode(rest), keyspace.TypeList, listEncoding(ctx.encLimits(), rest, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if removed > 0 {
		ctx.notify(notifyList, "lrem", key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	ctx.enc().WriteInteger(removed)
}

// listRemove returns the elements with up to the count matches of target
// removed, along with how many were removed. A positive count works from the
// head, a negative count from the tail, and zero removes all matches. The limit
// is clamped to the list length so a count near the int64 edge cannot overflow.
func listRemove(elems [][]byte, target []byte, count int64) ([][]byte, int) {
	fromTail := count < 0
	limit := 0 // 0 means unlimited
	switch {
	case count > 0:
		limit = int(min(count, int64(len(elems))))
	case count < 0:
		if count < -int64(len(elems)) {
			limit = len(elems)
		} else {
			limit = int(-count)
		}
	}

	keep := make([]bool, len(elems))
	for i := range keep {
		keep[i] = true
	}
	removed := 0
	if fromTail {
		for i := len(elems) - 1; i >= 0; i-- {
			if (limit == 0 || removed < limit) && bytes.Equal(elems[i], target) {
				keep[i] = false
				removed++
			}
		}
	} else {
		for i := range elems {
			if (limit == 0 || removed < limit) && bytes.Equal(elems[i], target) {
				keep[i] = false
				removed++
			}
		}
	}
	if removed == 0 {
		return elems, 0
	}
	rest := make([][]byte, 0, len(elems)-removed)
	for i, e := range elems {
		if keep[i] {
			rest = append(rest, e)
		}
	}
	return rest, removed
}

// handleLTrim implements LTRIM: keep only the inclusive [start, stop] range,
// deleting the key when that range is empty. It always replies OK.
func handleLTrim(ctx *Ctx) {
	key := ctx.Argv[1]
	start, ok1 := parseInteger(ctx.Argv[2])
	stop, ok2 := parseInteger(ctx.Argv[3])
	if !ok1 || !ok2 {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	var (
		wrongTyp bool
		trimmed  bool
		emptied  bool
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		elems, hdr, found, err := getList(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		kept := listSlice(elems, start, stop)
		if len(kept) == 0 {
			trimmed = true
			emptied = true
			_, err := db.Delete(key)
			return err
		}
		if len(kept) == len(elems) {
			return nil
		}
		trimmed = true
		return db.Set(key, listEncode(kept), keyspace.TypeList, listEncoding(ctx.encLimits(), kept, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if trimmed {
		ctx.notify(notifyList, "ltrim", key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	ctx.enc().WriteStatus("OK")
}
