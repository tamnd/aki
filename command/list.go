package command

import (
	"github.com/tamnd/aki/keyspace"
)

// listCommands returns the table for the core non-blocking list commands: the
// push, pop, length and range family (doc 09 §6). Index access, removal and the
// multi-key moves are separate later slices, and the blocking variants wait for
// the blocking-registry milestone.
func listCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "lpush", Group: GroupList, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, true, false) }},
		{Name: "rpush", Group: GroupList, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, false, false) }},
		{Name: "lpushx", Group: GroupList, Since: "2.2.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, true, true) }},
		{Name: "rpushx", Group: GroupList, Since: "2.2.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, false, true) }},
		{Name: "lpop", Group: GroupList, Since: "1.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { popList(ctx, true) }},
		{Name: "rpop", Group: GroupList, Since: "1.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { popList(ctx, false) }},
		{Name: "llen", Group: GroupList, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLLen},
		{Name: "lrange", Group: GroupList, Since: "1.0.0",
			Arity: 4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLRange},
	}
}

// pushList implements LPUSH, RPUSH and their X variants. head selects the end to
// push onto; mustExist makes it a no-op returning 0 when the key is absent.
func pushList(ctx *Ctx, head, mustExist bool) {
	key := ctx.Argv[1]
	vals := ctx.Argv[2:]
	var (
		wrongTyp bool
		absent   bool
		newLen   int64
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		if !found && mustExist {
			absent = true
			return nil
		}
		elems, err := listDecode(body)
		if err != nil {
			return err
		}
		if head {
			// LPUSH k a b c leaves [c, b, a, ...]: each element ends up at the
			// head, so the pushed run is the arguments reversed.
			next := make([][]byte, 0, len(vals)+len(elems))
			for i := len(vals) - 1; i >= 0; i-- {
				next = append(next, vals[i])
			}
			elems = append(next, elems...)
		} else {
			elems = append(elems, vals...)
		}
		newLen = int64(len(elems))
		prev := uint8(keyspace.EncListpack)
		if found {
			prev = hdr.Encoding
		}
		return db.Set(key, listEncode(elems), keyspace.TypeList, listEncoding(ctx.encLimits(), elems, prev), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if absent {
		ctx.enc().WriteInteger(0)
		return
	}
	event := "rpush"
	if head {
		event = "lpush"
	}
	ctx.notify(notifyList, event, key)
	ctx.signalReady(key)
	ctx.enc().WriteInteger(newLen)
}

// popList implements LPOP and RPOP, with or without a count. head selects the
// end to pop from.
func popList(ctx *Ctx, head bool) {
	if len(ctx.Argv) > 3 {
		name := "lpop"
		if !head {
			name = "rpop"
		}
		ctx.enc().WriteError("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	key := ctx.Argv[1]
	hasCount := len(ctx.Argv) == 3
	var count int64
	if hasCount {
		c, ok := parseInteger(ctx.Argv[2])
		if !ok || c < 0 {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		count = c
	}

	var (
		wrongTyp bool
		absent   bool
		emptied  bool
		popped   [][]byte
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found {
			absent = true
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		elems, err := listDecode(body)
		if err != nil {
			return err
		}
		n := 1
		if hasCount {
			n = int(min(count, int64(len(elems))))
		}
		if n == 0 {
			return nil
		}
		var rest [][]byte
		if head {
			popped = elems[:n]
			rest = elems[n:]
		} else {
			tail := elems[len(elems)-n:]
			popped = make([][]byte, n)
			for i := range tail {
				popped[i] = tail[n-1-i]
			}
			rest = elems[:len(elems)-n]
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
	if len(popped) > 0 {
		event := "rpop"
		if head {
			event = "lpop"
		}
		ctx.notify(notifyList, event, key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	enc := ctx.enc()
	if absent {
		if hasCount {
			enc.WriteNullArray()
		} else {
			enc.WriteNull()
		}
		return
	}
	if hasCount {
		enc.WriteArrayLen(len(popped))
		for _, e := range popped {
			enc.WriteBulkString(e)
		}
		return
	}
	enc.WriteBulkString(popped[0])
}

// handleLLen implements LLEN: the element count, or 0 when the key is absent.
func handleLLen(ctx *Ctx) {
	key := ctx.Argv[1]
	var (
		wrongTyp bool
		n        int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
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
		elems, err := listDecode(body)
		if err != nil {
			return err
		}
		n = int64(len(elems))
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// handleLRange implements LRANGE: the elements in the inclusive [start, stop]
// range, with Redis negative indexing and clamping.
func handleLRange(ctx *Ctx) {
	key := ctx.Argv[1]
	start, ok1 := parseInteger(ctx.Argv[2])
	stop, ok2 := parseInteger(ctx.Argv[3])
	if !ok1 || !ok2 {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	var (
		wrongTyp bool
		out      [][]byte
	)
	if !ctx.view(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
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
		elems, err := listDecode(body)
		if err != nil {
			return err
		}
		out = listSlice(elems, start, stop)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(out))
	for _, e := range out {
		enc.WriteBulkString(e)
	}
}

// listSlice resolves the LRANGE index rules: negative indices count from the
// tail, the range is inclusive and clamped, and an empty range yields nil.
func listSlice(elems [][]byte, start, stop int64) [][]byte {
	n := int64(len(elems))
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return nil
	}
	return elems[start : stop+1]
}
