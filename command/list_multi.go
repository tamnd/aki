package command

import (
	"bytes"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// listMultiCommands returns the multi-key and search list commands: RPOPLPUSH,
// LMOVE, LPOS and LMPOP (doc 09 §6.12 through §6.14). The blocking variants of
// these moves wait for the blocking-registry milestone.
func listMultiCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "rpoplpush", Group: GroupList, Since: "1.2.0",
			Arity: 3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: func(ctx *Ctx) { listMove(ctx, ctx.Argv[1], ctx.Argv[2], false, true) }},
		{Name: "lmove", Group: GroupList, Since: "6.2.0",
			Arity: 5, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleLMove},
		{Name: "lpos", Group: GroupList, Since: "6.0.6",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLPos},
		{Name: "lmpop", Group: GroupList, Since: "7.0.0",
			Arity: -4, Flags: FlagWrite, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: handleLMPop},
	}
}

// handleLMove parses the LEFT|RIGHT direction words and delegates to listMove.
func handleLMove(ctx *Ctx) {
	fromLeft, ok1 := parseLeftRight(ctx.Argv[3])
	toLeft, ok2 := parseLeftRight(ctx.Argv[4])
	if !ok1 || !ok2 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	listMove(ctx, ctx.Argv[1], ctx.Argv[2], fromLeft, toLeft)
}

// parseLeftRight reads a LEFT or RIGHT word, returning true for LEFT.
func parseLeftRight(arg []byte) (left bool, ok bool) {
	switch strings.ToUpper(string(arg)) {
	case "LEFT":
		return true, true
	case "RIGHT":
		return false, true
	default:
		return false, false
	}
}

// listMove pops one element from src (head when fromLeft) and pushes it onto dst
// (head when toLeft), all in one update so no client sees the intermediate
// state. It returns the moved element, nil when src is empty or absent, or
// WRONGTYPE when either key holds a non-list. src and dst may be the same key.
func listMove(ctx *Ctx, src, dst []byte, fromLeft, toLeft bool) {
	var (
		wrongTyp   bool
		absent     bool
		srcEmptied bool
		moved      []byte
	)
	done := ctx.update(func(db *keyspace.DB) error {
		srcBody, srcHdr, srcFound, err := db.Get(src)
		if err != nil {
			return err
		}
		if !srcFound {
			absent = true
			return nil
		}
		if srcHdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		sameKey := bytes.Equal(src, dst)
		var (
			dstBody  []byte
			dstHdr   keyspace.ValueHeader
			dstFound bool
		)
		if !sameKey {
			dstBody, dstHdr, dstFound, err = db.Get(dst)
			if err != nil {
				return err
			}
			if dstFound && dstHdr.Type != keyspace.TypeList {
				wrongTyp = true
				return nil
			}
		}
		srcElems, err := listDecode(srcBody)
		if err != nil {
			return err
		}
		if len(srcElems) == 0 {
			absent = true
			return nil
		}
		var elem []byte
		elem, srcElems = popEnd(srcElems, fromLeft)
		moved = elem

		if sameKey {
			srcElems = pushEnd(srcElems, elem, toLeft)
			return db.Set(src, listEncode(srcElems), keyspace.TypeList,
				listEncoding(ctx.encLimits(), srcElems, srcHdr.Encoding), keepTTL(srcHdr, srcFound))
		}

		if len(srcElems) == 0 {
			srcEmptied = true
			if _, err := db.Delete(src); err != nil {
				return err
			}
		} else if err := db.Set(src, listEncode(srcElems), keyspace.TypeList,
			listEncoding(ctx.encLimits(), srcElems, srcHdr.Encoding), keepTTL(srcHdr, srcFound)); err != nil {
			return err
		}

		dstElems, err := listDecode(dstBody)
		if err != nil {
			return err
		}
		dstElems = pushEnd(dstElems, elem, toLeft)
		dstPrev := uint8(keyspace.EncListpack)
		if dstFound {
			dstPrev = dstHdr.Encoding
		}
		return db.Set(dst, listEncode(dstElems), keyspace.TypeList,
			listEncoding(ctx.encLimits(), dstElems, dstPrev), keepTTL(dstHdr, dstFound))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if absent {
		ctx.enc().WriteNull()
		return
	}
	fromEvent := "rpop"
	if fromLeft {
		fromEvent = "lpop"
	}
	toEvent := "rpush"
	if toLeft {
		toEvent = "lpush"
	}
	ctx.notify(notifyList, fromEvent, src)
	ctx.notify(notifyList, toEvent, dst)
	if srcEmptied {
		ctx.notify(notifyGeneric, "del", src)
	}
	ctx.signalReady(dst)
	ctx.enc().WriteBulkString(moved)
}

// popEnd removes one element from the head (fromLeft) or tail of elems and
// returns it with the remaining slice.
func popEnd(elems [][]byte, fromLeft bool) (elem []byte, rest [][]byte) {
	if fromLeft {
		return elems[0], elems[1:]
	}
	return elems[len(elems)-1], elems[:len(elems)-1]
}

// pushEnd adds elem to the head (toLeft) or tail of elems.
func pushEnd(elems [][]byte, elem []byte, toLeft bool) [][]byte {
	if toLeft {
		return append([][]byte{elem}, elems...)
	}
	return append(elems, elem)
}

// handleLPos implements LPOS key element [RANK rank] [COUNT count] [MAXLEN n].
// Without COUNT it replies the first match as an integer, or nil. With COUNT it
// always replies an array of indices, possibly empty.
func handleLPos(ctx *Ctx) {
	key := ctx.Argv[1]
	element := ctx.Argv[2]
	rank := int64(1)
	count := int64(0)
	hasCount := false
	maxlen := int64(0)

	args := ctx.Argv[3:]
	for i := 0; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		if i+1 >= len(args) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		val, ok := parseInteger(args[i+1])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		i++
		switch opt {
		case "RANK":
			if val == 0 {
				ctx.enc().WriteError("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
				return
			}
			rank = val
		case "COUNT":
			if val < 0 {
				ctx.enc().WriteError("ERR COUNT can't be negative")
				return
			}
			count = val
			hasCount = true
		case "MAXLEN":
			if val < 0 {
				ctx.enc().WriteError("ERR MAXLEN can't be negative")
				return
			}
			maxlen = val
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	var (
		wrongTyp bool
		matches  []int64
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
		matches = lposScan(elems, element, rank, count, hasCount, maxlen)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	if !hasCount {
		if len(matches) == 0 {
			enc.WriteNull()
			return
		}
		enc.WriteInteger(matches[0])
		return
	}
	enc.WriteArrayLen(len(matches))
	for _, m := range matches {
		enc.WriteInteger(m)
	}
}

// lposScan returns the matching indices for LPOS. A positive rank scans head to
// tail, a negative rank scans tail to head, and rankAbs selects which match to
// start returning from. maxlen caps how many elements are compared. When
// hasCount is false only the first match is returned; otherwise count limits the
// result, with zero meaning all matches.
func lposScan(elems [][]byte, element []byte, rank, count int64, hasCount bool, maxlen int64) []int64 {
	rankAbs := rank
	backward := false
	if rank < 0 {
		rankAbs = -rank
		backward = true
	}

	var matches []int64
	matched := int64(0)
	compared := int64(0)
	scan := func(idx int) bool {
		if maxlen > 0 && compared >= maxlen {
			return false
		}
		compared++
		if !bytes.Equal(elems[idx], element) {
			return true
		}
		matched++
		if matched < rankAbs {
			return true
		}
		matches = append(matches, int64(idx))
		if !hasCount {
			return false
		}
		if count > 0 && int64(len(matches)) >= count {
			return false
		}
		return true
	}
	if backward {
		for i := len(elems) - 1; i >= 0; i-- {
			if !scan(i) {
				break
			}
		}
	} else {
		for i := range elems {
			if !scan(i) {
				break
			}
		}
	}
	return matches
}

// handleLMPop implements LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count].
// It pops up to count elements from the first non-empty list among the keys.
func handleLMPop(ctx *Ctx) {
	numkeys, ok := parseInteger(ctx.Argv[1])
	if !ok {
		ctx.enc().WriteError("ERR numkeys should be greater than 0")
		return
	}
	if numkeys < 0 {
		ctx.enc().WriteError("ERR numkeys can't be negative")
		return
	}
	if numkeys == 0 {
		ctx.enc().WriteError("ERR numkeys can't be zero")
		return
	}
	// argv: LMPOP numkeys key... direction [COUNT count]
	keyStart := 2
	dirIdx := keyStart + int(numkeys)
	if dirIdx >= len(ctx.Argv) {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	keys := ctx.Argv[keyStart:dirIdx]
	fromLeft, okDir := parseLeftRight(ctx.Argv[dirIdx])
	if !okDir {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	count := int64(1)
	rest := ctx.Argv[dirIdx+1:]
	if len(rest) > 0 {
		if len(rest) != 2 || strings.ToUpper(string(rest[0])) != "COUNT" {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		c, okc := parseInteger(rest[1])
		if !okc {
			ctx.enc().WriteError("ERR count should be greater than 0")
			return
		}
		if c < 0 {
			ctx.enc().WriteError("ERR COUNT can't be negative")
			return
		}
		if c == 0 {
			ctx.enc().WriteError("ERR COUNT can't be zero")
			return
		}
		count = c
	}

	var (
		wrongTyp  bool
		emptied   bool
		poppedKey []byte
		popped    [][]byte
	)
	done := ctx.update(func(db *keyspace.DB) error {
		for _, key := range keys {
			body, hdr, found, err := db.Get(key)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if hdr.Type != keyspace.TypeList {
				wrongTyp = true
				return nil
			}
			elems, err := listDecode(body)
			if err != nil {
				return err
			}
			if len(elems) == 0 {
				continue
			}
			n := int(min(count, int64(len(elems))))
			var leftover [][]byte
			if fromLeft {
				popped = elems[:n]
				leftover = elems[n:]
			} else {
				tail := elems[len(elems)-n:]
				popped = make([][]byte, n)
				for i := range tail {
					popped[i] = tail[n-1-i]
				}
				leftover = elems[:len(elems)-n]
			}
			poppedKey = key
			if len(leftover) == 0 {
				emptied = true
				_, err := db.Delete(key)
				return err
			}
			return db.Set(key, listEncode(leftover), keyspace.TypeList,
				listEncoding(ctx.encLimits(), leftover, hdr.Encoding), keepTTL(hdr, found))
		}
		return nil
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if poppedKey != nil {
		event := "rpop"
		if fromLeft {
			event = "lpop"
		}
		ctx.notify(notifyList, event, poppedKey)
		if emptied {
			ctx.notify(notifyGeneric, "del", poppedKey)
		}
	}
	enc := ctx.enc()
	if poppedKey == nil {
		enc.WriteNullArray()
		return
	}
	enc.WriteArrayLen(2)
	enc.WriteBulkString(poppedKey)
	enc.WriteArrayLen(len(popped))
	for _, e := range popped {
		enc.WriteBulkString(e)
	}
}
