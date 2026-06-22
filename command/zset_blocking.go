package command

import (
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// This file implements the blocking sorted-set commands (doc 11 §9): BZPOPMIN,
// BZPOPMAX and BZMPOP. They reuse the wait protocol from blocking.go (blockDrive,
// blockRegister, serveReady, propagateBlocking) and only differ in the per-key
// pop and the reply shape. A ZADD or store op signals the key ready, and the
// woken handler pops the lowest or highest member.

// blockingZSetCommands returns the blocking sorted-set command table (doc 11 §9).
func blockingZSetCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "bzpopmin", Group: GroupSortedSet, Since: "5.0.0",
			Arity: -3, Flags: FlagWrite | FlagFast | FlagBlocking, FirstKey: 1, LastKey: -2, Step: 1,
			Handler: func(ctx *Ctx) { blockZPop(ctx, true) }},
		{Name: "bzpopmax", Group: GroupSortedSet, Since: "5.0.0",
			Arity: -3, Flags: FlagWrite | FlagFast | FlagBlocking, FirstKey: 1, LastKey: -2, Step: 1,
			Handler: func(ctx *Ctx) { blockZPop(ctx, false) }},
		{Name: "bzmpop", Group: GroupSortedSet, Since: "7.0.0",
			Arity: -5, Flags: FlagWrite | FlagBlocking, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: handleBZMPop},
	}
}

// blockZPop implements BZPOPMIN (fromMin) and BZPOPMAX. It pops one member from
// the first key that has one, replying with a three-element array of the key, the
// member, and its score. On timeout it replies with a null array.
func blockZPop(ctx *Ctx, fromMin bool) {
	keys := ctx.Argv[1 : len(ctx.Argv)-1]
	timeout, errMsg := parseTimeout(ctx.Argv[len(ctx.Argv)-1])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	db := ctx.Conn.DB()
	attempt := func() bool {
		var (
			poppedKey []byte
			popped    zmember
			emptied   bool
			wrongTyp  bool
		)
		done := ctx.update(func(d *keyspace.DB) error {
			for _, key := range keys {
				members, hdr, found, err := getZSet(d, key)
				if err != nil {
					return err
				}
				if found && hdr.Type != keyspace.TypeZSet {
					wrongTyp = true
					return nil
				}
				if !found || len(members) == 0 {
					continue
				}
				var kept []zmember
				if fromMin {
					popped = members[0]
					kept = members[1:]
				} else {
					popped = members[len(members)-1]
					kept = members[:len(members)-1]
				}
				poppedKey = key
				if len(kept) == 0 {
					emptied = true
					_, err := d.Delete(key)
					return err
				}
				return d.Set(key, zsetEncode(kept), keyspace.TypeZSet,
					zsetEncoding(ctx.encLimits(), kept, hdr.Encoding), keepTTL(hdr, found))
			}
			return nil
		})
		if !done {
			return true
		}
		if wrongTyp {
			ctx.enc().WriteError(wrongTypeError)
			return true
		}
		if poppedKey == nil {
			return false
		}
		event := "zpopmax"
		resolved := "ZPOPMAX"
		if fromMin {
			event = "zpopmin"
			resolved = "ZPOPMIN"
		}
		ctx.notify(notifyZset, event, poppedKey)
		if emptied {
			ctx.notify(notifyGeneric, "del", poppedKey)
		}
		ctx.d.trackingInvalidateKey(poppedKey, ctx.Conn.ID())
		ctx.d.propagateBlocking(db, [][]byte{[]byte(resolved), poppedKey})
		if !emptied {
			ctx.d.serveReady(db, poppedKey, ctx.Conn.ID())
		}
		enc := ctx.enc()
		enc.WriteArrayLen(3)
		enc.WriteBulkString(poppedKey)
		enc.WriteBulkString(popped.member)
		enc.WriteDouble(popped.score)
		return true
	}
	ctx.d.blockDrive(ctx, keys, timeout, attempt, func() { ctx.enc().WriteNullArray() })
}

// handleBZMPop implements BZMPOP timeout numkeys key [key ...] MIN|MAX
// [COUNT count], the blocking form of ZMPOP. It pops from the first non-empty
// key and replies with the key and its popped member/score pairs.
func handleBZMPop(ctx *Ctx) {
	timeout, errMsg := parseTimeout(ctx.Argv[1])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	numkeys, ok := parseInteger(ctx.Argv[2])
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
	keyStart := 3
	dirIdx := keyStart + int(numkeys)
	if dirIdx >= len(ctx.Argv) {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	keys := ctx.Argv[keyStart:dirIdx]
	var fromMax bool
	switch strings.ToUpper(string(ctx.Argv[dirIdx])) {
	case "MIN":
		fromMax = false
	case "MAX":
		fromMax = true
	default:
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	count := int64(1)
	rest := ctx.Argv[dirIdx+1:]
	if len(rest) > 0 {
		if len(rest) != 2 || !strings.EqualFold(string(rest[0]), "COUNT") {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		c, okc := parseInteger(rest[1])
		if !okc {
			ctx.enc().WriteError("ERR count should be greater than 0")
			return
		}
		if c < 1 {
			ctx.enc().WriteError("ERR count should be greater than 0")
			return
		}
		count = c
	}

	db := ctx.Conn.DB()
	attempt := func() bool {
		var (
			poppedKey []byte
			popped    []zmember
			emptied   bool
			wrongTyp  bool
		)
		done := ctx.update(func(d *keyspace.DB) error {
			for _, key := range keys {
				members, hdr, found, err := getZSet(d, key)
				if err != nil {
					return err
				}
				if found && hdr.Type != keyspace.TypeZSet {
					wrongTyp = true
					return nil
				}
				if !found || len(members) == 0 {
					continue
				}
				n := int(min(count, int64(len(members))))
				var kept []zmember
				if fromMax {
					popped = make([]zmember, n)
					for i := range n {
						popped[i] = members[len(members)-1-i]
					}
					kept = members[:len(members)-n]
				} else {
					popped = append(popped, members[:n]...)
					kept = members[n:]
				}
				poppedKey = key
				if len(kept) == 0 {
					emptied = true
					_, err := d.Delete(key)
					return err
				}
				return d.Set(key, zsetEncode(kept), keyspace.TypeZSet,
					zsetEncoding(ctx.encLimits(), kept, hdr.Encoding), keepTTL(hdr, found))
			}
			return nil
		})
		if !done {
			return true
		}
		if wrongTyp {
			ctx.enc().WriteError(wrongTypeError)
			return true
		}
		if poppedKey == nil {
			return false
		}
		event := "zpopmin"
		resolved := "ZPOPMIN"
		if fromMax {
			event = "zpopmax"
			resolved = "ZPOPMAX"
		}
		ctx.notify(notifyZset, event, poppedKey)
		if emptied {
			ctx.notify(notifyGeneric, "del", poppedKey)
		}
		ctx.d.trackingInvalidateKey(poppedKey, ctx.Conn.ID())
		ctx.d.propagateBlocking(db, [][]byte{
			[]byte(resolved), poppedKey, []byte(strconv.Itoa(len(popped)))})
		if !emptied {
			ctx.d.serveReady(db, poppedKey, ctx.Conn.ID())
		}
		enc := ctx.enc()
		enc.WriteArrayLen(2)
		enc.WriteBulkString(poppedKey)
		writeScoredPairs(enc, popped)
		return true
	}
	ctx.d.blockDrive(ctx, keys, timeout, attempt, func() { ctx.enc().WriteNullArray() })
}
