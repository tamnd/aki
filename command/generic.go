package command

import (
	"github.com/tamnd/aki/keyspace"
)

// wrongTypeError is the reply when a command is run against a key holding a
// different value type. Redis uses this exact string.
const wrongTypeError = "WRONGTYPE Operation against a key holding the wrong kind of value"

// genericCommands returns the generic key-group command table for this slice:
// the commands that work on any key regardless of its value type. SCAN, KEYS,
// RENAME, EXPIRE and friends land in later slices.
func genericCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "del", Group: GroupGeneric, Since: "1.0.0",
			Arity: -2, Flags: FlagWrite, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handleDel},
		{Name: "exists", Group: GroupGeneric, Since: "1.0.0",
			Arity: -2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handleExists},
		{Name: "type", Group: GroupGeneric, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleType},
		{Name: "dbsize", Group: GroupServer, Since: "1.0.0",
			Arity: 1, Flags: FlagReadOnly | FlagFast, Handler: handleDbsize},
	}
}

// handleDel removes one or more keys and returns how many were actually
// deleted. UNLINK shares this handler once it is added.
func handleDel(ctx *Ctx) {
	keys := ctx.Argv[1:]
	var removed int64
	if ctx.update(func(db *keyspace.DB) error {
		for _, k := range keys {
			ok, err := db.Delete(k)
			if err != nil {
				return err
			}
			if ok {
				removed++
			}
		}
		return nil
	}) {
		ctx.enc().WriteInteger(removed)
	}
}

// handleExists counts how many of the given keys exist. A key named more than
// once is counted each time, matching Redis.
func handleExists(ctx *Ctx) {
	keys := ctx.Argv[1:]
	var count int64
	if ctx.view(func(db *keyspace.DB) error {
		for _, k := range keys {
			ok, err := db.Exists(k)
			if err != nil {
				return err
			}
			if ok {
				count++
			}
		}
		return nil
	}) {
		ctx.enc().WriteInteger(count)
	}
}

// handleType returns the value type of a key as a simple string, or none when
// the key does not exist.
func handleType(ctx *Ctx) {
	key := ctx.Argv[1]
	name := "none"
	if ctx.view(func(db *keyspace.DB) error {
		_, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found {
			name = typeName(hdr.Type)
		}
		return nil
	}) {
		ctx.enc().WriteStatus(name)
	}
}

// handleDbsize returns the number of keys in the current database.
func handleDbsize(ctx *Ctx) {
	var n int64
	if ctx.view(func(db *keyspace.DB) error {
		n = int64(db.Len())
		return nil
	}) {
		ctx.enc().WriteInteger(n)
	}
}

// typeName maps a value type code to the name TYPE reports.
func typeName(t uint8) string {
	switch t {
	case keyspace.TypeString:
		return "string"
	case keyspace.TypeList:
		return "list"
	case keyspace.TypeHash:
		return "hash"
	case keyspace.TypeSet:
		return "set"
	case keyspace.TypeZSet:
		return "zset"
	case keyspace.TypeStream:
		return "stream"
	default:
		return "none"
	}
}
