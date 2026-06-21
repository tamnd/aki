package command

import (
	"bytes"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// keyopsCommands returns the generic commands that move, copy or rename a key:
// RENAME, RENAMENX, COPY, MOVE, TOUCH and UNLINK. They all carry a value verbatim
// from one name or database to another, keeping its type, encoding and TTL.
func keyopsCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "rename", Group: GroupGeneric, Since: "1.0.0",
			Arity: 3, Flags: FlagWrite, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleRename},
		{Name: "renamenx", Group: GroupGeneric, Since: "1.0.0",
			Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleRenameNX},
		{Name: "copy", Group: GroupGeneric, Since: "6.2.0",
			Arity: -3, Flags: FlagWrite, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleCopy},
		{Name: "move", Group: GroupGeneric, Since: "1.0.0",
			Arity: 3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleMove},
		{Name: "touch", Group: GroupGeneric, Since: "3.2.1",
			Arity: -2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handleTouch},
		{Name: "unlink", Group: GroupGeneric, Since: "4.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handleDel},
	}
}

// handleRename renames src to dst, overwriting dst if it exists. The new key
// takes src's TTL. A missing src is an error; renaming a key to itself is a
// no-op that still replies OK.
func handleRename(ctx *Ctx) {
	src, dst := ctx.Argv[1], ctx.Argv[2]
	var noKey, renamed bool
	if !ctx.update(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(src)
		if err != nil {
			return err
		}
		if !found {
			noKey = true
			return nil
		}
		if bytes.Equal(src, dst) {
			return nil
		}
		if err := db.Set(dst, body, hdr.Type, hdr.Encoding, keepTTL(hdr, true)); err != nil {
			return err
		}
		if _, err := db.Delete(src); err != nil {
			return err
		}
		renamed = true
		return nil
	}) {
		return
	}
	if noKey {
		ctx.enc().WriteError("ERR no such key")
		return
	}
	if renamed {
		ctx.notify(notifyGeneric, "rename_from", src)
		ctx.notify(notifyGeneric, "rename_to", dst)
	}
	ctx.enc().WriteStatus("OK")
}

// handleRenameNX renames src to dst only when dst does not already exist. It
// replies 1 on a rename and 0 when dst is taken. A missing src is an error.
func handleRenameNX(ctx *Ctx) {
	src, dst := ctx.Argv[1], ctx.Argv[2]
	var noKey bool
	var res int64
	if !ctx.update(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(src)
		if err != nil {
			return err
		}
		if !found {
			noKey = true
			return nil
		}
		exists, err := db.Exists(dst)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		if err := db.Set(dst, body, hdr.Type, hdr.Encoding, keepTTL(hdr, true)); err != nil {
			return err
		}
		if _, err := db.Delete(src); err != nil {
			return err
		}
		res = 1
		return nil
	}) {
		return
	}
	if noKey {
		ctx.enc().WriteError("ERR no such key")
		return
	}
	if res == 1 {
		ctx.notify(notifyGeneric, "rename_from", src)
		ctx.notify(notifyGeneric, "rename_to", dst)
	}
	ctx.enc().WriteInteger(res)
}

// handleTouch counts how many of the given keys exist, applying lazy expiry. The
// LRU/LFU bump it would carry waits for the eviction milestone.
func handleTouch(ctx *Ctx) {
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

// handleMove moves a key to another database, keeping its value and TTL. It
// replies 0 when the source is missing or the destination already holds the key,
// and 1 on a move.
func handleMove(ctx *Ctx) {
	key := ctx.Argv[1]
	dbid, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	cur := ctx.Conn.DB()
	var res int64
	var errStr string
	if !ctx.updateKeyspace(func(ks *keyspace.Keyspace) error {
		if dbid < 0 || int(dbid) >= ks.DBCount() {
			errStr = "ERR DB index is out of range"
			return nil
		}
		if int(dbid) == cur {
			errStr = "ERR source and destination objects are the same"
			return nil
		}
		srcDB, err := ks.DB(cur)
		if err != nil {
			return err
		}
		dstDB, err := ks.DB(int(dbid))
		if err != nil {
			return err
		}
		body, hdr, found, err := srcDB.Get(key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		exists, err := dstDB.Exists(key)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		if err := dstDB.Set(key, body, hdr.Type, hdr.Encoding, keepTTL(hdr, true)); err != nil {
			return err
		}
		if _, err := srcDB.Delete(key); err != nil {
			return err
		}
		res = 1
		return nil
	}) {
		return
	}
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	if res == 1 {
		ctx.d.notifyKeyspaceEvent(cur, notifyGeneric, "move_from", string(key))
		ctx.d.notifyKeyspaceEvent(int(dbid), notifyGeneric, "move_to", string(key))
	}
	ctx.enc().WriteInteger(res)
}

// handleCopy copies src to dst, optionally into another database with DB and
// overwriting an existing dst with REPLACE. The copy keeps the value's type,
// encoding and TTL. It replies 1 on a copy and 0 when dst is taken without
// REPLACE or the source is missing.
func handleCopy(ctx *Ctx) {
	src, dst := ctx.Argv[1], ctx.Argv[2]
	cur := ctx.Conn.DB()
	destDB := cur
	replace := false
	for i := 3; i < len(ctx.Argv); {
		opt := strings.ToUpper(string(ctx.Argv[i]))
		switch opt {
		case "DB":
			if i+1 >= len(ctx.Argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			n, ok := parseInteger(ctx.Argv[i+1])
			if !ok {
				ctx.enc().WriteError("ERR value is not an integer or out of range")
				return
			}
			destDB = int(n)
			i += 2
		case "REPLACE":
			replace = true
			i++
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	var res int64
	var errStr string
	if !ctx.updateKeyspace(func(ks *keyspace.Keyspace) error {
		if destDB < 0 || destDB >= ks.DBCount() {
			errStr = "ERR DB index is out of range"
			return nil
		}
		if destDB == cur && bytes.Equal(src, dst) {
			errStr = "ERR source and destination objects are the same"
			return nil
		}
		srcDB, err := ks.DB(cur)
		if err != nil {
			return err
		}
		dstDBh, err := ks.DB(destDB)
		if err != nil {
			return err
		}
		body, hdr, found, err := srcDB.Get(src)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		exists, err := dstDBh.Exists(dst)
		if err != nil {
			return err
		}
		if exists {
			if !replace {
				return nil
			}
			if _, err := dstDBh.Delete(dst); err != nil {
				return err
			}
		}
		if err := dstDBh.Set(dst, body, hdr.Type, hdr.Encoding, keepTTL(hdr, true)); err != nil {
			return err
		}
		res = 1
		return nil
	}) {
		return
	}
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	if res == 1 {
		ctx.d.notifyKeyspaceEvent(destDB, notifyGeneric, "copy_to", string(dst))
	}
	ctx.enc().WriteInteger(res)
}
