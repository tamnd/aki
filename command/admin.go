package command

import (
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// adminCommands returns the database-admin commands FLUSHDB, FLUSHALL and
// SWAPDB.
func adminCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "flushdb", Group: GroupServer, Since: "1.0.0",
			Arity: -1, Flags: FlagWrite, Handler: handleFlushDB},
		{Name: "flushall", Group: GroupServer, Since: "1.0.0",
			Arity: -1, Flags: FlagWrite, Handler: handleFlushAll},
		{Name: "swapdb", Group: GroupServer, Since: "4.0.0",
			Arity: 3, Flags: FlagWrite | FlagFast, Handler: handleSwapDB},
	}
}

// flushMode validates the optional ASYNC or SYNC token. aki always flushes
// synchronously, so the token only has to parse. It reports an error string when
// the tail is not a single ASYNC or SYNC word.
func flushMode(argv [][]byte) (string, bool) {
	switch len(argv) {
	case 1:
		return "", true
	case 2:
		switch strings.ToUpper(string(argv[1])) {
		case "ASYNC", "SYNC":
			return "", true
		}
	}
	return "ERR syntax error", false
}

// handleFlushDB empties the current database.
func handleFlushDB(ctx *Ctx) {
	if errStr, ok := flushMode(ctx.Argv); !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	if ctx.update(func(db *keyspace.DB) error {
		db.Flush()
		return nil
	}) {
		ctx.enc().WriteStatus("OK")
	}
}

// handleFlushAll empties every database.
func handleFlushAll(ctx *Ctx) {
	if errStr, ok := flushMode(ctx.Argv); !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	if ctx.updateKeyspace(func(ks *keyspace.Keyspace) error {
		for i := range ks.DBCount() {
			db, err := ks.DB(i)
			if err != nil {
				return err
			}
			db.Flush()
		}
		return nil
	}) {
		ctx.enc().WriteStatus("OK")
	}
}

// handleSwapDB swaps two databases. Swapping a database with itself replies OK
// without writing.
func handleSwapDB(ctx *Ctx) {
	id1, ok1 := parseInteger(ctx.Argv[1])
	id2, ok2 := parseInteger(ctx.Argv[2])
	if !ok1 || !ok2 {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	var errStr string
	if !ctx.updateKeyspace(func(ks *keyspace.Keyspace) error {
		n := int64(ks.DBCount())
		if id1 < 0 || id1 >= n || id2 < 0 || id2 >= n {
			errStr = "ERR DB index is out of range"
			return nil
		}
		return ks.Swap(int(id1), int(id2))
	}) {
		return
	}
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	ctx.enc().WriteStatus("OK")
}
