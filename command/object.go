package command

import (
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// objectCommands returns the OBJECT container command and its subcommands. OBJECT
// inspects the internal representation of a key: its encoding, reference count,
// idle time and access frequency.
func objectCommands() []*CmdDesc {
	object := &CmdDesc{
		Name: "object", Group: GroupGeneric, Since: "2.2.3",
		Arity: -2, Flags: FlagReadOnly,
		Handler: handleObjectHelp,
		SubCmds: []*CmdDesc{
			{Name: "encoding", SubName: "object|encoding", Group: GroupGeneric, Since: "2.2.3",
				Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 2, LastKey: 2, Step: 1,
				Handler: handleObjectEncoding},
			{Name: "refcount", SubName: "object|refcount", Group: GroupGeneric, Since: "2.2.3",
				Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 2, LastKey: 2, Step: 1,
				Handler: handleObjectRefcount},
			{Name: "idletime", SubName: "object|idletime", Group: GroupGeneric, Since: "2.2.3",
				Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 2, LastKey: 2, Step: 1,
				Handler: handleObjectIdletime},
			{Name: "freq", SubName: "object|freq", Group: GroupGeneric, Since: "4.0.0",
				Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 2, LastKey: 2, Step: 1,
				Handler: handleObjectFreq},
			{Name: "help", SubName: "object|help", Group: GroupGeneric, Since: "6.2.0",
				Arity: 2, Flags: FlagReadOnly | FlagFast, Handler: handleObjectHelp},
		},
	}
	return []*CmdDesc{object}
}

// noSuchKeyError is the reply every OBJECT subcommand gives for a missing key,
// matching Redis 7.x.
const noSuchKeyError = "ERR no such key"

// handleObjectEncoding returns the logical encoding name of a key as a bulk
// string. The name is what Redis reports for the same data shape and size, not
// the physical paged layout aki stores it in.
func handleObjectEncoding(ctx *Ctx) {
	key := ctx.Argv[2]
	var name string
	var found bool
	if !ctx.view(func(db *keyspace.DB) error {
		_, hdr, ok, err := db.Peek(key)
		if err != nil {
			return err
		}
		found = ok
		if ok {
			name = encodingName(hdr.Encoding)
		}
		return nil
	}) {
		return
	}
	if !found {
		ctx.enc().WriteError(noSuchKeyError)
		return
	}
	ctx.enc().WriteBulkStringStr(name)
}

// handleObjectRefcount always returns 1. aki never shares value objects between
// keys, so every live key has a single reference, which is also what Redis 7.x
// reports for most objects.
func handleObjectRefcount(ctx *Ctx) {
	key := ctx.Argv[2]
	var found bool
	if !ctx.view(func(db *keyspace.DB) error {
		ok, err := db.Exists(key)
		found = ok
		return err
	}) {
		return
	}
	if !found {
		ctx.enc().WriteError(noSuchKeyError)
		return
	}
	ctx.enc().WriteInteger(1)
}

// handleObjectIdletime returns the whole seconds since the key was last accessed.
// It reads without touching the key, so asking for the idle time does not reset
// it. Redis rejects this under an LFU policy, where access time is not tracked.
func handleObjectIdletime(ctx *Ctx) {
	if strings.HasSuffix(ctx.confStr("maxmemory-policy", "noeviction"), "-lfu") {
		ctx.enc().WriteError("ERR An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.")
		return
	}
	key := ctx.Argv[2]
	var found bool
	var idle uint32
	if !ctx.view(func(db *keyspace.DB) error {
		ok, err := db.Exists(key)
		if err != nil {
			return err
		}
		found = ok
		if ok {
			idle = db.Idle(key)
		}
		return nil
	}) {
		return
	}
	if !found {
		ctx.enc().WriteError(noSuchKeyError)
		return
	}
	ctx.enc().WriteInteger(int64(idle))
}

// handleObjectFreq returns the LFU access frequency counter. It is only valid
// under an LFU policy, matching Redis, which tracks frequency only then.
func handleObjectFreq(ctx *Ctx) {
	if !strings.HasSuffix(ctx.confStr("maxmemory-policy", "noeviction"), "-lfu") {
		ctx.enc().WriteError("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.")
		return
	}
	key := ctx.Argv[2]
	var found bool
	var freq uint8
	if !ctx.view(func(db *keyspace.DB) error {
		ok, err := db.Exists(key)
		if err != nil {
			return err
		}
		found = ok
		if ok {
			freq = db.Freq(key)
		}
		return nil
	}) {
		return
	}
	if !found {
		ctx.enc().WriteError(noSuchKeyError)
		return
	}
	ctx.enc().WriteInteger(int64(freq))
}

// handleObjectHelp prints the subcommand summary, matching the shape of the
// Redis OBJECT HELP reply.
func handleObjectHelp(ctx *Ctx) {
	lines := []string{
		"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"ENCODING <key>",
		"    Return the kind of internal representation used in order to store the value associated with a <key>.",
		"FREQ <key>",
		"    Return the access frequency index of the <key>. The returned integer is proportional to the logarithm of the real access frequency.",
		"IDLETIME <key>",
		"    Return the idle time of the <key>, that is the approximated number of seconds elapsed since the last access to the key.",
		"REFCOUNT <key>",
		"    Return the number of references of the value associated with the specified <key>.",
		"HELP",
		"    Print this help.",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteBulkStringStr(l)
	}
}

// encodingName maps a stored encoding code to the string OBJECT ENCODING reports.
// The codes come from keyspace/value.go and match Redis exactly. Code 11 is the
// Redis 7.4 listpackex for a hash that carries per-field TTLs.
func encodingName(e uint8) string {
	switch e {
	case keyspace.EncInt:
		return "int"
	case keyspace.EncEmbStr:
		return "embstr"
	case keyspace.EncRaw:
		return "raw"
	case keyspace.EncListpack:
		return "listpack"
	case keyspace.EncQuicklist:
		return "quicklist"
	case keyspace.EncHashtable:
		return "hashtable"
	case keyspace.EncIntset:
		return "intset"
	case keyspace.EncSkiplist:
		return "skiplist"
	case keyspace.EncStream:
		return "stream"
	case keyspace.EncListpackex:
		return "listpackex"
	default:
		return "raw"
	}
}
