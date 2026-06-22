package command

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/resp"
	"github.com/tamnd/aki/respclient"
)

// migrateCommands returns MIGRATE, which moves one or more keys from this
// instance to a remote Redis-compatible instance (spec 2064 doc 17 section 5).
// It is built on the same DUMP and RESTORE codec the local commands use, plus a
// small outbound RESP client, so a key shipped to a real Redis is byte for byte
// what RESTORE would store there.
func migrateCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "migrate", Group: GroupGeneric, Since: "3.0.0",
			Arity: -6, Flags: FlagWrite | FlagMovableKeys, FirstKey: 3, LastKey: 3, Step: 1,
			Handler: handleMigrate},
	}
}

// migrateArgs holds the parsed MIGRATE invocation.
type migrateArgs struct {
	host    string
	port    string
	destDB  int
	timeout time.Duration
	copy    bool
	replace bool
	auth    [][]byte // AUTH password, or AUTH2 username password, as a ready RESP request; nil when absent
	keys    [][]byte
}

// handleMigrate parses the command, serializes each present key locally, ships
// the payloads to the target with RESTORE, and (unless COPY) deletes the keys it
// moved. It replies +OK when at least one key was migrated, +NOKEY when none of
// the requested keys existed, and the propagated -BUSYKEY or an -IOERR on a
// transport failure.
func handleMigrate(ctx *Ctx) {
	ma, errMsg := parseMigrateArgs(ctx.Argv)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}

	// Serialize every present key under one engine read. A missing key is simply
	// skipped, matching Redis: MIGRATE of an absent key is not an error.
	var (
		items  []migrateItem
		unsupp bool
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		for _, k := range ma.keys {
			_, hdr, found, perr := db.Peek(k)
			if perr != nil {
				return perr
			}
			if !found {
				continue
			}
			v, f, derr := readDumpValue(db, k)
			if derr != nil {
				if derr == errDumpUnsupported {
					unsupp = true
					return nil
				}
				return derr
			}
			if !f {
				continue
			}
			blob, merr := rdb.Marshal(v)
			if merr != nil {
				unsupp = true
				return nil
			}
			ttl := int64(0)
			if hdr.TTLms >= 0 {
				ttl = hdr.TTLms - keyspace.NowMillis()
				if ttl < 1 {
					ttl = 1
				}
			}
			items = append(items, migrateItem{key: append([]byte(nil), k...), ttl: ttl, blob: blob})
		}
		return nil
	})
	if !ok {
		return
	}
	if unsupp {
		ctx.enc().WriteError("ERR MIGRATE of this type is not supported yet")
		return
	}
	if len(items) == 0 {
		ctx.enc().WriteStatus("NOKEY")
		return
	}

	// Talk to the target. The whole exchange shares one socket deadline derived
	// from the millisecond timeout argument.
	cl, err := respclient.Dial(net.JoinHostPort(ma.host, ma.port), ma.timeout)
	if err != nil {
		ctx.enc().WriteError("IOERR error or timeout connecting to target instance")
		return
	}
	defer cl.Close()

	if ma.auth != nil {
		reply, aerr := cl.Call(ma.auth...)
		if aerr != nil {
			ctx.enc().WriteError("IOERR error or timeout writing to target instance")
			return
		}
		if reply.Type == resp.TypeError {
			ctx.enc().WriteError("ERR Target instance replied with error: " + reply.Err)
			return
		}
	}

	selectReq := [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(ma.destDB))}
	if reply, serr := cl.Call(selectReq...); serr != nil {
		ctx.enc().WriteError("IOERR error or timeout writing to target instance")
		return
	} else if reply.Type == resp.TypeError {
		ctx.enc().WriteError("ERR Target instance replied with error: " + reply.Err)
		return
	}

	if _, errMsg := restoreAll(cl, items2restore(items, ma.replace)); errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}

	// Every RESTORE landed. Drop the local copies unless COPY was asked for.
	if !ma.copy {
		keys := make([][]byte, len(items))
		for i, it := range items {
			keys[i] = it.key
		}
		deleted := ctx.update(func(db *keyspace.DB) error {
			for _, k := range keys {
				if _, derr := db.Delete(k); derr != nil {
					return derr
				}
			}
			return nil
		})
		if !deleted {
			return
		}
		for _, k := range keys {
			ctx.notify(notifyGeneric, "del", k)
		}
	}
	ctx.enc().WriteStatus("OK")
}

// migrateItem is one serialized key waiting to be shipped.
type migrateItem struct {
	key  []byte
	ttl  int64 // remaining TTL in ms, 0 when the key has no expiry
	blob []byte
}

// restoreReq is one RESTORE request ready to send.
type restoreReq [][]byte

// items2restore turns the serialized payloads into RESTORE requests.
func items2restore(items []migrateItem, replace bool) []restoreReq {
	reqs := make([]restoreReq, len(items))
	for i, it := range items {
		args := [][]byte{
			[]byte("RESTORE"),
			it.key,
			[]byte(strconv.FormatInt(it.ttl, 10)),
			it.blob,
		}
		if replace {
			args = append(args, []byte("REPLACE"))
		}
		reqs[i] = args
	}
	return reqs
}

// parseMigrateArgs reads the fixed positionals and the optional clauses. It
// returns an error message string (empty on success) so the caller can write it
// straight to the client.
func parseMigrateArgs(argv [][]byte) (migrateArgs, string) {
	var ma migrateArgs
	ma.host = string(argv[1])
	ma.port = string(argv[2])
	keyArg := argv[3]

	destDB, ok := parseInteger(argv[4])
	if !ok || destDB < 0 {
		return ma, "ERR DB index is out of range"
	}
	ma.destDB = int(destDB)

	timeoutMs, ok := parseInteger(argv[5])
	if !ok || timeoutMs < 0 {
		return ma, "ERR timeout is not an integer or out of range"
	}
	ma.timeout = time.Duration(timeoutMs) * time.Millisecond

	var (
		authPass []byte
		authUser []byte
		auth2    bool
		hasAuth  bool
	)
	i := 6
	for i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "COPY":
			ma.copy = true
			i++
		case "REPLACE":
			ma.replace = true
			i++
		case "AUTH":
			if i+1 >= len(argv) {
				return ma, "ERR syntax error"
			}
			authPass = argv[i+1]
			hasAuth = true
			i += 2
		case "AUTH2":
			if i+2 >= len(argv) {
				return ma, "ERR syntax error"
			}
			authUser = argv[i+1]
			authPass = argv[i+2]
			auth2 = true
			hasAuth = true
			i += 3
		case "KEYS":
			if len(keyArg) != 0 {
				return ma, "ERR When using MIGRATE KEYS option, the key argument must be set to the empty string"
			}
			ma.keys = argv[i+1:]
			i = len(argv)
		default:
			return ma, "ERR syntax error"
		}
	}

	if ma.keys == nil {
		ma.keys = [][]byte{keyArg}
	}
	if len(ma.keys) == 0 {
		return ma, "ERR syntax error"
	}

	if hasAuth {
		if auth2 {
			ma.auth = [][]byte{[]byte("AUTH"), authUser, authPass}
		} else {
			ma.auth = [][]byte{[]byte("AUTH"), authPass}
		}
	}
	return ma, ""
}

// restoreAll pipelines every RESTORE then reads the replies in order, the single
// round trip Redis uses for MIGRATE with KEYS. It returns busy true with the
// -BUSYKEY message when the target rejects an existing key without REPLACE, or a
// generic error message for any other failure.
func restoreAll(cl *respclient.Client, reqs []restoreReq) (busy bool, errMsg string) {
	for _, r := range reqs {
		if err := cl.Send(r); err != nil {
			return false, "IOERR error or timeout writing to target instance"
		}
	}
	for range reqs {
		reply, err := cl.ReadReply()
		if err != nil {
			return false, "IOERR error or timeout reading from target instance"
		}
		if reply.Type == resp.TypeError {
			if strings.HasPrefix(reply.Err, "BUSYKEY") {
				return true, reply.Err
			}
			return false, "ERR Target instance replied with error: " + reply.Err
		}
	}
	return false, ""
}
