package command

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/keyspace"
)

// This file implements the DEBUG command (doc 20 §7): the low-level test and
// introspection hooks the redis test suites lean on. aki supports the safe
// subset and rejects the crash-injection options outright.

func debugCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "debug", Group: GroupServer, Since: "1.0.0",
			Arity: -2, Flags: FlagAdmin | FlagLoading | FlagStale, Handler: handleDebug},
	}
}

// debugMaxSleepSeconds caps DEBUG SLEEP so a stray test cannot wedge a
// connection for an unbounded time.
const debugMaxSleepSeconds = 100.0

func handleDebug(ctx *Ctx) {
	sub := strings.ToUpper(string(ctx.Argv[1]))
	switch sub {
	case "OBJECT":
		debugObject(ctx)
	case "SLEEP":
		debugSleep(ctx)
	case "SET-ACTIVE-EXPIRE":
		debugSetActiveExpire(ctx)
	case "RELOAD":
		debugReload(ctx)
	case "QUICKLIST-PACKED-THRESHOLD":
		// Accepted no-op. This tunes the listpack-to-plain quicklist threshold,
		// machinery aki's paged storage does not expose at runtime.
		ctx.enc().WriteStatus("OK")
	case "CHANGE-REPL-ID":
		debugChangeReplID(ctx)
	case "JMAP":
		debugJMAP(ctx)
	case "STRINGMATCH-LEN":
		debugStringmatchLen(ctx)
	case "FLUSHALL":
		debugFlushAll(ctx)
	case "LOADAOF":
		debugLoadAOF(ctx)
	case "SEGFAULT", "PANIC", "OOM":
		ctx.enc().WriteError("ERR DEBUG " + sub + " is disabled in aki")
	case "AOFSTATS", "DISABLE-REPLICATION-CACHING", "GETANDPROPAG", "SFLAGS", "SETOBJ":
		ctx.enc().WriteError("ERR not supported")
	default:
		ctx.enc().WriteError("ERR Unknown DEBUG option '" + string(ctx.Argv[1]) + "'")
	}
}

// debugObject prints the internal-details line for a key. The pointer and LRU
// fields are reported as zero because aki stores values in paged form, not as
// long-lived heap objects with an address or an LRU clock.
func debugObject(ctx *Ctx) {
	if len(ctx.Argv) != 3 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'debug|object' command")
		return
	}
	key := ctx.Argv[2]
	var (
		found  bool
		enc    string
		typ    string
		serlen int
		isQL   bool
	)
	if !ctx.view(func(db *keyspace.DB) error {
		body, hdr, ok, err := db.Get(key)
		if err != nil {
			return err
		}
		found = ok
		if ok {
			enc = encodingName(hdr.Encoding)
			typ = typeName(hdr.Type)
			serlen = len(body)
			isQL = hdr.Encoding == keyspace.EncQuicklist
		}
		return nil
	}) {
		return
	}
	if !found {
		ctx.enc().WriteError(noSuchKeyError)
		return
	}
	line := fmt.Sprintf("Value at:0x0 refcount:1 encoding:%s serializedlength:%d lru:0 lru_seconds_idle:0 type:%s",
		enc, serlen, typ)
	if isQL {
		line += " ql_nodes:1 ql_avg:0.00 ql_ziplist_max:0 ql_compressed:0 ql_uncompressed:1"
	}
	ctx.enc().WriteBulkStringStr(line)
}

// debugSleep blocks this connection for the given number of seconds. It blocks
// only the calling goroutine, not the whole server, which is enough for the test
// suites that use it to hold a slot open.
func debugSleep(ctx *Ctx) {
	if len(ctx.Argv) != 3 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'debug|sleep' command")
		return
	}
	secs, err := strconv.ParseFloat(string(ctx.Argv[2]), 64)
	if err != nil || secs < 0 {
		ctx.enc().WriteError("ERR invalid value for seconds")
		return
	}
	secs = min(secs, debugMaxSleepSeconds)
	time.Sleep(time.Duration(secs * float64(time.Second)))
	ctx.enc().WriteStatus("OK")
}

// debugSetActiveExpire turns the background expiry cycle on or off. Tests use the
// 0 form to freeze a key with a past TTL in place so they can observe lazy expiry
// in isolation. A non-zero value re-enables the cycle.
func debugSetActiveExpire(ctx *Ctx) {
	if len(ctx.Argv) != 3 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'debug|set-active-expire' command")
		return
	}
	v, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	ctx.d.activeExpire.Store(v != 0)
	ctx.enc().WriteStatus("OK")
}

// debugStringmatchLen runs the glob engine against a pattern and string so the
// test suite can verify KEYS and SCAN matching. The optional third argument turns
// on case-insensitive matching.
func debugStringmatchLen(ctx *Ctx) {
	if len(ctx.Argv) != 4 && len(ctx.Argv) != 5 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'debug|stringmatch-len' command")
		return
	}
	nocase := len(ctx.Argv) == 5
	if stringMatch(ctx.Argv[2], ctx.Argv[3], nocase) {
		ctx.enc().WriteInteger(1)
		return
	}
	ctx.enc().WriteInteger(0)
}

// debugLoadAOF reloads the dataset from the appendonlydir: it flushes the
// in-memory data, loads the base RDB, and replays the incremental files. It is the
// AOF counterpart to DEBUG RELOAD and needs appendonly to be on.
func debugLoadAOF(ctx *Ctx) {
	if ctx.confStr("appendonly", "no") != "yes" {
		ctx.enc().WriteError("ERR AOF not enabled")
		return
	}
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return
	}
	if !ctx.d.aofManifestExists() {
		ctx.enc().WriteError("ERR No AOF directory found")
		return
	}
	if err := ctx.d.loadAOF(); err != nil {
		ctx.enc().WriteError("ERR Error trying to load the AOF: " + err.Error())
		return
	}
	ctx.enc().WriteStatus("OK")
}

// debugFlushAll empties every database synchronously, the reset the test suite
// expects between cases.
func debugFlushAll(ctx *Ctx) {
	if ctx.updateKeyspace(func(ks *keyspace.Keyspace) error {
		for i := range ks.DBCount() {
			db, err := ks.DB(i)
			if err != nil {
				return err
			}
			if err := db.Flush(); err != nil {
				return err
			}
		}
		return nil
	}) {
		ctx.enc().WriteStatus("OK")
	}
}

// debugChangeReplID rolls the replication id the way a promotion does: the
// current replid moves to replid2 with its second-offset window, and a fresh
// replid takes its place. A replica that reconnects after this presents the old
// replid, which no longer matches, so the next PSYNC falls back to a full
// resync. The test suites use this to simulate a failover without dropping the
// link.
func debugChangeReplID(ctx *Ctx) {
	d := ctx.d
	d.repl.mu.Lock()
	d.repl.replid2 = d.repl.replid
	d.repl.secondOffset = d.repl.offset + 1
	d.repl.replid = newRunID()
	d.repl.mu.Unlock()
	ctx.enc().WriteStatus("OK")
}

// debugJMAP writes a Go heap profile to aki-heap-<unix>.prof in the working
// directory. This stands in for Redis's jemalloc JMAP hook, which has no Go
// equivalent. A GC runs first so the profile reflects live memory rather than
// not-yet-collected garbage.
func debugJMAP(ctx *Ctx) {
	runtime.GC()
	name := "aki-heap-" + strconv.FormatInt(time.Now().Unix(), 10) + ".prof"
	f, err := os.Create(name)
	if err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	defer func() { _ = f.Close() }()
	if err := pprof.WriteHeapProfile(f); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	ctx.enc().WriteStatus("OK")
}
