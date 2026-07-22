package dispatch

import (
	"strconv"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The DEBUG surface (spec 2064/f3/17 section 15). DEBUG is a grab bag of test and
// introspection subcommands; f3 answers the ones test harnesses depend on and
// gives truthful stubs for the internals it has no equivalent of, rather than
// erroring as an unknown command and failing a harness on an unrelated setup
// step. The subcommand sits at args[0] here (register strips the verb).
//
// DEBUG SLEEP is answered in the network layer on the default driver (client.go,
// doDebugSleep), where it blocks only the one connection. It also has a handler
// here for the reactor driver, which has no network intercept: there it sleeps
// the owning shard worker, a briefer block than redis's whole-server sleep. Both
// paths parse the same seconds argument.
//
// DEBUG OBJECT builds its line in debugobject.go from the OBJECT ENCODING chain and
// the DUMP serialize primitive; DEBUG routes on args[1] (keyAt=1 in Dispatch) so the
// OBJECT subcommand reaches the key's owning shard, and the keyless subcommands
// either fall through (no args[1]) or land on a hashed shard where their stub is
// shard-agnostic.
func debugCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch upperVerb(args[0]) {
	case "OBJECT":
		debugObject(cx, args, r)
	case "SLEEP":
		// The reactor path (the default driver intercepts this before dispatch).
		if len(args) != 2 {
			r.Err("ERR wrong number of arguments for 'debug|sleep' command")
			return
		}
		secs, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil || secs < 0 {
			r.Err("ERR value is not a valid float")
			return
		}
		time.Sleep(time.Duration(secs * float64(time.Second)))
		r.Status("OK")
	case "SET-ACTIVE-EXPIRE":
		// Toggle the active-expiry cycle (expirecycle.go) for the whole process,
		// the real effect redis's knob carries: 0 pauses the background reap of
		// untouched expired keys, 1 resumes it. Lazy expiry is untouched either
		// way, so a key is still reaped on its next access; a test that wants a key
		// to survive untouched until it reads it sets this to 0. A missing or
		// unparseable argument leaves the state as is and still answers OK, the
		// forgiving shape the harnesses that poke this expect.
		if len(args) >= 2 {
			if v, err := strconv.Atoi(string(args[1])); err == nil {
				shard.SetActiveExpire(v != 0)
			}
		}
		r.Status("OK")
	case "RELOAD":
		// DEBUG RELOAD saves the dataset and reloads it from disk. f3 keeps every
		// write in the durable .aki log, so a reload would rebuild the identical
		// state it replays on open; there is no volatile in-memory-only data to
		// drop and re-read. It cannot tear the live workers down and re-run the open
		// sequence in place, so it forces the durable barrier (the save half) and
		// acks, the honest realization of "the dataset is on disk and would reload
		// unchanged". The optional NOSAVE/NOFLUSH tokens redis accepts are ignored.
		if rt := cx.Runtime(); rt != nil {
			if err := rt.SyncDurable(); err != nil {
				r.Err("ERR Error trying to save the DB: " + err.Error())
				return
			}
		}
		r.Status("OK")
	case "JMAP", "QUICKLIST-PACKED-THRESHOLD",
		"STRINGMATCH-LEN", "CHANGE-REPL-ID", "FLUSHALL", "DEBUG":
		// Truthful stubs: these poke redis-internal machinery (the JVM-style
		// object map, quicklist packing, a repl id rotation) that f3 either does
		// not have or does not expose through a debug hook. A harness sets them for
		// the side effect and only checks for the OK; f3 acknowledges without
		// pretending to have changed state it does not keep.
		r.Status("OK")
	default:
		r.Err("ERR DEBUG subcommand not supported")
	}
}
