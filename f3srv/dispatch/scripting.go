package dispatch

import "github.com/tamnd/aki/engine/f3/shard"

// The scripting deferral surface (spec 2064/f3/17 section 17, decision F18).
// EVAL and the function API are deferred to f4, and this file fixes the boundary
// so a client sees one coherent story instead of a ragged mix of unknown-command
// and half-working replies.
//
// Every scripting verb answers a single plain error:
//
//	ERR unsupported command 'EVAL' (scripting is not available in this build)
//
// The error deliberately does not mimic NOSCRIPT. NOSCRIPT tells a client to
// load the script and retry with EVAL, and the retry would fail too, so a client
// library that reads NOSCRIPT enters a load-and-retry loop and livelocks. A plain
// unsupported-command error stops that cold. SCRIPT EXISTS for the same reason
// does not answer an array of zeros, because pretending scripts can be absent
// implies EVAL could run them; it returns the same error class.
//
// aki keeps no COMMAND DOCS/INFO surface yet, so these verbs are already absent
// from capability probes with nothing to omit them from. When that surface lands
// it must leave the scripting verbs unlisted, which is the truthful signal the
// section wants a probing client to get cheaply.
//
// The verbs register with minArgs 0 and maxArgs -1 so the deferral error answers
// for every arity, including the malformed ones, rather than letting an arity
// error mask the real story. The container verbs SCRIPT and FUNCTION cover all
// their subcommands the same way: the whole family is deferred, so the top-level
// verb rejects before any subcommand parse.
func init() {
	for _, name := range []string{
		"EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO",
		"FCALL", "FCALL_RO",
		"FUNCTION", "SCRIPT",
	} {
		register(name, scriptingDeferred(name), 0, -1, false)
	}
}

// scriptingDeferred builds the handler for one deferred scripting verb: it names
// the verb in the error so a client learns which command it reached for, and it
// touches no keyspace, so it routes the keyless point path like PING. The message
// is built once at registration, not per call, because the error path never wants
// to allocate on a live connection.
func scriptingDeferred(name string) shard.Handler {
	msg := "ERR unsupported command '" + name + "' (scripting is not available in this build)"
	return func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		r.Err(msg)
	}
}
