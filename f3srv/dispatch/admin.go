package dispatch

import (
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The standalone-node admin surface (spec 2064/f3/11, the M11 command-closure
// milestone): WAIT, FAILOVER, LATENCY, and SLOWLOG. A client and its tooling
// call these to learn a server's replication and health state. f3 is a single
// standalone node with no replicas, and it keeps no latency time series or slow
// command log, so each of these answers the true empty or zero state rather than
// erroring as an unknown command. They are honest stubs, not placeholders: the
// answers are correct for what f3 is, and the day replication or a slow log lands
// they report live figures through these same verbs.

// waitCmd answers WAIT numreplicas timeout. A standalone node has no replicas, so
// no positive replica count is ever reached; f3 answers 0 at once rather than
// blocking for the timeout, since the result cannot change. The two arguments
// are still parsed so a non-integer is refused the way Redis refuses it.
func waitCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if _, err := strconv.ParseInt(string(args[0]), 10, 64); err != nil {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	if _, err := strconv.ParseInt(string(args[1]), 10, 64); err != nil {
		r.Err("ERR timeout is not an integer or out of range")
		return
	}
	r.Int(0)
}

// failoverCmd answers FAILOVER. A standalone node has no replica to promote, so a
// bare FAILOVER reports the missing-replicas case, and FAILOVER ABORT reports
// that nothing is in progress, the two states Redis reports here.
func failoverCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args) > 0 && upperVerb(args[0]) == "ABORT" {
		r.Err("ERR No failover in progress.")
		return
	}
	r.Err("ERR FAILOVER requires connected replicas.")
}

// latencyCmd answers the LATENCY family. f3 records no latency time series, so
// RESET reports zero events cleared, HISTORY and LATEST are empty, and DOCTOR
// gives the all-clear. The subcommand sits at args[0]; register bounds the arity
// so it is always present.
func latencyCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch upperVerb(args[0]) {
	case "RESET":
		r.Int(0)
	case "HISTORY":
		// LATENCY HISTORY event: the empty time series for any event.
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
	case "LATEST":
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
	case "DOCTOR":
		r.Bulk([]byte("Dave, I have observed the system, no worrying latency spikes. Everything looks fine."))
	default:
		r.Err("ERR Unknown LATENCY subcommand or wrong number of arguments")
	}
}

// slowlogCmd answers the SLOWLOG family. f3 keeps no slow command log (recording
// one would put timing on the hot path the 2x gate protects), so GET is empty,
// LEN is zero, and RESET acks.
func slowlogCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch upperVerb(args[0]) {
	case "GET":
		// SLOWLOG GET [count]: the empty log. A count argument is accepted for
		// arity and ignored, since the log is always empty.
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
	case "LEN":
		r.Int(0)
	case "RESET":
		r.Status("OK")
	default:
		r.Err("ERR Unknown SLOWLOG subcommand or wrong number of arguments")
	}
}
