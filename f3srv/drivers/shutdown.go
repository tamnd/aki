// SHUTDOWN ends the server (doc 17 section 13's connection-control lane). A
// bare SHUTDOWN, or one carrying NOSAVE/SAVE/NOW/FORCE, terminates the process
// through the server's exit hook; SHUTDOWN ABORT is the honest no-op, since f3
// never has a shutdown pending to cancel. It is network-layer state like the
// other lifecycle verbs, answered in connIntercept on the goroutine driver ahead
// of the shard hop; the reactor answers it with the unknown-command reply, the
// pre-existing gap the other intercepts carry.
//
// f3's durability is the continuous fsync-durable .aki append log (M8), not an
// exit-time snapshot, so a committed write is already on disk when SHUTDOWN
// runs. SAVE and NOSAVE therefore reach the same exit here: there is no volatile
// dataset to checkpoint or to drop. The distinction is preserved in the surface
// (both are accepted) but has no separate persistence step to model.
package drivers

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// doShutdown answers SHUTDOWN [NOSAVE|SAVE|NOW|FORCE|ABORT]. On a real shutdown
// it calls the server's exit hook and, in production, never returns; a
// successful SHUTDOWN sends no reply, matching redis (the connection dies with
// the process). SHUTDOWN ABORT replies with the no-shutdown-in-progress error.
// ABORT does not combine with the exit flags, and an unknown token is the syntax
// error redis gives.
func (s *Server) doShutdown(c *shard.Conn, args [][]byte) {
	abort := false
	for _, tok := range args[1:] {
		switch {
		case eqFold(tok, "ABORT"):
			abort = true
		case eqFold(tok, "NOSAVE"), eqFold(tok, "SAVE"), eqFold(tok, "NOW"), eqFold(tok, "FORCE"):
			// Accepted exit modifiers; see the file comment on SAVE vs NOSAVE.
		default:
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
	}
	if abort {
		// ABORT cancels a shutdown that a NOW/FORCE grace period is waiting out.
		// f3 exits at once with no grace period, so there is never one pending;
		// ABORT combined with any other flag is the syntax error redis gives.
		if len(args) != 2 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
		_ = c.InlineReply(resp.AppendError(nil, "ERR No shutdown in progress"))
		return
	}
	// Terminate. No reply: redis answers a successful SHUTDOWN with silence, the
	// connection closing as the process goes down. In production exitFn is
	// os.Exit and control never returns; a test swaps in a recorder so the exit
	// path is covered without ending the test binary.
	s.exitFn(0)
}
