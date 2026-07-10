package str

import (
	"github.com/tamnd/aki/engine/f3/shard"
)

// FlushShard answers a FLUSHALL (or FLUSHDB) sub-command: the owner resets
// its whole store between commands, the quiesced-by-construction window
// store.Reset asks for. The reset drops the index, rewinds the arena and
// hands its touched pages back to the OS, truncates the value log, and zeroes
// every ledger, so DBSIZE, used_memory, and the band census all read zero
// afterwards and the resident footprint actually falls.
//
// The partial is the FanOK empty partial. The gather replies +OK only after
// every shard's partial has landed, and each shard executes its flush
// sub-command in the connection's per-shard order, so a command pipelined
// after the flush always sees the empty store: barrier fan-out, not
// fire-and-forget.
func FlushShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	cx.St.Reset()
	r.FanOK()
}
