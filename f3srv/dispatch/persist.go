package dispatch

import (
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The persistence-command surface (spec 2064/f3/17 section 13): SAVE, BGSAVE,
// BGREWRITEAOF, and LASTSAVE. In redis these drive the RDB snapshot and the AOF
// rewrite child; f3 has neither, because its durability is the continuous
// fsync-durable .aki append log (M8, doc 07). A committed write is already on
// disk, so "save the dataset" means force the log's fsync barrier now rather than
// fork a child to write a separate file. These verbs answer for what f3 is: SAVE
// and BGSAVE flush and fsync the log and stamp the last-save clock, BGREWRITEAOF
// acks the rewrite the log self-manages through its checkpoint, and LASTSAVE
// reports the clock. They are honest, not placeholders: the flush is real and the
// data is durable when SAVE returns.

// lastSaveUnix is the unix-seconds time of the last successful SAVE/BGSAVE, the
// value LASTSAVE returns and redis mirrors as rdb_last_save_time. It is process
// wide (a f3 process is one server) and seeded to process start, so a client that
// polls it before any explicit save still reads a sane baseline the way redis
// seeds it at startup.
var lastSaveUnix atomic.Int64

func init() { lastSaveUnix.Store(time.Now().Unix()) }

// saveCmd forces a synchronous durability barrier on the .aki log and acks. It is
// the point where "save now" means flush every accepted record and fsync, not
// write a separate snapshot file. On the non-durable scratch path the barrier is
// a no-op and the ack still stands, since there is no on-disk copy to make current.
func saveCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if rt := cx.Runtime(); rt != nil {
		if err := rt.SyncDurable(); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
	}
	lastSaveUnix.Store(time.Now().Unix())
	r.Status("OK")
}

// bgsaveCmd runs the same durability barrier and reports the background-save
// acknowledgement redis gives. f3 has no fork-and-dump child: the append log is
// already the durable copy, so the flush is synchronous and cheap and completes
// before the reply, which is why clients that poll rdb_bgsave_in_progress see it
// clear at once. A SCHEDULE token is accepted for arity and ignored.
func bgsaveCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if rt := cx.Runtime(); rt != nil {
		if err := rt.SyncDurable(); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
	}
	lastSaveUnix.Store(time.Now().Unix())
	r.Status("Background saving started")
}

// bgrewriteaofCmd acks an append-only-file rewrite. f3's .aki log is the
// append-only durable store, and it compacts through the checkpoint the runtime
// writes rather than an online rewrite child, so there is no separate rewrite pass
// to start here. It reports redis's start acknowledgement and does not move the
// last-save clock, matching redis, where BGREWRITEAOF leaves rdb_last_save_time
// untouched.
func bgrewriteaofCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.Status("Background append only file rewriting started")
}

// lastsaveCmd reports the unix time of the last successful SAVE/BGSAVE as an
// integer, the value redis returns and rdb_last_save_time mirrors.
func lastsaveCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.Int(lastSaveUnix.Load())
}
