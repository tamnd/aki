// MOVE relocates a key to another numbered database. f3 keeps a single keyspace
// with no numbered databases (SELECT accepts only 0), so MOVE has no destination to
// move to and always declines with an honest error rather than being an unknown
// command: MOVE to database 0 is the current one, the source-and-destination error;
// any other index is out of range; a non-integer is the value error. The
// destination checks come before the key is consulted, matching redis, so a missing
// key still answers the database error.
package dispatch

import (
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
)

// moveCmd answers MOVE key db. It validates the destination database the way SELECT
// does and stops there: with one database, db 0 is the current keyspace (the
// source-and-destination-are-the-same error) and every other index is out of range,
// so no key ever actually moves.
func moveCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	if n != 0 {
		r.Err("ERR DB index is out of range")
		return
	}
	r.Err("ERR source and destination objects are the same")
}
