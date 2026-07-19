// SWAPDB swaps the contents of two numbered databases. f3 keeps a single keyspace
// with no numbered databases (SELECT accepts only 0), so the only swap it can name
// is database 0 with itself, a no-op it confirms; any other index is out of range.
// Answering it this way rather than leaving it an unknown command keeps a client
// that probes SWAPDB from mistaking f3 for a server that lacks the verb.
package dispatch

import (
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
)

// swapdbCmd answers SWAPDB index1 index2. It validates both indices the way SELECT
// validates its one: each must parse as an integer and each must be 0, the only
// database f3 has. Two zeroes swap database 0 with itself and confirm; any other
// index is out of range. A non-integer is the value error, matching redis.
func swapdbCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	for _, a := range args[:2] {
		n, err := strconv.ParseInt(string(a), 10, 64)
		if err != nil {
			r.Err("ERR value is not an integer or out of range")
			return
		}
		if n != 0 {
			r.Err("ERR DB index is out of range")
			return
		}
	}
	r.Status("OK")
}
