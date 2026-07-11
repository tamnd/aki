package list

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The live differential suite. Every command runs on both the in-process list
// handlers and a live redis-server, and the two replies must decode to the same
// value: same reply shape, same integers and bulks, same error text. It is
// skipped unless AKI_REDIS_ADDR points at a server, so it is the confirmation
// lever, not a required gate. redis-server 8.8.0 is the reference.

// cmdName maps a harness op to the Redis verb, so one call drives both sides.
var cmdName = map[byte]string{
	opLpush:   "LPUSH",
	opRpush:   "RPUSH",
	opLpushx:  "LPUSHX",
	opRpushx:  "RPUSHX",
	opLpop:    "LPOP",
	opRpop:    "RPOP",
	opLlen:    "LLEN",
	opLindex:  "LINDEX",
	opLrange:  "LRANGE",
	opLset:    "LSET",
	opLrem:    "LREM",
	opLtrim:   "LTRIM",
	opLinsert: "LINSERT",
	opLpos:    "LPOS",
	opObject:  "OBJECT",
}

// differ pairs the two backends for a run.
type differ struct {
	t *testing.T
	c *shard.Conn
	r *redisConn
}

func newDiffer(t *testing.T) *differ {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay list commands against a live Redis")
	}
	rc, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(rc.close)
	rt := newHarness(t)
	return &differ{t: t, c: rt.NewConn(), r: rc}
}

// agree runs one command on both backends and fails unless the replies match.
func (d *differ) agree(op byte, args ...string) any {
	d.t.Helper()
	var raw []byte
	if op == opObject {
		raw = doAt(d.t, d.c, op, 1, args...)
	} else {
		raw = do(d.t, d.c, op, args...)
	}
	mine := decodeReply(d.t, raw)

	redisArgs := append([]string{cmdName[op]}, args...)
	theirs, err := d.r.cmdReply(redisArgs...)
	if err != nil {
		d.t.Fatalf("%v: redis transport error: %v", redisArgs, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("%v: aki %v, redis %v", redisArgs, render(mine), render(theirs))
	}
	return mine
}

// freshKey returns a key unused on either backend so a block starts empty on
// both. The aki harness has no DEL, so a per-block fresh key is how a block
// resets; the Redis side is still deleted in case a prior run left the key.
func (d *differ) freshKey(name string) string {
	k := "aki:ldiff:" + name
	d.r.cmd("DEL", k)
	return k
}

func equalReply(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case errReply:
		bv, ok := b.(errReply)
		return ok && av.msg == bv.msg
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !equalReply(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// A scripted pass over every reply shape and error corner the slice owns. Each
// block takes a fresh key so both backends start it empty, and an absent key is
// a distinct name that is never written.
func TestListAgainstRedis(t *testing.T) {
	d := newDiffer(t)
	absent := d.freshKey("absent")

	// Push forms and running length.
	k := d.freshKey("push")
	d.agree(opRpush, k, "a", "b", "c")
	d.agree(opLpush, k, "x", "y")
	d.agree(opLlen, k)
	d.agree(opLpushx, k, "z")
	d.agree(opRpushx, k, "w")
	d.agree(opLpushx, absent, "q") // 0, no create
	d.agree(opRpushx, absent, "q")

	// Range with negative indices.
	d.agree(opLrange, k, "0", "-1")
	d.agree(opLrange, k, "-3", "-1")
	d.agree(opLrange, k, "2", "1") // empty
	d.agree(opLrange, k, "-100", "2")
	d.agree(opLrange, absent, "0", "-1")

	// Index, both signs and out of range.
	d.agree(opLindex, k, "0")
	d.agree(opLindex, k, "-1")
	d.agree(opLindex, k, "999")
	d.agree(opLindex, absent, "0")

	// LSET including the out-of-range and no-such-key errors.
	d.agree(opLset, k, "0", "HEAD")
	d.agree(opLset, k, "-1", "TAIL")
	d.agree(opLset, k, "999", "z")
	d.agree(opLset, absent, "0", "z")

	// LINSERT before/after and the missing pivot.
	d.agree(opLinsert, k, "BEFORE", "a", "A1")
	d.agree(opLinsert, k, "AFTER", "a", "A2")
	d.agree(opLinsert, k, "BEFORE", "no-such-pivot", "Z")
	d.agree(opLinsert, absent, "BEFORE", "a", "Z")
	d.agree(opLinsert, k, "sideways", "a", "Z") // syntax error

	// LREM count-sign semantics.
	rem := d.freshKey("rem")
	d.agree(opRpush, rem, "a", "b", "a", "c", "a", "b", "a")
	d.agree(opLrem, rem, "2", "a")  // head
	d.agree(opLrem, rem, "-1", "a") // tail
	d.agree(opLrem, rem, "0", "b")  // all
	d.agree(opLrange, rem, "0", "-1")

	// LPOS COUNT and RANK, including negative rank and MAXLEN.
	pos := d.freshKey("pos")
	d.agree(opRpush, pos, "a", "b", "c", "a", "b", "a")
	d.agree(opLpos, pos, "a")
	d.agree(opLpos, pos, "a", "RANK", "2")
	d.agree(opLpos, pos, "a", "RANK", "-1")
	d.agree(opLpos, pos, "a", "RANK", "-2")
	d.agree(opLpos, pos, "a", "COUNT", "0")
	d.agree(opLpos, pos, "a", "COUNT", "2")
	d.agree(opLpos, pos, "a", "RANK", "-1", "COUNT", "0")
	d.agree(opLpos, pos, "a", "COUNT", "0", "MAXLEN", "4")
	d.agree(opLpos, pos, "zzz")
	d.agree(opLpos, pos, "zzz", "COUNT", "0")
	d.agree(opLpos, pos, "a", "RANK", "0")   // rank-zero error
	d.agree(opLpos, pos, "a", "COUNT", "-1") // count-negative error
	d.agree(opLpos, pos, "a", "MAXLEN", "-1")

	// LTRIM including the empties-to-delete case.
	trim := d.freshKey("trim")
	d.agree(opRpush, trim, "a", "b", "c", "d", "e")
	d.agree(opLtrim, trim, "1", "3")
	d.agree(opLrange, trim, "0", "-1")
	d.agree(opLtrim, trim, "5", "9") // clears
	d.agree(opLlen, trim)

	// Pop shapes: single, count, empty, null bulk, null array.
	pop := d.freshKey("pop")
	d.agree(opRpush, pop, "a", "b", "c", "d", "e")
	d.agree(opLpop, pop)
	d.agree(opRpop, pop)
	d.agree(opLpop, pop, "2")
	d.agree(opLpop, pop, "0")
	d.agree(opLpop, absent)
	d.agree(opRpop, absent, "3")
	d.agree(opLpop, pop, "-1") // positive-count error
	d.agree(opRpop, pop, "9")  // drains the rest
	d.agree(opLlen, pop)

	// Encoding transition reported the same on both sides. The elements are
	// pushed one per command, the incremental path both backends convert on at
	// the byte budget (a single multi-element RPUSH takes Redis's looser bulk
	// path, which TestEncodingAgainstRedis documents).
	enc := d.freshKey("enc")
	d.agree(opRpush, enc, "a", "b", "c")
	d.agree(opObject, "ENCODING", enc)
	blk := string(make([]byte, 100))
	for i := 0; i < 100; i++ {
		d.agree(opRpush, enc, blk)
	}
	d.agree(opObject, "ENCODING", enc)
}

// A randomized churn replay: the same random push/pop/insert/remove stream on
// both backends, with LRANGE 0 -1 and OBJECT ENCODING checked after each step,
// across the inline band and the promotion into the native band.
func TestListChurnAgainstRedis(t *testing.T) {
	d := newDiffer(t)
	for _, width := range []int{4, 64} {
		key := d.freshKey("churn:" + strconv.Itoa(width))
		rng := rand.New(rand.NewPCG(11, uint64(width)))
		val := func() string {
			b := make([]byte, 1+rng.IntN(width))
			for i := range b {
				b[i] = byte('a' + rng.IntN(26))
			}
			return string(b)
		}
		for step := 0; step < 400; step++ {
			switch rng.IntN(6) {
			case 0:
				d.agree(opRpush, key, val())
			case 1:
				d.agree(opLpush, key, val())
			case 2:
				d.agree(opLpop, key)
			case 3:
				d.agree(opRpop, key)
			case 4:
				d.agree(opLinsert, key, "BEFORE", val(), val())
			case 5:
				d.agree(opLrem, key, strconv.Itoa(rng.IntN(5)-2), val())
			}
			got := d.agree(opLrange, key, "0", "-1")
			// OBJECT ENCODING only agrees while the key exists: an emptied list
			// is deleted, and OBJECT on a missing key is the set slice's "no such
			// key" against Redis 8.8's nil, a pre-existing OBJECT divergence.
			if arr, ok := got.([]any); ok && len(arr) > 0 {
				d.agree(opObject, "ENCODING", key)
			}
		}
	}
}
