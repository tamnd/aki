package dispatch

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// MSETEX (Redis 8.4, spec 2064/f3/17 command-coverage): atomically set several
// string keys with one shared expiration, with an optional NX/XX guard on the
// whole set.
//
//	MSETEX numkeys key value [key value ...] [NX | XX]
//	       [EX seconds | PX ms | EXAT unix-s | PXAT unix-ms | KEEPTTL]
//
// It replies 1 when every pair was written and 0 when the NX/XX guard declined
// the whole command, and it is all-or-nothing: a reader never sees a
// half-applied MSETEX. NX writes only when none of the keys exist (in any
// keyspace, the way SET NX and MSETNX probe), XX only when all of them exist.
// The expiry, if given, is the same deadline on every key; KEEPTTL keeps each
// key's current deadline, and with no expiry option the keys are set with no
// TTL, like MSET.
//
// It routes exactly like MSETNX and the numkeys-led SINTERCARD: the leading
// numkeys means the routing key is the first operand (keyAt=1), a co-located key
// set runs the whole probe-then-write on that one owner, and a key set that
// spans shards rides the F17 intent barrier so the probe and the writes are one
// atomic step.

// msetexNX and msetexXX are the two exclusive condition flags. Zero means the
// command writes unconditionally.
const (
	msetexNX = 1 << iota
	msetexXX
)

// msetexPlan is the parsed MSETEX tail: the key/value pairs, the condition, and
// the shared expiry as a (unit, timeArg) pair plus the KEEPTTL flag. The
// deadline is folded from (unit, timeArg) against the owner's batch clock at
// write time, the same way SET does, so relative EX/PX read the same clock the
// rest of the command runs under.
type msetexPlan struct {
	keys    [][]byte
	vals    [][]byte
	cond    int
	unit    int
	timeArg int64
	keepTTL bool
}

const errMsetexArgs = "ERR wrong number of arguments for 'msetex' command"

// parseMsetex parses an MSETEX tail (args after the verb): numkeys, that many
// key/value pairs, then the option list. The condition and expiry options may
// appear in either order (the command's documented flexible parsing). It returns
// the wire error text when the tail is malformed, empty on success.
func parseMsetex(tail [][]byte) (msetexPlan, string) {
	var p msetexPlan
	numkeys, ok := store.ParseInt(tail[0])
	if !ok || numkeys <= 0 {
		return p, "ERR numkeys should be greater than 0"
	}
	nk := int(numkeys)
	// numkeys pairs = 2*nk data arguments must follow the count.
	if 2*nk > len(tail)-1 {
		return p, errMsetexArgs
	}
	p.keys = make([][]byte, nk)
	p.vals = make([][]byte, nk)
	for i := 0; i < nk; i++ {
		p.keys[i] = tail[1+2*i]
		p.vals[i] = tail[2+2*i]
	}
	for i := 1 + 2*nk; i < len(tail); i++ {
		opt := tail[i]
		switch {
		case eqFold(opt, "NX"):
			if p.cond != 0 {
				return p, "ERR syntax error"
			}
			p.cond = msetexNX
		case eqFold(opt, "XX"):
			if p.cond != 0 {
				return p, "ERR syntax error"
			}
			p.cond = msetexXX
		case eqFold(opt, "KEEPTTL"):
			if p.unit != unitNone || p.keepTTL {
				return p, "ERR syntax error"
			}
			p.keepTTL = true
		case eqFold(opt, "EX"), eqFold(opt, "PX"), eqFold(opt, "EXAT"), eqFold(opt, "PXAT"):
			if p.unit != unitNone || p.keepTTL || i+1 >= len(tail) {
				return p, "ERR syntax error"
			}
			n, ok := store.ParseInt(tail[i+1])
			if !ok {
				return p, "ERR value is not an integer or out of range"
			}
			i++
			p.timeArg = n
			switch {
			case eqFold(opt, "EX"):
				p.unit = unitEXsec
			case eqFold(opt, "PX"):
				p.unit = unitPXms
			case eqFold(opt, "EXAT"):
				p.unit = unitEXat
			default:
				p.unit = unitPXat
			}
		default:
			return p, "ERR syntax error"
		}
	}
	return p, ""
}

// msetexDeadline folds the plan's (unit, timeArg) into an absolute unix-ms
// deadline against nowMs, reporting false for a non-positive or overflowing
// expiry. unitNone (no expiry option) yields (0, true): a plain set with no TTL.
func (p msetexPlan) msetexDeadline(nowMs int64) (int64, bool) {
	if p.unit == unitNone {
		return 0, true
	}
	return expireDeadline(nowMs, p.unit, p.timeArg)
}

// msetexGuardDeclines reports whether the NX/XX condition rejects the write
// given the existence probe results: NX declines when any key exists, XX when
// any key is missing.
func msetexGuardDeclines(cond int, anyExists, allExist bool) bool {
	return (cond == msetexNX && anyExists) || (cond == msetexXX && !allExist)
}

// msetexKeys returns MSETEX's operand keys for dispatch's co-location check,
// nil when the tail is malformed (the point path then answers the parse error).
func msetexKeys(tail [][]byte) [][]byte {
	p, msg := parseMsetex(tail)
	if msg != "" {
		return nil
	}
	return p.keys
}

// msetexCmd answers a co-located MSETEX (every key on one owner via keyAt=1),
// the single-shard fast path. It probes every key once for the NX/XX guard and,
// only if the guard permits, writes each pair with the shared deadline; a
// declined command writes nothing and replies 0. Under memory pressure a write
// that cannot allocate parks and the worker retries it, resuming at the unwritten
// pair (ResumeIndex) so the committed prefix is not re-applied: the guard probe
// runs only on the fresh entry, because once a write has landed the command is
// past the decision point and must finish.
func msetexCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	p, msg := parseMsetex(args)
	if msg != "" {
		r.Err(msg)
		return
	}
	at, ok := p.msetexDeadline(cx.NowMs)
	if !ok {
		r.Err("ERR invalid expire time in 'msetex' command")
		return
	}
	start := cx.ResumeIndex()
	if start == 0 && p.cond != 0 {
		anyExists, allExist := false, true
		for _, key := range p.keys {
			if keyExistsAnywhere(cx, key) {
				anyExists = true
			} else {
				allExist = false
			}
		}
		if msetexGuardDeclines(p.cond, anyExists, allExist) {
			r.Int(0)
			return
		}
	}
	for i := start; i < len(p.keys); i++ {
		if err := cx.St.SetString(p.keys[i], p.vals[i], cx.NowMs, at, p.keepTTL); err != nil {
			if cx.ParkFullAt(err, i) {
				return
			}
			r.Err(msetnxStoreErr(err))
			return
		}
		// Each written pair fires its own set event, the per-key notification
		// MSET and MSETNX fire. The resume starts past a committed pair, so none
		// fires twice.
		cx.NotifyKeyspaceEvent(shard.NotifyString, "set", p.keys[i])
	}
	r.Int(1)
}

// msetexCross runs a cross-shard MSETEX under a transaction holding a write
// intent on every key. It probes each key at its owner for the guard and, only
// if the guard permits, writes each pair at its owner with the shared deadline;
// a declined command writes nothing and replies 0. The barrier makes the probe
// and the writes one atomic step from every other command's view. The deadline
// is folded once, from the first owner's clock, so every key across every shard
// gets the identical absolute expiry. A write that fails (an oversize value, or
// memory pressure the cross path cannot park) stops the command and reports the
// first error; the barrier still releases every intent on return.
func msetexCross(t *shard.Txn, tail [][]byte) []byte {
	p, msg := parseMsetex(tail)
	if msg != "" {
		return resp.AppendError(nil, msg)
	}

	anyExists, allExist := false, true
	var at int64
	deadlineOK := true
	deadlineDone := false
	for i := range p.keys {
		key := p.keys[i]
		t.Do(key, func(cx *shard.Ctx) {
			if !deadlineDone {
				at, deadlineOK = p.msetexDeadline(cx.NowMs)
				deadlineDone = true
			}
			if keyExistsAnywhere(cx, key) {
				anyExists = true
			} else {
				allExist = false
			}
		})
	}
	if !deadlineOK {
		return resp.AppendError(nil, "ERR invalid expire time in 'msetex' command")
	}
	if msetexGuardDeclines(p.cond, anyExists, allExist) {
		return resp.AppendInt(nil, 0)
	}

	var writeErr error
	for i := range p.keys {
		if writeErr != nil {
			break
		}
		key, val := p.keys[i], p.vals[i]
		t.Do(key, func(cx *shard.Ctx) {
			if err := cx.St.SetString(key, val, cx.NowMs, at, p.keepTTL); err != nil {
				writeErr = err
				return
			}
			cx.NotifyKeyspaceEvent(shard.NotifyString, "set", key)
		})
	}
	if writeErr != nil {
		return resp.AppendError(nil, msetnxStoreErr(writeErr))
	}
	return resp.AppendInt(nil, 1)
}
