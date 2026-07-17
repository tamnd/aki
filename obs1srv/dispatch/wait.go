package dispatch

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// WAIT and WAITAOF (spec 2064/obs1 doc 04 section 3.3), the durability
// barriers. The error texts and check order below are verified against
// redis-server 8.8.0: WAIT parses numreplicas then timeout, WAITAOF parses
// numlocal, numreplicas, then timeout, and only after all three does it
// refuse a numlocal ask on a node with no AOF analog, which for obs1 is a
// volatile node with no write log.

const (
	errWaitNotInt      = "ERR value is not an integer or out of range"
	errWaitTimeoutInt  = "ERR timeout is not an integer or out of range"
	errWaitTimeoutNeg  = "ERR timeout is negative"
	errWaitLocalRange  = "ERR value is out of range, value must between 0 and 1"
	errWaitReplicasPos = "ERR value is out of range, must be positive"
	errWaitAOFNoLog    = "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled."
)

// parseLong parses one integer argument with Redis's strict long long
// grammar: an optional sign and digits, no floats, no blanks. WAIT is never
// hot, so the string conversion's allocation is fine here.
func parseLong(b []byte) (int64, bool) {
	v, err := strconv.ParseInt(string(b), 10, 64)
	return v, err == nil
}

// waitCmd answers WAIT numreplicas timeout: block until that many standbys
// acked the connection's last write. The standby count is zero this
// generation (O6b wires the real seam), so an ask at or under zero answers
// the achieved count of 0 in place, exactly as Redis does on a replica-less
// master, and a positive ask can never be met: the reply parks and the
// owner's timer delivers 0 when the timeout runs out. A zero timeout with a
// positive ask parks forever, Redis semantics verbatim; nothing can serve
// the park early, so the timer needs no live guard, and a connection that
// dies first just never collects the loopback.
func waitCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	numreplicas, ok := parseLong(args[0])
	if !ok {
		r.Err(errWaitNotInt)
		return
	}
	timeout, ok := parseLong(args[1])
	if !ok {
		r.Err(errWaitTimeoutInt)
		return
	}
	if timeout < 0 {
		r.Err(errWaitTimeoutNeg)
		return
	}
	if numreplicas <= 0 {
		r.Int(0)
		return
	}
	conn, seq := cx.CurConn(), cx.CurSeq()
	if timeout > 0 {
		deadline := cx.NowMs + timeout
		if deadline < cx.NowMs {
			// An overflowing deadline is forever anyway.
			deadline = math.MaxInt64
		}
		cx.ArmTimer(deadline, func(*shard.Ctx) {
			conn.CompleteBlocked(seq, resp.AppendInt(nil, 0))
		})
	}
	r.Park()
}

// waitaofSub is WAITAOF's per-shard sub-command: it does nothing, because
// its arrival at the gather is the point, proof this shard has executed and
// emitted everything the connection enqueued ahead of the WAITAOF.
func waitaofSub(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.FanOK()
}

// dispatchWaitAOF routes WAITAOF: the arguments parse reader-side in Redis's
// order, the volatile-node refusal follows the parses exactly as Redis
// orders its appendonly check, and the valid form scatters the barrier fan.
// The reader barrier arms on a clean enqueue like a blocking verb's.
func dispatchWaitAOF(c *shard.Conn, e *entry, args [][]byte) error {
	numlocal, ok := parseLong(args[1])
	if !ok {
		return oops(c, errWaitNotInt)
	}
	if numlocal < 0 || numlocal > 1 {
		return oops(c, errWaitLocalRange)
	}
	// Redis folds the numreplicas parse and sign check into one range read,
	// so a non-integer and a negative share the error text.
	numreplicas, ok := parseLong(args[2])
	if !ok || numreplicas < 0 {
		return oops(c, errWaitReplicasPos)
	}
	timeout, ok := parseLong(args[3])
	if !ok {
		return oops(c, errWaitTimeoutInt)
	}
	if timeout < 0 {
		return oops(c, errWaitTimeoutNeg)
	}
	if numlocal > 0 && !c.WriteLogged() {
		// The volatile node is obs1's appendonly-disabled: there is no
		// commit a local ask could wait for, the AKI.DURABILITY STRICT
		// refusal in Redis's own words.
		return oops(c, errWaitAOFNoLog)
	}
	err := c.DoWaitAOF(e.fanOp, numlocal, numreplicas, timeout)
	if err == shard.ErrTooBig {
		return oops(c, "ERR command too large")
	}
	if err == nil {
		c.ArmBlock()
	}
	return err
}
