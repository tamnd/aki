package shard

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/obs1srv/resp"
)

// WAITAOF numlocal numreplicas timeout (spec 2064/obs1 doc 04 section 3.3):
// numlocal 1 maps to the chain commit barrier, the bucket being the AOF, and
// numreplicas maps to the hot standby's commit knowledge, which is zero
// standbys this generation (O6b wires the real seam). The command rides a
// barrier fan rather than the keyless point path because its promise reaches
// backwards: a write pipelined ahead of the WAITAOF must be inside the commit
// snapshot, and only the per-shard no-op sub-commands, each running behind
// everything this connection enqueued earlier on its shard, put the gather
// after every such write's emission. The gathered merge parks the reply on
// the write log's NotifyAllCommitted barrier; a finite timeout races it with
// a timer, whichever wins the CAS delivers, and a zero timeout parks forever,
// Redis semantics verbatim. The reply is the Redis 7.2 [numlocal,
// numreplicas] achieved pair, honest at delivery time: [1, 0] from the
// barrier, [0 or 1, 0] from the timer, [0, 0] on a volatile node.

// waitAOFSpec is WAITAOF's validated argument set, parsed reader-side and
// carried on the fan coordinator to the merge.
type waitAOFSpec struct {
	numlocal    int64
	numreplicas int64
	timeoutMs   int64
}

// waitAOFHold is one parked WAITAOF reply. The commit barrier callback runs
// on the fold goroutine (or inline on the writer), the timer on a runtime
// timer goroutine, and Conn.CompleteBlocked is safe from all three, so the
// CAS on done is the only ordering the delivery needs; the loser's late fire
// is a no-op. localDone is written by the barrier callback and read at
// delivery from either goroutine, hence atomic; conn and seq are set before
// any callback can run.
type waitAOFHold struct {
	conn      *Conn
	seq       uint32
	done      atomic.Bool
	localDone atomic.Bool
}

// deliver sends the [local, 0] achieved pair if this caller wins the race.
func (h *waitAOFHold) deliver() {
	if !h.done.CompareAndSwap(false, true) {
		return
	}
	local := int64(0)
	if h.localDone.Load() {
		local = 1
	}
	out := resp.AppendArrayHeader(nil, 2)
	out = resp.AppendInt(out, local)
	out = resp.AppendInt(out, 0)
	h.conn.CompleteBlocked(h.seq, out)
}

// holdWaitAOF finishes a gathered WAITAOF on the connection's writer
// goroutine: it registers the commit barrier and either delivers now or
// leaves the reply parked. An inline barrier fire pushes the loopback node
// onto the outbound queue the current drain pass is still popping, the
// holdFan rule, so an already-covered barrier still answers in this pass.
func (c *Conn) holdWaitAOF(seq uint32, spec *waitAOFSpec) {
	h := &waitAOFHold{conn: c, seq: seq}
	if log := c.rt.wlog; log != nil {
		// The barrier tracks the local verdict whatever numlocal asked,
		// because the achieved pair reports honestly even for an ask of
		// zero; it only delivers when the replica ask is already met.
		log.NotifyAllCommitted(func() {
			h.localDone.Store(true)
			if spec.numreplicas == 0 {
				h.deliver()
			}
		})
	}
	if spec.numreplicas == 0 && spec.numlocal == 0 {
		// Nothing left to wait for: answer with the barrier's current
		// verdict. The CAS makes the race with an inline fire benign.
		h.deliver()
		return
	}
	if h.done.Load() {
		// The barrier fired inline and delivered; skipping the timer here
		// is an optimization, a lost CAS would no-op it anyway.
		return
	}
	if spec.timeoutMs > 0 && spec.timeoutMs <= math.MaxInt64/int64(time.Millisecond) {
		// A timeout past the Duration range parks as forever, which is
		// what a timer three centuries out means anyway. The hold's shared
		// state is fully built before the timer arms, so an early fire
		// reads a complete hold.
		time.AfterFunc(time.Duration(spec.timeoutMs)*time.Millisecond, h.deliver)
	}
	// A zero timeout parks forever: only the commit barrier can deliver
	// (numreplicas 0), and a replica ask outlives the connection until a
	// standby generation exists, the Redis contract on a replica-less node.
}

// DoWaitAOF scatters WAITAOF's barrier fan: one keyless no-op sub-command
// per shard, op being the registered sub handler, so every shard has
// executed and emitted everything this connection enqueued before the
// WAITAOF by the time the last partial gathers. The arguments arrive
// validated by the reader.
func (c *Conn) DoWaitAOF(op byte, numlocal, numreplicas, timeoutMs int64) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	// The countdown is final before the first enqueue; see DoFan.
	fc := &fanCmd{kind: FanWaitAOF, pending: int32(len(c.rt.workers)), waitaof: &waitAOFSpec{
		numlocal:    numlocal,
		numreplicas: numreplicas,
		timeoutMs:   timeoutMs,
	}}
	for sh := range c.rt.workers {
		if err := c.enqueueFan(sh, op, nil, fc); err != nil {
			return err
		}
	}
	c.seq++
	return nil
}
