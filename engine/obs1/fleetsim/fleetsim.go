// Package fleetsim is the multi-node simulator harness of spec
// 2064/obs1 doc 10: N node stacks over one simulated bucket, a shared
// deterministic clock, and scripted fault schedules, so the doc 02
// section 6 degraded modes run as plain tests and the Gate D3 takeover
// runs have a fleet to measure.
//
// Each Node is the full crash-taker composition the engine tests built
// one piece at a time: fold, gate, tail window, liveness, appender,
// lease manager, takeover judge, and balancer. The Fleet drives every
// live node through one duty cycle per tick: follow the chain,
// reconcile, heartbeat when due, plan and attempt takeovers, and run
// the balancer when its jittered interval fires. Duty-cycle errors are
// tolerated and counted, because a degraded bucket is exactly the
// condition under test; the assertions live in what the folds agree on
// afterwards, never in every call succeeding.
//
// The harness is single-goroutine by design: ticks advance the shared
// clock and run nodes in id order, so a schedule's outcome is a pure
// function of the seed and the script.
package fleetsim

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// Clock is the fleet's shared deterministic clock.
type Clock struct{ t time.Time }

// Now is the current simulated instant.
func (c *Clock) Now() time.Time { return c.t }

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// Config sizes a fleet. The zero value is unusable; Nodes and NGroups
// must be positive.
type Config struct {
	Seed    uint64
	Nodes   int
	NGroups int
	// Prefix is the chain namespace; empty means "db/t".
	Prefix string
	// BalanceEvery is each node's balancer interval; zero means the
	// doc 02 default, which tests usually shorten.
	BalanceEvery time.Duration
}

// Node is one member's full stack. Fields are exported so tests assert
// straight against the fold, the gate, and the manager.
type Node struct {
	Self uint64
	Inc  uint32

	Fold  *obs1.LeaseFold
	Gate  *obs1.LeaseGate
	Win   *obs1.TailWindow
	Live  *obs1.Liveness
	Ap    *obs1.ChainAppender
	Mgr   *obs1.LeaseManager
	Judge *obs1.TakeoverJudge
	Bal   *obs1.Balancer

	// Crashed nodes skip the duty cycle; their stack stays reachable
	// so zombie behaviour can be scripted against the old life.
	Crashed bool
	// Errs counts tolerated duty-cycle failures, the outage evidence.
	Errs int

	walSeq uint64
}

// Fleet is N nodes over one simulated bucket.
type Fleet struct {
	Store *sim.Sim
	Clk   *Clock

	cfg    Config
	prefix string

	mu    sync.Mutex
	fault sim.FaultFn

	nodes map[uint64]*Node
	order []uint64
}

// New builds a fleet of cfg.Nodes stacks, ids 1..N, over one bucket
// whose fault hook indirects through the fleet so schedules swap
// mid-run. Nothing joins the chain yet.
func New(cfg Config) (*Fleet, error) {
	if cfg.Nodes <= 0 || cfg.NGroups <= 0 {
		return nil, fmt.Errorf("fleetsim: fleet needs nodes and groups")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "db/t"
	}
	if cfg.BalanceEvery <= 0 {
		cfg.BalanceEvery = obs1.DefaultBalanceEvery
	}
	f := &Fleet{
		cfg:    cfg,
		prefix: cfg.Prefix,
		Clk:    &Clock{t: time.UnixMilli(1_000_000)},
		nodes:  make(map[uint64]*Node),
	}
	f.Store = sim.New(sim.Config{Seed: cfg.Seed, Fault: func(op sim.Op, key string) *sim.Fault {
		f.mu.Lock()
		fn := f.fault
		f.mu.Unlock()
		if fn == nil {
			return nil
		}
		return fn(op, key)
	}})
	for id := uint64(1); id <= uint64(cfg.Nodes); id++ {
		n, err := f.newNode(id, 1)
		if err != nil {
			return nil, err
		}
		f.nodes[id] = n
		f.order = append(f.order, id)
	}
	return f, nil
}

// SetFault installs a fault schedule; nil clears it.
func (f *Fleet) SetFault(fn sim.FaultFn) {
	f.mu.Lock()
	f.fault = fn
	f.mu.Unlock()
}

// Node returns the stack for id.
func (f *Fleet) Node(id uint64) *Node { return f.nodes[id] }

func (f *Fleet) newNode(self uint64, inc uint32) (*Node, error) {
	fold := obs1.NewLeaseFold()
	gate := obs1.NewLeaseGate(0, 0)
	win, err := obs1.NewTailWindow(fold, fold)
	if err != nil {
		return nil, err
	}
	live, err := obs1.NewLiveness(win, obs1.DefaultLeaseTTL, obs1.DefaultSkewBound, f.Clk.Now)
	if err != nil {
		return nil, err
	}
	ap, err := obs1.NewChainAppender(f.Store, f.prefix, 0, self, inc, obs1.ChainPos{}, live)
	if err != nil {
		return nil, err
	}
	mgr, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{
		Self: self, Appender: ap, Fold: fold, Gate: gate, Now: f.Clk.Now,
	})
	if err != nil {
		return nil, err
	}
	bal, err := obs1.NewBalancer(mgr, f.cfg.NGroups, f.cfg.BalanceEvery, f.Clk.Now)
	if err != nil {
		return nil, err
	}
	n := &Node{
		Self: self, Inc: inc,
		Fold: fold, Gate: gate, Win: win, Live: live, Ap: ap, Mgr: mgr,
		Judge: obs1.NewTakeoverJudge(fold, live, 0),
		Bal:   bal,
	}
	bal.Alive = func(node uint64) bool {
		return node == self || !live.Suspect(node, f.Clk.Now())
	}
	bal.Shed = func(ctx context.Context, group uint16) error {
		return mgr.Handoff(ctx, group, nil)
	}
	return n, nil
}

// member is the row a node publishes for itself; the resp endpoint is
// per-node so demotion redirects name their winner.
func (f *Fleet) member(n *Node) obs1.Member {
	return obs1.Member{
		Node: n.Self, Incarnation: n.Inc,
		Resp: fmt.Sprintf("r%d", n.Self), Mesh: fmt.Sprintf("m%d", n.Self),
		Weight: 1, Version: "dev",
	}
}

// Join appends id's own join record from its own appender, so every
// member enters the chain already stamped as a live writer.
func (f *Fleet) Join(ctx context.Context, id uint64) error {
	n := f.nodes[id]
	_, err := n.Ap.Append(ctx, []obs1.ChainRecord{
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: f.member(n)},
	})
	if err == nil {
		n.Mgr.NoteAppend(f.Clk.Now())
	}
	return err
}

// survivors is the member table filtered to the nodes this observer
// does not suspect, self always included, the doc 02 section 4.4 view.
func (f *Fleet) survivors(n *Node, at time.Time) []obs1.Member {
	var out []obs1.Member
	for _, m := range n.Fold.Members() {
		if m.Node == n.Self || !n.Live.Suspect(m.Node, at) {
			out = append(out, m)
		}
	}
	return out
}

// Tick advances the clock by step and runs every live node's duty
// cycle in id order.
func (f *Fleet) Tick(ctx context.Context, step time.Duration) {
	f.Clk.Advance(step)
	for _, id := range f.order {
		n := f.nodes[id]
		if n.Crashed {
			continue
		}
		f.tickNode(ctx, n)
	}
}

// Run ticks the fleet for d of simulated time.
func (f *Fleet) Run(ctx context.Context, d, step time.Duration) {
	for elapsed := time.Duration(0); elapsed < d; elapsed += step {
		f.Tick(ctx, step)
	}
}

func (f *Fleet) tickNode(ctx context.Context, n *Node) {
	now := f.Clk.Now()
	if err := n.Ap.Follow(ctx); err != nil {
		n.Errs++
	}
	n.Mgr.Reconcile()
	if n.Mgr.HeartbeatDue(now) {
		if err := n.Mgr.Heartbeat(ctx); err != nil {
			n.Errs++
		}
	}
	for _, g := range obs1.PlanTakeover(n.Self, f.cfg.NGroups, f.survivors(n, now), n.Judge, now) {
		if _, err := n.Mgr.Takeover(ctx, g); err != nil {
			n.Errs++
		}
	}
	if n.Bal.Due(now) {
		if err := n.Bal.Tick(ctx); err != nil {
			n.Errs++
		}
	}
}

// Crash marks id dead: its duty cycle stops, its stack stays for
// zombie scripting, and the fleet only notices through staleness.
func (f *Fleet) Crash(id uint64) *Node {
	n := f.nodes[id]
	n.Crashed = true
	return n
}

// Restart replaces id's stack with a fresh one at the next incarnation,
// follows the chain from the head, and rejoins per doc 02 section 4.5:
// still-held leases adopt at unchanged epochs, nothing from the old
// life's memory survives.
func (f *Fleet) Restart(ctx context.Context, id uint64) (*Node, error) {
	old := f.nodes[id]
	n, err := f.newNode(id, old.Inc+1)
	if err != nil {
		return nil, err
	}
	if err := n.Ap.Follow(ctx); err != nil {
		return nil, err
	}
	if err := n.Mgr.Rejoin(ctx, f.member(n)); err != nil {
		return nil, err
	}
	f.nodes[id] = n
	return n, nil
}

// FollowAll catches every live node up, the pre-assertion sync.
func (f *Fleet) FollowAll(ctx context.Context) error {
	for _, id := range f.order {
		n := f.nodes[id]
		if n.Crashed {
			continue
		}
		if err := n.Ap.Follow(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Coverage reports every group's holder as one live observer's fold
// sees it; full is true when every group has one.
func (f *Fleet) Coverage(observer uint64) (holders map[uint16]uint64, full bool) {
	n := f.nodes[observer]
	holders = make(map[uint16]uint64, f.cfg.NGroups)
	full = true
	for g := 0; g < f.cfg.NGroups; g++ {
		node, _, ok := n.Fold.Holder(uint16(g))
		if !ok {
			full = false
			continue
		}
		holders[uint16(g)] = node
	}
	return holders, full
}

// FlushData PUTs one WAL object of frames [first..last] for group under
// id's namespace and appends the commit record, the doc 02 section 4.3
// step 3 shape, so takeover replay has real data to prove itself on.
func (f *Fleet) FlushData(ctx context.Context, id uint64, group uint16, first, last uint64) error {
	n := f.nodes[id]
	_, epoch, ok := n.Fold.Holder(group)
	if !ok {
		return fmt.Errorf("fleetsim: node %d flushing unheld group %d", id, group)
	}
	frames := make([]obs1.WALFrame, 0, last-first+1)
	for seq := first; seq <= last; seq++ {
		frames = append(frames, obs1.WALFrame{
			Kind: 0x01, Slot: 9, Seq: seq,
			Key:     []byte(fmt.Sprintf("fk%06d", seq)),
			Payload: []byte(fmt.Sprintf("fv%06d", seq)),
		})
	}
	body, err := obs1.AppendWAL(nil, id, []obs1.WALSection{{Group: group, Epoch: epoch, Frames: frames}})
	if err != nil {
		return err
	}
	n.walSeq++
	key := fmt.Sprintf("%s/wal/%016x/%016d", f.prefix, id, n.walSeq)
	if _, err := f.Store.Put(ctx, key, body); err != nil {
		return err
	}
	off, flen, err := obs1.ParseTail(body[len(body)-obs1.TailSize:])
	if err != nil {
		return err
	}
	entries, err := obs1.ParseWALFooter(body[off : off+uint64(flen)])
	if err != nil {
		return err
	}
	rec := obs1.CommitRecord{WALNode: id, WALSeq: n.walSeq, WALSize: uint64(len(body))}
	for _, e := range entries {
		rec.Sections = append(rec.Sections, e.CommitSection())
	}
	if _, err := n.Ap.Append(ctx, []obs1.ChainRecord{rec}); err != nil {
		return err
	}
	n.Mgr.NoteAppend(f.Clk.Now())
	return nil
}
