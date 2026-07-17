// The lease fold (spec 2064/obs1 doc 02 section 3): grants, releases,
// heartbeats, and membership folded from the chain into the lease table.
// The fold is a pure function of the chain per invariant C-I2, so it is
// deliberately clock-free: chain records carry no timestamps, which means
// freeness-by-staleness (doc 02 section 3.2 case b) is writer discipline
// that the fold cannot and does not verify. What the fold does enforce is
// deterministic: a grant folds only when its epoch is exactly the group's
// current epoch plus one (the epoch half of C-I4), and a commit's sections
// fold only when the commit's epoch matches the table, the batch writer is
// the holder, and the lease was not released (C-I3, widened; see
// LeaseFold). Every folder sees the same chain in the same order, so every
// folder reaches the same table and the same verdicts, and a zombie's
// commits are dead on arrival everywhere.
//
// Deadlines are the one lease field that cannot be chain-derived: doc 02
// section 3.1 defines the deadline from the holder's wall clock, which
// never appears on the chain. The fold therefore tracks renewal positions,
// not deadlines; the DeadlineMS a checkpoint carries is the checkpoint
// writer's local observation, advisory per C-I7, and is stamped by the
// checkpoint-writing slice, not here.
package obs1

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Lease timing defaults (doc 02 section 5). All three are lab knobs,
// provisional until the clock-skew lab confirms them.
const (
	DefaultLeaseTTL       = 3000 * time.Millisecond
	DefaultSkewBound      = 500 * time.Millisecond
	DefaultHeartbeatEvery = 1000 * time.Millisecond
)

// CommitVerdict reports one commit record's fencing outcome to the data
// fold: Live[i] is whether Sections[i] folds. The lease fold computes the
// verdict; what to do with live sections is the doc 06 fold's business.
type CommitVerdict struct {
	Pos    ChainPos
	Writer uint64
	Commit CommitRecord
	Live   []bool
}

// FoldStats counts the records the fold rejected, each for a
// deterministic reason every folder agrees on. They exist for tests and
// the fence-torture lab; nothing reads them on a hot path.
type FoldStats struct {
	GrantsRejected   uint64 // epoch was not current plus one
	ReleasesRejected uint64 // wrong epoch, wrong writer, or already released
	SectionsDead     uint64 // commit sections that failed the fence
	MembersStale     uint64 // member records that lost the incarnation guard
}

type groupLease struct {
	node     uint64
	epoch    uint32
	released bool
	renew    ChainPos
}

// LeaseFold is the lease table as a fold over one domain's chain. It
// implements ChainApplier for the #902 appender and EpochHistory for
// manifest selection.
//
// The commit fence is one check wider than doc 02 section 3.3's literal
// rule: besides the epoch match, the batch writer must be the group's
// holder and the lease must not have been released. Epoch match already
// implies both for every writer that follows the protocol; the extra
// checks only change the verdict for a writer that commits under an epoch
// it was never granted or after it gave the lease back, and rejecting
// those deterministically is strictly safer.
type LeaseFold struct {
	dd      uint8
	haveDD  bool
	next    uint64 // seq the next ApplyChain must carry
	groups  map[uint16]*groupLease
	members map[uint64]Member
	grants  map[uint16]map[uint32]ChainPos // group, epoch: where that epoch was granted
	ckpt    ChainPos                       // newest checkpoint record seen
	primed  bool

	// OnCommit, when set, receives every commit record's verdict in chain
	// order. An error stops the fold and, through the appender, freezes
	// the tail. The sink observes; it must not mutate the fold.
	OnCommit func(CommitVerdict) error

	Stats FoldStats
}

// NewLeaseFold starts an empty fold that expects the chain from its first
// object. Prime it from a checkpoint first when booting from one.
func NewLeaseFold() *LeaseFold {
	return &LeaseFold{
		groups:  make(map[uint16]*groupLease),
		members: make(map[uint64]Member),
		grants:  make(map[uint16]map[uint32]ChainPos),
	}
}

// Prime loads the checkpoint tables, per the BootChain compose order:
// BootChain, Prime, Follow. A released group's lease row travels with
// Node zero, which keeps the epoch counter across the summary; node ids
// are nonzero everywhere else. Prime must run before any ApplyChain.
func (f *LeaseFold) Prime(c Checkpoint) error {
	if f.next != 0 || f.primed {
		return fmt.Errorf("obs1: lease fold already primed or applying")
	}
	for _, m := range c.Members {
		if m.Node == 0 {
			return fmt.Errorf("obs1: checkpoint member with node id 0")
		}
		f.members[m.Node] = m
	}
	for _, l := range c.Leases {
		f.groups[l.Group] = &groupLease{
			node:     l.Node,
			epoch:    l.Epoch,
			released: l.Node == 0,
			renew:    c.Through,
		}
	}
	f.dd = c.Through.DD
	f.haveDD = true
	f.next = c.Through.Seq + 1
	f.primed = true
	return nil
}

// ApplyChain folds one batch. Batches must arrive dense and in order,
// which is exactly what the appender guarantees; a gap or a replay would
// silently fork the table, so both are errors.
func (f *LeaseFold) ApplyChain(pos ChainPos, h Header, batch ChainBatch) error {
	if !f.haveDD {
		f.dd = pos.DD
		f.haveDD = true
		f.next = 1
	}
	if pos.DD != f.dd {
		return fmt.Errorf("obs1: lease fold for domain %d got batch in domain %d", f.dd, pos.DD)
	}
	if f.next == 0 {
		f.next = 1
	}
	if pos.Seq != f.next {
		return fmt.Errorf("obs1: lease fold expected seq %d, got %d", f.next, pos.Seq)
	}
	for _, r := range batch.Records {
		if err := f.applyRecord(pos, h.Writer, r); err != nil {
			return err
		}
	}
	f.next = pos.Seq + 1
	return nil
}

func (f *LeaseFold) applyRecord(pos ChainPos, writer uint64, r ChainRecord) error {
	switch rec := r.(type) {
	case GrantRecord:
		f.applyGrant(pos, rec)
	case ReleaseRecord:
		f.applyRelease(pos, writer, rec)
	case HeartbeatRecord:
		f.renewHeld(pos, writer)
	case CommitRecord:
		if err := f.applyCommit(pos, writer, rec); err != nil {
			return err
		}
	case MemberRecord:
		f.applyMember(rec)
	case CheckpointRecord:
		if f.ckpt.Seq < rec.Pos.Seq {
			f.ckpt = rec.Pos
		}
	default:
		return fmt.Errorf("obs1: lease fold got unknown record type %T", r)
	}
	return nil
}

// applyGrant is C-I4's epoch half: the grant folds only when its epoch is
// exactly the group's current epoch plus one. The freeness half (release
// seen, or staleness observed plus the taker's full-TTL wait) is the
// grant writer's discipline; the fold cannot check it without a clock,
// and per doc 02 section 3.3 it does not need to, because an early grant
// still moves the epoch deterministically for every folder.
func (f *LeaseFold) applyGrant(pos ChainPos, rec GrantRecord) {
	g := f.groups[rec.Group]
	var cur uint32
	if g != nil {
		cur = g.epoch
	}
	if rec.Node == 0 || rec.Epoch != cur+1 {
		f.Stats.GrantsRejected++
		return
	}
	f.groups[rec.Group] = &groupLease{node: rec.Node, epoch: rec.Epoch, renew: pos}
	spans := f.grants[rec.Group]
	if spans == nil {
		spans = make(map[uint32]ChainPos)
		f.grants[rec.Group] = spans
	}
	spans[rec.Epoch] = pos
}

func (f *LeaseFold) applyRelease(pos ChainPos, writer uint64, rec ReleaseRecord) {
	g := f.groups[rec.Group]
	if g == nil || g.released || g.epoch != rec.Epoch || g.node != writer {
		f.Stats.ReleasesRejected++
		return
	}
	g.released = true
	g.renew = pos
}

// applyCommit fences each section (C-I3) and then renews the writer's
// leases: doc 02 section 3.1 makes renewal implicit in every commit and
// heartbeat.
func (f *LeaseFold) applyCommit(pos ChainPos, writer uint64, rec CommitRecord) error {
	live := make([]bool, len(rec.Sections))
	for i, s := range rec.Sections {
		g := f.groups[s.Group]
		ok := g != nil && !g.released && g.epoch == s.Epoch && g.node == writer
		live[i] = ok
		if !ok {
			f.Stats.SectionsDead++
		}
	}
	if f.OnCommit != nil {
		if err := f.OnCommit(CommitVerdict{Pos: pos, Writer: writer, Commit: rec, Live: live}); err != nil {
			return err
		}
	}
	f.renewHeld(pos, writer)
	return nil
}

func (f *LeaseFold) renewHeld(pos ChainPos, writer uint64) {
	for _, g := range f.groups {
		if g.node == writer && !g.released {
			g.renew = pos
		}
	}
}

// applyMember guards both directions with the incarnation: a delayed join
// must not downgrade a rejoined node, a delayed leave must not remove it.
func (f *LeaseFold) applyMember(rec MemberRecord) {
	old, ok := f.members[rec.Node]
	switch rec.Op {
	case MemberJoin:
		if rec.Node == 0 || (ok && rec.Incarnation < old.Incarnation) {
			f.Stats.MembersStale++
			return
		}
		f.members[rec.Node] = rec.Member
	case MemberLeave:
		if !ok || rec.Incarnation < old.Incarnation {
			f.Stats.MembersStale++
			return
		}
		delete(f.members, rec.Node)
	default:
		f.Stats.MembersStale++
	}
}

// Holder reports a group's current lease: who holds it at what epoch.
// ok is false when the group was never granted or stands released.
func (f *LeaseFold) Holder(group uint16) (node uint64, epoch uint32, ok bool) {
	g := f.groups[group]
	if g == nil || g.released {
		return 0, 0, false
	}
	return g.node, g.epoch, true
}

// LastRenewal is the chain position of the group's last grant, release,
// commit, or heartbeat by its holder. This is the chain-observed renewal
// doc 02 section 3.2 case (b) reasons about; turning it into a staleness
// judgment takes an observer clock, which is the caller's.
func (f *LeaseFold) LastRenewal(group uint16) (ChainPos, bool) {
	g := f.groups[group]
	if g == nil {
		return ChainPos{}, false
	}
	return g.renew, true
}

// HeldBy lists the groups a node currently holds, ascending.
func (f *LeaseFold) HeldBy(node uint64) []uint16 {
	var out []uint16
	for id, g := range f.groups {
		if g.node == node && !g.released {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// Members returns the member table, ascending by node id, in checkpoint
// row order.
func (f *LeaseFold) Members() []Member {
	out := make([]Member, 0, len(f.members))
	for _, m := range f.members {
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b Member) int { return cmp.Compare(a.Node, b.Node) })
	return out
}

// Leases returns the lease table in checkpoint row order: released groups
// travel as Node zero with the epoch kept, DeadlineMS stays zero because
// deadlines are observer-stamped, never folded.
func (f *LeaseFold) Leases() []LeaseEntry {
	out := make([]LeaseEntry, 0, len(f.groups))
	for id, g := range f.groups {
		e := LeaseEntry{Group: id, Node: g.node, Epoch: g.epoch}
		if g.released {
			e.Node = 0
		}
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b LeaseEntry) int { return cmp.Compare(a.Group, b.Group) })
	return out
}

// Applied is the last chain position folded, the checkpoint's Through
// when nothing has been applied since Prime.
func (f *LeaseFold) Applied() ChainPos {
	if f.next == 0 {
		return ChainPos{}
	}
	return ChainPos{DD: f.dd, Seq: f.next - 1}
}

// EpochCurrentAtOrAfter implements EpochHistory (manifest selection's
// zombie-folder defense). An epoch's span runs from its grant to the next
// grant; the current epoch's span is open. For an epoch that ended before
// the fold was primed the span is unknown, and the answer is false: saying
// true without proof would let a zombie folder's manifest through, while
// false only rejects manifests older than the checkpoint this fold booted
// from, which the group cursor already points past.
func (f *LeaseFold) EpochCurrentAtOrAfter(group uint16, epoch uint32, from ChainPos) bool {
	g := f.groups[group]
	if g == nil || epoch == 0 || epoch > g.epoch {
		return false
	}
	if epoch == g.epoch {
		return true
	}
	end, ok := f.grants[group][epoch+1]
	return ok && from.Seq < end.Seq
}

// StateSum is a canonical digest of the deterministic fold state, the
// C-I2 comparison the fence-torture lab makes across independent folders:
// two folders that consumed the same chain must return the same sum.
func (f *LeaseFold) StateSum() uint32 {
	var b []byte
	b = append(b, f.dd)
	b = binary.LittleEndian.AppendUint64(b, f.Applied().Seq)
	b = binary.LittleEndian.AppendUint64(b, f.ckpt.Seq)
	for _, l := range f.Leases() {
		g := f.groups[l.Group]
		b = binary.LittleEndian.AppendUint16(b, l.Group)
		b = binary.LittleEndian.AppendUint64(b, l.Node)
		b = binary.LittleEndian.AppendUint32(b, l.Epoch)
		b = binary.LittleEndian.AppendUint64(b, g.renew.Seq)
	}
	for _, m := range f.Members() {
		b = binary.LittleEndian.AppendUint64(b, m.Node)
		b = binary.LittleEndian.AppendUint32(b, m.Incarnation)
		b = binary.LittleEndian.AppendUint16(b, m.Weight)
		b = append(b, m.Resp...)
		b = append(b, 0)
		b = append(b, m.Mesh...)
		b = append(b, 0)
		b = append(b, m.Version...)
		b = append(b, 0)
	}
	return crc32c(b)
}

// LeaseGuard is the holder side of doc 02 section 3.5: a node's belief
// about its own deadlines, tracked from its own successful appends, never
// from the chain. When an append cannot land before a group's believed
// deadline minus the skew bound, the group suspends: writes park with
// reason lease, reads continue flagged stale, retries keep going. A retry
// that lands before any foreign grant un-suspends the group at the same
// epoch through Renewed; a foreign grant in the fold demotes it through
// Drop. All times are injected, so the guard is deterministic under test;
// one #899 flag rides with it: heartbeat appends get their own cadence
// and must never queue behind data commits, or a saturated chain expires
// live leases.
type LeaseGuard struct {
	mu       sync.Mutex
	ttl      time.Duration
	skew     time.Duration
	deadline map[uint16]time.Time
}

// NewLeaseGuard builds a guard; zero durations take the doc 02 defaults.
func NewLeaseGuard(ttl, skew time.Duration) *LeaseGuard {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	if skew <= 0 {
		skew = DefaultSkewBound
	}
	return &LeaseGuard{ttl: ttl, skew: skew, deadline: make(map[uint16]time.Time)}
}

// Renewed records a successful append that renewed the group (a grant to
// ourselves, a commit, or a heartbeat) at local time at, extending the
// believed deadline to at plus the TTL.
func (g *LeaseGuard) Renewed(group uint16, at time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.deadline[group] = at.Add(g.ttl)
}

// Drop forgets a group: released voluntarily or demoted by a foreign
// grant.
func (g *LeaseGuard) Drop(group uint16) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.deadline, group)
}

// Suspended reports whether the group must suspend at local time now: the
// believed deadline minus the skew bound has passed, so an ack given now
// could race a legitimate takeover. A group the guard never saw renewed
// is suspended by definition.
func (g *LeaseGuard) Suspended(group uint16, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	d, ok := g.deadline[group]
	if !ok {
		return true
	}
	return !now.Before(d.Add(-g.skew))
}

// Horizon reports the earliest moment any tracked group suspends: the
// minimum believed deadline minus the skew bound. Before it, no tracked
// group is suspended, which is the whole-guard fast path the serving gate
// caches, and it is also the latest instant a heartbeat must land by, the
// scheduling input the heartbeat loop reads. False when the guard tracks
// nothing, in which case every group is suspended by definition.
func (g *LeaseGuard) Horizon() (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var h time.Time
	ok := false
	for _, d := range g.deadline {
		if !ok || d.Before(h) {
			h = d
			ok = true
		}
	}
	if !ok {
		return time.Time{}, false
	}
	return h.Add(-g.skew), true
}
