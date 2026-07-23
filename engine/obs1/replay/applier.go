// Package replay turns committed WAL frames back into store state at
// boot (spec 2064/obs1 doc 04 sections 2 and 6): the consuming half of
// the op vocabulary opframe.go encodes. Recovery walks the chain, plans
// the live sections above each group's fold cursor, and hands every
// surviving frame to an Applier, which dispatches on the decoded op and
// mutates the store the way the owning command already decided. Frames
// carry post-decision effects, so application never re-runs randomness,
// clocks, or arithmetic, and every store call is stamped with now zero:
// lazy expiry stays out of replay entirely, a deadline that passed while
// the node was down falls to serve-time lazy expiry, and the rebuilt
// state is exactly the acked state whatever the boot clock says.
//
// Sequencing is recovery's job, not this package's: Apply trusts the seq
// gating recover.go already enforced and applies frames in arrival
// order. Boot replay is single-threaded, before any shard goroutine
// starts, so store and registry calls are plain single-owner calls under
// the BootCtx contract.
//
// Collection frames apply through each type package's exported Replay
// functions with plain arguments, so the registries' shapes stay
// private. The set, hash, and zset planes are wired; the remaining
// collection kinds are refused loudly until their planes land, never
// skipped.
package replay

import (
	"bytes"
	"fmt"
	"math"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/hash"
	"github.com/tamnd/aki/engine/obs1/set"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/zset"
)

// Config wires an Applier.
type Config struct {
	// Ctx routes a key to the shard context that owns it, the server's
	// key-to-shard mapping evaluated at boot over Runtime.BootCtx; a
	// single-shard test returns its one Ctx unconditionally. The string
	// plane writes Ctx(key).St and the collection planes hand the Ctx to
	// the type registries, so replayed state lands exactly where the
	// owner goroutine will look for it after Start. Keys inside one txn
	// run always share a group and so a shard, since a run rides one
	// commit section and a section is one group's frames.
	Ctx func(key []byte) *shard.Ctx
}

// Stats counts what an Applier did, the recovery report's store half.
type Stats struct {
	Frames    uint64 // every frame accepted, markers and noops included
	StrSets   uint64
	Dels      uint64
	DelMisses uint64 // keydels naming a key absent everywhere, the idempotent case
	Expires   uint64
	Noops     uint64
	TxnRuns   uint64 // closed runs, however many frames each carried
	CollNews  uint64
	CollDrops uint64
	SAdds     uint64
	SRems     uint64
	HSets     uint64
	HDels     uint64
	HExpires  uint64
	ZAdds     uint64
	ZRems     uint64
}

// pending is one buffered frame inside an open txn run. The key and
// value slices alias the section buffer recovery fetched, which is safe
// because a run closes inside its own section (the doc 04 contiguity
// rule) and recovery applies a whole section before fetching the next.
type pending struct {
	key []byte
	op  obs1.Op
}

// fresh is a collnew waiting for the colldelta that populates it: the
// emitters always frame the pair adjacently in one run, so the very next
// applied frame in the group must be that delta, on the same key, with a
// sub-op of the collnew's type. The key aliases the section buffer under
// the same contract as pending.
type fresh struct {
	key []byte
	typ uint8
}

// Applier applies decoded ops to the store and registries, buffering txn
// runs so a run lands atomically or not at all.
type Applier struct {
	cfg   Config
	open  map[uint16][]pending
	news  map[uint16]fresh
	stats Stats
}

// New builds an Applier over cfg.
func New(cfg Config) *Applier {
	return &Applier{cfg: cfg, open: make(map[uint16][]pending), news: make(map[uint16]fresh)}
}

// Stats returns the running counts.
func (a *Applier) Stats() Stats { return a.stats }

// Apply consumes one committed frame, the RecoverConfig.Apply seam.
// Errors are loud and terminal: a frame that cannot decode or a keyed op
// whose target diverged from the frame stream means the store and the
// log disagree, and recovery must stop rather than serve the difference.
func (a *Applier) Apply(group uint16, f obs1.WALFrame) error {
	op, err := obs1.DecodeOp(f)
	if err != nil {
		return err
	}
	if t, ok := op.(obs1.Txn); ok {
		if n, dangling := a.news[group]; dangling {
			return fmt.Errorf("obs1 replay: group %d hits a txn marker while collnew %q awaits its delta", group, n.key)
		}
		if t.Begin {
			if _, dup := a.open[group]; dup {
				return fmt.Errorf("obs1 replay: group %d opens a txn run inside an open run", group)
			}
			a.open[group] = nil
		} else {
			run, ok := a.open[group]
			if !ok {
				return fmt.Errorf("obs1 replay: group %d closes a txn run none is open", group)
			}
			delete(a.open, group)
			for _, p := range run {
				if err := a.applyOne(group, p.key, p.op); err != nil {
					return err
				}
			}
			a.stats.TxnRuns++
		}
		a.stats.Frames++
		return nil
	}
	if run, ok := a.open[group]; ok {
		a.open[group] = append(run, pending{key: f.Key, op: op})
		a.stats.Frames++
		return nil
	}
	a.stats.Frames++
	return a.applyOne(group, f.Key, op)
}

// Finish checks the terminal state after the last frame. A run still
// open means the stream ended mid-txn, which the commit path rules out
// (a run rides one section, contiguous, and recovery replays only
// committed sections), so it is a corruption signal here, not the doc 04
// tail-cut case; that cut happens before commit and replays as nothing
// upstream of this package. A collnew still waiting for its delta is the
// same class of signal, since the emitters frame the pair together.
func (a *Applier) Finish() error {
	for g, run := range a.open {
		return fmt.Errorf("obs1 replay: group %d ends with an open txn run of %d frames", g, len(run))
	}
	for g, n := range a.news {
		return fmt.Errorf("obs1 replay: group %d ends with collnew %q awaiting its delta", g, n.key)
	}
	return nil
}

// deadline converts a frame's absolute expiry ms to the store's signed
// form, refusing the values no emitter writes.
func deadline(ms uint64) (int64, error) {
	if ms > math.MaxInt64 {
		return 0, fmt.Errorf("obs1 replay: expiry %d ms overflows the store's deadline", ms)
	}
	return int64(ms), nil
}

func (a *Applier) applyOne(group uint16, key []byte, op obs1.Op) error {
	if _, ok := op.(obs1.Noop); ok {
		a.stats.Noops++
		return nil
	}
	cx := a.cfg.Ctx(key)
	if n, ok := a.news[group]; ok {
		delete(a.news, group)
		d, isDelta := op.(obs1.CollDelta)
		if !isDelta || !bytes.Equal(key, n.key) {
			return fmt.Errorf("obs1 replay: collnew %q in group %d is not followed by its delta", n.key, group)
		}
		return a.applyDelta(cx, key, d, true, n.typ)
	}
	switch o := op.(type) {
	case obs1.StrSet:
		at, err := deadline(o.ExpiryMS)
		if err != nil {
			return err
		}
		if err := cx.St.SetString(key, o.Value, 0, at, false); err != nil {
			return fmt.Errorf("obs1 replay: strset %q: %w", key, err)
		}
		a.stats.StrSets++
	case obs1.KeyDel:
		// A keydel removes a key of any type, so it probes both
		// keyspaces: the string store and every wired registry. It may
		// name a key absent everywhere (doc 04: BITOP's all-empty-source
		// form frames one), so a full miss is the idempotent no-op,
		// unlike every other keyed kind.
		hit := cx.St.Del(key, 0)
		if set.ReplayDrop(cx, key) {
			hit = true
		}
		if hash.ReplayDrop(cx, key) {
			hit = true
		}
		if zset.ReplayDrop(cx, key) {
			hit = true
		}
		if hit {
			a.stats.Dels++
		} else {
			a.stats.DelMisses++
		}
	case obs1.Expire:
		at, err := deadline(o.ExpiryMS)
		if err != nil {
			return err
		}
		// Read-modify-write through the normal set path: a record without
		// a TTL slot cannot take a deadline in place, and SetString's
		// band selection rebuilds the record with one when needed. An
		// expire frame is post-decision, so its key existed when the
		// owner framed it; a miss here is divergence.
		v, ok := cx.St.GetString(key, 0, nil)
		if !ok {
			return fmt.Errorf("obs1 replay: expire names absent key %q, the store and the frame stream diverged", key)
		}
		if err := cx.St.SetString(key, v, 0, at, false); err != nil {
			return fmt.Errorf("obs1 replay: expire %q: %w", key, err)
		}
		a.stats.Expires++
	case obs1.CollNew:
		// The hint bytes are doc 08's encoding hints, opaque here and
		// empty from every current emitter; application waits for the
		// paired delta, which carries the members that decide the shape.
		if o.Type != obs1.CollSet && o.Type != obs1.CollHash && o.Type != obs1.CollZSet {
			return fmt.Errorf("obs1 replay: collnew type 0x%02x is not wired for replay yet", o.Type)
		}
		a.news[group] = fresh{key: key, typ: o.Type}
		a.stats.CollNews++
	case obs1.CollDelta:
		return a.applyDelta(cx, key, o, false, 0)
	case obs1.CollDrop:
		// Typed drop, so a miss is corruption, unlike keydel's probe.
		if !set.ReplayDrop(cx, key) && !hash.ReplayDrop(cx, key) && !zset.ReplayDrop(cx, key) {
			return fmt.Errorf("obs1 replay: colldrop names key %q but no collection exists", key)
		}
		a.stats.CollDrops++
	case obs1.GroupDelta:
		return fmt.Errorf("obs1 replay: op kind 0x%02x is a collection op, collection replay is not wired yet", opKind(op))
	default:
		return fmt.Errorf("obs1 replay: op %T has no applier", op)
	}
	return nil
}

// applyDelta dispatches one colldelta sub-op. create is true when a
// collnew led this frame, and typ is that collnew's collection type,
// which the sub-op must match: a collnew whose delta belongs to another
// type means the frame stream is corrupt.
func (a *Applier) applyDelta(cx *shard.Ctx, key []byte, d obs1.CollDelta, create bool, typ uint8) error {
	if create {
		want, wired := deltaType(d.Sub)
		if !wired || want != typ {
			return fmt.Errorf("obs1 replay: collnew type 0x%02x on %q is followed by sub-op %T", typ, key, d.Sub)
		}
	}
	switch s := d.Sub.(type) {
	case obs1.SAdd:
		if err := set.ReplayAdd(cx, key, s.Members, create); err != nil {
			return err
		}
		a.stats.SAdds++
	case obs1.SRem:
		if err := set.ReplayRem(cx, key, s.Members); err != nil {
			return err
		}
		a.stats.SRems++
	case obs1.HSet:
		if err := hash.ReplayHSet(cx, key, flattenPairs(s.Pairs), create); err != nil {
			return err
		}
		a.stats.HSets++
	case obs1.HDel:
		if err := hash.ReplayHDel(cx, key, s.Fields); err != nil {
			return err
		}
		a.stats.HDels++
	case obs1.HExpire:
		if err := hash.ReplayHExpire(cx, key, s.AtMs, s.Fields); err != nil {
			return err
		}
		a.stats.HExpires++
	case obs1.ZAdd:
		scores, members := splitEntries(s.Entries)
		if err := zset.ReplayZAdd(cx, key, scores, members, create); err != nil {
			return err
		}
		a.stats.ZAdds++
	case obs1.ZRem:
		if err := zset.ReplayZRem(cx, key, s.Members); err != nil {
			return err
		}
		a.stats.ZRems++
	default:
		return fmt.Errorf("obs1 replay: colldelta sub-op %T is not wired for replay yet", d.Sub)
	}
	return nil
}

// flattenPairs lays field-value pairs out as the flat alternation the
// type seams speak, the same shape the emission side consumed.
func flattenPairs(pairs []obs1.FieldValue) [][]byte {
	out := make([][]byte, 0, 2*len(pairs))
	for _, p := range pairs {
		out = append(out, p.Field, p.Value)
	}
	return out
}

// splitEntries lays scored pairs out as the parallel slices the zset
// seam speaks, the same shape the emission side consumed.
func splitEntries(entries []obs1.ScoreMember) ([]float64, [][]byte) {
	scores := make([]float64, len(entries))
	members := make([][]byte, len(entries))
	for i, e := range entries {
		scores[i] = e.Score
		members[i] = e.Member
	}
	return scores, members
}

// deltaType maps a wired sub-op to the collection type its collnew must
// carry; wired is false for the sub-ops whose planes have not landed.
func deltaType(sub obs1.CollSub) (typ uint8, wired bool) {
	switch sub.(type) {
	case obs1.SAdd, obs1.SRem:
		return obs1.CollSet, true
	case obs1.HSet, obs1.HDel, obs1.HExpire:
		return obs1.CollHash, true
	case obs1.ZAdd, obs1.ZRem:
		return obs1.CollZSet, true
	}
	return 0, false
}

// opKind names an op's wire kind for error text without re-encoding.
func opKind(op obs1.Op) uint8 {
	switch op.(type) {
	case obs1.StrSet:
		return obs1.OpStrSet
	case obs1.KeyDel:
		return obs1.OpKeyDel
	case obs1.Expire:
		return obs1.OpExpire
	case obs1.CollDelta:
		return obs1.OpCollDelta
	case obs1.CollNew:
		return obs1.OpCollNew
	case obs1.CollDrop:
		return obs1.OpCollDrop
	case obs1.Txn:
		return obs1.OpTxn
	case obs1.Noop:
		return obs1.OpNoop
	case obs1.GroupDelta:
		return obs1.OpGroupDelta
	}
	return 0
}
