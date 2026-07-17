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
// starts, so store calls are plain single-owner calls.
//
// This slice covers the string plane (strset, keydel, expire, txn,
// noop); the collection kinds land with the collection registries'
// applier and are refused loudly until then, never skipped.
package replay

import (
	"fmt"
	"math"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// Config wires an Applier.
type Config struct {
	// Store routes a key to the store that owns it, the server's
	// key-to-shard mapping evaluated at boot; a single-store test
	// returns its one store unconditionally. Keys inside one txn run
	// always share a group and so a store, since a run rides one commit
	// section and a section is one group's frames.
	Store func(key []byte) *store.Store
}

// Stats counts what an Applier did, the recovery report's store half.
type Stats struct {
	Frames    uint64 // every frame accepted, markers and noops included
	StrSets   uint64
	Dels      uint64
	DelMisses uint64 // keydels naming an absent key, the idempotent case
	Expires   uint64
	Noops     uint64
	TxnRuns   uint64 // closed runs, however many frames each carried
}

// pending is one buffered frame inside an open txn run. The key and
// value slices alias the section buffer recovery fetched, which is safe
// because a run closes inside its own section (the doc 04 contiguity
// rule) and recovery applies a whole section before fetching the next.
type pending struct {
	key []byte
	op  obs1.Op
}

// Applier applies decoded ops to the store, buffering txn runs so a run
// lands atomically or not at all.
type Applier struct {
	cfg   Config
	open  map[uint16][]pending
	stats Stats
}

// New builds an Applier over cfg.
func New(cfg Config) *Applier {
	return &Applier{cfg: cfg, open: make(map[uint16][]pending)}
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
				if err := a.applyOne(p.key, p.op); err != nil {
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
	return a.applyOne(f.Key, op)
}

// Finish checks the terminal state after the last frame. A run still
// open means the stream ended mid-txn, which the commit path rules out
// (a run rides one section, contiguous, and recovery replays only
// committed sections), so it is a corruption signal here, not the doc 04
// tail-cut case; that cut happens before commit and replays as nothing
// upstream of this package.
func (a *Applier) Finish() error {
	for g, run := range a.open {
		return fmt.Errorf("obs1 replay: group %d ends with an open txn run of %d frames", g, len(run))
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

func (a *Applier) applyOne(key []byte, op obs1.Op) error {
	if _, ok := op.(obs1.Noop); ok {
		a.stats.Noops++
		return nil
	}
	st := a.cfg.Store(key)
	switch o := op.(type) {
	case obs1.StrSet:
		at, err := deadline(o.ExpiryMS)
		if err != nil {
			return err
		}
		if err := st.SetString(key, o.Value, 0, at, false); err != nil {
			return fmt.Errorf("obs1 replay: strset %q: %w", key, err)
		}
		a.stats.StrSets++
	case obs1.KeyDel:
		// A keydel may name an already absent key (doc 04: BITOP's
		// all-empty-source form frames one), so a miss is the idempotent
		// no-op, unlike every other keyed kind.
		if st.Del(key, 0) {
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
		v, ok := st.GetString(key, 0, nil)
		if !ok {
			return fmt.Errorf("obs1 replay: expire names absent key %q, the store and the frame stream diverged", key)
		}
		if err := st.SetString(key, v, 0, at, false); err != nil {
			return fmt.Errorf("obs1 replay: expire %q: %w", key, err)
		}
		a.stats.Expires++
	case obs1.CollDelta, obs1.CollNew, obs1.CollDrop, obs1.GroupDelta:
		return fmt.Errorf("obs1 replay: op kind 0x%02x is a collection op, collection replay is not wired yet", opKind(op))
	default:
		return fmt.Errorf("obs1 replay: op %T has no applier", op)
	}
	return nil
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
