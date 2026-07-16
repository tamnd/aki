// Checkpoint writing and chain trimming (spec 2064/obs1 doc 02 section
// 2.5). Every 4096 records or 60 seconds, whichever first, the node that
// appends the triggering record writes a checkpoint object, appends a
// kind 0x06 record pointing at it, and advances the root pointer; chain
// objects older than the second-newest checkpoint become trimmable, and
// the previous checkpoint is always retained so a reader mid-replay never
// sees its floor vanish.
//
// The Checkpointer is the observer that stamps what the fold cannot: a
// checkpoint's DeadlineMS is the writer's local view (arrival time of the
// group's last renewal plus the TTL), advisory per C-I7, because chain
// records carry no timestamps and the fold is clock-free by design.
package obs1

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Checkpoint cadence defaults (doc 02 section 2.5); both are lab knobs.
const (
	DefaultCheckpointRecords = 4096
	DefaultCheckpointEvery   = 60 * time.Second
)

// Checkpointer wraps the lease fold as a node's ChainApplier and tracks
// the checkpoint cadence. The record count is chain-global: every record
// counts no matter who appended it, and any checkpoint record resets it.
// The trigger belongs to whoever appended the crossing record, so Due
// turns true only when one of our own batches crossed the threshold;
// if the responsible node dies before writing, the count keeps climbing
// and our next own append inherits the trigger.
//
// Appending the 0x06 record from inside ApplyChain would re-enter the
// appender, so the Checkpointer only raises Due; the owner checks Due
// after each of its own appends and calls WriteCheckpoint outside the
// apply path.
type Checkpointer struct {
	fold *LeaseFold
	self uint64
	ttl  time.Duration
	now  func() time.Time

	maxRecords int
	maxAge     time.Duration

	records int
	sinceMS time.Time // local time the cadence last reset
	due     bool

	newest ChainPos // the two newest checkpoints observed via 0x06
	second ChainPos
	floor  uint64 // first chain seq not yet handed to trimming

	stamps map[uint16]time.Time // local arrival of each group's last renewal
}

// NewCheckpointer wraps fold for the node self. Zero maxRecords, maxAge,
// or ttl take the doc 02 defaults. Groups already held at construction
// (a fold primed from a checkpoint) are stamped now: this observer first
// saw them alive at boot.
func NewCheckpointer(fold *LeaseFold, self uint64, ttl time.Duration, maxRecords int, maxAge time.Duration, now func() time.Time) (*Checkpointer, error) {
	if fold == nil || now == nil {
		return nil, fmt.Errorf("obs1: checkpointer needs a fold and a clock")
	}
	if self == 0 {
		return nil, fmt.Errorf("obs1: checkpointer needs a nonzero writer id")
	}
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	if maxRecords <= 0 {
		maxRecords = DefaultCheckpointRecords
	}
	if maxAge <= 0 {
		maxAge = DefaultCheckpointEvery
	}
	c := &Checkpointer{
		fold: fold, self: self, ttl: ttl, now: now,
		maxRecords: maxRecords, maxAge: maxAge,
		sinceMS: now(), stamps: make(map[uint16]time.Time),
	}
	if primed := fold.Applied(); primed.Seq > 0 {
		c.primedAt(primed)
	}
	return c, nil
}

// Fold is the wrapped lease fold, for reads and manifest selection.
func (c *Checkpointer) Fold() *LeaseFold { return c.fold }

// Prime primes the wrapped fold from a checkpoint and takes over the
// observer bookkeeping: the summarized position is the newest known
// checkpoint and the trim floor, and every held group is stamped now,
// since this observer first saw those leases alive at boot. The stamp is
// advisory like everything DeadlineMS-shaped (C-I7); the checkpoint
// writer's own stamp was another node's clock and does not travel.
// Boot order is BootChain, Prime, Follow: BootChain needs the applier
// before the checkpoint exists, so priming cannot happen in the
// constructor for that flow.
func (c *Checkpointer) Prime(ck Checkpoint) error {
	if err := c.fold.Prime(ck); err != nil {
		return err
	}
	c.primedAt(ck.Through)
	return nil
}

// primedAt records a summarized position picked up at construction or
// via Prime.
func (c *Checkpointer) primedAt(pos ChainPos) {
	c.newest = pos
	c.floor = pos.Seq
	t := c.now()
	for _, l := range c.fold.Leases() {
		if l.Node != 0 {
			c.stamps[l.Group] = t
		}
	}
}

// ApplyChain folds the batch, then does the observer's bookkeeping:
// renewal stamps, the two-newest checkpoint positions, and the cadence.
func (c *Checkpointer) ApplyChain(pos ChainPos, h Header, batch ChainBatch) error {
	if err := c.fold.ApplyChain(pos, h, batch); err != nil {
		return err
	}
	t := c.now()
	for _, l := range c.fold.Leases() {
		if l.Node == 0 {
			continue
		}
		if p, ok := c.fold.LastRenewal(l.Group); ok && p == pos {
			c.stamps[l.Group] = t
		}
	}
	reset := false
	for _, r := range batch.Records {
		c.records++
		if ck, ok := r.(CheckpointRecord); ok && ck.Pos.Seq > c.newest.Seq {
			c.second, c.newest = c.newest, ck.Pos
			reset = true
		}
	}
	if reset {
		c.records = 0
		c.sinceMS = t
		c.due = false
		return nil
	}
	if c.records >= c.maxRecords || t.Sub(c.sinceMS) >= c.maxAge {
		if h.Writer == c.self {
			c.due = true
		}
	}
	return nil
}

// Due reports whether one of our own appends crossed the cadence and the
// checkpoint is ours to write.
func (c *Checkpointer) Due() bool { return c.due }

// Snapshot renders the fold's tables as a checkpoint through the fold's
// applied position, with DeadlineMS stamped from this observer's clock:
// arrival of the group's last renewal plus the TTL. Released rows keep
// deadline zero.
func (c *Checkpointer) Snapshot() Checkpoint {
	leases := c.fold.Leases()
	for i, l := range leases {
		if l.Node == 0 {
			continue
		}
		if t, ok := c.stamps[l.Group]; ok {
			leases[i].DeadlineMS = uint64(t.Add(c.ttl).UnixMilli())
		}
	}
	return Checkpoint{
		Through: c.fold.Applied(),
		Members: c.fold.Members(),
		Leases:  leases,
	}
}

// WriteCheckpoint is the doc 02 section 2.5 trigger action, run by the
// owner outside the apply path: write the checkpoint object, append the
// 0x06 record pointing at it, advance the root. Losing the object CAS to
// another node checkpointing the same seq is success, both are valid
// summaries. The returned position is what the 0x06 record names; the
// cadence resets when that record folds back through ApplyChain.
func (c *Checkpointer) WriteCheckpoint(ctx context.Context, s Store, prefix string, fallback bool, a *ChainAppender) (ChainPos, error) {
	ck := c.Snapshot()
	if ck.Through.Seq == 0 {
		return ChainPos{}, fmt.Errorf("obs1: nothing applied, nothing to checkpoint")
	}
	body, err := AppendCheckpoint(nil, c.self, ck)
	if err != nil {
		return ChainPos{}, err
	}
	key := chainCkptKey(prefix, ck.Through.DD, ck.Through.Seq)
	_, err = s.PutIfAbsent(ctx, key, body, WriteTag{Writer: fmt.Sprintf("%016x", c.self), Batch: seq16(ck.Through.Seq)})
	switch {
	case err == nil, errors.Is(err, ErrPrecondition):
		// Ours landed, or another node summarized the same seq first;
		// either object is a valid summary of the same chain prefix.
	case errors.Is(err, ErrAmbiguous):
		if _, _, gerr := s.Get(ctx, key); gerr != nil {
			return ChainPos{}, fmt.Errorf("obs1: checkpoint %s ambiguous and unreadable: %w", key, gerr)
		}
	default:
		return ChainPos{}, err
	}
	if _, err := a.Append(ctx, []ChainRecord{CheckpointRecord{Pos: ck.Through}}); err != nil {
		return ChainPos{}, err
	}
	root, err := LoadRoot(ctx, s, prefix, fallback)
	if err != nil {
		return ChainPos{}, err
	}
	root.CkptSeq = ck.Through.Seq
	root.CkptDD = uint16(ck.Through.DD)
	if err := AdvanceRoot(ctx, s, prefix, fallback, c.self, root); err != nil {
		return ChainPos{}, err
	}
	return ck.Through, nil
}

// Trimmable is the current trim work: chain seqs in [from, floor) are
// older than the second-newest checkpoint and deletable, the previous
// checkpoint itself stays. A second call after Trimmed returns an empty
// range until another checkpoint lands.
func (c *Checkpointer) Trimmable() (from, floor uint64) {
	if c.second.Seq == 0 {
		return 0, 0
	}
	if c.floor >= c.second.Seq {
		return 0, 0
	}
	return c.floor, c.second.Seq
}

// Trimmed records that the range up to floor has been handed to
// deletion.
func (c *Checkpointer) Trimmed(floor uint64) {
	if floor > c.floor {
		c.floor = floor
	}
}

// TrimChain deletes chain objects and stale checkpoint objects with seq
// in [from, floor), in DeleteObjects batches. It never LISTs (C-I6): the
// range is swept by name, and deleting a key that never existed is a
// no-op on every S3-class store. This is the inline primitive; the doc
// 06 delayed-deletion queue will own when to call it.
func TrimChain(ctx context.Context, s Store, prefix string, dd uint8, from, floor uint64) error {
	if from == 0 {
		from = 1
	}
	var keys []string
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		err := s.DeleteObjects(ctx, keys)
		keys = keys[:0]
		return err
	}
	for seq := from; seq < floor; seq++ {
		keys = append(keys, chainKey(prefix, dd, seq), chainCkptKey(prefix, dd, seq))
		if len(keys) >= deleteBatchMax {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}
