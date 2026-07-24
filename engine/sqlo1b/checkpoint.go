package sqlo1b

import (
	"fmt"
	"time"
)

// The checkpoint protocol (doc 03 section 13): move durability from
// the WAL into the data file and advance the trim barrier. Six
// steps, all owner-driven:
//
//  1. freeze the target seq T
//  2. drain records at or below T into vlog extents
//  3. flush dirty index chunks and directory pages at or below T
//  4. write allocmap and stats snapshots
//  5. commit the alternate superblock (seq+1, wal_trim_seq T, new
//     roots) and sync it
//  6. emit the CKPT frame, advance the in-RAM trim barrier, release
//     quarantine
//
// A crash between any two steps recovers to the previous superblock
// and replays from its wal_trim_seq; structures written in steps
// 2..4 that the old superblock does not reference sit in
// quarantined-or-free extents and are reclaimed by normal
// allocation. That is why step 5 is the only commit point.

// Roots is what steps 3 and 4 produce for the new superblock.
// HighWater is the drain batch mark frozen with T: batches applied
// after the freeze land in WAL frames past T and replay after this
// checkpoint's trim barrier.
type Roots struct {
	Dir, Allocmap, Dict, Stats FullPtr
	RecordCount, GarbageBytes  uint64
	KeyRecordCount             uint64
	HighWater                  int64
}

// CheckpointSource is the store side of the protocol; the format
// core orchestrates, the store slices implement. Every method gets
// the same frozen T no matter how far the WAL has grown since.
type CheckpointSource interface {
	// Drain makes every dirty record with seq at or below T durable
	// in vlog extents: covering extents sealed or the active tail
	// synced (step 2).
	Drain(t uint64) error
	// FlushIndex does the same for dirty index chunks and directory
	// pages (step 3).
	FlushIndex(t uint64) error
	// Snapshot writes the allocmap and stats extents and returns the
	// new roots (step 4).
	Snapshot(t uint64) (Roots, error)
}

// CheckpointWAL is what the protocol needs from the sidecar;
// sqlo1.WAL satisfies it.
type CheckpointWAL interface {
	LastSeq() uint64
	Append(shard uint16, op, oflags uint8, payload []byte) (uint64, error)
	Flush() error
	SetTrim(seq uint64)
}

// Checkpointer runs the protocol against one shard's file, WAL, and
// grid.
type Checkpointer struct {
	WAL  CheckpointWAL
	File FileIO
	Grid *Grid
	// crash is the test failpoint: called after each numbered step,
	// a non-nil return abandons the checkpoint exactly there. Nil in
	// production.
	crash func(step int) error
}

// SetCrashPoint installs the step failpoint: fn runs after each
// numbered step and a non-nil return abandons the checkpoint exactly
// there. Harness use only; the B1 crash matrix in cmd/sqlo1crash
// drives it. Nil clears it.
func (c *Checkpointer) SetCrashPoint(fn func(step int) error) { c.crash = fn }

func (c *Checkpointer) boundary(step int) error {
	if c.crash == nil {
		return nil
	}
	return c.crash(step)
}

// Run executes one checkpoint from the current superblock and
// returns the committed successor. On any error the data file still
// opens through cur: nothing before step 5 touches either superblock
// slot, and step 5 overwrites only the slot cur does not occupy.
func (c *Checkpointer) Run(cur *Superblock, src CheckpointSource) (*Superblock, error) {
	t := c.WAL.LastSeq() // step 1: freeze the target
	if err := c.boundary(1); err != nil {
		return nil, err
	}
	if err := src.Drain(t); err != nil {
		return nil, fmt.Errorf("sqlo1b: checkpoint drain: %w", err)
	}
	if err := c.boundary(2); err != nil {
		return nil, err
	}
	if err := src.FlushIndex(t); err != nil {
		return nil, fmt.Errorf("sqlo1b: checkpoint index flush: %w", err)
	}
	if err := c.boundary(3); err != nil {
		return nil, err
	}
	roots, err := src.Snapshot(t)
	if err != nil {
		return nil, fmt.Errorf("sqlo1b: checkpoint snapshot: %w", err)
	}
	if err := c.boundary(4); err != nil {
		return nil, err
	}

	next := *cur // step 5: the only commit point
	next.Seq++
	next.WALTrimSeq = t
	next.DirRoot = roots.Dir
	next.AllocmapRoot = roots.Allocmap
	next.DictRoot = roots.Dict
	next.StatsRoot = roots.Stats
	next.RecordCount = roots.RecordCount
	next.KeyRecordCount = roots.KeyRecordCount
	next.GarbageBytes = roots.GarbageBytes
	next.HighWater = roots.HighWater
	if err := CommitSuperblock(c.File, &next); err != nil {
		return nil, fmt.Errorf("sqlo1b: checkpoint superblock: %w", err)
	}
	if err := c.boundary(5); err != nil {
		return &next, err
	}

	// Step 6: the CKPT frame is informational for replay (recovery
	// trusts the superblock, not the frame), so it rides the next
	// group commit rather than forcing its own fsync.
	if _, err := c.WAL.Append(0, FrameCkpt, 0, CkptOp{SuperSeq: next.Seq}.Encode()); err != nil {
		return &next, fmt.Errorf("sqlo1b: checkpoint CKPT frame: %w", err)
	}
	if err := c.WAL.Flush(); err != nil {
		return &next, fmt.Errorf("sqlo1b: checkpoint CKPT flush: %w", err)
	}
	c.WAL.SetTrim(t)
	if c.Grid != nil {
		c.Grid.ReleaseQuarantine(next.Seq)
	}
	if err := c.boundary(6); err != nil {
		return &next, err
	}
	return &next, nil
}

// CheckpointPolicy is the cadence decision (doc 03 section 13, doc
// 04 config): checkpoint on WAL bytes or elapsed time, whichever
// trips first. The runtime owns the counters and the clock; this is
// only the decision.
type CheckpointPolicy struct {
	Bytes    int64
	Interval time.Duration
}

// DefaultCheckpointPolicy is 256 MiB or 60 seconds.
func DefaultCheckpointPolicy() CheckpointPolicy {
	return CheckpointPolicy{Bytes: 256 << 20, Interval: 60 * time.Second}
}

// Due reports whether a checkpoint should run given WAL bytes
// written since the last one and time elapsed since it finished.
func (p CheckpointPolicy) Due(bytesSince int64, elapsed time.Duration) bool {
	return bytesSince >= p.Bytes || elapsed >= p.Interval
}
