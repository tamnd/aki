package sqlo1b

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/tamnd/aki/engine/sqlo1"
)

// Recovery, doc 03 section 14: pick the surviving superblock, open
// the sidecar at its trim barrier, and replay the tail to a
// consistent tip. Cost is O(WAL tail past the last checkpoint) plus
// O(1) root reads and never O(data), which is invariant F5; the only
// data-file bytes this code touches are the two superblock slots.

// RecoverySink receives the replayed data frames (PUT, DEL, PEXPIRE,
// GENBUMP) in seq order, exactly like live writes with drain and
// fsync suppressed. The frame payload aliases the replay buffer and
// is only valid inside the call; a sink keeps a copy or nothing.
// Store slices implement this; a nil sink folds format state only.
type RecoverySink interface {
	ApplyData(fr sqlo1.WALFrame) error
}

// Recovery is the outcome of a successful Recover: the surviving
// root, the open sidecar positioned to keep appending, the folded
// format state, and the tip the replay reached.
type Recovery struct {
	Super *Superblock
	Slot  int
	WAL   *sqlo1.WAL
	// Format is the fold over the replayed tail: seal registry,
	// last CKPT, trim echo.
	Format FormatState
	// Tip is the last replayed seq, or the trim barrier when the
	// tail was empty; the consistent tip the store serves from.
	Tip uint64
}

// WALDBID is the frame identity the sidecar carries: the low half of
// the superblock's db_id (the frame field is db_id_lo).
func (sb *Superblock) WALDBID() uint64 {
	return binary.LittleEndian.Uint64(sb.DBID[:8])
}

// Recover runs the section 14 algorithm. Step 1 picks the surviving
// superblock and refuses to open when neither slot verifies (no
// heroics on a lost root). Step 2, root checksum verification, is
// lazy by design and belongs to the structures' first touch. Step 3
// opens the sidecar under the superblock's db_id (the transport
// refuses foreign frames) and seeks to the trim barrier. Steps 4 and
// 5 replay to the first torn frame or the end, folding format ops
// and handing data ops to the sink. Advisory state (step 6) is lazy
// and correctness never depends on it.
//
// On success the caller owns closing the returned WAL.
func Recover(data io.ReaderAt, walPath string, walSegSize int64, sink RecoverySink) (*Recovery, error) {
	sb, slot, err := ReadSuperblock(data)
	if err != nil {
		return nil, err
	}
	w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), walSegSize)
	if err != nil {
		return nil, fmt.Errorf("sqlo1b: recovery wal: %w", err)
	}
	w.SetTrim(sb.WALTrimSeq)
	r := &Recovery{Super: sb, Slot: slot, WAL: w, Tip: sb.WALTrimSeq}
	err = w.Replay(func(fr sqlo1.WALFrame) error {
		if err := r.Format.Apply(fr.Seq, fr.Op, fr.Payload); err != nil {
			return err
		}
		// A CKPT frame is emitted only after its superblock is
		// durable, so one naming a seq past the survivor means a
		// committed root was destroyed; that is not a torn tail.
		if r.Format.CkptSuper > sb.Seq {
			return fmt.Errorf("sqlo1b: wal seq %d checkpoints superblock %d but the survivor is %d",
				fr.Seq, r.Format.CkptSuper, sb.Seq)
		}
		if fr.Op >= sqlo1.WALOpPut && fr.Op <= sqlo1.WALOpGenbump && sink != nil {
			if err := sink.ApplyData(fr); err != nil {
				return err
			}
		}
		r.Tip = fr.Seq
		return nil
	})
	if err != nil {
		w.Close()
		return nil, err
	}
	return r, nil
}

// Quarantine returns the extents sealed after the surviving
// superblock, by seq: every replayed SEAL sits past the trim
// barrier, so the superblock's allocmap snapshot predates all of
// them and recovery must account for them itself.
func (r *Recovery) Quarantine() []SealFrame {
	return r.Format.SealsAfter(r.Super.WALTrimSeq)
}

// RestoreGrid folds the quarantine set into a grid loaded from the
// superblock's allocmap snapshot: each extent sealed after the
// superblock becomes not-free (sealed), growing the grid when the
// file grew past the snapshot. Extents freed after the superblock
// stay not-free in the snapshot, which leaks space until scrub
// reclaims it but can never hand out a referenced extent; that is
// the conservative side of retirement-by-root-sequence.
func (r *Recovery) RestoreGrid(g *Grid) error {
	for _, s := range r.Quarantine() {
		if s.Extent >= g.ExtentCount() {
			g.Grow(s.Extent - g.ExtentCount() + 1)
		}
		switch st := g.State(s.Extent); st {
		case StateFree, StateSealed:
			g.states[s.Extent] = StateSealed
		default:
			return fmt.Errorf("sqlo1b: replayed seal on extent %d in state %s", s.Extent, st)
		}
	}
	return nil
}
