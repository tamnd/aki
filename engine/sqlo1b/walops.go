package sqlo1b

import (
	"encoding/binary"
	"fmt"
)

// Format WAL ops (doc 03 section 12.2, ops 5..7). These are the
// frames the format layer itself emits: SEAL when an extent seals,
// CKPT when a checkpoint's superblock is durable, TRIM as the
// advisory trim barrier echo. The transport is the S1 WAL in
// engine/sqlo1; this file owns only the payloads and the replay
// fold. Data ops (PUT and friends) belong to the store slices.

// Format op codes, matching sqlo1.WALOpSeal and friends; kept as
// plain values here so the codec layer has no transport import.
const (
	FrameSeal uint8 = 5
	FrameCkpt uint8 = 6
	FrameTrim uint8 = 7
)

// SealOp records that an extent sealed: its number, the xxhash64 the
// referencer now holds, and the extent kind.
type SealOp struct {
	Extent uint64
	Sum    uint64
	Kind   uint8
}

// Encode is extent u64, checksum u64, kind u8.
func (s SealOp) Encode() []byte {
	b := make([]byte, 17)
	binary.LittleEndian.PutUint64(b[0:], s.Extent)
	binary.LittleEndian.PutUint64(b[8:], s.Sum)
	b[16] = s.Kind
	return b
}

// DecodeSealOp rejects short payloads and out-of-range kinds.
func DecodeSealOp(b []byte) (SealOp, error) {
	if len(b) != 17 {
		return SealOp{}, fmt.Errorf("sqlo1b: SEAL payload is %d bytes, want 17", len(b))
	}
	s := SealOp{
		Extent: binary.LittleEndian.Uint64(b[0:]),
		Sum:    binary.LittleEndian.Uint64(b[8:]),
		Kind:   b[16],
	}
	if s.Kind < KindVlog || s.Kind > KindStats {
		return SealOp{}, fmt.Errorf("sqlo1b: SEAL kind %d out of range", s.Kind)
	}
	return s, nil
}

// CkptOp marks a completed checkpoint: the frame is emitted only
// after the superblock carrying SuperSeq is durable (doc 03 section
// 13 step 6), so replay seeing it knows that root exists on disk.
type CkptOp struct {
	SuperSeq uint64
}

func (c CkptOp) Encode() []byte {
	return binary.LittleEndian.AppendUint64(nil, c.SuperSeq)
}

func DecodeCkptOp(b []byte) (CkptOp, error) {
	if len(b) != 8 {
		return CkptOp{}, fmt.Errorf("sqlo1b: CKPT payload is %d bytes, want 8", len(b))
	}
	return CkptOp{SuperSeq: binary.LittleEndian.Uint64(b)}, nil
}

// TrimOp echoes the advisory trim barrier.
type TrimOp struct {
	WALSeq uint64
}

func (t TrimOp) Encode() []byte {
	return binary.LittleEndian.AppendUint64(nil, t.WALSeq)
}

func DecodeTrimOp(b []byte) (TrimOp, error) {
	if len(b) != 8 {
		return TrimOp{}, fmt.Errorf("sqlo1b: TRIM payload is %d bytes, want 8", len(b))
	}
	return TrimOp{WALSeq: binary.LittleEndian.Uint64(b)}, nil
}

// SealFrame is a replayed SEAL with the WAL seq it arrived under;
// recovery quarantines extents whose seal seq lands after the
// surviving superblock's checkpoint (doc 03 section 14).
type SealFrame struct {
	WALSeq uint64
	SealOp
}

// FormatState folds the format frames out of a WAL replay. Frames
// must arrive in strictly increasing seq order, which is what the
// transport's chain check delivers; Apply enforces it anyway because
// recovery correctness rides on it.
type FormatState struct {
	Seals       []SealFrame
	CkptSuper   uint64 // superblock seq of the last CKPT, 0 when none
	CkptWALSeq  uint64 // WAL seq that CKPT arrived under
	TrimEcho    uint64 // last TRIM echo, advisory
	lastApplied uint64
}

// Apply folds one frame. Data ops pass through untouched (the store
// replay owns them); unknown ops are an error because a format we do
// not recognize is not a format we can recover.
func (st *FormatState) Apply(walSeq uint64, op uint8, payload []byte) error {
	if walSeq <= st.lastApplied {
		return fmt.Errorf("sqlo1b: replay seq %d after %d, frames must arrive in order", walSeq, st.lastApplied)
	}
	st.lastApplied = walSeq
	switch op {
	case FrameSeal:
		s, err := DecodeSealOp(payload)
		if err != nil {
			return err
		}
		st.Seals = append(st.Seals, SealFrame{WALSeq: walSeq, SealOp: s})
	case FrameCkpt:
		c, err := DecodeCkptOp(payload)
		if err != nil {
			return err
		}
		st.CkptSuper = c.SuperSeq
		st.CkptWALSeq = walSeq
	case FrameTrim:
		t, err := DecodeTrimOp(payload)
		if err != nil {
			return err
		}
		st.TrimEcho = t.WALSeq
	case 1, 2, 3, 4: // data ops, folded by the store replay
	default:
		return fmt.Errorf("sqlo1b: wal op %d unknown to the format layer", op)
	}
	return nil
}

// SealsAfter returns the seal frames that arrived after the given
// WAL seq, in replay order: the quarantine set when recovering to a
// superblock whose checkpoint froze at that seq.
func (st *FormatState) SealsAfter(walSeq uint64) []SealFrame {
	for i, s := range st.Seals {
		if s.WALSeq > walSeq {
			return st.Seals[i:]
		}
	}
	return nil
}
