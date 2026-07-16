// Manifests (spec 2064/obs1 doc 03 section 6): the per-group truth about
// which segments are live and where fold stands. Each manifest is
// complete, small, and supersedes all before it, written as a dense
// CAS-create sequence man/g<ggg>/<seq16>.
//
// The reader rule is what makes zombie folders harmless: a manifest is
// valid only if its epoch was the group's current epoch at some chain
// position at or after the previous valid manifest's fold cursor. The
// lease history lives on the chain (C-I2), so every reader applies the
// same rule and picks the same winner. The chain-derived fact source is
// an interface here; the lease fold slice implements it.
package obs1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// ChainPos is a position in the commit chain: log domain, then sequence
// within it. The packed u64 form (domain in the top byte, seq in the low
// 56 bits) orders numerically exactly as (DD, Seq) orders lexically,
// which is why manifests and checkpoints can carry one u64. 56 bits of
// seq holds the doc 02 ceiling of 10^16 appends with room to spare.
type ChainPos struct {
	DD  uint8
	Seq uint64
}

const chainSeqMax = 1<<56 - 1

// Pack encodes the position into the on-bucket u64.
func (p ChainPos) Pack() (uint64, error) {
	if p.Seq > chainSeqMax {
		return 0, fmt.Errorf("obs1: chain seq %d exceeds the packed 56-bit ceiling", p.Seq)
	}
	return uint64(p.DD)<<56 | p.Seq, nil
}

// UnpackChainPos decodes the on-bucket u64.
func UnpackChainPos(v uint64) ChainPos {
	return ChainPos{DD: uint8(v >> 56), Seq: v & chainSeqMax}
}

// Before is the chain's total order: domain first, then seq.
func (p ChainPos) Before(q ChainPos) bool {
	if p.DD != q.DD {
		return p.DD < q.DD
	}
	return p.Seq < q.Seq
}

// ManifestSeg is one live segment's row.
type ManifestSeg struct {
	SegSeq   uint64
	Level    uint8
	TTLClass uint8
	Size     uint64 // object size in the bucket
	NRecords uint64
	RawBytes uint64
	MinExpMS uint64
	MaxExpMS uint64
	DeadFrac uint16 // per-mille dead-record estimate (doc 06)
}

// Manifest is one complete statement of a group's fold state.
type Manifest struct {
	Group   uint16
	Epoch   uint32 // folder's lease epoch at write time
	ManSeq  uint64
	FoldPos ChainPos // chain position through which WAL frames are folded
	Segs    []ManifestSeg
}

const (
	manFixed = 2 + 4 + 8 + 8 + 4 // group..nsegs
	manSeg   = 8 + 1 + 1 + 8 + 8 + 8 + 8 + 8 + 2
)

// manifestKey renders man/g<ggg>/<seq16> under the database prefix.
func manifestKey(prefix string, group uint16, seq uint64) string {
	return dbKey(prefix, fmt.Sprintf("man/g%03d/%s", group, seq16(seq)))
}

// AppendManifest appends the encoded manifest, header included. Segment
// rows must be in strictly increasing SegSeq order, the manifest's
// canonical form, so two folders can never emit the same truth with
// different bytes.
func AppendManifest(b []byte, writer uint64, m Manifest) ([]byte, error) {
	packed, err := m.FoldPos.Pack()
	if err != nil {
		return nil, err
	}
	for i, s := range m.Segs {
		if i > 0 && s.SegSeq <= m.Segs[i-1].SegSeq {
			return nil, fmt.Errorf("obs1: manifest seg %d seq %d after %d, rows must be strictly increasing", i, s.SegSeq, m.Segs[i-1].SegSeq)
		}
		if s.Level > 1 {
			return nil, fmt.Errorf("obs1: manifest seg %d level %d, doc 06 defines 0 and 1", i, s.Level)
		}
		if s.TTLClass == 0 {
			if s.MinExpMS != 0 || s.MaxExpMS != 0 {
				return nil, fmt.Errorf("obs1: manifest seg %d is TTL class 0 but carries expiry bounds", i)
			}
		} else if s.MinExpMS == 0 || s.MinExpMS > s.MaxExpMS {
			return nil, fmt.Errorf("obs1: manifest seg %d TTL class %d has expiry bounds %d..%d", i, s.TTLClass, s.MinExpMS, s.MaxExpMS)
		}
		if s.DeadFrac > 1000 {
			return nil, fmt.Errorf("obs1: manifest seg %d deadfrac %d per-mille", i, s.DeadFrac)
		}
	}
	b = AppendHeader(b, Header{Format: FormatManifest, FVersion: 1, Writer: writer})
	p := len(b)
	b = binary.LittleEndian.AppendUint16(b, m.Group)
	b = binary.LittleEndian.AppendUint32(b, m.Epoch)
	b = binary.LittleEndian.AppendUint64(b, m.ManSeq)
	b = binary.LittleEndian.AppendUint64(b, packed)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Segs)))
	for _, s := range m.Segs {
		b = binary.LittleEndian.AppendUint64(b, s.SegSeq)
		b = append(b, s.Level, s.TTLClass)
		b = binary.LittleEndian.AppendUint64(b, s.Size)
		b = binary.LittleEndian.AppendUint64(b, s.NRecords)
		b = binary.LittleEndian.AppendUint64(b, s.RawBytes)
		b = binary.LittleEndian.AppendUint64(b, s.MinExpMS)
		b = binary.LittleEndian.AppendUint64(b, s.MaxExpMS)
		b = binary.LittleEndian.AppendUint16(b, s.DeadFrac)
	}
	return binary.LittleEndian.AppendUint32(b, crc32c(b[p:])), nil
}

// ParseManifest reads a manifest object, enforcing the same shape rules
// the encoder does so accepted bytes re-encode exactly.
func ParseManifest(b []byte) (Manifest, Header, error) {
	var m Manifest
	h, err := ParseHeaderAs(b, FormatManifest)
	if err != nil {
		return m, Header{}, err
	}
	if h.FVersion != 1 {
		return m, Header{}, fmt.Errorf("obs1: manifest fversion %d, this build reads 1", h.FVersion)
	}
	p := b[HeaderSize:]
	if len(p) < manFixed+4 {
		return m, Header{}, fmt.Errorf("obs1: manifest payload is %d bytes, want at least %d", len(p), manFixed+4)
	}
	body, crc := p[:len(p)-4], binary.LittleEndian.Uint32(p[len(p)-4:])
	if got := crc32c(body); got != crc {
		return m, Header{}, fmt.Errorf("obs1: manifest crc 0x%08x, computed 0x%08x", crc, got)
	}
	m.Group = binary.LittleEndian.Uint16(body[0:2])
	m.Epoch = binary.LittleEndian.Uint32(body[2:6])
	m.ManSeq = binary.LittleEndian.Uint64(body[6:14])
	m.FoldPos = UnpackChainPos(binary.LittleEndian.Uint64(body[14:22]))
	nsegs := int(binary.LittleEndian.Uint32(body[22:26]))
	if len(body) != manFixed+nsegs*manSeg {
		return m, Header{}, fmt.Errorf("obs1: manifest with %d segs wants %d payload bytes, has %d", nsegs, manFixed+nsegs*manSeg+4, len(p))
	}
	if nsegs > 0 {
		m.Segs = make([]ManifestSeg, nsegs)
	}
	for i := range m.Segs {
		r := body[manFixed+i*manSeg:]
		m.Segs[i] = ManifestSeg{
			SegSeq:   binary.LittleEndian.Uint64(r[0:8]),
			Level:    r[8],
			TTLClass: r[9],
			Size:     binary.LittleEndian.Uint64(r[10:18]),
			NRecords: binary.LittleEndian.Uint64(r[18:26]),
			RawBytes: binary.LittleEndian.Uint64(r[26:34]),
			MinExpMS: binary.LittleEndian.Uint64(r[34:42]),
			MaxExpMS: binary.LittleEndian.Uint64(r[42:50]),
			DeadFrac: binary.LittleEndian.Uint16(r[50:52]),
		}
	}
	if again, err := AppendManifest(nil, h.Writer, m); err != nil {
		return Manifest{}, Header{}, err
	} else if !bytes.Equal(again, b) {
		return Manifest{}, Header{}, fmt.Errorf("obs1: manifest is not in canonical form")
	}
	return m, h, nil
}

// PutManifest CAS-creates the manifest at its dense-sequence key. A
// second folder racing for the same seq loses with ErrPrecondition and
// must re-read the tail before trying again.
func PutManifest(ctx context.Context, s Store, prefix string, writer uint64, m Manifest) error {
	b, err := AppendManifest(nil, writer, m)
	if err != nil {
		return err
	}
	_, err = s.PutIfAbsent(ctx, manifestKey(prefix, m.Group, m.ManSeq), b, WriteTag{Writer: fmt.Sprintf("%016x", writer), Batch: seq16(m.ManSeq)})
	return err
}

// LoadManifests walks the group's dense sequence GET-next from fromSeq
// (a checkpoint's man_seq hint, or 0 on a fresh group) to the tail, per
// C-I6, never a LIST. Missing means an empty slice, not an error.
func LoadManifests(ctx context.Context, s Store, prefix string, group uint16, fromSeq uint64) ([]Manifest, error) {
	var out []Manifest
	for seq := fromSeq; ; seq++ {
		b, _, err := s.Get(ctx, manifestKey(prefix, group, seq))
		if errors.Is(err, ErrNotFound) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		m, _, err := ParseManifest(b)
		if err != nil {
			return nil, err
		}
		if m.Group != group || m.ManSeq != seq {
			return nil, fmt.Errorf("obs1: manifest at %s says group %d seq %d", manifestKey(prefix, group, seq), m.Group, m.ManSeq)
		}
		out = append(out, m)
	}
}

// EpochHistory answers epoch-validity questions from the chain's lease
// history. The lease fold slice implements it; the manifest reader only
// needs this one question answered.
type EpochHistory interface {
	// EpochCurrentAtOrAfter reports whether epoch was the group's current
	// lease epoch at some chain position at or after from.
	EpochCurrentAtOrAfter(group uint16, epoch uint32, from ChainPos) bool
}

// SelectManifest applies the doc 03 section 6 reader rule to manifests in
// ManSeq order: a manifest is valid only if its epoch was current at or
// after the previous valid manifest's fold cursor, and the highest-seq
// valid manifest wins. Every reader sees the same chain, so every reader
// picks the same winner, which is what defangs a zombie folder that
// slipped a manifest in after losing its lease. False means no valid
// manifest exists.
func SelectManifest(group uint16, ms []Manifest, hist EpochHistory) (Manifest, bool) {
	var best Manifest
	var cursor ChainPos
	var prevSeq uint64
	seen, found := false, false
	for _, m := range ms {
		if m.Group != group {
			continue
		}
		if seen && m.ManSeq <= prevSeq {
			continue
		}
		prevSeq, seen = m.ManSeq, true
		if !hist.EpochCurrentAtOrAfter(group, m.Epoch, cursor) {
			continue
		}
		best, found = m, true
		cursor = m.FoldPos
	}
	return best, found
}
