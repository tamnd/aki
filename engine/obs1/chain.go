// Chain record batches (spec 2064/obs1 doc 03 section 7): the objects the
// commit chain is made of, one CAS-created batch per append at
// chain/<dd>/<seq16>. A batch carries a writer-local batch id for the
// ambiguous-PUT re-check (doc 02 section 2.4) and one or more records of
// the six chain kinds. The append loop itself is the next slice; this
// layer owns the bytes.
//
// The commit record repeats the WAL footer's per-section index, so a
// folder or replayer plans its ranged GETs from the chain alone and never
// touches a WAL footer on the hot path.
package obs1

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Chain record kinds (doc 03 section 7).
const (
	recCommit     = 0x01
	recGrant      = 0x02
	recRelease    = 0x03
	recHeartbeat  = 0x04
	recMember     = 0x05
	recCheckpoint = 0x06
)

// ChainRecord is one record in a batch: exactly one of the six kinds.
type ChainRecord interface {
	recordKind() uint8
	appendBody(b []byte) ([]byte, error)
}

// CommitSection is one row of a commit record's section index: the WAL
// footer's index repeated onto the chain, enough to plan the ranged GET
// for one group's frames.
type CommitSection struct {
	Group     uint16
	Epoch     uint32
	Offset    uint64
	StoredLen uint32
	NFrames   uint32
	FirstSeq  uint64
	LastSeq   uint64
}

// CommitSection converts a parsed WAL footer row into the commit record's
// form, dropping RawLen, which only the section parser needs.
func (e WALIndexEntry) CommitSection() CommitSection {
	return CommitSection{
		Group: e.Group, Epoch: e.Epoch, Offset: e.Offset,
		StoredLen: e.StoredLen, NFrames: e.NFrames,
		FirstSeq: e.FirstSeq, LastSeq: e.LastSeq,
	}
}

// CommitRecord (0x01) publishes one WAL object's contents to the chain.
type CommitRecord struct {
	WALNode  uint64
	WALSeq   uint64
	WALSize  uint64
	Sections []CommitSection
}

// GrantRecord (0x02) hands a group's lease to a node at a new epoch.
type GrantRecord struct {
	Group uint16
	Node  uint64
	Epoch uint32
}

// ReleaseRecord (0x03) gives a group's lease back voluntarily.
type ReleaseRecord struct {
	Group uint16
	Epoch uint32
}

// HeartbeatRecord (0x04) has an empty body: the common header's writer
// field identifies the node and the lease table says what it holds.
type HeartbeatRecord struct{}

// Member is one node's row: identity, endpoints, weight, version. The
// member record and the checkpoint's member table share it.
type Member struct {
	Node        uint64
	Incarnation uint32
	Resp        string
	Mesh        string
	Weight      uint16
	Version     string
}

func (m Member) valid() error {
	if len(m.Resp) > 0xFFFF || len(m.Mesh) > 0xFFFF {
		return fmt.Errorf("obs1: member endpoint is over 65535 bytes")
	}
	if len(m.Version) > 0xFF {
		return fmt.Errorf("obs1: member version is over 255 bytes")
	}
	return nil
}

// Member ops.
const (
	MemberJoin  = 1
	MemberLeave = 2
)

// MemberRecord (0x05) is a membership change.
type MemberRecord struct {
	Op uint8 // MemberJoin or MemberLeave
	Member
}

// CheckpointRecord (0x06) points at a checkpoint object summarizing the
// chain through Pos.
type CheckpointRecord struct {
	Pos ChainPos
}

// ChainBatch is one chain object's payload.
type ChainBatch struct {
	BatchID     uint64 // writer-local unique id, the ambiguity re-check key
	Incarnation uint32
	Records     []ChainRecord
}

const (
	chainBatchHdr  = 8 + 4 + 2
	commitFixed    = 8 + 8 + 8 + 2
	commitSection  = 2 + 4 + 8 + 4 + 4 + 8 + 8
	recordBodyMax  = 0xFFFF - 1 // rlen covers kind plus body
	chainRecordMax = 0xFFFF
)

// chainKey renders chain/<dd>/<seq16> under the database prefix.
func chainKey(prefix string, dd uint8, seq uint64) string {
	return dbKey(prefix, fmt.Sprintf("chain/%02d/%s", dd, seq16(seq)))
}

// chainCkptKey renders chain/<dd>/ckpt/<seq16>.
func chainCkptKey(prefix string, dd uint8, seq uint64) string {
	return dbKey(prefix, fmt.Sprintf("chain/%02d/ckpt/%s", dd, seq16(seq)))
}

func (CommitRecord) recordKind() uint8 { return recCommit }

func (r CommitRecord) appendBody(b []byte) ([]byte, error) {
	if len(r.Sections) == 0 {
		return nil, fmt.Errorf("obs1: a commit record needs at least one section")
	}
	b = binary.LittleEndian.AppendUint64(b, r.WALNode)
	b = binary.LittleEndian.AppendUint64(b, r.WALSeq)
	b = binary.LittleEndian.AppendUint64(b, r.WALSize)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(r.Sections)))
	for i, s := range r.Sections {
		if s.NFrames == 0 || s.FirstSeq > s.LastSeq {
			return nil, fmt.Errorf("obs1: commit section %d has %d frames spanning seq %d..%d", i, s.NFrames, s.FirstSeq, s.LastSeq)
		}
		b = binary.LittleEndian.AppendUint16(b, s.Group)
		b = binary.LittleEndian.AppendUint32(b, s.Epoch)
		b = binary.LittleEndian.AppendUint64(b, s.Offset)
		b = binary.LittleEndian.AppendUint32(b, s.StoredLen)
		b = binary.LittleEndian.AppendUint32(b, s.NFrames)
		b = binary.LittleEndian.AppendUint64(b, s.FirstSeq)
		b = binary.LittleEndian.AppendUint64(b, s.LastSeq)
	}
	return b, nil
}

func (GrantRecord) recordKind() uint8 { return recGrant }

func (r GrantRecord) appendBody(b []byte) ([]byte, error) {
	b = binary.LittleEndian.AppendUint16(b, r.Group)
	b = binary.LittleEndian.AppendUint64(b, r.Node)
	return binary.LittleEndian.AppendUint32(b, r.Epoch), nil
}

func (ReleaseRecord) recordKind() uint8 { return recRelease }

func (r ReleaseRecord) appendBody(b []byte) ([]byte, error) {
	b = binary.LittleEndian.AppendUint16(b, r.Group)
	return binary.LittleEndian.AppendUint32(b, r.Epoch), nil
}

func (HeartbeatRecord) recordKind() uint8 { return recHeartbeat }

func (HeartbeatRecord) appendBody(b []byte) ([]byte, error) { return b, nil }

func (MemberRecord) recordKind() uint8 { return recMember }

func (r MemberRecord) appendBody(b []byte) ([]byte, error) {
	if r.Op != MemberJoin && r.Op != MemberLeave {
		return nil, fmt.Errorf("obs1: member record op %d, want join 1 or leave 2", r.Op)
	}
	if err := r.valid(); err != nil {
		return nil, err
	}
	b = append(b, r.Op)
	return appendMember(b, r.Member), nil
}

func appendMember(b []byte, m Member) []byte {
	b = binary.LittleEndian.AppendUint64(b, m.Node)
	b = binary.LittleEndian.AppendUint32(b, m.Incarnation)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(m.Resp)))
	b = append(b, m.Resp...)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(m.Mesh)))
	b = append(b, m.Mesh...)
	b = binary.LittleEndian.AppendUint16(b, m.Weight)
	b = append(b, uint8(len(m.Version)))
	return append(b, m.Version...)
}

func (CheckpointRecord) recordKind() uint8 { return recCheckpoint }

func (r CheckpointRecord) appendBody(b []byte) ([]byte, error) {
	b = append(b, r.Pos.DD)
	return binary.LittleEndian.AppendUint64(b, r.Pos.Seq), nil
}

// AppendChainBatch appends a complete chain object: header, batch header,
// records, payload crc.
func AppendChainBatch(b []byte, writer uint64, batch ChainBatch) ([]byte, error) {
	if len(batch.Records) == 0 {
		return nil, fmt.Errorf("obs1: a chain batch needs at least one record")
	}
	if len(batch.Records) > chainRecordMax {
		return nil, fmt.Errorf("obs1: chain batch has %d records, the format caps at %d", len(batch.Records), chainRecordMax)
	}
	b = AppendHeader(b, Header{Format: FormatChain, FVersion: 1, Writer: writer})
	p := len(b)
	b = binary.LittleEndian.AppendUint64(b, batch.BatchID)
	b = binary.LittleEndian.AppendUint32(b, batch.Incarnation)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(batch.Records)))
	for i, rec := range batch.Records {
		lenAt := len(b)
		b = append(b, 0, 0, rec.recordKind())
		bodyAt := len(b)
		var err error
		b, err = rec.appendBody(b)
		if err != nil {
			return nil, fmt.Errorf("obs1: chain record %d: %w", i, err)
		}
		if len(b)-bodyAt > recordBodyMax {
			return nil, fmt.Errorf("obs1: chain record %d body is %d bytes, the format caps at %d", i, len(b)-bodyAt, recordBodyMax)
		}
		binary.LittleEndian.PutUint16(b[lenAt:], uint16(1+len(b)-bodyAt))
	}
	return binary.LittleEndian.AppendUint32(b, crc32c(b[p:])), nil
}

func parseCommitBody(b []byte) (ChainRecord, error) {
	if len(b) < commitFixed {
		return nil, fmt.Errorf("obs1: commit record body is %d bytes, want at least %d", len(b), commitFixed)
	}
	r := CommitRecord{
		WALNode: binary.LittleEndian.Uint64(b[0:8]),
		WALSeq:  binary.LittleEndian.Uint64(b[8:16]),
		WALSize: binary.LittleEndian.Uint64(b[16:24]),
	}
	n := int(binary.LittleEndian.Uint16(b[24:26]))
	if n == 0 {
		return nil, fmt.Errorf("obs1: commit record with no sections")
	}
	if len(b) != commitFixed+n*commitSection {
		return nil, fmt.Errorf("obs1: commit record with %d sections wants %d body bytes, has %d", n, commitFixed+n*commitSection, len(b))
	}
	r.Sections = make([]CommitSection, n)
	for i := range r.Sections {
		s := b[commitFixed+i*commitSection:]
		r.Sections[i] = CommitSection{
			Group:     binary.LittleEndian.Uint16(s[0:2]),
			Epoch:     binary.LittleEndian.Uint32(s[2:6]),
			Offset:    binary.LittleEndian.Uint64(s[6:14]),
			StoredLen: binary.LittleEndian.Uint32(s[14:18]),
			NFrames:   binary.LittleEndian.Uint32(s[18:22]),
			FirstSeq:  binary.LittleEndian.Uint64(s[22:30]),
			LastSeq:   binary.LittleEndian.Uint64(s[30:38]),
		}
	}
	return r, nil
}

// parseMember reads one member row and returns the remainder.
func parseMember(b []byte) (Member, []byte, error) {
	var m Member
	take := func(n int, what string) ([]byte, error) {
		if len(b) < n {
			return nil, fmt.Errorf("obs1: member row truncated at %s", what)
		}
		v := b[:n]
		b = b[n:]
		return v, nil
	}
	v, err := take(14, "identity")
	if err != nil {
		return m, nil, err
	}
	m.Node = binary.LittleEndian.Uint64(v[0:8])
	m.Incarnation = binary.LittleEndian.Uint32(v[8:12])
	respLen := int(binary.LittleEndian.Uint16(v[12:14]))
	if v, err = take(respLen+2, "resp endpoint"); err != nil {
		return m, nil, err
	}
	m.Resp = string(v[:respLen])
	meshLen := int(binary.LittleEndian.Uint16(v[respLen:]))
	if v, err = take(meshLen+3, "mesh endpoint"); err != nil {
		return m, nil, err
	}
	m.Mesh = string(v[:meshLen])
	m.Weight = binary.LittleEndian.Uint16(v[meshLen:])
	verLen := int(v[meshLen+2])
	if v, err = take(verLen, "version"); err != nil {
		return m, nil, err
	}
	m.Version = string(v)
	return m, b, nil
}

func parseMemberBody(b []byte) (ChainRecord, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("obs1: member record body is empty")
	}
	r := MemberRecord{Op: b[0]}
	if r.Op != MemberJoin && r.Op != MemberLeave {
		return nil, fmt.Errorf("obs1: member record op %d, want join 1 or leave 2", r.Op)
	}
	m, rest, err := parseMember(b[1:])
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("obs1: member record has %d trailing bytes", len(rest))
	}
	r.Member = m
	return r, nil
}

func parseRecordBody(kind uint8, b []byte) (ChainRecord, error) {
	exact := func(n int) error {
		if len(b) != n {
			return fmt.Errorf("obs1: chain record kind 0x%02x body is %d bytes, want %d", kind, len(b), n)
		}
		return nil
	}
	switch kind {
	case recCommit:
		return parseCommitBody(b)
	case recGrant:
		if err := exact(14); err != nil {
			return nil, err
		}
		return GrantRecord{
			Group: binary.LittleEndian.Uint16(b[0:2]),
			Node:  binary.LittleEndian.Uint64(b[2:10]),
			Epoch: binary.LittleEndian.Uint32(b[10:14]),
		}, nil
	case recRelease:
		if err := exact(6); err != nil {
			return nil, err
		}
		return ReleaseRecord{
			Group: binary.LittleEndian.Uint16(b[0:2]),
			Epoch: binary.LittleEndian.Uint32(b[2:6]),
		}, nil
	case recHeartbeat:
		if err := exact(0); err != nil {
			return nil, err
		}
		return HeartbeatRecord{}, nil
	case recMember:
		return parseMemberBody(b)
	case recCheckpoint:
		if err := exact(9); err != nil {
			return nil, err
		}
		return CheckpointRecord{Pos: ChainPos{DD: b[0], Seq: binary.LittleEndian.Uint64(b[1:9])}}, nil
	}
	// Unknown kinds are rejected, not skipped: this build reads fversion 1,
	// where exactly six kinds exist, and a seventh arrives with a bump.
	return nil, fmt.Errorf("obs1: chain record kind 0x%02x is not a doc 03 kind", kind)
}

// ParseChainBatch reads a chain object, enforcing canonical form by
// re-encoding what it accepted and requiring the input bytes back.
func ParseChainBatch(b []byte) (ChainBatch, Header, error) {
	var batch ChainBatch
	h, err := ParseHeaderAs(b, FormatChain)
	if err != nil {
		return batch, Header{}, err
	}
	if h.FVersion != 1 {
		return batch, Header{}, fmt.Errorf("obs1: chain batch fversion %d, this build reads 1", h.FVersion)
	}
	p := b[HeaderSize:]
	if len(p) < chainBatchHdr+4 {
		return batch, Header{}, fmt.Errorf("obs1: chain batch payload is %d bytes, want at least %d", len(p), chainBatchHdr+4)
	}
	body, crc := p[:len(p)-4], binary.LittleEndian.Uint32(p[len(p)-4:])
	if got := crc32c(body); got != crc {
		return batch, Header{}, fmt.Errorf("obs1: chain batch crc 0x%08x, computed 0x%08x", crc, got)
	}
	batch.BatchID = binary.LittleEndian.Uint64(body[0:8])
	batch.Incarnation = binary.LittleEndian.Uint32(body[8:12])
	n := int(binary.LittleEndian.Uint16(body[12:14]))
	if n == 0 {
		return ChainBatch{}, Header{}, fmt.Errorf("obs1: chain batch with no records")
	}
	q := body[chainBatchHdr:]
	batch.Records = make([]ChainRecord, n)
	for i := range batch.Records {
		if len(q) < 3 {
			return ChainBatch{}, Header{}, fmt.Errorf("obs1: chain batch truncated at record %d", i)
		}
		rlen := int(binary.LittleEndian.Uint16(q[0:2]))
		if rlen < 1 || len(q) < 2+rlen {
			return ChainBatch{}, Header{}, fmt.Errorf("obs1: chain record %d rlen %d overruns the payload", i, rlen)
		}
		rec, err := parseRecordBody(q[2], q[3:2+rlen])
		if err != nil {
			return ChainBatch{}, Header{}, err
		}
		batch.Records[i] = rec
		q = q[2+rlen:]
	}
	if len(q) != 0 {
		return ChainBatch{}, Header{}, fmt.Errorf("obs1: chain batch has %d bytes after its last record", len(q))
	}
	if again, err := AppendChainBatch(nil, h.Writer, batch); err != nil {
		return ChainBatch{}, Header{}, err
	} else if !bytes.Equal(again, b) {
		return ChainBatch{}, Header{}, fmt.Errorf("obs1: chain batch is not in canonical form")
	}
	return batch, h, nil
}
