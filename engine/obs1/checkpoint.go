// Checkpoint objects (spec 2064/obs1 doc 03 section 7): a summary of the
// chain through one position, written at chain/<dd>/ckpt/<seq16> so a
// booting node replays from here instead of from zero. Everything in a
// checkpoint is derivable by replaying the chain, so per C-I7 checkpoints
// are never authority; deleting them all loses boot speed, not data.
package obs1

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// LeaseEntry is one row of the lease table: who holds a group, at what
// epoch, until when.
type LeaseEntry struct {
	Group      uint16
	Node       uint64
	Epoch      uint32
	DeadlineMS uint64
}

// GroupCursor is one group's fold state: its manifest pointer and the
// chain position its folder has consumed through.
type GroupCursor struct {
	ManSeq  uint64
	FoldPos ChainPos
}

// Checkpoint is the full summary: member table, lease table, and every
// group's cursor, indexed by group id.
type Checkpoint struct {
	Through ChainPos
	Members []Member
	Leases  []LeaseEntry
	Groups  []GroupCursor
}

const (
	ckptLease = 2 + 8 + 4 + 8
	ckptGroup = 8 + 8
	// through, three empty length-prefixed crc'd sections, final crc
	ckptMin = 9 + 3*(4+2+4) + 4
)

func (c Checkpoint) valid() error {
	if len(c.Members) > 0xFFFF || len(c.Leases) > 0xFFFF || len(c.Groups) > 0xFFFF {
		return fmt.Errorf("obs1: checkpoint table is over 65535 rows")
	}
	for i, m := range c.Members {
		if err := m.valid(); err != nil {
			return err
		}
		if i > 0 && m.Node <= c.Members[i-1].Node {
			return fmt.Errorf("obs1: checkpoint member %d node %d after %d, rows must ascend strictly", i, m.Node, c.Members[i-1].Node)
		}
	}
	for i, l := range c.Leases {
		if i > 0 && l.Group <= c.Leases[i-1].Group {
			return fmt.Errorf("obs1: checkpoint lease %d group %d after %d, one row per group ascending", i, l.Group, c.Leases[i-1].Group)
		}
	}
	for g, cur := range c.Groups {
		if _, err := cur.FoldPos.Pack(); err != nil {
			return fmt.Errorf("obs1: checkpoint group %d: %w", g, err)
		}
	}
	return nil
}

// appendCkptSection wraps a section body with its length prefix and crc.
func appendCkptSection(b []byte, body func([]byte) []byte) []byte {
	lenAt := len(b)
	b = append(b, 0, 0, 0, 0)
	at := len(b)
	b = body(b)
	binary.LittleEndian.PutUint32(b[lenAt:], uint32(len(b)-at))
	return binary.LittleEndian.AppendUint32(b, crc32c(b[at:]))
}

// AppendCheckpoint appends a complete checkpoint object: header, through
// position, the three tables each length-prefixed and crc'd, final crc.
func AppendCheckpoint(b []byte, writer uint64, c Checkpoint) ([]byte, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	b = AppendHeader(b, Header{Format: FormatCheckpoint, FVersion: 1, Writer: writer})
	p := len(b)
	b = append(b, c.Through.DD)
	b = binary.LittleEndian.AppendUint64(b, c.Through.Seq)
	b = appendCkptSection(b, func(b []byte) []byte {
		b = binary.LittleEndian.AppendUint16(b, uint16(len(c.Members)))
		for _, m := range c.Members {
			b = appendMember(b, m)
		}
		return b
	})
	b = appendCkptSection(b, func(b []byte) []byte {
		b = binary.LittleEndian.AppendUint16(b, uint16(len(c.Leases)))
		for _, l := range c.Leases {
			b = binary.LittleEndian.AppendUint16(b, l.Group)
			b = binary.LittleEndian.AppendUint64(b, l.Node)
			b = binary.LittleEndian.AppendUint32(b, l.Epoch)
			b = binary.LittleEndian.AppendUint64(b, l.DeadlineMS)
		}
		return b
	})
	b = appendCkptSection(b, func(b []byte) []byte {
		b = binary.LittleEndian.AppendUint16(b, uint16(len(c.Groups)))
		for _, cur := range c.Groups {
			packed, _ := cur.FoldPos.Pack() // validated above
			b = binary.LittleEndian.AppendUint64(b, cur.ManSeq)
			b = binary.LittleEndian.AppendUint64(b, packed)
		}
		return b
	})
	return binary.LittleEndian.AppendUint32(b, crc32c(b[p:])), nil
}

// ParseCheckpoint reads a checkpoint object, enforcing canonical form by
// re-encoding what it accepted and requiring the input bytes back.
func ParseCheckpoint(b []byte) (Checkpoint, Header, error) {
	var c Checkpoint
	h, err := ParseHeaderAs(b, FormatCheckpoint)
	if err != nil {
		return c, Header{}, err
	}
	if h.FVersion != 1 {
		return c, Header{}, fmt.Errorf("obs1: checkpoint fversion %d, this build reads 1", h.FVersion)
	}
	p := b[HeaderSize:]
	if len(p) < ckptMin {
		return c, Header{}, fmt.Errorf("obs1: checkpoint payload is %d bytes, want at least %d", len(p), ckptMin)
	}
	body, crc := p[:len(p)-4], binary.LittleEndian.Uint32(p[len(p)-4:])
	if got := crc32c(body); got != crc {
		return c, Header{}, fmt.Errorf("obs1: checkpoint crc 0x%08x, computed 0x%08x", crc, got)
	}
	c.Through = ChainPos{DD: body[0], Seq: binary.LittleEndian.Uint64(body[1:9])}
	q := body[9:]
	section := func(what string) ([]byte, error) {
		if len(q) < 8 {
			return nil, fmt.Errorf("obs1: checkpoint truncated at the %s section", what)
		}
		n := int(binary.LittleEndian.Uint32(q[0:4]))
		if len(q) < 4+n+4 {
			return nil, fmt.Errorf("obs1: checkpoint %s section length %d overruns the payload", what, n)
		}
		sec := q[4 : 4+n]
		want := binary.LittleEndian.Uint32(q[4+n:])
		if got := crc32c(sec); got != want {
			return nil, fmt.Errorf("obs1: checkpoint %s section crc 0x%08x, computed 0x%08x", what, want, got)
		}
		q = q[8+n:]
		return sec, nil
	}

	sec, err := section("member")
	if err != nil {
		return c, Header{}, err
	}
	if len(sec) < 2 {
		return c, Header{}, fmt.Errorf("obs1: checkpoint member section is %d bytes", len(sec))
	}
	rest := sec[2:]
	for range binary.LittleEndian.Uint16(sec[0:2]) {
		var m Member
		m, rest, err = parseMember(rest)
		if err != nil {
			return Checkpoint{}, Header{}, err
		}
		c.Members = append(c.Members, m)
	}
	if len(rest) != 0 {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint member section has %d trailing bytes", len(rest))
	}

	sec, err = section("lease")
	if err != nil {
		return Checkpoint{}, Header{}, err
	}
	if len(sec) < 2 {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint lease section is %d bytes", len(sec))
	}
	nl := int(binary.LittleEndian.Uint16(sec[0:2]))
	if len(sec) != 2+nl*ckptLease {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint lease section with %d rows wants %d bytes, has %d", nl, 2+nl*ckptLease, len(sec))
	}
	for i := range nl {
		r := sec[2+i*ckptLease:]
		c.Leases = append(c.Leases, LeaseEntry{
			Group:      binary.LittleEndian.Uint16(r[0:2]),
			Node:       binary.LittleEndian.Uint64(r[2:10]),
			Epoch:      binary.LittleEndian.Uint32(r[10:14]),
			DeadlineMS: binary.LittleEndian.Uint64(r[14:22]),
		})
	}

	sec, err = section("group")
	if err != nil {
		return Checkpoint{}, Header{}, err
	}
	if len(sec) < 2 {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint group section is %d bytes", len(sec))
	}
	ng := int(binary.LittleEndian.Uint16(sec[0:2]))
	if len(sec) != 2+ng*ckptGroup {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint group section with %d rows wants %d bytes, has %d", ng, 2+ng*ckptGroup, len(sec))
	}
	for i := range ng {
		r := sec[2+i*ckptGroup:]
		c.Groups = append(c.Groups, GroupCursor{
			ManSeq:  binary.LittleEndian.Uint64(r[0:8]),
			FoldPos: UnpackChainPos(binary.LittleEndian.Uint64(r[8:16])),
		})
	}

	if len(q) != 0 {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint has %d bytes after its last section", len(q))
	}
	if again, err := AppendCheckpoint(nil, h.Writer, c); err != nil {
		return Checkpoint{}, Header{}, err
	} else if !bytes.Equal(again, b) {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint is not in canonical form")
	}
	return c, h, nil
}
