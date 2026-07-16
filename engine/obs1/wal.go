// WAL objects (spec 2064/obs1 doc 03 section 4): one object per flush per
// node, framed write records for every group dirty in that window, grouped
// into per-group sections so a replayer or folder reads one group's bytes
// with one ranged GET. The commit record on the chain repeats the footer's
// index, so the footer and tail exist for recovery and the orphan sweep,
// not the hot path.
//
// This layer owns the framing only. Frame payloads are opaque bytes whose
// semantics doc 04 defines, and section compression is a format field this
// slice carries but does not exercise: the writer emits comp 0 and the
// parser rejects anything else until the milestone that owns the codec.
package obs1

import (
	"encoding/binary"
	"fmt"
)

// WALFrame is one op frame inside a section. Key and Payload are opaque
// here; kind, flags, and payload semantics live in doc 04.
type WALFrame struct {
	Kind    uint8
	Flags   uint8
	Slot    uint16 // redundant with the key, kept for replay filtering
	Seq     uint64 // per-group monotone op sequence
	Key     []byte
	Payload []byte
}

// WALSection is one group's run of frames in a WAL object.
type WALSection struct {
	Group  uint16
	Epoch  uint32 // the writer's lease epoch for this group
	Frames []WALFrame
}

// WALIndexEntry is one footer index row, enough to plan the ranged GET
// that fetches its section without touching the rest of the object.
type WALIndexEntry struct {
	Group     uint16
	Epoch     uint32
	Offset    uint64 // section start, from the top of the object
	StoredLen uint32 // stored frame bytes, excluding section header and crc
	RawLen    uint32
	NFrames   uint32
	FirstSeq  uint64
	LastSeq   uint64
}

const (
	walSectionHdr = 2 + 4 + 1 + 3 + 4 + 4 + 8 + 8 // group..last_seq
	walFrameFixed = 4 + 1 + 1 + 2 + 8 + 2         // flen..klen
	walIndexEntry = 2 + 4 + 8 + 4 + 4 + 4 + 8 + 8
	walTailSize   = 16
)

// SectionSpan is the byte range a ranged GET needs for the entry's whole
// section: header, stored frames, and trailing crc.
func (e WALIndexEntry) SectionSpan() (off, n int64) {
	return int64(e.Offset), walSectionHdr + int64(e.StoredLen) + 4
}

// AppendWAL appends a complete WAL object: header, sections back to back,
// footer, tail. Sections must be non-empty and frame seqs strictly
// increasing within each section, because first_seq and last_seq are
// derived, never trusted from the caller.
func AppendWAL(b []byte, writer uint64, sections []WALSection) ([]byte, error) {
	if len(sections) == 0 {
		return nil, fmt.Errorf("obs1: a WAL object needs at least one section")
	}
	start := len(b)
	b = AppendHeader(b, Header{Format: FormatWAL, FVersion: 1, Writer: writer})
	index := make([]WALIndexEntry, 0, len(sections))
	for si, s := range sections {
		if len(s.Frames) == 0 {
			return nil, fmt.Errorf("obs1: WAL section %d (group %d) has no frames", si, s.Group)
		}
		off := len(b) - start
		b = binary.LittleEndian.AppendUint16(b, s.Group)
		b = binary.LittleEndian.AppendUint32(b, s.Epoch)
		b = append(b, 0, 0, 0, 0) // comp 0, reserved
		lenAt := len(b)
		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0) // rawlen, storedlen backfilled
		b = binary.LittleEndian.AppendUint64(b, s.Frames[0].Seq)
		b = binary.LittleEndian.AppendUint64(b, s.Frames[len(s.Frames)-1].Seq)
		framesAt := len(b)
		last := uint64(0)
		for fi, f := range s.Frames {
			if fi > 0 && f.Seq <= last {
				return nil, fmt.Errorf("obs1: WAL section %d seq %d after %d, must be strictly increasing", si, f.Seq, last)
			}
			last = f.Seq
			if len(f.Key) > 0xFFFF {
				return nil, fmt.Errorf("obs1: WAL frame key is %d bytes, the format caps keys at 65535", len(f.Key))
			}
			flen := walFrameFixed + len(f.Key) + len(f.Payload)
			if int64(flen) > 0xFFFFFFFF {
				return nil, fmt.Errorf("obs1: WAL frame is %d bytes, the format caps frames at 4 GiB", flen)
			}
			b = binary.LittleEndian.AppendUint32(b, uint32(flen))
			b = append(b, f.Kind, f.Flags)
			b = binary.LittleEndian.AppendUint16(b, f.Slot)
			b = binary.LittleEndian.AppendUint64(b, f.Seq)
			b = binary.LittleEndian.AppendUint16(b, uint16(len(f.Key)))
			b = append(b, f.Key...)
			b = append(b, f.Payload...)
		}
		raw := uint32(len(b) - framesAt)
		binary.LittleEndian.PutUint32(b[lenAt:], raw)
		binary.LittleEndian.PutUint32(b[lenAt+4:], raw) // storedlen == rawlen at comp 0
		b = binary.LittleEndian.AppendUint32(b, crc32c(b[framesAt:]))
		index = append(index, WALIndexEntry{
			Group: s.Group, Epoch: s.Epoch,
			Offset: uint64(off), StoredLen: raw, RawLen: raw,
			NFrames:  uint32(len(s.Frames)),
			FirstSeq: s.Frames[0].Seq, LastSeq: s.Frames[len(s.Frames)-1].Seq,
		})
	}
	footerOff := uint64(len(b) - start)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(index)))
	for _, e := range index {
		b = binary.LittleEndian.AppendUint16(b, e.Group)
		b = binary.LittleEndian.AppendUint32(b, e.Epoch)
		b = binary.LittleEndian.AppendUint64(b, e.Offset)
		b = binary.LittleEndian.AppendUint32(b, e.StoredLen)
		b = binary.LittleEndian.AppendUint32(b, e.RawLen)
		b = binary.LittleEndian.AppendUint32(b, e.NFrames)
		b = binary.LittleEndian.AppendUint64(b, e.FirstSeq)
		b = binary.LittleEndian.AppendUint64(b, e.LastSeq)
	}
	footerLen := uint64(len(b)-start) - footerOff
	b = binary.LittleEndian.AppendUint32(b, crc32c(b[start+int(footerOff):]))
	footerLen += 4
	tailAt := len(b)
	b = binary.LittleEndian.AppendUint64(b, footerOff)
	b = binary.LittleEndian.AppendUint32(b, uint32(footerLen))
	return binary.LittleEndian.AppendUint32(b, crc32c(b[tailAt:])), nil
}

// ParseWALTail reads the 16-byte tail every multi-part obs1 object ends
// with and returns where the footer lives.
func ParseWALTail(tail []byte) (footerOff uint64, footerLen uint32, err error) {
	if len(tail) != walTailSize {
		return 0, 0, fmt.Errorf("obs1: tail is %d bytes, want %d", len(tail), walTailSize)
	}
	if got, want := crc32c(tail[:12]), binary.LittleEndian.Uint32(tail[12:]); got != want {
		return 0, 0, fmt.Errorf("obs1: tail crc 0x%08x, computed 0x%08x", want, got)
	}
	return binary.LittleEndian.Uint64(tail[0:8]), binary.LittleEndian.Uint32(tail[8:12]), nil
}

// ParseWALFooter reads the footer bytes (crc included) into the index.
func ParseWALFooter(b []byte) ([]WALIndexEntry, error) {
	if len(b) < 2+4 {
		return nil, fmt.Errorf("obs1: WAL footer is %d bytes, want at least 6", len(b))
	}
	body, crc := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if got := crc32c(body); got != crc {
		return nil, fmt.Errorf("obs1: WAL footer crc 0x%08x, computed 0x%08x", crc, got)
	}
	n := int(binary.LittleEndian.Uint16(body[0:2]))
	if n == 0 {
		return nil, fmt.Errorf("obs1: WAL footer indexes no sections")
	}
	if len(body) != 2+n*walIndexEntry {
		return nil, fmt.Errorf("obs1: WAL footer is %d bytes, %d sections want %d", len(b), n, 2+n*walIndexEntry+4)
	}
	index := make([]WALIndexEntry, n)
	for i := range index {
		p := body[2+i*walIndexEntry:]
		index[i] = WALIndexEntry{
			Group:     binary.LittleEndian.Uint16(p[0:2]),
			Epoch:     binary.LittleEndian.Uint32(p[2:6]),
			Offset:    binary.LittleEndian.Uint64(p[6:14]),
			StoredLen: binary.LittleEndian.Uint32(p[14:18]),
			RawLen:    binary.LittleEndian.Uint32(p[18:22]),
			NFrames:   binary.LittleEndian.Uint32(p[22:26]),
			FirstSeq:  binary.LittleEndian.Uint64(p[26:34]),
			LastSeq:   binary.LittleEndian.Uint64(p[34:42]),
		}
	}
	return index, nil
}

// ParseWALSection decodes exactly the bytes SectionSpan describes and
// cross-checks every redundant fact against the index entry, so a footer
// and a section that disagree are an error, never a silent preference.
func ParseWALSection(b []byte, e WALIndexEntry) (WALSection, error) {
	if want := walSectionHdr + int(e.StoredLen) + 4; len(b) != want {
		return WALSection{}, fmt.Errorf("obs1: WAL section is %d bytes, index says %d", len(b), want)
	}
	s := WALSection{
		Group: binary.LittleEndian.Uint16(b[0:2]),
		Epoch: binary.LittleEndian.Uint32(b[2:6]),
	}
	if s.Group != e.Group || s.Epoch != e.Epoch {
		return WALSection{}, fmt.Errorf("obs1: WAL section says group %d epoch %d, index says group %d epoch %d", s.Group, s.Epoch, e.Group, e.Epoch)
	}
	if comp := b[6]; comp != 0 {
		return WALSection{}, fmt.Errorf("obs1: WAL section comp %d, the codec for it has not landed", comp)
	}
	if b[7] != 0 || b[8] != 0 || b[9] != 0 {
		return WALSection{}, fmt.Errorf("obs1: WAL section reserved bytes not zero")
	}
	rawlen := binary.LittleEndian.Uint32(b[10:14])
	storedlen := binary.LittleEndian.Uint32(b[14:18])
	firstSeq := binary.LittleEndian.Uint64(b[18:26])
	lastSeq := binary.LittleEndian.Uint64(b[26:34])
	if rawlen != e.RawLen || storedlen != e.StoredLen || firstSeq != e.FirstSeq || lastSeq != e.LastSeq {
		return WALSection{}, fmt.Errorf("obs1: WAL section header disagrees with the index entry")
	}
	if rawlen != storedlen {
		return WALSection{}, fmt.Errorf("obs1: WAL section rawlen %d storedlen %d at comp 0", rawlen, storedlen)
	}
	frames := b[walSectionHdr : walSectionHdr+int(storedlen)]
	if got, want := crc32c(frames), binary.LittleEndian.Uint32(b[len(b)-4:]); got != want {
		return WALSection{}, fmt.Errorf("obs1: WAL section crc 0x%08x, computed 0x%08x", want, got)
	}
	for len(frames) > 0 {
		if len(frames) < walFrameFixed {
			return WALSection{}, fmt.Errorf("obs1: WAL frame truncated at %d trailing bytes", len(frames))
		}
		flen := int(binary.LittleEndian.Uint32(frames[0:4]))
		klen := int(binary.LittleEndian.Uint16(frames[16:18]))
		if flen < walFrameFixed+klen || flen > len(frames) {
			return WALSection{}, fmt.Errorf("obs1: WAL frame length %d does not fit its section", flen)
		}
		f := WALFrame{
			Kind: frames[4], Flags: frames[5],
			Slot: binary.LittleEndian.Uint16(frames[6:8]),
			Seq:  binary.LittleEndian.Uint64(frames[8:16]),
		}
		if klen > 0 {
			f.Key = append([]byte(nil), frames[walFrameFixed:walFrameFixed+klen]...)
		}
		if plen := flen - walFrameFixed - klen; plen > 0 {
			f.Payload = append([]byte(nil), frames[walFrameFixed+klen:flen]...)
		}
		if n := len(s.Frames); n > 0 && f.Seq <= s.Frames[n-1].Seq {
			return WALSection{}, fmt.Errorf("obs1: WAL frame seq %d after %d, must be strictly increasing", f.Seq, s.Frames[n-1].Seq)
		}
		s.Frames = append(s.Frames, f)
		frames = frames[flen:]
	}
	if uint32(len(s.Frames)) != e.NFrames {
		return WALSection{}, fmt.Errorf("obs1: WAL section holds %d frames, index says %d", len(s.Frames), e.NFrames)
	}
	if s.Frames[0].Seq != firstSeq || s.Frames[len(s.Frames)-1].Seq != lastSeq {
		return WALSection{}, fmt.Errorf("obs1: WAL section seq range %d..%d, header says %d..%d", s.Frames[0].Seq, s.Frames[len(s.Frames)-1].Seq, firstSeq, lastSeq)
	}
	return s, nil
}

// ParseWAL decodes a whole WAL object, the recovery and fuzz shape; the
// hot path plans ranged GETs from the chain's copy of the index instead.
// Sections must tile the object exactly from header to footer, so an
// accepted object re-encodes byte for byte.
func ParseWAL(b []byte) ([]WALSection, Header, error) {
	h, err := ParseHeaderAs(b, FormatWAL)
	if err != nil {
		return nil, Header{}, err
	}
	if h.FVersion != 1 {
		return nil, Header{}, fmt.Errorf("obs1: WAL fversion %d, this build reads 1", h.FVersion)
	}
	if len(b) < HeaderSize+walTailSize {
		return nil, Header{}, fmt.Errorf("obs1: WAL object is %d bytes, too short for a tail", len(b))
	}
	footerOff, footerLen, err := ParseWALTail(b[len(b)-walTailSize:])
	if err != nil {
		return nil, Header{}, err
	}
	end := uint64(len(b) - walTailSize)
	if footerOff < HeaderSize || footerOff+uint64(footerLen) != end {
		return nil, Header{}, fmt.Errorf("obs1: WAL footer at %d+%d does not reach the tail at %d", footerOff, footerLen, end)
	}
	index, err := ParseWALFooter(b[footerOff:end])
	if err != nil {
		return nil, Header{}, err
	}
	sections := make([]WALSection, len(index))
	next := uint64(HeaderSize)
	for i, e := range index {
		if e.Offset != next {
			return nil, Header{}, fmt.Errorf("obs1: WAL section %d at offset %d, expected %d, sections must tile", i, e.Offset, next)
		}
		off, n := e.SectionSpan()
		if uint64(off)+uint64(n) > footerOff {
			return nil, Header{}, fmt.Errorf("obs1: WAL section %d runs past the footer", i)
		}
		sections[i], err = ParseWALSection(b[off:off+n], e)
		if err != nil {
			return nil, Header{}, err
		}
		next = uint64(off + n)
	}
	if next != footerOff {
		return nil, Header{}, fmt.Errorf("obs1: %d unindexed bytes between the last section and the footer", footerOff-next)
	}
	return sections, h, nil
}
