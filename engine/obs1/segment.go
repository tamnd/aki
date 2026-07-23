// Segments (spec 2064/obs1 doc 03 section 5): immutable objects of packed
// chunks produced by fold. The payload is divided into compression blocks,
// 128 KiB uncompressed by default, each independently checksummed; chunks
// never span blocks, and a chunk bigger than the block size gets a jumbo
// block of exactly its size. Blocks are the unit of GET planning, NVMe
// caching, and checksumming.
//
// Chunk interiors are opaque here: they are f3 chunk frames (OB9) whose
// codecs port with the engine, and the only fact this layer reads is the
// frame's leading total length, which is what lets the chunk index carry
// no lengths. Compression is a format field this slice carries but does
// not exercise, same stance as the WAL: writers emit comp 0, parsers
// reject anything else until the milestone that owns the codec. The block
// size and bloom parameters stay provisional until the O1c labs.
package obs1

import (
	"encoding/binary"
	"fmt"
)

// SegmentBlockSize is the default uncompressed block size.
const SegmentBlockSize = 128 << 10

const (
	segBlockHdr      = 4 + 4 + 1 + 3 // rawlen, storedlen, comp, reserved
	segFooterFixed   = 2 + 4 + 8 + 1 + 1 + 8 + 8 + 4
	segBlockEntry    = 8 + 4 + 4 + 4
	segChunkFixed    = 4 + 4 + 2 + 1 + 1 + 8 + 2 + 2 + 8 + 8 // everything but the key
	segFooterTrailer = 4 + 8 + 8 + 4                         // bloomlen, nrecords, rawbytes, fcrc
)

// SegmentBlockEntry is one block index row: where the block sits in the
// object and what its bytes must decode to.
type SegmentBlockEntry struct {
	Offset    uint64 // block start, from the top of the object
	StoredLen uint32 // stored data, excluding block header and crc
	RawLen    uint32
	CRC       uint32 // crc32c of the stored data, repeated from the block
}

// BlockSpan is the byte range a ranged GET needs for the entry's whole
// block: header, stored data, and trailing crc.
func (e SegmentBlockEntry) BlockSpan() (off, n int64) {
	return int64(e.Offset), segBlockHdr + int64(e.StoredLen) + 4
}

// SegmentChunkEntry is one chunk index row. The chunk's length is not
// here by design: the f3 frame's leading total field carries it, so the
// index stays small enough to live in the resident directory (doc 05).
type SegmentChunkEntry struct {
	Block      uint32
	OffInBlock uint32
	Key        []byte
	Kind       uint8
	Flags      uint8 // the frame's flags byte: ChunkFlagRun, ChunkFlagTTLBitmap
	FirstDisc  uint64
	Count      uint16 // records in the chunk
	LiveHint   uint16

	// MinExpMS and MaxExpMS bound the deadlines the chunk's expiry
	// bearers carry, absolute unix ms; both zero when no record in the
	// chunk carries one. The bounds are an optimization, never authority
	// (doc 03 section 5.1): every record keeps its deadline inline, and
	// readers check it, so a planner may use the bounds to skip or retire
	// but correctness never leans on them.
	MinExpMS uint64
	MaxExpMS uint64
}

// SegmentFooter is everything a reader learns from the footer alone.
// NRecords and RawBytes are derived facts the encoder recomputes; a
// footer that disagrees with its blocks or chunks does not parse.
type SegmentFooter struct {
	Group    uint16
	Epoch    uint32 // folder's lease epoch
	SegSeq   uint64
	Level    uint8 // 0 fresh fold, 1 rewritten (doc 06)
	TTLClass uint8 // section 5.1; 0 means no expiry
	MinExpMS uint64
	MaxExpMS uint64
	Blocks   []SegmentBlockEntry
	Chunks   []SegmentChunkEntry
	Bloom    []byte
	NRecords uint64
	RawBytes uint64
}

// Segment is a whole decoded segment: the footer plus each block's
// uncompressed data.
type Segment struct {
	Footer    SegmentFooter
	BlockData [][]byte
}

// SegmentChunk is the builder's input: one packed chunk frame and the
// index facts that describe it. Data must be a whole f3 frame, leading
// total field included.
type SegmentChunk struct {
	Key       []byte
	Kind      uint8
	Flags     uint8
	FirstDisc uint64
	Count     uint16
	LiveHint  uint16
	Data      []byte

	// MinExpMS and MaxExpMS are the chunk's expiry bounds and Bearers the
	// count of records carrying a deadline, all zero on a TTL-free chunk.
	// The builder copies the bounds into the index entry; Bearers stays
	// builder-side, feeding the segment's class decision (a class is
	// assigned only when every record in every chunk bears a deadline, so
	// the whole object can die at MaxExpMS).
	MinExpMS uint64
	MaxExpMS uint64
	Bearers  uint16
}

// BuildSegment packs chunks into blocks greedily: a chunk that does not
// fit the open block closes it short rather than spanning, and a chunk
// bigger than blockSize gets a jumbo block of exactly its size. The bloom
// is built over memberKeys, which the framing cannot extract from the
// opaque chunks itself. blockSize 0 means SegmentBlockSize.
func BuildSegment(f SegmentFooter, chunks []SegmentChunk, memberKeys [][]byte, blockSize int) (*Segment, error) {
	if blockSize <= 0 {
		blockSize = SegmentBlockSize
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("obs1: a segment needs at least one chunk")
	}
	seg := &Segment{Footer: f}
	seg.Footer.Blocks, seg.Footer.Chunks = nil, nil
	seg.Footer.Bloom = BuildBloom(memberKeys)
	var cur []byte
	for i, c := range chunks {
		if len(c.Data) < 4 || int(binary.LittleEndian.Uint32(c.Data[0:4])) != len(c.Data) {
			return nil, fmt.Errorf("obs1: chunk %d data is %d bytes but its frame total disagrees", i, len(c.Data))
		}
		if len(c.Key) > 0xFFFF {
			return nil, fmt.Errorf("obs1: chunk %d key is %d bytes, the format caps keys at 65535", i, len(c.Key))
		}
		if c.Count == 0 {
			return nil, fmt.Errorf("obs1: chunk %d holds no records", i)
		}
		if len(cur) > 0 && len(cur)+len(c.Data) > blockSize {
			seg.BlockData = append(seg.BlockData, cur)
			cur = nil
		}
		seg.Footer.Chunks = append(seg.Footer.Chunks, SegmentChunkEntry{
			Block: uint32(len(seg.BlockData)), OffInBlock: uint32(len(cur)),
			Key: c.Key, Kind: c.Kind, Flags: c.Flags, FirstDisc: c.FirstDisc,
			Count: c.Count, LiveHint: c.LiveHint,
			MinExpMS: c.MinExpMS, MaxExpMS: c.MaxExpMS,
		})
		cur = append(cur, c.Data...)
		if len(cur) >= blockSize {
			seg.BlockData = append(seg.BlockData, cur)
			cur = nil
		}
	}
	if len(cur) > 0 {
		seg.BlockData = append(seg.BlockData, cur)
	}
	return seg, nil
}

// AppendSegment appends a complete segment object: header, blocks back to
// back, footer, tail. The block index, NRecords, and RawBytes are derived
// from the data, never trusted from the caller, and every chunk entry is
// checked against the block bytes it points into.
func AppendSegment(b []byte, writer uint64, seg *Segment) ([]byte, error) {
	f := seg.Footer
	if err := validateSegmentShape(&f, seg.BlockData); err != nil {
		return nil, err
	}
	start := len(b)
	b = AppendHeader(b, Header{Format: FormatSegment, FVersion: 1, Writer: writer})
	blocks := make([]SegmentBlockEntry, len(seg.BlockData))
	rawBytes := uint64(0)
	for i, data := range seg.BlockData {
		blocks[i] = SegmentBlockEntry{
			Offset:    uint64(len(b) - start),
			StoredLen: uint32(len(data)),
			RawLen:    uint32(len(data)),
			CRC:       crc32c(data),
		}
		rawBytes += uint64(len(data))
		b = binary.LittleEndian.AppendUint32(b, blocks[i].RawLen)
		b = binary.LittleEndian.AppendUint32(b, blocks[i].StoredLen)
		b = append(b, 0, 0, 0, 0) // comp 0, reserved
		b = append(b, data...)
		b = binary.LittleEndian.AppendUint32(b, blocks[i].CRC)
	}
	nrecords := uint64(0)
	for _, c := range f.Chunks {
		nrecords += uint64(c.Count)
	}
	footerOff := uint64(len(b) - start)
	b = binary.LittleEndian.AppendUint16(b, f.Group)
	b = binary.LittleEndian.AppendUint32(b, f.Epoch)
	b = binary.LittleEndian.AppendUint64(b, f.SegSeq)
	b = append(b, f.Level, f.TTLClass)
	b = binary.LittleEndian.AppendUint64(b, f.MinExpMS)
	b = binary.LittleEndian.AppendUint64(b, f.MaxExpMS)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(blocks)))
	for _, e := range blocks {
		b = binary.LittleEndian.AppendUint64(b, e.Offset)
		b = binary.LittleEndian.AppendUint32(b, e.StoredLen)
		b = binary.LittleEndian.AppendUint32(b, e.RawLen)
		b = binary.LittleEndian.AppendUint32(b, e.CRC)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(f.Chunks)))
	for _, c := range f.Chunks {
		b = binary.LittleEndian.AppendUint32(b, c.Block)
		b = binary.LittleEndian.AppendUint32(b, c.OffInBlock)
		b = binary.LittleEndian.AppendUint16(b, uint16(len(c.Key)))
		b = append(b, c.Key...)
		b = append(b, c.Kind, c.Flags)
		b = binary.LittleEndian.AppendUint64(b, c.FirstDisc)
		b = binary.LittleEndian.AppendUint16(b, c.Count)
		b = binary.LittleEndian.AppendUint16(b, c.LiveHint)
		b = binary.LittleEndian.AppendUint64(b, c.MinExpMS)
		b = binary.LittleEndian.AppendUint64(b, c.MaxExpMS)
	}
	b = append(b, f.Bloom...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(f.Bloom)))
	b = binary.LittleEndian.AppendUint64(b, nrecords)
	b = binary.LittleEndian.AppendUint64(b, rawBytes)
	footerLen := uint64(len(b)-start) - footerOff
	b = binary.LittleEndian.AppendUint32(b, crc32c(b[start+int(footerOff):]))
	return appendTail(b, footerOff, uint32(footerLen)+4), nil
}

// TTLClassOf maps a segment's latest deadline onto the doc 03 section 5.1
// retirement class relative to the folder's clock at build time: classes 1
// through 24 are the next 24 hourly windows, 25 and up count daily windows
// past the first day, capped at 255; a deadline already due lands in class 1
// so the reaper visits it first. Zero maxExpMS means no class. The class is
// an optimization, never authority: every record carries its deadline inline
// and readers check it, so a stale class only delays or hastens a retirement
// scan, never an answer.
func TTLClassOf(maxExpMS, nowMS uint64) uint8 {
	if maxExpMS == 0 {
		return 0
	}
	if maxExpMS <= nowMS {
		return 1
	}
	if hours := (maxExpMS - nowMS) / 3_600_000; hours < 24 {
		return uint8(hours) + 1
	}
	days := (maxExpMS - nowMS) / 86_400_000
	if days > 255-24 {
		return 255
	}
	return uint8(24 + days)
}

// validateSegmentShape holds the facts both the encoder and the
// whole-object parser insist on: identity rules, and every chunk entry
// pointing at a whole in-bounds frame in ascending non-overlapping order.
func validateSegmentShape(f *SegmentFooter, blockData [][]byte) error {
	if len(blockData) == 0 {
		return fmt.Errorf("obs1: a segment needs at least one block")
	}
	if len(f.Chunks) == 0 {
		return fmt.Errorf("obs1: a segment needs at least one chunk")
	}
	if f.Level > 1 {
		return fmt.Errorf("obs1: segment level %d, doc 06 defines 0 and 1", f.Level)
	}
	if f.TTLClass == 0 {
		if f.MinExpMS != 0 || f.MaxExpMS != 0 {
			return fmt.Errorf("obs1: TTL class 0 segment carries expiry bounds")
		}
	} else if f.MinExpMS == 0 || f.MinExpMS > f.MaxExpMS {
		return fmt.Errorf("obs1: TTL class %d segment has expiry bounds %d..%d", f.TTLClass, f.MinExpMS, f.MaxExpMS)
	}
	if len(f.Bloom) == 0 || len(f.Bloom)%bloomBlockBytes != 0 {
		return fmt.Errorf("obs1: bloom filter is %d bytes, want a whole number of %d-byte blocks", len(f.Bloom), bloomBlockBytes)
	}
	prevBlock, prevEnd := uint32(0), uint32(0)
	for i, c := range f.Chunks {
		if len(c.Key) > 0xFFFF {
			return fmt.Errorf("obs1: chunk %d key is %d bytes, the format caps keys at 65535", i, len(c.Key))
		}
		if c.Count == 0 {
			return fmt.Errorf("obs1: chunk %d holds no records", i)
		}
		if c.MinExpMS == 0 {
			if c.MaxExpMS != 0 {
				return fmt.Errorf("obs1: chunk %d has a max expiry bound but no min", i)
			}
		} else if c.MinExpMS > c.MaxExpMS {
			return fmt.Errorf("obs1: chunk %d expiry bounds %d..%d run backward", i, c.MinExpMS, c.MaxExpMS)
		}
		if int(c.Block) >= len(blockData) {
			return fmt.Errorf("obs1: chunk %d points at block %d of %d", i, c.Block, len(blockData))
		}
		if c.Block < prevBlock || (c.Block == prevBlock && i > 0 && c.OffInBlock < prevEnd) {
			return fmt.Errorf("obs1: chunk %d out of order or overlapping", i)
		}
		data := blockData[c.Block]
		if int64(c.OffInBlock)+4 > int64(len(data)) {
			return fmt.Errorf("obs1: chunk %d starts past its block", i)
		}
		total := binary.LittleEndian.Uint32(data[c.OffInBlock:])
		if total < 4 || int64(c.OffInBlock)+int64(total) > int64(len(data)) {
			return fmt.Errorf("obs1: chunk %d frame total %d runs past its block", i, total)
		}
		prevBlock, prevEnd = c.Block, c.OffInBlock+total
	}
	return nil
}

// ParseSegmentFooter reads the footer bytes (fcrc included). Facts that
// need block data, chunk bounds and the derived totals, are checked by
// ParseSegment; a footer alone is what the resident directory loads.
func ParseSegmentFooter(b []byte) (SegmentFooter, error) {
	var f SegmentFooter
	if len(b) < segFooterFixed+segFooterTrailer {
		return f, fmt.Errorf("obs1: segment footer is %d bytes, want at least %d", len(b), segFooterFixed+segFooterTrailer)
	}
	body, crc := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if got := crc32c(body); got != crc {
		return f, fmt.Errorf("obs1: segment footer crc 0x%08x, computed 0x%08x", crc, got)
	}
	f.Group = binary.LittleEndian.Uint16(body[0:2])
	f.Epoch = binary.LittleEndian.Uint32(body[2:6])
	f.SegSeq = binary.LittleEndian.Uint64(body[6:14])
	f.Level = body[14]
	f.TTLClass = body[15]
	f.MinExpMS = binary.LittleEndian.Uint64(body[16:24])
	f.MaxExpMS = binary.LittleEndian.Uint64(body[24:32])
	nblocks := int(binary.LittleEndian.Uint32(body[32:36]))
	p := segFooterFixed
	if nblocks == 0 || len(body) < p+nblocks*segBlockEntry {
		return f, fmt.Errorf("obs1: segment footer cannot hold %d block entries", nblocks)
	}
	f.Blocks = make([]SegmentBlockEntry, nblocks)
	for i := range f.Blocks {
		e := body[p+i*segBlockEntry:]
		f.Blocks[i] = SegmentBlockEntry{
			Offset:    binary.LittleEndian.Uint64(e[0:8]),
			StoredLen: binary.LittleEndian.Uint32(e[8:12]),
			RawLen:    binary.LittleEndian.Uint32(e[12:16]),
			CRC:       binary.LittleEndian.Uint32(e[16:20]),
		}
	}
	p += nblocks * segBlockEntry
	if len(body) < p+4 {
		return f, fmt.Errorf("obs1: segment footer ends inside the chunk count")
	}
	nchunks := int(binary.LittleEndian.Uint32(body[p : p+4]))
	p += 4
	if nchunks == 0 {
		return f, fmt.Errorf("obs1: segment footer indexes no chunks")
	}
	f.Chunks = make([]SegmentChunkEntry, nchunks)
	for i := range f.Chunks {
		if len(body) < p+10 {
			return f, fmt.Errorf("obs1: segment footer ends inside chunk entry %d", i)
		}
		c := SegmentChunkEntry{
			Block:      binary.LittleEndian.Uint32(body[p : p+4]),
			OffInBlock: binary.LittleEndian.Uint32(body[p+4 : p+8]),
		}
		klen := int(binary.LittleEndian.Uint16(body[p+8 : p+10]))
		if len(body) < p+segChunkFixed+klen {
			return f, fmt.Errorf("obs1: segment footer ends inside chunk entry %d", i)
		}
		if klen > 0 {
			c.Key = append([]byte(nil), body[p+10:p+10+klen]...)
		}
		q := p + 10 + klen
		c.Kind = body[q]
		c.Flags = body[q+1]
		c.FirstDisc = binary.LittleEndian.Uint64(body[q+2 : q+10])
		c.Count = binary.LittleEndian.Uint16(body[q+10 : q+12])
		c.LiveHint = binary.LittleEndian.Uint16(body[q+12 : q+14])
		c.MinExpMS = binary.LittleEndian.Uint64(body[q+14 : q+22])
		c.MaxExpMS = binary.LittleEndian.Uint64(body[q+22 : q+30])
		f.Chunks[i] = c
		p += segChunkFixed + klen
	}
	trailer := len(body) - (segFooterTrailer - 4)
	if trailer < p {
		return f, fmt.Errorf("obs1: segment footer has no room for its trailer")
	}
	bloomLen := int(binary.LittleEndian.Uint32(body[trailer : trailer+4]))
	if bloomLen != trailer-p {
		return f, fmt.Errorf("obs1: bloomlen %d but %d bytes sit between the chunk index and the trailer", bloomLen, trailer-p)
	}
	f.Bloom = append([]byte(nil), body[p:trailer]...)
	f.NRecords = binary.LittleEndian.Uint64(body[trailer+4 : trailer+12])
	f.RawBytes = binary.LittleEndian.Uint64(body[trailer+12 : trailer+20])
	return f, nil
}

// ParseSegmentBlock decodes exactly the bytes BlockSpan describes and
// cross-checks the block against its index entry, returning the
// uncompressed data.
func ParseSegmentBlock(b []byte, e SegmentBlockEntry) ([]byte, error) {
	if want := segBlockHdr + int(e.StoredLen) + 4; len(b) != want {
		return nil, fmt.Errorf("obs1: segment block is %d bytes, index says %d", len(b), want)
	}
	rawlen := binary.LittleEndian.Uint32(b[0:4])
	storedlen := binary.LittleEndian.Uint32(b[4:8])
	if rawlen != e.RawLen || storedlen != e.StoredLen {
		return nil, fmt.Errorf("obs1: segment block header disagrees with the index entry")
	}
	if comp := b[8]; comp != 0 {
		return nil, fmt.Errorf("obs1: segment block comp %d, the codec for it has not landed", comp)
	}
	if b[9] != 0 || b[10] != 0 || b[11] != 0 {
		return nil, fmt.Errorf("obs1: segment block reserved bytes not zero")
	}
	if rawlen != storedlen {
		return nil, fmt.Errorf("obs1: segment block rawlen %d storedlen %d at comp 0", rawlen, storedlen)
	}
	data := b[segBlockHdr : segBlockHdr+int(storedlen)]
	crc := binary.LittleEndian.Uint32(b[len(b)-4:])
	if got := crc32c(data); got != crc || got != e.CRC {
		return nil, fmt.Errorf("obs1: segment block crc 0x%08x, index says 0x%08x, computed 0x%08x", crc, e.CRC, got)
	}
	return data, nil
}

// ParseSegment decodes a whole segment object, the recovery and fuzz
// shape; the serving path reads the footer once and plans ranged block
// GETs instead. Blocks must tile the object exactly from header to
// footer, and the footer's derived totals must match the data, so an
// accepted object re-encodes byte for byte.
func ParseSegment(b []byte) (*Segment, Header, error) {
	h, err := ParseHeaderAs(b, FormatSegment)
	if err != nil {
		return nil, Header{}, err
	}
	if h.FVersion != 1 {
		return nil, Header{}, fmt.Errorf("obs1: segment fversion %d, this build reads 1", h.FVersion)
	}
	if len(b) < HeaderSize+TailSize {
		return nil, Header{}, fmt.Errorf("obs1: segment object is %d bytes, too short for a tail", len(b))
	}
	footerOff, footerLen, err := ParseTail(b[len(b)-TailSize:])
	if err != nil {
		return nil, Header{}, err
	}
	end := uint64(len(b) - TailSize)
	if footerOff < HeaderSize || footerOff+uint64(footerLen) != end {
		return nil, Header{}, fmt.Errorf("obs1: segment footer at %d+%d does not reach the tail at %d", footerOff, footerLen, end)
	}
	f, err := ParseSegmentFooter(b[footerOff:end])
	if err != nil {
		return nil, Header{}, err
	}
	seg := &Segment{Footer: f, BlockData: make([][]byte, len(f.Blocks))}
	next, rawBytes := uint64(HeaderSize), uint64(0)
	for i, e := range f.Blocks {
		if e.Offset != next {
			return nil, Header{}, fmt.Errorf("obs1: segment block %d at offset %d, expected %d, blocks must tile", i, e.Offset, next)
		}
		off, n := e.BlockSpan()
		if uint64(off)+uint64(n) > footerOff {
			return nil, Header{}, fmt.Errorf("obs1: segment block %d runs past the footer", i)
		}
		seg.BlockData[i], err = ParseSegmentBlock(b[off:off+n], e)
		if err != nil {
			return nil, Header{}, err
		}
		rawBytes += uint64(e.RawLen)
		next = uint64(off + n)
	}
	if next != footerOff {
		return nil, Header{}, fmt.Errorf("obs1: %d unindexed bytes between the last block and the footer", footerOff-next)
	}
	if err := validateSegmentShape(&f, seg.BlockData); err != nil {
		return nil, Header{}, err
	}
	nrecords := uint64(0)
	for _, c := range f.Chunks {
		nrecords += uint64(c.Count)
	}
	if f.NRecords != nrecords || f.RawBytes != rawBytes {
		return nil, Header{}, fmt.Errorf("obs1: segment footer says %d records %d raw bytes, the data says %d and %d", f.NRecords, f.RawBytes, nrecords, rawBytes)
	}
	return seg, h, nil
}
