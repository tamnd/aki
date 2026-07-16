package akifile

import (
	"fmt"
	"io"
	"sort"
)

// SlotReport is one meta slot's state as a verify tool sees it: where it sits,
// whether its checksum validates, and the fields a reader compares to pick the
// live root. Err is why a slot did not validate (a torn write, media rot), left
// nil when the slot is good.
type SlotReport struct {
	Off           uint64
	Valid         bool
	CommitSeq     uint64
	CleanShutdown bool
	Err           error
}

// Report is the whole file as a read-only tool sees it (the aki file-info and
// verify output, spec 2064/f3/07 section 6): the immutable prefix, both meta
// slots, which one is live, the roots the live slot names, and the segment
// population. A torn root (SRT or extent map) is recorded as a finding, not a
// hard failure, so a tool can still print the rest of the file's shape. Live is
// the live slot index (0 or 1), or -1 when both slots are torn.
type Report struct {
	Prefix   *Prefix
	Slots    [2]SlotReport
	Live     int
	SRT      *SRT
	SRTErr   error
	Extents  []Extent
	ExtErr   error
	Segments SegmentTally
}

// Inspect reads a file end to end and assembles a Report without changing a byte:
// the prefix, both meta slots (each judged on its own so a tool sees exactly which
// one tore), the live root, the SRT and extent map the live root names, and the
// segment population from a grid walk. It fails only on what a tool cannot work
// around: an unreadable or wrong-format prefix, or a device it cannot size or
// walk. A torn root is left in SRTErr or ExtErr for the caller to report.
func Inspect(dev Device) (*Report, error) {
	hb := make([]byte, PrefixSize)
	if _, err := dev.ReadAt(hb, 0); err != nil {
		return nil, err
	}
	prefix, err := ParsePrefix(hb)
	if err != nil {
		return nil, err
	}
	rep := &Report{Prefix: prefix, Live: -1}

	// Judge each slot on its own so the report shows which sector tore, then pick
	// the live root the way recovery does: the valid slot with the higher commit_seq,
	// ties to slot A.
	offs := [2]uint64{prefix.MetaSlotAOff, prefix.MetaSlotBOff}
	var slots [2]*MetaSlot
	for i, off := range offs {
		rep.Slots[i].Off = off
		buf := make([]byte, MetaSlotSize)
		if _, err := dev.ReadAt(buf, int64(off)); err != nil {
			rep.Slots[i].Err = err
			continue
		}
		m, err := ParseMetaSlot(buf, prefix.ChecksumKind)
		if err != nil {
			rep.Slots[i].Err = err
			continue
		}
		slots[i] = m
		rep.Slots[i].Valid = true
		rep.Slots[i].CommitSeq = m.CommitSeq
		rep.Slots[i].CleanShutdown = m.CleanShutdown == 1
	}

	var live *MetaSlot
	switch {
	case slots[0] != nil && slots[1] != nil:
		if slots[1].CommitSeq > slots[0].CommitSeq {
			rep.Live, live = 1, slots[1]
		} else {
			rep.Live, live = 0, slots[0]
		}
	case slots[0] != nil:
		rep.Live, live = 0, slots[0]
	case slots[1] != nil:
		rep.Live, live = 1, slots[1]
	}

	// The roots the live slot names, best effort: a torn root is a finding a tool
	// prints, not a reason to abandon the whole report.
	if live != nil {
		rep.SRT, rep.SRTErr = ReadSRT(dev, prefix, live)
		rep.Extents, rep.ExtErr = ReadExtentTable(dev, live)
	}

	// The segment population is always walkable from the header page, even with no
	// trusted root.
	size, err := dev.Size()
	if err != nil {
		return nil, err
	}
	seg, err := ScanSegments(dev, prefix, PageSize, uint64(size))
	if err != nil {
		return nil, err
	}
	rep.Segments = seg
	return rep, nil
}

// Findings lists the integrity problems a verify pass reports, in file order: a
// meta slot that did not validate, the both-slots-torn case where the file has no
// trusted root at all, and a live root (the shard root table or the extent map)
// that did not read back. An empty slice means the file verifies clean: both
// slots valid and every root the live slot names read without error.
//
// The segment tail is deliberately not a finding. A never-synced or half-written
// tail is a normal state a scan simply stops at (the durable tail), so it costs
// only the un-acked segments past it, not the file's integrity.
func (r *Report) Findings() []string {
	var fs []string
	for i, s := range r.Slots {
		if !s.Valid {
			fs = append(fs, fmt.Sprintf("meta slot %s did not validate: %v", slotName(i), s.Err))
		}
	}
	if r.Live < 0 {
		fs = append(fs, "no trusted meta slot: both tore, recovery falls back to a full segment scan")
	}
	if r.SRTErr != nil {
		fs = append(fs, fmt.Sprintf("shard root table did not read: %v", r.SRTErr))
	}
	if r.ExtErr != nil {
		fs = append(fs, fmt.Sprintf("extent map did not read: %v", r.ExtErr))
	}
	return fs
}

// slotName is the human label for a meta slot index: 0 is A, 1 is B.
func slotName(i int) string {
	if i == 1 {
		return "B"
	}
	return "A"
}

// WriteReport prints a Report as the aki file-info output: the format header, both
// meta slots with which one is live, the roots the live slot names, the segment
// population by kind, and the verify findings last. It is the human face of Inspect
// and, like Inspect, changes not a byte of the file. Segment kinds print in a fixed
// order so the same file always renders the same report. It returns the first write
// error and then stops, so a broken pipe surfaces instead of scrolling past.
func WriteReport(w io.Writer, r *Report) error {
	ew := &errWriter{w: w}
	p := r.Prefix
	ew.printf("format: aki store v%d.%d  shards %d  checksum %s\n",
		p.FormatMajor, p.FormatMinor, p.ShardCount, checksumName(p.ChecksumKind))

	for i, s := range r.Slots {
		live := ""
		if r.Live == i {
			live = "  (live)"
		}
		if !s.Valid {
			ew.printf("meta slot %s @%d: torn: %v\n", slotName(i), s.Off, s.Err)
			continue
		}
		state := "crashed"
		if s.CleanShutdown {
			state = "clean"
		}
		ew.printf("meta slot %s @%d: valid  commit_seq %d  %s%s\n",
			slotName(i), s.Off, s.CommitSeq, state, live)
	}

	switch {
	case r.SRT != nil:
		ew.printf("shard root table: gen %d, %d rows\n", r.SRT.Gen, len(r.SRT.Rows))
	case r.SRTErr != nil:
		ew.printf("shard root table: unreadable: %v\n", r.SRTErr)
	default:
		ew.printf("shard root table: none\n")
	}

	switch {
	case r.Extents != nil:
		ew.printf("extent map: %d extents\n", len(r.Extents))
	case r.ExtErr != nil:
		ew.printf("extent map: unreadable: %v\n", r.ExtErr)
	default:
		ew.printf("extent map: none\n")
	}

	ew.printf("segments: %d total, durable tail @%d\n", r.Segments.Total, r.Segments.DurableTail)
	kinds := make([]int, 0, len(r.Segments.ByKind))
	for k := range r.Segments.ByKind {
		kinds = append(kinds, int(k))
	}
	sort.Ints(kinds)
	for _, k := range kinds {
		ew.printf("  %-12s %d\n", kindName(uint16(k)), r.Segments.ByKind[uint16(k)])
	}

	fs := r.Findings()
	if len(fs) == 0 {
		ew.printf("findings: none\n")
		return ew.err
	}
	ew.printf("findings: %d\n", len(fs))
	for _, f := range fs {
		ew.printf("  - %s\n", f)
	}
	return ew.err
}

// errWriter is a sticky-error writer: once a write fails it holds the error and
// skips the rest, so a report body reads as a run of prints with the error checked
// once at the end rather than on every line.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, a ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, a...)
}

// checksumName is the human label for a checksum kind.
func checksumName(kind uint32) string {
	switch kind {
	case ChecksumCRC32C:
		return "crc32c"
	case ChecksumXXH3:
		return "xxh3-64"
	default:
		return fmt.Sprintf("unknown(%d)", kind)
	}
}

// kindName is the human label for a segment kind, so a report reads by name rather
// than by the on-disk number.
func kindName(k uint16) string {
	switch k {
	case KindLog:
		return "log"
	case KindColdChunk:
		return "cold_chunk"
	case KindValueLog:
		return "value_log"
	case KindIndexCkpt:
		return "index_ckpt"
	case KindChunkDir:
		return "chunk_dir"
	case KindSegStats:
		return "seg_stats"
	case KindSRT:
		return "srt"
	case KindExtentTable:
		return "extent_table"
	case KindFreeMap:
		return "free_map"
	case KindBarrier:
		return "barrier"
	case KindTTLIndex:
		return "ttl_index"
	case KindMetaKV:
		return "meta_kv"
	case KindFree:
		return "free"
	default:
		return fmt.Sprintf("kind(%d)", k)
	}
}

// SegmentTally is the segment population a grid walk found: a count per kind, the
// total, and the durable tail the walk stopped at (spec 2064/f3/07 section 6). It
// carries no payload, only the shape of what is on disk.
type SegmentTally struct {
	ByKind      map[uint16]int
	Total       int
	DurableTail uint64
}

// ScanSegments walks the append-space segment grid from `from` and tallies every
// intact segment by kind, stopping at the durable tail (the first torn or
// never-synced segment, or the end of the walked range). It is the read-only
// backbone of the file-info and verify tools: it reports the file's segment
// population without interpreting any payload, so a torn tail costs only the
// segments past it, never a decode error.
//
// It shares the recovery tail-replay walk, so what a tool reports as the durable
// tail is exactly where recovery would resume.
func ScanSegments(dev Device, prefix *Prefix, from, size uint64) (SegmentTally, error) {
	tally := SegmentTally{ByKind: make(map[uint16]int)}
	end, err := ReplayTail(dev, prefix, from, size, func(_ uint64, h *SegHeader, _ []byte) error {
		tally.ByKind[h.Kind]++
		tally.Total++
		return nil
	})
	if err != nil {
		return SegmentTally{}, err
	}
	tally.DurableTail = end
	return tally, nil
}
