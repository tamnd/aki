package stream

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// Range reads over both bands (spec 2064/f3/14 section 6.3). XRANGE and
// XREVRANGE resolve their two bounds to a [lo, hi] window, seek the block the
// window opens in through the directory (O(log C), a single tail append never
// having touched it beyond one insert per block close), then decode entries
// contiguously across blocks. The inline band has no directory: its one block is
// walked directly. This is the packed-walk regime doc section 3.1 sizes at
// 1-2ns per element, against Redis's per-entry rax descent.

// bound is one parsed range endpoint: an ID plus whether the endpoint itself is
// excluded (the "(" prefix).
type bound struct {
	id   streamID
	excl bool
}

// aboveLo reports whether id clears the low bound.
func aboveLo(id streamID, lo bound) bool {
	c := id.cmp(lo.id)
	if lo.excl {
		return c > 0
	}
	return c >= 0
}

// belowHi reports whether id clears the high bound.
func belowHi(id streamID, hi bound) bool {
	c := id.cmp(hi.id)
	if hi.excl {
		return c < 0
	}
	return c <= 0
}

// rangeEntry is one entry gathered for a reply: its ID and its field headers,
// which are views into the owning block's blob and stay valid for the whole
// single-threaded reply build (no mutation runs between the gather and the
// emit).
type rangeEntry struct {
	id     streamID
	fields []field
}

// collectRange gathers the live entries in [lo, hi] in output order (ascending
// for a forward read, descending for a reverse one), stopping at limit entries
// when limit is positive. It seeks the starting block through the directory in
// the native band and walks the single block in the inline band.
func (s *stream) collectRange(lo, hi bound, rev bool, limit int) []rangeEntry {
	if len(s.blocks) == 0 || limit == 0 {
		return nil
	}
	if rev {
		return s.collectReverse(lo, hi, limit)
	}
	return s.collectForward(lo, hi, limit)
}

func (s *stream) collectForward(lo, hi bound, limit int) []rangeEntry {
	start := 0
	if s.dir != nil {
		start = s.floorBlock(lo.id)
	}
	var out []rangeEntry
	var scratch []field
	for i := start; i < len(s.blocks); i++ {
		stop := false
		clone := blockClone(s.blocks[i])
		s.walkBlock(s.blocks[i], scratch, func(id streamID, fields []field) bool {
			if !aboveLo(id, lo) {
				return true // still below the window, keep scanning
			}
			if !belowHi(id, hi) {
				stop = true // past the window; entries only climb from here
				return false
			}
			out = append(out, rangeEntry{id: id, fields: clone(fields)})
			if limit > 0 && len(out) >= limit {
				stop = true
				return false
			}
			return true
		})
		if stop {
			break
		}
	}
	return out
}

func (s *stream) collectReverse(lo, hi bound, limit int) []rangeEntry {
	start := len(s.blocks) - 1
	if s.dir != nil {
		start = s.floorBlock(hi.id)
	}
	var out []rangeEntry
	var blockEntries []rangeEntry
	var scratch []field
	for i := start; i >= 0; i-- {
		blockEntries = blockEntries[:0]
		clone := blockClone(s.blocks[i])
		s.walkBlock(s.blocks[i], scratch, func(id streamID, fields []field) bool {
			blockEntries = append(blockEntries, rangeEntry{id: id, fields: clone(fields)})
			return true
		})
		stop := false
		for j := len(blockEntries) - 1; j >= 0; j-- {
			e := blockEntries[j]
			if !belowHi(e.id, hi) {
				continue // above the window (the block's tail), keep descending
			}
			if !aboveLo(e.id, lo) {
				stop = true // below the window; entries only fall from here
				break
			}
			out = append(out, e)
			if limit > 0 && len(out) >= limit {
				stop = true
				break
			}
		}
		if stop {
			break
		}
	}
	return out
}

// walkBlock decodes block b's live entries in order the same way b.walk does, but
// preads the payload first when b is cold so a range read crosses a demoted block
// transparently (cold.go, M7). A resident block walks its own blob with no pread; a
// cold block reads its payload once into the stream's shared scratch and walks that
// with the block's resident master schema (the names offsets index into the payload,
// byte-identical to the shed blob). A torn cold frame yields no entries, the
// empty-block reading a broken cold region degrades to.
func (s *stream) walkBlock(b *block, scratch []field, fn func(id streamID, fields []field) bool) {
	if !b.cold() {
		b.walk(scratch, fn)
		return
	}
	b.walkIn(s.cold.payload(b.coldOff), scratch, fn)
}

// blockClone picks the field-copy a gather from block b must use. A cold block's
// walk yields views into the shared pread scratch, which the next cold block's pread
// clobbers, so a gathered cold entry must deep-copy its name and value bytes; a
// resident block's views alias its stable blob and keep the cheap header-only clone.
// Only a read that actually crosses a cold block pays the deep copy.
func blockClone(b *block) func([]field) []field {
	if b.cold() {
		return deepCloneFields
	}
	return cloneFields
}

// cloneFields copies the field headers the block walk reuses per entry, so a
// gathered entry keeps its own view slice across later walk steps. The name and
// value bytes are not copied, only the slice headers pointing into the stable
// blob.
func cloneFields(fields []field) []field {
	return append([]field(nil), fields...)
}

// deepCloneFields copies each field's name and value bytes, not just the slice
// headers, so a gathered entry survives the next cold block's pread reusing the
// shared scratch buffer. It is the cold-path clone: a resident walk keeps
// cloneFields since its blob is stable, but a cold walk's views alias scratch, which
// the walk of the next cold block overwrites. Only the cold range paths pay this
// copy, so the resident hot path is unchanged.
func deepCloneFields(fields []field) []field {
	out := make([]field, len(fields))
	for i := range fields {
		out[i] = field{
			name:  append([]byte(nil), fields[i].name...),
			value: append([]byte(nil), fields[i].value...),
		}
	}
	return out
}

// entryAt returns the live entry with exactly id and ok=true, or ok=false when no
// live entry has that id (it was never added, or an XDEL tombstoned it). The group
// re-read path joins a pending ID to its log entry this way, framing [id, nil] for
// a pending entry whose log entry is gone since the PEL outlives it (section 7.5).
func (s *stream) entryAt(id streamID) (fields []field, ok bool) {
	if s == nil {
		return nil, false
	}
	b := s.blockFor(id)
	if b == nil || !b.covers(id) {
		return nil, false
	}
	var scratch []field
	clone := blockClone(b)
	s.walkBlock(b, scratch, func(eid streamID, ef []field) bool {
		if eid == id {
			fields, ok = clone(ef), true
			return false
		}
		return eid.cmp(id) < 0 // stop once the walk climbs past id
	})
	return fields, ok
}

// Xrange answers XRANGE key start end [COUNT n]: the live entries with IDs in
// [start, end], oldest first, as an array of [id, [field value ...]] pairs
// (section 6.3). Xrevrange answers XREVRANGE key end start [COUNT n], the same
// window newest first with the two bound arguments swapped.
func Xrange(cx *shard.Ctx, args [][]byte, r shard.Reply) { rangeReply(cx, args, r, false) }

// Xrevrange answers XREVRANGE key end start [COUNT n], the reverse-order sibling
// of XRANGE. Its bound arguments arrive high-then-low, so the reply order flips
// but the window is the same.
func Xrevrange(cx *shard.Ctx, args [][]byte, r shard.Reply) { rangeReply(cx, args, r, true) }

func rangeReply(cx *shard.Ctx, args [][]byte, r shard.Reply, rev bool) {
	// XREVRANGE lists its bounds high-then-low; normalize to [lo, hi] so the
	// window is parsed identically for both directions.
	startArg, endArg := args[1], args[2]
	if rev {
		startArg, endArg = args[2], args[1]
	}
	lo, ok := parseBound(startArg, true)
	if !ok {
		r.Err(errInvalidID)
		return
	}
	hi, ok := parseBound(endArg, false)
	if !ok {
		r.Err(errInvalidID)
		return
	}
	limit, ok := parseCount(args[3:])
	if !ok {
		r.Err("ERR syntax error")
		return
	}

	s, wrong := registry(cx).lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	out := cx.Aux[:0]
	if s == nil {
		out = resp.AppendArrayHeader(out, 0)
		cx.Aux = out
		r.Raw(out)
		return
	}
	entries := s.collectRange(lo, hi, rev, limit)
	out = resp.AppendArrayHeader(out, len(entries))
	for i := range entries {
		out = appendEntryReply(out, entries[i].id, entries[i].fields)
	}
	cx.Aux = out
	r.Raw(out)
}

// appendEntryReply writes one entry as the [id, [field value ...]] pair the
// stream read commands reply in, the flat field array Redis uses.
func appendEntryReply(dst []byte, id streamID, fields []field) []byte {
	dst = resp.AppendArrayHeader(dst, 2)
	var idbuf [40]byte
	dst = resp.AppendBulk(dst, formatID(idbuf[:0], id))
	dst = resp.AppendArrayHeader(dst, 2*len(fields))
	for i := range fields {
		dst = resp.AppendBulk(dst, fields[i].name)
		dst = resp.AppendBulk(dst, fields[i].value)
	}
	return dst
}

// parseBound parses one range endpoint. "-" and "+" are the minimum and maximum
// IDs; a "(" prefix makes the endpoint exclusive; a bare "ms" completes its seq
// to 0 for a start and to the maximum for an end, so "ms" names the whole
// millisecond (section 6.3). ok is false on a malformed argument.
func parseBound(arg []byte, isStart bool) (bound, bool) {
	if len(arg) == 0 {
		return bound{}, false
	}
	if len(arg) == 1 {
		switch arg[0] {
		case '-':
			return bound{id: streamID{ms: 0, seq: 0}}, true
		case '+':
			return bound{id: streamID{ms: ^uint64(0), seq: ^uint64(0)}}, true
		}
	}
	excl := false
	if arg[0] == '(' {
		excl = true
		arg = arg[1:]
		if len(arg) == 0 {
			return bound{}, false
		}
	}
	id, ok := parseRangeID(arg, isStart)
	if !ok {
		return bound{}, false
	}
	return bound{id: id, excl: excl}, true
}

// parseRangeID parses "ms-seq", or "ms" with the seq completed to the low end of
// the millisecond for a start and the high end for an end.
func parseRangeID(arg []byte, isStart bool) (streamID, bool) {
	msPart, seqPart, hasSeq := splitID(arg)
	ms, ok := parseUint(msPart)
	if !ok {
		return streamID{}, false
	}
	if !hasSeq {
		if isStart {
			return streamID{ms: ms, seq: 0}, true
		}
		return streamID{ms: ms, seq: ^uint64(0)}, true
	}
	seq, ok := parseUint(seqPart)
	if !ok {
		return streamID{}, false
	}
	return streamID{ms: ms, seq: seq}, true
}

// parseCount reads the optional trailing COUNT clause. limit is -1 (unbounded)
// when the clause is absent and the parsed count when present; ok is false on any
// other trailing token.
func parseCount(rest [][]byte) (limit int, ok bool) {
	if len(rest) == 0 {
		return -1, true
	}
	if len(rest) != 2 || !eqFold(rest[0], "COUNT") {
		return 0, false
	}
	n, nok := parseUint(rest[1])
	if !nok {
		return 0, false
	}
	return int(n), true
}
