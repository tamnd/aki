package sqlo1

// Str is the string type layer, doc 05 section 1: the representation
// ladder (plain, blob, rope) over one Tiered runtime. Plain and blob
// are the same thing up here, a single record under the user key
// whose placement (slotted group or blob run) is the backend's
// business; the ladder's one real boundary is rope, where the value
// splits into chunk segments under a minted rooth and the user key
// holds only the root payload.
//
// Crash discipline for every plane change (create, rewrite, delete):
// a fresh rooth is minted per rope image, its chunks drain first, a
// Flush barrier makes them durable, and only then does the root PUT
// dirty the user key, with the retired plane's generation bump
// registered to ride that same batch (the #824 contract). Any crash
// prefix of that sequence leaves the previous value fully readable:
// the new plane is unreferenced until the root batch lands, and the
// old plane dies in the batch that lands it. The cost is that a crash
// between chunk drain and root drain strands the unreferenced plane;
// the generation probe cannot see such a plane (it was never bumped),
// so reclaiming it belongs to the scrub, noted in the T1 milestone.
//
// Like Tiered, a Str is single-owner: one goroutine, and returned
// values alias internal buffers only until the next call.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

// ErrValueTooLong rejects a write past the 512 MiB Redis value cap.
var ErrValueTooLong = errors.New("sqlo1: string value exceeds 512 MiB")

// inlineCap bounds what the ladder will store as one record: Track
// B's blob run must fit inside one 1 MiB extent behind its 64-byte
// header (doc 03 section 6.5), and the key and envelope ride in the
// same record. One group of headroom keeps the ladder safely inside
// that ceiling without importing backend geometry; if the unitsize
// verdict moves the extent size, this constant moves with it.
const inlineCap = 1<<20 - 4096

// recEnvelopeMax overestimates the doc 03 record envelope (fixed
// fields plus optional expiry and rootgen) for the inlineCap check.
const recEnvelopeMax = 32

// strReadRound is how many chunk subkeys one BatchGet round carries
// during rope assembly; each round's values are copied out before the
// next round invalidates them.
const strReadRound = 256

// defaultLeaseN is how many rooth counters one MintLease reserves.
// Counters a crash strands in a lease cost address space and nothing
// else (48 bits per shard), so the lease is sized for write bursts.
const defaultLeaseN = 1024

// StrConfig sizes a Str. Zero values take the defaults.
type StrConfig struct {
	// RopeMin is the blob-to-rope boundary: values of at least this
	// many bytes are written as ropes. Default DefaultRopeMin.
	RopeMin int
	// Log2Chunk is the chunk size exponent for new ropes. Default
	// DefaultLog2Chunk; existing ropes keep the size in their root.
	Log2Chunk uint8
	// Shard namespaces the rooth mint, doc 03 section 6.3.
	Shard uint16
	// LeaseN is the mint lease size. Default defaultLeaseN.
	LeaseN uint64
}

// Str is the string ladder over one Tiered. Construction requires the
// store to expose the Minter capability, because ropes cannot exist
// without durable rooth leases.
type Str struct {
	t    *Tiered
	mint Minter
	cfg  StrConfig

	// The current mint lease: counters [leaseNext, leaseEnd) are ours.
	leaseNext uint64
	leaseEnd  uint64

	// Reusable scratch. val carries rope assembly and append images,
	// rootBuf the encoded root payload, chunkScratch one chunk RMW,
	// kbuf the current chunk subkey for writes, chunkKeys/chunkVals
	// one read round of subkeys and values.
	val          []byte
	rootBuf      []byte
	chunkScratch []byte
	kbuf         [SubkeySize]byte
	chunkKeys    [][]byte
	chunkVals    [][]byte
}

// NewStr builds the string layer over t.
func NewStr(t *Tiered, cfg StrConfig) (*Str, error) {
	mint, ok := t.st.(Minter)
	if !ok {
		return nil, fmt.Errorf("sqlo1: store %T lacks the Minter capability the string ladder needs", t.st)
	}
	if cfg.RopeMin == 0 {
		cfg.RopeMin = DefaultRopeMin
	}
	if cfg.Log2Chunk == 0 {
		cfg.Log2Chunk = DefaultLog2Chunk
	}
	if cfg.LeaseN == 0 {
		cfg.LeaseN = defaultLeaseN
	}
	if cfg.Log2Chunk < minLog2Chunk || cfg.Log2Chunk > maxLog2Chunk {
		return nil, fmt.Errorf("sqlo1: log2chunk %d outside [%d, %d]", cfg.Log2Chunk, minLog2Chunk, maxLog2Chunk)
	}
	if cfg.RopeMin < 0 || cfg.RopeMin > MaxValueLen {
		return nil, fmt.Errorf("sqlo1: rope boundary %d outside (0, %d]", cfg.RopeMin, MaxValueLen)
	}
	return &Str{t: t, mint: mint, cfg: cfg}, nil
}

// needsRope decides the ladder rung for a value: past the configured
// boundary, or too large to survive as one record under the backend's
// blob ceiling (oversized keys shrink the inline room; the format
// refuses records past one extent, so this clause is load-bearing).
func (s *Str) needsRope(key []byte, valLen int) bool {
	return valLen >= s.cfg.RopeMin || len(key)+valLen+recEnvelopeMax > inlineCap
}

// strMeta is what Set, Del, and Append need to know about a key's
// current representation: whether it exists, and if it is a rope, the
// decoded root (copied out of the aliased read before the next call).
type strMeta struct {
	exists bool
	rope   bool
	root   ropeRoot
}

// metaOf reads key's representation. The value bytes it looked at are
// dead after the next Tiered call; only the decoded struct survives.
func (s *Str) metaOf(ctx context.Context, key []byte) (strMeta, error) {
	v, root, ok, err := s.t.Lookup(ctx, key)
	if err != nil || !ok {
		return strMeta{}, err
	}
	if !root {
		return strMeta{exists: true}, nil
	}
	// The only root payload a string key can hold in T1 is a rope;
	// cross-type overwrites learn to sniff the shared root header
	// when the collection types land.
	r, err := decodeRopeRoot(v)
	if err != nil {
		return strMeta{}, err
	}
	return strMeta{exists: true, rope: true, root: r}, nil
}

// retire registers the generation bump that kills a rope's plane,
// riding whatever op next dirties key (the replacement image or the
// tombstone), per the #824 same-batch contract.
func (s *Str) retire(key []byte, r ropeRoot) {
	s.t.Bump(key, r.rooth, r.rootgen+1)
}

// Set writes key's full value through the ladder.
func (s *Str) Set(ctx context.Context, key, val []byte) error {
	if len(val) > MaxValueLen {
		return ErrValueTooLong
	}
	m, err := s.metaOf(ctx, key)
	if err != nil {
		return err
	}
	if !s.needsRope(key, len(val)) {
		if m.rope {
			s.retire(key, m.root)
		}
		return s.t.Set(ctx, key, val, TagString)
	}
	return s.setRope(ctx, key, val, m)
}

// setRope writes val as a fresh rope plane and lands the root last.
// The plane is minted new even when the old value was a rope: reusing
// a rooth would overwrite live chunk subkeys in place, and a crash
// midway would tear the old value. A fresh plane is unreferenced
// until the final batch, so every crash prefix reads the old value.
func (s *Str) setRope(ctx context.Context, key, val []byte, m strMeta) error {
	rooth, err := s.nextRooth(ctx)
	if err != nil {
		return err
	}
	cs := 1 << s.cfg.Log2Chunk
	n := (len(val) + cs - 1) / cs
	for i := range n {
		lo := i * cs
		hi := min(lo+cs, len(val))
		putChunkKey(s.kbuf[:], rooth, uint64(i))
		if err := s.t.SetGen(ctx, s.kbuf[:], val[lo:hi], TagString, 1); err != nil {
			return err
		}
	}
	// The barrier: every chunk of the new plane is durable before the
	// root that references it can drain. Without it, the root's op
	// could ride an earlier batch than the chunks (a re-dirtied key
	// keeps its old drain-queue position), and a crash between the
	// two would leave a root reading absent chunks as zeros.
	if err := s.t.Flush(ctx); err != nil {
		return err
	}
	if m.rope {
		s.retire(key, m.root)
	}
	s.rootBuf = appendRopeRoot(s.rootBuf[:0], ropeRoot{
		log2chunk:  s.cfg.Log2Chunk,
		rootgen:    1,
		rooth:      rooth,
		totalLen:   uint64(len(val)),
		chunkCount: uint64(n),
	})
	return s.t.Set(ctx, key, s.rootBuf, TagString|TagRoot)
}

// Get reads key's full value. Like Tiered.Get, the returned bytes
// alias internal buffers and are valid until the next call.
func (s *Str) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	v, root, ok, err := s.t.Lookup(ctx, key)
	if err != nil || !ok {
		return nil, false, err
	}
	if !root {
		return v, true, nil
	}
	r, err := decodeRopeRoot(v)
	if err != nil {
		return nil, false, err
	}
	return s.readRope(ctx, r)
}

// readRope assembles a whole rope into s.val.
func (s *Str) readRope(ctx context.Context, r ropeRoot) ([]byte, bool, error) {
	return s.readRopeRange(ctx, r, 0, r.totalLen)
}

// readRopeRange assembles bytes [lo, hi) of a rope into s.val, one
// BatchGet round of chunks at a time, touching only the chunks the
// window overlaps (S-I1). The caller clamps hi to totalLen. An absent
// chunk, or the tail a short chunk does not cover, reads as zeros:
// full-value writes never produce either, but the lazy zero-fill of
// the range surface (doc 05 section 2) does, and the assembly is
// defined once for both.
func (s *Str) readRopeRange(ctx context.Context, r ropeRoot, lo, hi uint64) ([]byte, bool, error) {
	cs := r.chunkSize()
	s.val = grow(s.val, int(hi-lo))
	if hi == lo {
		return s.val[:0], true, nil
	}
	if cap(s.chunkKeys) < strReadRound {
		s.chunkKeys = make([][]byte, strReadRound)
		for i := range s.chunkKeys {
			s.chunkKeys[i] = make([]byte, SubkeySize)
		}
	}
	c0 := lo >> r.log2chunk
	c1 := ((hi - 1) >> r.log2chunk) + 1
	for base := c0; base < c1; base += strReadRound {
		n := min(strReadRound, c1-base)
		keys := s.chunkKeys[:n]
		for j := range keys {
			putChunkKey(keys[j], r.rooth, base+uint64(j))
		}
		out, err := s.t.BatchGet(ctx, keys, s.chunkVals)
		s.chunkVals = out[:0]
		if err != nil {
			return nil, false, err
		}
		for j, cv := range out {
			if uint64(len(cv)) > cs {
				return nil, false, fmt.Errorf("sqlo1: rope chunk %d holds %d bytes, chunk size %d", base+uint64(j), len(cv), cs)
			}
			// A chunk longer than the root's tail is trimmed, not an
			// error: an append lands its chunks before the root whose
			// total_len commits them, so a crash between the two
			// legally leaves extra bytes past the logical length.
			cstart := (base + uint64(j)) << r.log2chunk
			a, b := max(cstart, lo), min(cstart+cs, hi)
			span := s.val[a-lo : b-lo]
			var src []byte
			if skip := a - cstart; uint64(len(cv)) > skip {
				src = cv[skip:]
			}
			copied := copy(span, src)
			clear(span[copied:])
		}
	}
	return s.val[:hi-lo], true, nil
}

// Range returns the SUBSTR and GETRANGE window of key's value: start
// and end are inclusive byte offsets, negative counts from the end,
// and both clamp to the value the way Redis clamps. A missing key or
// an empty window is the empty string. Rope keys read only the chunks
// the window overlaps.
func (s *Str) Range(ctx context.Context, key []byte, start, end int64) ([]byte, error) {
	v, root, ok, err := s.t.Lookup(ctx, key)
	if err != nil || !ok {
		return nil, err
	}
	if !root {
		lo, hi, some := clampRange(start, end, int64(len(v)))
		if !some {
			return nil, nil
		}
		return v[lo:hi], nil
	}
	r, err := decodeRopeRoot(v)
	if err != nil {
		return nil, err
	}
	lo, hi, some := clampRange(start, end, int64(r.totalLen))
	if !some {
		return nil, nil
	}
	out, _, err := s.readRopeRange(ctx, r, uint64(lo), uint64(hi))
	return out, err
}

// clampRange resolves Redis range arguments against a value of n bytes
// into a half-open [lo, hi) window; some is false when the window is
// empty.
func clampRange(start, end, n int64) (lo, hi int64, some bool) {
	if start < 0 {
		start = max(n+start, 0)
	}
	if end < 0 {
		end = n + end
	}
	end = min(end, n-1)
	if n == 0 || start > end {
		return 0, 0, false
	}
	return start, end + 1, true
}

// Strlen answers without assembling rope values: the root already
// carries total_len (S-I2's point).
func (s *Str) Strlen(ctx context.Context, key []byte) (int64, bool, error) {
	v, root, ok, err := s.t.Lookup(ctx, key)
	if err != nil || !ok {
		return 0, false, err
	}
	if !root {
		return int64(len(v)), true, nil
	}
	r, err := decodeRopeRoot(v)
	if err != nil {
		return 0, false, err
	}
	return int64(r.totalLen), true, nil
}

// Entry reports existence and the exact expire_ms (0 for none) without
// assembling rope values; the TTL family and SET's NX, XX, and KEEPTTL
// checks live on it.
func (s *Str) Entry(ctx context.Context, key []byte) (exists bool, expMs int64, err error) {
	_, _, expMs, ok, err := s.t.LookupEntry(ctx, key)
	return ok, expMs, err
}

// ExpireAt stamps an absolute expire_ms on key (0 is PERSIST) and
// reports whether the key existed. A rope's expiry lives on its root
// record, so this is one stamp for every representation; the chunks a
// root leaves behind when it expires are the doc 11 lazy-bump gap
// noted in the T1 milestone.
func (s *Str) ExpireAt(ctx context.Context, key []byte, atMs int64) (bool, error) {
	return s.t.ExpireAt(ctx, key, atMs)
}

// Encoding is the OBJECT ENCODING answer: int, embstr, raw, or rope,
// per doc 05 section 2. The int and embstr reads come from the value
// bytes for now; the intshadow slice moves the int answer to a header
// flag stamped at write time, which is what S-I2 ultimately wants.
func (s *Str) Encoding(ctx context.Context, key []byte) (string, bool, error) {
	v, root, ok, err := s.t.Lookup(ctx, key)
	if err != nil || !ok {
		return "", false, err
	}
	switch {
	case root:
		return "rope", true, nil
	case intShaped(v):
		return "int", true, nil
	case len(v) <= embstrMax:
		return "embstr", true, nil
	}
	return "raw", true, nil
}

// embstrMax is Redis's embedded-string boundary; values at or under it
// report embstr, longer plain values report raw.
const embstrMax = 44

// intShaped reports whether v is the canonical decimal form of an
// int64, which is what Redis requires before it reports the int
// encoding ("0123" and "+1" are raw bytes, not integers).
func intShaped(v []byte) bool {
	if len(v) == 0 || len(v) > 20 {
		return false
	}
	n, err := strconv.ParseInt(string(v), 10, 64)
	return err == nil && strconv.FormatInt(n, 10) == string(v)
}

// Del removes key, retiring its plane first when it is a rope so the
// bump rides the tombstone's batch and the chunks die with the root.
func (s *Str) Del(ctx context.Context, key []byte) (bool, error) {
	m, err := s.metaOf(ctx, key)
	if err != nil || !m.exists {
		return false, err
	}
	if m.rope {
		s.retire(key, m.root)
	}
	return s.t.Del(ctx, key)
}

// Append extends key's value and returns the new length. Below the
// rope boundary it rewrites the one record, which is O(value) but the
// values there are small by definition. The write that crosses the
// boundary pays the one-time O(value) plane build (doc 05 section
// 1.2), and every append after it touches the tail chunk, whatever
// new chunks the suffix fills, and the root. A missing key appends to
// the empty string, per Redis.
func (s *Str) Append(ctx context.Context, key, suffix []byte) (int64, error) {
	v, root, ok, err := s.t.Lookup(ctx, key)
	if err != nil {
		return 0, err
	}
	if !ok {
		if err := s.Set(ctx, key, suffix); err != nil {
			return 0, err
		}
		return int64(len(suffix)), nil
	}
	if !root {
		newLen := len(v) + len(suffix)
		if newLen > MaxValueLen {
			return 0, ErrValueTooLong
		}
		// v aliases the arena, so the image is built before any
		// write can recycle it.
		s.val = append(append(s.val[:0], v...), suffix...)
		if !s.needsRope(key, newLen) {
			return int64(newLen), s.t.Set(ctx, key, s.val, TagString)
		}
		return int64(newLen), s.setRope(ctx, key, s.val, strMeta{})
	}
	r, err := decodeRopeRoot(v)
	if err != nil {
		return 0, err
	}
	return s.appendRope(ctx, key, r, suffix)
}

// appendRope grows an existing rope in place: fill the tail chunk,
// add whole chunks for the rest, then land the root with the new
// length. The root is written after the chunks on purpose: its
// total_len is the commit point, so a crash that keeps the old root
// keeps the old length and never reads the new tail.
func (s *Str) appendRope(ctx context.Context, key []byte, r ropeRoot, suffix []byte) (int64, error) {
	newLen := r.totalLen + uint64(len(suffix))
	if newLen > MaxValueLen {
		return 0, ErrValueTooLong
	}
	if len(suffix) == 0 {
		return int64(r.totalLen), nil
	}
	// If the root is still dirty from an earlier write it holds a
	// drain-queue position ahead of the chunks this append writes, and
	// a batch split there would land the new total_len before the new
	// tail. Draining it out first keeps the root the commit point; the
	// WAL command fencing of doc 05 section 5 will retire this flush.
	if s.t.ht.dirtyKey(key) {
		if err := s.t.Flush(ctx); err != nil {
			return 0, err
		}
	}
	cs := r.chunkSize()
	rem := suffix
	last := (r.totalLen - 1) >> r.log2chunk
	tail := r.totalLen - (last << r.log2chunk)
	if tail < cs {
		// Read-modify the short tail chunk. The chunk may itself be
		// short of the logical tail; the gap is lazy zeros.
		putChunkKey(s.kbuf[:], r.rooth, last)
		s.chunkKeys = append(s.chunkKeys[:0], s.kbuf[:])
		out, err := s.t.BatchGet(ctx, s.chunkKeys, s.chunkVals)
		s.chunkVals = out[:0]
		if err != nil {
			return 0, err
		}
		s.chunkScratch = grow(s.chunkScratch, int(tail))
		filled := copy(s.chunkScratch[:tail], out[0])
		clear(s.chunkScratch[filled:tail])
		take := min(cs-tail, uint64(len(rem)))
		s.chunkScratch = append(s.chunkScratch[:tail], rem[:take]...)
		rem = rem[take:]
		putChunkKey(s.kbuf[:], r.rooth, last)
		if err := s.t.SetGen(ctx, s.kbuf[:], s.chunkScratch, TagString, r.rootgen); err != nil {
			return 0, err
		}
	}
	for i := last + 1; len(rem) > 0; i++ {
		take := min(cs, uint64(len(rem)))
		putChunkKey(s.kbuf[:], r.rooth, i)
		if err := s.t.SetGen(ctx, s.kbuf[:], rem[:take], TagString, r.rootgen); err != nil {
			return 0, err
		}
		rem = rem[take:]
	}
	r.totalLen = newLen
	r.chunkCount = (newLen + cs - 1) >> r.log2chunk
	s.rootBuf = appendRopeRoot(s.rootBuf[:0], r)
	if err := s.t.Set(ctx, key, s.rootBuf, TagString|TagRoot); err != nil {
		return 0, err
	}
	return int64(newLen), nil
}

// nextRooth mints one rooth, taking a fresh durable lease when the
// current one is spent.
func (s *Str) nextRooth(ctx context.Context) (uint64, error) {
	if s.leaseNext == s.leaseEnd {
		start, err := s.mint.MintLease(ctx, s.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		end, err := LeaseEnd(start, s.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		s.leaseNext, s.leaseEnd = start, end
	}
	c := s.leaseNext
	s.leaseNext++
	return MintRooth(s.cfg.Shard, c)
}

// grow returns b with length n, reallocating only to gain capacity;
// contents are unspecified.
func grow(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}
