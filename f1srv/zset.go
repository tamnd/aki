package f1srv

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
)

// errInvalidFloat is the sentinel parseScore returns for any input Redis's string2d would
// reject; the call sites translate it to the exact Redis wire message.
var errInvalidFloat = errors.New("invalid float")

// Sorted set is the fourth collection type on f1raw, and it is the first with two element-row
// families under one collection key (spec 2064/f1_rewrite_ltm/07 section 2): a member-family
// row for the map view (ZSCORE, existence, the old-score read inside ZADD/ZINCRBY/ZREM) and a
// score-family row for the order view (ZRANGE and the rank commands, later slices). Both are
// element-per-row like the hash and set, so a billion-member board mutates and reads a bounded
// handful of rows and never materializes the collection.
//
// The two families share the composite-key shape the hash and set use, plus a one-byte family
// discriminator so the single ordered element index keeps them apart. The member family key is
// uvarint(len(zkey)) | zkey | 'm' | member, sorted by member bytes; the score family key is
// uvarint(len(zkey)) | zkey | 's' | sortable(score) | member, sorted by score then member. The
// length prefix makes the collection key injective (no two zsets share a row), and 'm' < 's'
// keeps every member row of a zset ahead of its score rows, so a prefix scan bounded by
// uvarint(len(zkey))|zkey|'m' enumerates exactly the member family and the |'s' prefix exactly
// the score family. The discriminator lives in the key bytes, not just the record kind, because
// the ordered index scans and removes by key bytes and must never intermix the two families.
//
// The member family row value is the raw IEEE-754 score bits, little-endian, so ZSCORE decodes
// with one Float64frombits and never touches the score family or the sortable codec (spec 5.3
// raw-bits option). The score family row carries nothing: the presence of the key is the whole
// content, and its sortable-score prefix is what makes a plain byte comparison equal the numeric
// score order (section 5.2).
//
// A per-zset header row (kindZsetMeta under the bare key) holds the maintained cardinality and
// the folded encoding tag, so ZCARD is one header read and OBJECT ENCODING answers in O(1). The
// header count and the live member-row count stay exactly equal because every add pairs a member
// CollInsert with a count bump and every ZREM pairs a member CollRemove with a decrement.
//
// Write serialization: ZADD/ZREM/ZINCRBY take the per-key stripe lock (shared with the INCR
// family, the hash, and the set) so a zset's two families and its header count stay consistent
// under concurrent writers. Reads (ZSCORE/ZMSCORE/ZCARD) are lock-free.
const (
	kindZsetMember byte = 0x03 // a member-family row, value is the 8-byte little-endian score bits
	kindZsetScore  byte = 0x04 // a score-family row, empty value, keyed by sortable score then member
	kindZsetMeta   byte = 0x0A // the per-zset header row (coll_header)
)

// Family discriminator bytes placed after the length-prefixed collection key. 'm' sorts before
// 's', so a zset's member rows precede its score rows in the shared ordered index; each family's
// prefix scan is bounded to its own rows.
const (
	zsetMemberTag byte = 'm'
	zsetScoreTag  byte = 's'
)

// encSkiplist is the zset counterpart of the set/hash encoding tags in object.go: a zset starts
// listpack and folds one-way to skiplist. It reuses the shared encoding byte space (encNone=0,
// encListpack=2 already defined there) with a value distinct from the set tags so a stored tag
// is unambiguous.
const encSkiplist byte = 4

// Redis ship defaults for the zset encoding thresholds (zset-max-listpack-entries and
// zset-max-listpack-value). CONFIG is a no-op on f1srv, so these are the defaults every stock
// Redis and Valkey runs, which is what a client comparing OBJECT ENCODING expects.
const (
	zsetMaxListpackEntries = 128
	zsetMaxListpackValue   = 64
)

// foldZsetEnc applies Redis's one-way zset encoding upgrade for a member being added, given the
// encoding so far and the cardinality after the add. A zset starts listpack and upgrades to
// skiplist once it outgrows the entry limit or any member outgrows the value-length limit;
// skiplist is terminal, so a removal never walks it back. It mirrors the listpack/skiplist choice
// in zsetTypeMaybeConvert.
func foldZsetEnc(cur byte, member []byte, newCount uint64) byte {
	if cur == encSkiplist {
		return encSkiplist
	}
	if newCount > zsetMaxListpackEntries || len(member) > zsetMaxListpackValue {
		return encSkiplist
	}
	return encListpack
}

// zsetEncodingName maps a stored zset encoding tag to the wire name Redis reports.
func zsetEncodingName(enc byte) string {
	if enc == encSkiplist {
		return "skiplist"
	}
	return "listpack"
}

// encodeSortableScore writes the order-preserving 8-byte encoding of a score into dst (spec
// section 5.2): take the IEEE-754 bits, flip the sign bit when it is clear (positives sort above
// the transformed negatives) or flip all 64 bits when it is set (larger-magnitude negatives sort
// first), then store big-endian so the most significant byte compares first. After this a plain
// byte comparison of two score keys equals the numeric order, -inf transforms to the smallest
// bytes and +inf to the largest, and +0.0/-0.0 map to the same value.
func encodeSortableScore(dst []byte, score float64) {
	bits := math.Float64bits(score)
	if bits&(1<<63) == 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	binary.BigEndian.PutUint64(dst, bits)
}

// decodeSortableScore inverts encodeSortableScore: it reads the order-preserving 8-byte
// big-endian encoding back to the original score. If the top bit is set the source score
// was non-negative and only the sign bit was flipped; if it is clear the source was
// negative and all 64 bits were flipped. This lets ZRANGE WITHSCORES report a score
// straight from the score-family key it already walked, with no second read of the
// member family. +0.0 and -0.0 both encoded to the same bytes and decode to +0.0, which
// matches the -0 normalization at ingest.
func decodeSortableScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&(1<<63) != 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}

// normalizeZero maps -0.0 to +0.0 and leaves every other value untouched. Redis's default
// listpack zset collapses -0 to integer 0 (via string2ll), so a score ingested as -0.0 must
// surface as "0", not "-0"; we normalize at ingest, before the score reaches either family or
// formatScore, so ZSCORE and the ZADD-INCR/ZINCRBY replies match the default encoding.
func normalizeZero(f float64) float64 {
	if f == 0 {
		return 0
	}
	return f
}

// parseScore parses a score argument the way Redis's string2d/getDouble does: the whole string
// must be a valid float, "inf"/"+inf"/"-inf"/"infinity" are accepted, leading or trailing junk
// and whitespace are rejected, an out-of-range magnitude (which strtod would flag ERANGE) is
// rejected, and nan is rejected. Go's ParseFloat already rejects whitespace and trailing junk and
// returns a non-nil error on the ERANGE cases, so an error from it is exactly Redis's reject set;
// the extra guards reject an underscore separator ParseFloat would otherwise accept but strtod
// would not, and reject a nan that parsed cleanly.
func parseScore(b []byte) (float64, error) {
	for _, ch := range b {
		if ch == '_' {
			return 0, errInvalidFloat
		}
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, errInvalidFloat
	}
	if math.IsNaN(f) {
		return 0, errInvalidFloat
	}
	return f, nil
}

// zmemberKey builds the member-family composite key for (zkey, member) into the reused kbuf.
func (c *connState) zmemberKey(zkey, member []byte) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(zkey)))
	b = append(b, tmp[:n]...)
	b = append(b, zkey...)
	b = append(b, zsetMemberTag)
	b = append(b, member...)
	c.kbuf = b
	return b
}

// zscoreKey builds the score-family composite key for (zkey, score, member) into the reused
// kbuf: the length-prefixed key, the score-family tag, the 8 sortable score bytes, then the
// member. It shares kbuf with zmemberKey, so a caller finishes with a member key before building
// a score key.
func (c *connState) zscoreKey(zkey []byte, score float64, member []byte) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(zkey)))
	b = append(b, tmp[:n]...)
	b = append(b, zkey...)
	b = append(b, zsetScoreTag)
	var sortable [8]byte
	encodeSortableScore(sortable[:], score)
	b = append(b, sortable[:]...)
	b = append(b, member...)
	c.kbuf = b
	return b
}

// zscorePrefix builds the score-family enumeration prefix for zkey into the reused pbuf:
// uvarint(len(zkey)) | zkey | 's'. Every score-family row of the zset carries this prefix
// and no other family does, so a rank or scan bounded by it sees exactly the score rows in
// numeric order. It uses pbuf, not kbuf, so a caller can hold the prefix across a kbuf
// rebuild (the score-family key the rank primitive ranks against lives in kbuf).
func (c *connState) zscorePrefix(zkey []byte) []byte {
	b := c.pbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(zkey)))
	b = append(b, tmp[:n]...)
	b = append(b, zkey...)
	b = append(b, zsetScoreTag)
	c.pbuf = b
	return b
}

// zsetHeader reads a zset's header row: the maintained cardinality and the encoding tag folded
// forward by its writers. ok is false when the zset has no header (no members); the encoding is
// the 9th header byte, encNone for a header written before the tag existed.
func (c *connState) zsetHeader(zkey []byte) (count uint64, enc byte, ok bool) {
	var cb [9]byte
	v, got := c.srv.store.GetKind(zkey, cb[:0], kindZsetMeta)
	if !got || len(v) < 8 {
		return 0, encNone, false
	}
	enc = encNone
	if len(v) >= 9 {
		enc = v[8]
	}
	return binary.LittleEndian.Uint64(v), enc, true
}

// zsetCard reads a zset's maintained cardinality, 0 when it has no members. The read goes
// through the header record's seqlock so it never tears against an in-place ZREM decrement
// and takes no lock. The full header (count and encoding) is still read via zsetHeader where
// the encoding tag is needed.
func (c *connState) zsetCard(zkey []byte) uint64 {
	n, ok := c.srv.store.CountInt64(zkey, kindZsetMeta)
	if !ok || n < 0 {
		return 0
	}
	return uint64(n)
}

// zsetPutHeader writes a zset's cardinality and encoding tag, or deletes the header when the
// count reaches zero so the key stops existing (empty zset is no zset). It is the write the
// growing paths (ZADD, ZINCRBY into a new member) use, which know the fresh encoding.
func (c *connState) zsetPutHeader(zkey []byte, count uint64, enc byte) error {
	if count == 0 {
		c.srv.store.DeleteKind(zkey, kindZsetMeta)
		return nil
	}
	var ob [9]byte
	binary.LittleEndian.PutUint64(ob[:8], count)
	ob[8] = enc
	_, err := c.srv.store.PutKind(zkey, ob[:], kindZsetMeta)
	return err
}

// zsetSetCard writes a zset's cardinality while preserving its recorded encoding, or deletes the
// header at zero. It is the write the shrinking path (ZREM) uses: a removal never changes the
// encoding (Redis never downgrades), so it keeps the tag the zset already carries.
func (c *connState) zsetSetCard(zkey []byte, count uint64) error {
	if count == 0 {
		c.srv.store.DeleteKind(zkey, kindZsetMeta)
		return nil
	}
	_, enc, _ := c.zsetHeader(zkey)
	return c.zsetPutHeader(zkey, count, enc)
}

// zsetInsertNew writes a brand-new member's two rows in one logical step (spec section 2.3
// new-member case): the member-family row carrying the score bits, then the score-family row.
// The caller holds the stripe lock, has verified the member is absent, and bumps the header
// count separately. mk is the member key already built in kbuf; this consumes it before
// rebuilding kbuf for the score key.
func (c *connState) zsetInsertNew(zkey, member, mk []byte, score float64) error {
	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], math.Float64bits(score))
	if _, err := c.srv.store.PutKind(mk, sb[:], kindZsetMember); err != nil {
		return err
	}
	c.srv.store.CollInsert(mk, kindZsetMember)
	sk := c.zscoreKey(zkey, score, member)
	if _, err := c.srv.store.PutKind(sk, nil, kindZsetScore); err != nil {
		return err
	}
	c.srv.store.CollInsert(sk, kindZsetScore)
	return nil
}

// zsetUpdateScore moves an existing member from oldScore to newScore (spec section 2.4): a
// score change is a member-row value overwrite plus a score-row delete-then-insert, because the
// score-family key encodes the score and a row whose key changes is a delete of the old key and
// an insert of the new. Cardinality is unchanged. The caller holds the stripe lock and has
// verified newScore differs from oldScore. mk is consumed before kbuf is rebuilt for the score
// keys.
func (c *connState) zsetUpdateScore(zkey, member, mk []byte, oldScore, newScore float64) error {
	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], math.Float64bits(newScore))
	if _, err := c.srv.store.PutKind(mk, sb[:], kindZsetMember); err != nil {
		return err
	}
	oldSK := c.zscoreKey(zkey, oldScore, member)
	if c.srv.store.DeleteKind(oldSK, kindZsetScore) {
		c.srv.store.CollRemove(oldSK)
	}
	newSK := c.zscoreKey(zkey, newScore, member)
	if _, err := c.srv.store.PutKind(newSK, nil, kindZsetScore); err != nil {
		return err
	}
	c.srv.store.CollInsert(newSK, kindZsetScore)
	return nil
}

// writeScore replies with a member's score as a RESP2 bulk string, formatted the way Redis
// formats zset scores (formatScore is the byte-for-byte port of Redis's d2string). It reuses
// sbuf so the reply costs no allocation.
func (c *connState) writeScore(value float64) {
	c.sbuf = formatScore(c.sbuf[:0], value)
	c.writeBulk(c.sbuf)
}

func (c *connState) cmdZAdd(argv [][]byte) {
	// ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'zadd' command")
		return
	}
	zkey := argv[1]

	var nx, xx, gt, lt, ch, incr bool
	i := 2
scanFlags:
	for ; i < len(argv); i++ {
		switch {
		case eqFold(argv[i], "NX"):
			nx = true
		case eqFold(argv[i], "XX"):
			xx = true
		case eqFold(argv[i], "GT"):
			gt = true
		case eqFold(argv[i], "LT"):
			lt = true
		case eqFold(argv[i], "CH"):
			ch = true
		case eqFold(argv[i], "INCR"):
			incr = true
		default:
			break scanFlags
		}
	}
	rest := argv[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		c.writeErr("ERR syntax error")
		return
	}
	pairs := len(rest) / 2

	if nx && xx {
		c.writeErr("ERR XX and NX options at the same time are not compatible")
		return
	}
	if (gt && nx) || (lt && nx) || (gt && lt) {
		c.writeErr("ERR GT, LT, and/or NX options at the same time are not compatible")
		return
	}
	if incr && pairs > 1 {
		c.writeErr("ERR INCR option supports a single increment-element pair")
		return
	}

	// Parse every score up front so a malformed float rejects the whole command before any
	// write, matching Redis, which fills its score array before touching the keyspace.
	scores := c.zscores[:0]
	for j := 0; j < pairs; j++ {
		s, err := parseScore(rest[j*2])
		if err != nil {
			c.writeErr("ERR value is not a valid float")
			return
		}
		scores = append(scores, s)
	}
	c.zscores = scores

	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}

	count, enc, _ := c.zsetHeader(zkey)
	added := 0
	changed := 0
	var incrScore float64
	incrSuppressed := false

	for j := 0; j < pairs; j++ {
		member := rest[j*2+1]
		score := normalizeZero(scores[j])

		mk := c.zmemberKey(zkey, member)
		oldv, present := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
		c.vbuf = oldv

		if present {
			oldScore := math.Float64frombits(binary.LittleEndian.Uint64(oldv))
			newScore := score
			if incr {
				newScore = normalizeZero(oldScore + score)
				if math.IsNaN(newScore) {
					mu.Unlock()
					c.writeErr("ERR resulting score is not a number (NaN)")
					return
				}
			}
			if nx {
				incrSuppressed = true
				continue
			}
			if (lt && newScore >= oldScore) || (gt && newScore <= oldScore) {
				incrSuppressed = true
				continue
			}
			incrScore = newScore
			if newScore != oldScore {
				if err := c.zsetUpdateScore(zkey, member, mk, oldScore, newScore); err != nil {
					mu.Unlock()
					c.writeErr("ERR " + err.Error())
					return
				}
				changed++
			}
			continue
		}

		// Absent member. GT/LT gate only the update path, so a new member is still added
		// unless XX forbids new members.
		if xx {
			incrSuppressed = true
			continue
		}
		if err := c.zsetInsertNew(zkey, member, mk, score); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		count++
		enc = foldZsetEnc(enc, member, count)
		added++
		changed++
		incrScore = score
	}

	if added > 0 {
		if err := c.zsetPutHeader(zkey, count, enc); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()

	// A sorted set that gained members can serve a client blocked on BZPOPMIN/BZPOPMAX/BZMPOP.
	// The atomic fast-out in signalListKey keeps this off the registry lock when nobody blocks.
	if added > 0 {
		c.srv.signalListKey(zkey)
	}

	if incr {
		if incrSuppressed {
			c.writeNil()
			return
		}
		c.writeScore(incrScore)
		return
	}
	if ch {
		c.writeInt(int64(changed))
		return
	}
	c.writeInt(int64(added))
}

func (c *connState) cmdZIncrBy(argv [][]byte) {
	// ZINCRBY key increment member
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'zincrby' command")
		return
	}
	incr, err := parseScore(argv[2])
	if err != nil {
		c.writeErr("ERR value is not a valid float")
		return
	}
	zkey := argv[1]
	member := argv[3]

	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}

	mk := c.zmemberKey(zkey, member)
	oldv, present := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
	c.vbuf = oldv

	newScore := incr
	if present {
		oldScore := math.Float64frombits(binary.LittleEndian.Uint64(oldv))
		newScore = oldScore + incr
		if math.IsNaN(newScore) {
			mu.Unlock()
			c.writeErr("ERR resulting score is not a number (NaN)")
			return
		}
		newScore = normalizeZero(newScore)
		if newScore != oldScore {
			if err := c.zsetUpdateScore(zkey, member, mk, oldScore, newScore); err != nil {
				mu.Unlock()
				c.writeErr("ERR " + err.Error())
				return
			}
		}
		mu.Unlock()
		c.writeScore(newScore)
		return
	}

	newScore = normalizeZero(newScore)
	if err := c.zsetInsertNew(zkey, member, mk, newScore); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	count, enc, _ := c.zsetHeader(zkey)
	count++
	enc = foldZsetEnc(enc, member, count)
	if err := c.zsetPutHeader(zkey, count, enc); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()
	// A new member can serve a client blocked on BZPOPMIN/BZPOPMAX/BZMPOP.
	c.srv.signalListKey(zkey)
	c.writeScore(newScore)
}

func (c *connState) cmdZScore(argv [][]byte) {
	// ZSCORE key member
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'zscore' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	mk := c.zmemberKey(argv[1], argv[2])
	v, ok := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
	c.vbuf = v
	if !ok {
		c.writeNil()
		return
	}
	c.writeScore(math.Float64frombits(binary.LittleEndian.Uint64(v)))
}

func (c *connState) cmdZMScore(argv [][]byte) {
	// ZMSCORE key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'zmscore' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	c.writeArrayHeader(len(argv) - 2)
	for _, member := range argv[2:] {
		mk := c.zmemberKey(argv[1], member)
		v, ok := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
		c.vbuf = v
		if !ok {
			c.writeNil()
			continue
		}
		c.writeScore(math.Float64frombits(binary.LittleEndian.Uint64(v)))
	}
}

func (c *connState) cmdZCard(argv [][]byte) {
	// ZCARD key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'zcard' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	c.writeInt(int64(c.zsetCard(argv[1])))
}

func (c *connState) cmdZRem(argv [][]byte) {
	// ZREM key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'zrem' command")
		return
	}
	zkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}

	removed := 0
	for _, member := range argv[2:] {
		mk := c.zmemberKey(zkey, member)
		// Take reads the score and deletes the member row in one index probe, where a GetKind
		// then DeleteKind would find the same record twice. The score is copied into vbuf
		// before the slot clears, so it stays readable below to address the score row directly,
		// the point of storing the score in the member row (spec section 2.5).
		v, ok := c.srv.store.TakeKind(mk, c.vbuf[:0], kindZsetMember)
		c.vbuf = v
		if !ok {
			continue
		}
		score := math.Float64frombits(binary.LittleEndian.Uint64(v))
		c.srv.store.CollRemove(mk)
		sk := c.zscoreKey(zkey, score, member)
		if c.srv.store.DeleteKind(sk, kindZsetScore) {
			c.srv.store.CollRemove(sk)
		}
		removed++
	}

	if removed > 0 {
		n, ok := c.srv.store.CountAddInt64(zkey, kindZsetMeta, -int64(removed))
		if !ok || n <= 0 {
			c.srv.store.DeleteKind(zkey, kindZsetMeta)
		}
	}
	mu.Unlock()
	c.writeInt(int64(removed))
}

// cmdZRank answers ZRANK and ZREVRANK (rev selects the reverse order). It reads the
// member's score from the member family, rebuilds that member's score-family key, and
// asks the ordered index for that key's position within the score-family prefix, an
// O(log n) order-statistic seek (spec section 6.1, the model that closed the ZRANK LTM
// gap). The forward rank is the count of members that sort below (score, then member);
// the reverse rank is card-1-forward. An absent member replies nil (a null array under
// WITHSCORE, a null bulk otherwise). The optional WITHSCORE trailer adds the score.
func (c *connState) cmdZRank(argv [][]byte, rev bool) {
	// ZRANK key member [WITHSCORE] / ZREVRANK key member [WITHSCORE]
	name := "zrank"
	if rev {
		name = "zrevrank"
	}
	if len(argv) < 3 || len(argv) > 4 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	withScore := false
	if len(argv) == 4 {
		if !eqFold(argv[3], "WITHSCORE") {
			c.writeErr("ERR syntax error")
			return
		}
		withScore = true
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	zkey := argv[1]
	member := argv[2]

	mk := c.zmemberKey(zkey, member)
	v, ok := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
	c.vbuf = v
	if !ok {
		if withScore {
			c.writeNilArray()
		} else {
			c.writeNil()
		}
		return
	}
	score := math.Float64frombits(binary.LittleEndian.Uint64(v))

	prefix := c.zscorePrefix(zkey)         // held in pbuf across the kbuf rebuild below
	sk := c.zscoreKey(zkey, score, member) // rebuilds kbuf into the score-family key
	rank := c.srv.store.CollRankOf(prefix, sk)
	if rev {
		rank = int(c.zsetCard(zkey)) - 1 - rank
	}

	if withScore {
		c.writeArrayHeader(2)
		c.writeInt(int64(rank))
		c.writeScore(score)
		return
	}
	c.writeInt(int64(rank))
}

// cmdZRange is the unified range read: ZRANGE key start stop [BYSCORE | BYLEX] [REV]
// [LIMIT offset count] [WITHSCORES]. Without BYSCORE or BYLEX it is the rank-indexed window
// (zrangeByIndex); BYSCORE routes to zByScore and BYLEX to zByLex, the same value-bounded cursor
// paths ZRANGEBYSCORE/ZRANGEBYLEX use. REV reverses the order, and for the value forms it also
// means start/stop are given as max then min (Redis's rule), so the bounds are swapped before the
// shared path sees them.
func (c *connState) cmdZRange(argv [][]byte) {
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'zrange' command")
		return
	}
	rev := false
	withScores := false
	byScore := false
	byLex := false
	hasLimit := false
	offset := 0
	count := 0
	for i := 4; i < len(argv); i++ {
		switch {
		case eqFold(argv[i], "WITHSCORES"):
			withScores = true
		case eqFold(argv[i], "REV"):
			rev = true
		case eqFold(argv[i], "BYSCORE"):
			byScore = true
		case eqFold(argv[i], "BYLEX"):
			byLex = true
		case eqFold(argv[i], "LIMIT"):
			if i+2 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			o, err1 := strconv.Atoi(string(argv[i+1]))
			n, err2 := strconv.Atoi(string(argv[i+2]))
			if err1 != nil || err2 != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			hasLimit = true
			offset = o
			count = n
			i += 2
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	if byScore && byLex {
		c.writeErr("ERR syntax error, BYSCORE and BYLEX options at the same time are not compatible")
		return
	}
	if hasLimit && !byScore && !byLex {
		c.writeErr("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
		return
	}
	if withScores && byLex {
		c.writeErr("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
		return
	}

	if byScore {
		lo, hi := argv[2], argv[3]
		if rev {
			lo, hi = argv[3], argv[2]
		}
		c.zByScore(argv[1], lo, hi, rev, withScores, hasLimit, offset, count)
		return
	}
	if byLex {
		lo, hi := argv[2], argv[3]
		if rev {
			lo, hi = argv[3], argv[2]
		}
		c.zByLex(argv[1], lo, hi, rev, hasLimit, offset, count)
		return
	}
	c.zrangeByIndex(argv[1], argv[2], argv[3], rev, withScores)
}

func (c *connState) cmdZRevRange(argv [][]byte) {
	// ZREVRANGE key start stop [WITHSCORES]
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'zrevrange' command")
		return
	}
	withScores := false
	for _, opt := range argv[4:] {
		if eqFold(opt, "WITHSCORES") {
			withScores = true
			continue
		}
		c.writeErr("ERR syntax error")
		return
	}
	c.zrangeByIndex(argv[1], argv[2], argv[3], true, withScores)
}

// zrangeByIndex answers the rank-indexed ZRANGE/ZREVRANGE window. It normalizes the
// start/stop indices against the cardinality (negative counts from the end, out-of-range
// clamps or empties), maps the requested indices to a forward score-order window, and
// selects that window off the score-family order index: one O(log n) seek to the window's
// first key (CollSelectAt) then a bounded forward scan for the rest (CollScan). A forward
// range emits the window in ascending score order; a reverse range emits it descending.
// The cost tracks the window, not the cardinality, so a 100-element window over a
// billion-member board reads a bounded handful of rows.
func (c *connState) zrangeByIndex(zkey, startArg, stopArg []byte, rev, withScores bool) {
	keys, plen, errMsg := c.zIndexWindow(zkey, startArg, stopArg, rev)
	if errMsg != "" {
		c.writeErr(errMsg)
		return
	}
	mult := 1
	if withScores {
		mult = 2
	}
	c.writeArrayHeader(len(keys) * mult)
	if rev {
		for i := len(keys) - 1; i >= 0; i-- {
			c.emitZrangeMember(keys[i], plen, withScores)
		}
		return
	}
	for _, k := range keys {
		c.emitZrangeMember(k, plen, withScores)
	}
}

// zIndexWindow computes the rank-indexed window ZRANGE/ZREVRANGE and the default form of ZRANGESTORE
// share. It normalizes the start/stop indices against the cardinality (negative counts from the end,
// out-of-range clamps or empties), maps the requested indices to a forward score-order window, and
// selects that window off the score-family order index: one O(log n) seek to the first key
// (CollSelectAt) then a bounded forward scan for the rest (CollScan). The returned keys are
// score-family rows in ascending score order; the caller emits them forward or reversed, or stores
// them. errMsg is non-empty on a wrong-type key or a non-integer index, and empty with no keys for a
// valid but empty range. The cost tracks the window, not the cardinality.
func (c *connState) zIndexWindow(zkey, startArg, stopArg []byte, rev bool) (keys [][]byte, plen int, errMsg string) {
	if c.stringConflict(zkey) {
		return nil, 0, wrongType
	}
	start, err1 := strconv.ParseInt(string(startArg), 10, 64)
	stop, err2 := strconv.ParseInt(string(stopArg), 10, 64)
	if err1 != nil || err2 != nil {
		return nil, 0, "ERR value is not an integer or out of range"
	}
	card := int64(c.zsetCard(zkey))
	if card == 0 {
		return nil, 0, ""
	}
	if start < 0 {
		start += card
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += card
	}
	if start > stop || start >= card {
		return nil, 0, ""
	}
	if stop >= card {
		stop = card - 1
	}

	// Map the requested indices to a forward (ascending score) window. A reverse request
	// for indices [start, stop] is the forward window [card-1-stop, card-1-start] read
	// back to front.
	fStart, fStop := start, stop
	if rev {
		fStart = card - 1 - stop
		fStop = card - 1 - start
	}
	n := fStop - fStart + 1

	prefix := c.zscorePrefix(zkey)
	plen = len(prefix)
	first, ok := c.srv.store.CollSelectAt(prefix, int(fStart))
	if !ok {
		return nil, plen, ""
	}
	keys = append(c.zkeys[:0], first)
	if n > 1 {
		keys, _ = c.srv.store.CollScan(prefix, first, int(n-1), keys)
	}
	c.zkeys = keys
	return keys, plen, ""
}

// emitZrangeMember writes one score-family row as a ZRANGE reply element: the member
// bytes (the key tail past the prefix and the 8 sortable-score bytes), and when
// withScores is set the score decoded straight from those sortable bytes, so no second
// read of the member family is needed.
func (c *connState) emitZrangeMember(k []byte, plen int, withScores bool) {
	c.writeBulk(k[plen+8:])
	if withScores {
		c.writeScore(decodeSortableScore(k[plen : plen+8]))
	}
}
