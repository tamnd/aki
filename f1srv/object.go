package f1srv

import "math"

// OBJECT ENCODING and TYPE are the "what is this key" surface (spec 2064/f1_rewrite_ltm/06
// section 12). f1raw stores every collection element-per-row regardless of size, so it has
// no intset/listpack/hashtable representation of its own, but clients and compatibility
// suites still ask OBJECT ENCODING and expect the encoding Redis would pick for the same
// contents. The set answers that in O(1) from a one-way state machine folded into the header
// row as SADD/SMOVE/the STORE forms grow it, never a scan: exactly Redis's own upgrade path,
// which only ever moves intset -> listpack -> hashtable and never downgrades on removal.

// Set encoding tags carried in the set header row (set.go stores them in the 9th header byte).
// encNone is the zero value, meaning no encoding has been recorded yet (a fresh set before its
// first member); every set creator folds it forward to a concrete tag on the first insert.
const (
	encNone      byte = 0
	encIntset    byte = 1
	encListpack  byte = 2
	encHashtable byte = 3
)

// Redis ship defaults for the set encoding thresholds. CONFIG is a no-op on f1srv (it replies
// OK without storing), so the thresholds are the defaults every stock Redis and Valkey runs,
// which is what a client comparing OBJECT ENCODING against them expects to see.
const (
	setMaxIntsetEntries   = 512
	setMaxListpackEntries = 128
	setMaxListpackValue   = 64
)

// foldSetEnc applies Redis's one-way set encoding upgrade for a single member being added,
// given the encoding so far and the set's cardinality after the add. It mirrors setTypeAdd in
// t_set.c: a set starts intset if its first member is an integer (else listpack, or hashtable
// if that first member is already too long), an intset upgrades to listpack or hashtable when a
// non-integer arrives or it outgrows the intset entry limit, and a listpack upgrades to
// hashtable when it outgrows the listpack entry or value limit. hashtable is terminal, so a
// removal never walks any of this back.
func foldSetEnc(cur byte, member []byte, newCount uint64) byte {
	if cur == encHashtable {
		return encHashtable
	}
	_, isInt := parseInt64Strict(member)
	mlen := len(member)

	if cur == encNone {
		// First member sets the starting encoding.
		switch {
		case isInt && setMaxIntsetEntries > 0:
			cur = encIntset
		case mlen <= setMaxListpackValue:
			cur = encListpack
		default:
			return encHashtable
		}
	}

	if cur == encIntset {
		switch {
		case !isInt:
			// A non-integer breaks the intset: fall to listpack if it still fits, else hashtable.
			if newCount <= setMaxListpackEntries && mlen <= setMaxListpackValue {
				cur = encListpack
			} else {
				return encHashtable
			}
		case newCount > setMaxIntsetEntries:
			// Too many integers for an intset: listpack if it fits the entry limit, else hashtable.
			if newCount <= setMaxListpackEntries {
				cur = encListpack
			} else {
				return encHashtable
			}
		}
	}

	if cur == encListpack {
		if newCount > setMaxListpackEntries || mlen > setMaxListpackValue {
			return encHashtable
		}
	}
	return cur
}

// setEncodingName maps a stored set encoding tag to the wire name Redis reports. A present but
// unrecorded encoding (encNone on a non-empty set) cannot arise after this change since every
// set creator folds the tag forward, but it maps to the small-set default defensively rather
// than to an empty reply.
func setEncodingName(enc byte) string {
	switch enc {
	case encIntset:
		return "intset"
	case encHashtable:
		return "hashtable"
	default:
		return "listpack"
	}
}

// parseInt64Strict reports whether b is the canonical decimal form of an int64, matching
// Redis's string2ll: it rejects an empty string, a lone sign, a leading '+', leading zeros
// (so "007" is not an integer), and anything that overflows int64. "0" and "-0" both parse to
// 0. This is the exact test Redis uses both to decide a string's "int" encoding and to decide
// whether a set member keeps a set in the intset encoding, so f1srv's OBJECT ENCODING lines up
// with Redis on both.
func parseInt64Strict(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	p := b
	neg := false
	if p[0] == '-' {
		neg = true
		p = p[1:]
		if len(p) == 0 {
			return 0, false
		}
	}
	if p[0] == '0' {
		if len(p) == 1 {
			return 0, true // "0" or "-0", both value 0
		}
		return 0, false // leading zero
	}
	var v uint64
	for _, ch := range p {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		d := uint64(ch - '0')
		if v > (math.MaxUint64-d)/10 {
			return 0, false
		}
		v = v*10 + d
	}
	if neg {
		if v > uint64(math.MaxInt64)+1 {
			return 0, false
		}
		return -int64(v), true
	}
	if v > uint64(math.MaxInt64) {
		return 0, false
	}
	return int64(v), true
}

// keyKind is the resolved type of a key, used by both TYPE and OBJECT ENCODING so the two
// commands agree on what a key holds.
type keyKind int

const (
	keyMissing keyKind = iota
	keyString
	keyHash
	keySet
	keyZset
)

// keyTypeOf resolves a key to exactly one type. A key can only ever be one type at a time
// because every write guards the other namespaces with WRONGTYPE, so the probe order only
// decides which lookup runs first, not the answer. The string namespace is checked first
// because it is the cheapest and the most common.
func (c *connState) keyTypeOf(key []byte) keyKind {
	if _, ok := c.srv.store.Get(key, c.vbuf[:0]); ok {
		return keyString
	}
	if c.srv.store.ExistsKind(key, kindHashMeta) {
		return keyHash
	}
	if c.srv.store.ExistsKind(key, kindSetMeta) {
		return keySet
	}
	if c.srv.store.ExistsKind(key, kindZsetMeta) {
		return keyZset
	}
	return keyMissing
}

// cmdType implements TYPE: a simple-string reply of the key's type name, or "none" when the
// key does not exist. It never errors on type, unlike the data commands.
func (c *connState) cmdType(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'type' command")
		return
	}
	switch c.keyTypeOf(argv[1]) {
	case keyString:
		c.writeSimple("string")
	case keyHash:
		c.writeSimple("hash")
	case keySet:
		c.writeSimple("set")
	case keyZset:
		c.writeSimple("zset")
	default:
		c.writeSimple("none")
	}
}

// stringEncodingName reports the encoding Redis assigns a string value: "int" when the whole
// value is a canonical int64 (and short enough to hold as a long), "embstr" for a small value,
// "raw" otherwise. The 44-byte embstr cutoff is OBJ_ENCODING_EMBSTR_SIZE_LIMIT.
func stringEncodingName(v []byte) string {
	if len(v) <= 20 {
		if _, ok := parseInt64Strict(v); ok {
			return "int"
		}
	}
	if len(v) <= 44 {
		return "embstr"
	}
	return "raw"
}

// cmdObject implements the OBJECT subcommands clients and compatibility suites reach for.
// ENCODING is the one that matters here and is answered exactly for strings and sets; REFCOUNT
// and IDLETIME return stable stand-ins (f1raw does not share objects or track per-key LRU yet),
// FREQ refuses under the non-LFU default policy exactly as Redis does, and HELP lists the forms.
// Every key-taking form replies with a nil bulk on a missing key, not an error: Redis 8.8 and
// Valkey 9.1 both look the key up first and return the null reply when it is absent.
func (c *connState) cmdObject(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'object' command")
		return
	}
	sub := argv[1]
	if eqFold(sub, "HELP") {
		help := []string{
			"OBJECT <subcommand> [<arg> ...]. Subcommands are:",
			"ENCODING <key>",
			"    Return the kind of internal representation used to store the value.",
			"REFCOUNT <key>",
			"    Return the number of references of the value.",
			"IDLETIME <key>",
			"    Return the idle time of the key, that is the approximated number of",
			"    seconds elapsed since the last access to the key.",
			"FREQ <key>",
			"    Return the access frequency index of the key.",
			"HELP",
			"    Print this help.",
		}
		c.writeArrayHeader(len(help))
		for _, line := range help {
			c.writeBulk([]byte(line))
		}
		return
	}
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'object' command")
		return
	}
	key := argv[2]

	switch {
	case eqFold(sub, "ENCODING"):
		switch c.keyTypeOf(key) {
		case keyString:
			v, _ := c.srv.store.Get(key, c.vbuf[:0])
			c.vbuf = v
			c.writeBulk([]byte(stringEncodingName(v)))
		case keyHash:
			c.writeBulk([]byte(c.hashEncodingName(key)))
		case keySet:
			_, enc, _ := c.setHeader(key)
			c.writeBulk([]byte(setEncodingName(enc)))
		case keyZset:
			_, enc, _ := c.zsetHeader(key)
			c.writeBulk([]byte(zsetEncodingName(enc)))
		default:
			c.writeNil()
		}
	case eqFold(sub, "REFCOUNT"):
		if c.keyTypeOf(key) == keyMissing {
			c.writeNil()
			return
		}
		c.writeInt(1)
	case eqFold(sub, "IDLETIME"):
		if c.keyTypeOf(key) == keyMissing {
			c.writeNil()
			return
		}
		c.writeInt(0)
	case eqFold(sub, "FREQ"):
		if c.keyTypeOf(key) == keyMissing {
			c.writeNil()
			return
		}
		// FREQ is only meaningful under an LFU maxmemory policy. f1srv runs the stock default
		// (non-LFU) policy, so, like Redis and Valkey under that policy, it refuses FREQ on a
		// key that exists rather than inventing a frequency.
		c.writeErr("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.")
	default:
		c.writeErr("ERR Unknown OBJECT subcommand or wrong number of arguments. Try OBJECT HELP.")
	}
}

// hashEncodingName reports a hash's encoding. f1raw does not yet maintain a one-way encoding
// state machine for hashes (that is the M4 follow-up), so this is a best-effort answer from the
// field count alone: "listpack" for a small hash, "hashtable" once it outgrows the listpack
// entry limit. It ignores the per-field value-length threshold, so a small hash holding one
// long value reads as listpack here where Redis would say hashtable.
func (c *connState) hashEncodingName(hkey []byte) string {
	if c.hashCount(hkey) > setMaxListpackEntries {
		return "hashtable"
	}
	return "listpack"
}
