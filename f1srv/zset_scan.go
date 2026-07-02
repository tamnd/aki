package f1srv

import (
	"encoding/binary"
	"encoding/hex"
	"math"
)

// cmdZScan is the LTM-safe incremental sorted-set enumeration (spec 2064/f1_rewrite_ltm/07
// section 8): each call scans a bounded window of member rows and returns an opaque cursor to
// resume, so a client walks a billion-member zset without the server ever materializing it. The
// walk is over the member-family index in member-byte order, which is the same order SSCAN uses
// on a set, so resuming is a successor seek rather than a rescan. Each member carries a score, so
// ZSCAN returns member+score pairs, and unlike HSCAN it has no NOVALUES/NOSCORES option: Redis
// rejects any such token as a syntax error, so the only options are MATCH and COUNT.
//
// Redis validates the command key-first: zscanCommand parses the cursor, then looks the key up,
// and a missing key replies with the empty-scan sentinel before any option is parsed, so a bad
// option on a missing key still returns "0, []" rather than an error. We mirror that order: the
// type probe (which also reaps an expired key) runs before the option loop.
//
// Cursor encoding mirrors HSCAN and SSCAN: "0" starts a fresh iteration and "0" is returned when
// it completes, and any live position is the hex of the last composite key returned. A composite
// key always carries the uvarint length prefix, so it is never empty and its hex is never the
// single byte "0", which keeps a live cursor from ever colliding with the done sentinel.
//
// Cursor stability: a member present for the whole scan and never removed is returned exactly
// once (the ordered member index walks each key once and the cursor resumes strictly after the
// last one), and a member added or removed mid-scan may or may not appear. The score is
// re-resolved through the authoritative member row for each surviving key, so a mid-scan ZADD
// that rescored a member never yields a stale score. The scan is lock-free like the other zset
// reads.
func (c *connState) cmdZScan(argv [][]byte) {
	// ZSCAN key cursor [MATCH pattern] [COUNT count]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'zscan' command")
		return
	}
	zkey := argv[1]

	var after []byte
	if len(argv[2]) != 1 || argv[2][0] != '0' {
		dec, err := hex.DecodeString(string(argv[2]))
		if err != nil {
			c.writeErr("ERR invalid cursor")
			return
		}
		after = dec
	}

	// Key-first, matching Redis: a missing key emits the empty-scan sentinel before options are
	// parsed, and a non-zset key is a WRONGTYPE error. keyTypeOf reaps an expired key first.
	switch c.keyTypeOf(zkey) {
	case keyMissing:
		c.writeArrayHeader(2)
		c.writeBulk([]byte{'0'})
		c.writeArrayHeader(0)
		return
	case keyZset:
	default:
		c.writeErr(wrongType)
		return
	}

	count := 10
	var pattern []byte
	for i := 3; i < len(argv); i++ {
		switch {
		case eqFold(argv[i], "MATCH"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			pattern = argv[i+1]
			i++
		case eqFold(argv[i], "COUNT"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, err := atoi64(argv[i+1])
			if err != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			if n < 1 {
				c.writeErr("ERR syntax error")
				return
			}
			count = int(n)
			i++
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	prefix := c.zmemberPrefix(zkey)
	// Cap the initial slice header allocation so a client's large COUNT hint cannot make the
	// server preallocate a giant slice; append grows it as the batch actually fills.
	initCap := count
	if initCap > hashScanBatch {
		initCap = hashScanBatch
	}
	scan := make([][]byte, 0, initCap)
	keys, last := c.srv.store.CollScan(prefix, after, count, scan)

	plen := len(prefix)
	matched := keys[:0]
	for _, k := range keys {
		if pattern != nil && !globMatch(pattern, k[plen:]) {
			continue
		}
		matched = append(matched, k)
	}

	// A short batch (fewer than COUNT scanned) means the prefix is exhausted, so the iteration
	// is complete and the cursor is the done sentinel; otherwise resume past the last scanned key.
	var cursor []byte
	if len(keys) < count || last == nil {
		cursor = []byte{'0'}
	} else {
		cursor = []byte(hex.EncodeToString(last))
	}

	c.writeArrayHeader(2)
	c.writeBulk(cursor)
	c.writeArrayHeader(len(matched) * 2)
	for _, k := range matched {
		c.writeBulk(k[plen:])
		v, _ := c.srv.store.GetKind(k, c.vbuf[:0], kindZsetMember)
		c.vbuf = v
		c.writeScore(math.Float64frombits(binary.LittleEndian.Uint64(v)))
	}
}
