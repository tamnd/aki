package command

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// aggScanCommands returns the per-key scan commands HSCAN, SSCAN and ZSCAN.
func aggScanCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "hscan", Group: GroupHash, Since: "2.8.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHScan},
		{Name: "sscan", Group: GroupSet, Since: "2.8.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSScan},
		{Name: "zscan", Group: GroupSortedSet, Since: "2.8.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZScan},
	}
}

// defaultScanCount is the COUNT hint when the caller gives none, matching Redis.
const defaultScanCount = 10

// aggScanOpts holds the parsed cursor, MATCH, COUNT and NOVALUES options shared by
// the per-key scans.
type aggScanOpts struct {
	cursor   []byte
	match    []byte
	count    int
	novalues bool
}

// parseAggScan reads the cursor and the MATCH, COUNT and optional NOVALUES tail.
// The cursor is accepted as either the "0" start sentinel, a legacy numeric token,
// or the hex sub-key token a coll-form page returns; anything else is an invalid
// cursor. COUNT bounds the rows a coll-form page examines (default 10); on a blob
// the whole value fits in one page so COUNT is only validated.
func parseAggScan(argv [][]byte, allowNoValues bool) (aggScanOpts, string, bool) {
	o := aggScanOpts{count: defaultScanCount}
	if !validScanCursor(argv[2]) {
		return o, "ERR invalid cursor", false
	}
	o.cursor = argv[2]
	for i := 3; i < len(argv); {
		opt := strings.ToUpper(string(argv[i]))
		switch opt {
		case "MATCH":
			if i+1 >= len(argv) {
				return o, "ERR syntax error", false
			}
			o.match = argv[i+1]
			i += 2
		case "COUNT":
			if i+1 >= len(argv) {
				return o, "ERR syntax error", false
			}
			n, ok := parseInteger(argv[i+1])
			if !ok || n < 1 {
				return o, "ERR syntax error", false
			}
			o.count = int(min(n, 1<<20))
			i += 2
		case "NOVALUES":
			if !allowNoValues {
				return o, "ERR syntax error", false
			}
			o.novalues = true
			i++
		default:
			return o, "ERR syntax error", false
		}
	}
	return o, "", true
}

// validScanCursor reports whether a cursor token is one we accept: the "0" start/
// end sentinel, a legacy decimal token, or the even-length hex sub-key a coll-form
// page emits. Anything else (the classic "SCAN key garbage" case) is rejected.
func validScanCursor(c []byte) bool {
	if _, err := strconv.ParseUint(string(c), 10, 64); err == nil {
		return true
	}
	_, err := hex.DecodeString(string(c))
	return err == nil
}

// scanCursorSeek decodes a coll-form SCAN cursor into the sub-key to seek to. The
// "0" token (and an empty token) starts the scan; any other token is the
// hex-encoded sub-key of the next unread row, as emitted by the previous page.
//
// The token is decoded as hex first, not parsed as a number. A continuation token
// is hex of a non-empty sub-key, and that hex can read as all decimal digits, for
// example a list position key or a set member like "12" whose row bytes encode to
// "3132". Parsing the token as a number first would mistake such a token for a
// numeric cursor and restart the scan, which loops forever across pages.
func scanCursorSeek(c []byte) (seek []byte, start bool) {
	if len(c) == 0 || string(c) == "0" {
		return nil, true
	}
	dec, err := hex.DecodeString(string(c))
	if err != nil || len(dec) == 0 {
		return nil, true
	}
	return dec, false
}

// matchMember reports whether a member passes the optional MATCH filter. An empty
// pattern matches everything.
func matchMember(pattern, member []byte) bool {
	return pattern == nil || stringMatch(pattern, member, false)
}

// scanRow is one kept element of a coll-form scan page: the member name (the field
// for a hash, the member for a set or sorted set) and its reply value (the hash
// value, the raw score bits for a sorted set, nil for a set).
type scanRow struct {
	member []byte
	val    []byte
}

// collScanPage walks one COUNT-bounded page of a coll-form collection's sub-tree
// starting at the cursor, restricted to rows whose key begins with prefix (nil for
// all rows, used to skip a sorted set's score-index family). decode turns a raw
// (key, value) row into the member name and reply value and reports whether the row
// survives type-specific filtering (an expired hash field is dropped). A row that
// is examined but dropped by decode or MATCH still counts against COUNT and
// advances the cursor, so COUNT bounds work done, not rows returned, matching Redis.
//
// The page allocates O(COUNT) rows plus the next token, never O(n): this is the
// whole point of the cursor path over the old materialize-everything scan, which
// cloned the entire collection onto the heap and OOM-killed the server under a
// tight memory cap.
func collScanPage(db *keyspace.DB, key, prefix, cursor []byte, count int, match []byte,
	decode func(k, v []byte) (member, val []byte, keep bool)) (rows []scanRow, next string, err error) {
	seek, start := scanCursorSeek(cursor)
	next = "0"
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		// One page seeks to the start row then walks at most count rows forward; the
		// forward arena keeps that walk's page decoding to a small constant instead of
		// allocating fresh key/value slices per cell, so SSCAN/HSCAN/ZSCAN over a
		// multi-million-element coll-form collection stays O(count), not O(n).
		c.UseArena()
		var e error
		if start {
			e = c.Seek(prefix)
		} else {
			e = c.Seek(seek)
		}
		if e != nil {
			return e
		}
		examined := 0
		for c.Valid() {
			k := c.Key()
			if len(prefix) > 0 && (len(k) < len(prefix) || !bytes.Equal(k[:len(prefix)], prefix)) {
				break // walked off the end of this row family
			}
			if examined == count {
				next = hex.EncodeToString(k) // more rows remain; resume here next page
				return nil
			}
			examined++
			member, val, keep := decode(k, c.Value())
			if keep && matchMember(match, member) {
				row := scanRow{member: append([]byte(nil), member...)}
				if val != nil {
					row.val = append([]byte(nil), val...)
				}
				rows = append(rows, row)
			}
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return rows, next, err
}

// handleHScan returns a hash's fields, with their values unless NOVALUES is set.
func handleHScan(ctx *Ctx) {
	key := ctx.Argv[1]
	o, errStr, ok := parseAggScan(ctx.Argv, true)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}

	var (
		wrongTyp bool
		isColl   bool
		cursor   string
		rows     []scanRow
		fields   []hashField // blob path only
	)
	if !ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := hashHeader(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		if found && hdr.IsColl() {
			isColl = true
			rs, next, e := collScanPage(db, key, nil, o.cursor, o.count, o.match,
				func(k, v []byte) ([]byte, []byte, bool) {
					ttl, val, de := hashRowDecode(v)
					if de != nil {
						return nil, nil, false
					}
					return k, val, !hashRowExpired(ttl)
				})
			rows, cursor = rs, next
			return e
		}
		// Blob form: bounded by the listpack threshold, one page.
		fs, _, _, e := getHash(db, key)
		fields = fs
		cursor = "0"
		return e
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr(cursor)
	if isColl {
		if o.novalues {
			enc.WriteArrayLen(len(rows))
			for _, r := range rows {
				enc.WriteBulkString(r.member)
			}
			return
		}
		enc.WriteArrayLen(len(rows) * 2)
		for _, r := range rows {
			enc.WriteBulkString(r.member)
			enc.WriteBulkString(r.val)
		}
		return
	}
	var kept []hashField
	for _, f := range fields {
		if matchMember(o.match, f.field) {
			kept = append(kept, f)
		}
	}
	if o.novalues {
		enc.WriteArrayLen(len(kept))
		for _, f := range kept {
			enc.WriteBulkString(f.field)
		}
		return
	}
	enc.WriteArrayLen(len(kept) * 2)
	for _, f := range kept {
		enc.WriteBulkString(f.field)
		enc.WriteBulkString(f.value)
	}
}

// handleSScan returns a set's members.
func handleSScan(ctx *Ctx) {
	key := ctx.Argv[1]
	o, errStr, ok := parseAggScan(ctx.Argv, false)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}

	var (
		wrongTyp bool
		isColl   bool
		cursor   string
		rows     []scanRow
		members  [][]byte // blob path only
	)
	if !ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := setHeader(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		if found && hdr.IsColl() {
			isColl = true
			rs, next, e := collScanPage(db, key, nil, o.cursor, o.count, o.match,
				func(k, _ []byte) ([]byte, []byte, bool) { return k, nil, true })
			rows, cursor = rs, next
			return e
		}
		ms, _, _, e := getSet(db, key)
		members = ms
		cursor = "0"
		return e
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr(cursor)
	if isColl {
		enc.WriteArrayLen(len(rows))
		for _, r := range rows {
			enc.WriteBulkString(r.member)
		}
		return
	}
	var kept [][]byte
	for _, m := range members {
		if matchMember(o.match, m) {
			kept = append(kept, m)
		}
	}
	enc.WriteArrayLen(len(kept))
	for _, m := range kept {
		enc.WriteBulkString(m)
	}
}

// handleZScan returns a sorted set's members with their scores as bulk strings.
func handleZScan(ctx *Ctx) {
	key := ctx.Argv[1]
	o, errStr, ok := parseAggScan(ctx.Argv, false)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}

	var (
		wrongTyp bool
		isColl   bool
		cursor   string
		rows     []scanRow
		members  []zmember // blob path only
	)
	if !ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := zsetHeader(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		if found && hdr.IsColl() {
			isColl = true
			// Walk only the member-index family ('m' + member -> scoreBits); the
			// score-index rows sort after it and are skipped by the prefix bound.
			rs, next, e := collScanPage(db, key, []byte{zRowMember}, o.cursor, o.count, o.match,
				func(k, v []byte) ([]byte, []byte, bool) { return k[1:], v, true })
			rows, cursor = rs, next
			return e
		}
		ms, _, _, e := getZSet(db, key)
		members = ms
		cursor = "0"
		return e
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr(cursor)
	if isColl {
		enc.WriteArrayLen(len(rows) * 2)
		for _, r := range rows {
			enc.WriteBulkString(r.member)
			score := zScoreUnbits(binary.BigEndian.Uint64(r.val))
			enc.WriteBulkStringStr(resp.FormatDouble(score))
		}
		return
	}
	var kept []zmember
	for _, m := range members {
		if matchMember(o.match, m.member) {
			kept = append(kept, m)
		}
	}
	enc.WriteArrayLen(len(kept) * 2)
	for _, m := range kept {
		enc.WriteBulkString(m.member)
		enc.WriteBulkStringStr(resp.FormatDouble(m.score))
	}
}
