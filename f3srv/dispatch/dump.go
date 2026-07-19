// DUMP and RESTORE serialize one key to an opaque blob and rebuild it. Both are
// single-key and single-shard: DUMP reads the owning shard and answers the sealed
// payload, RESTORE writes it back onto the same owner, so neither leaves the point
// path or needs a cross-shard hop. The payload is f3's own format, read only by
// f3's RESTORE (it is not wire-compatible with a redis RDB blob and makes no claim
// to be): a leading tag byte names the type, the body is the value bytes for a
// string or the M8 snapshot frame for a collection, and a version word plus a
// CRC64 footer seals it the way redis seals its own. Reusing the M8 buildXSnapshot
// and applySnapshot encoders is what lets a collection round-trip at full fidelity
// (a set's encoding band, a zset's scores, a hash's field TTLs, a stream's counters
// and its consumer-group and PEL ledger) through the same bytes a checkpoint writes,
// with no DUMP-specific codec to drift from the durable one.
package dispatch

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"strconv"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/stream"
	"github.com/tamnd/aki/engine/f3/zset"
)

// dumpTagString marks a DUMP payload whose body is a raw string value. It sits at
// 0x00, the one byte the collection kinds (set 0x01 through stream 0x05) leave free,
// so the leading byte alone disambiguates a string payload from a collection
// snapshot frame without a second tag.
const dumpTagString = 0x00

// dumpVersion is the format word f3 stamps into a DUMP footer. RESTORE refuses a
// payload stamped with a newer version, the same forward-compatibility gate redis
// keeps, so a future format change can bump this and reject blobs it cannot read.
const dumpVersion uint16 = 1

// dumpTable is the CRC64 table sealing a DUMP payload. f3's blob is opaque and only
// f3 reads it, so the polynomial only has to be stable across one f3's DUMP and its
// own RESTORE; the ISO table is the stdlib's, no vendored constant to carry.
var dumpTable = crc64.MakeTable(crc64.ISO)

// errBadPayload is the RESTORE routing failure for a CRC-clean payload whose tag
// byte names no type or whose collection frame will not parse. The CRC has already
// cleared a torn blob, so this guards only a well-formed-but-nonsense payload.
var errBadPayload = errors.New("bad restore payload")

// dumpCmd answers DUMP key: the sealed serialization of the value at key, or the
// null bulk when key holds nothing (absent everywhere or lazily expired). The
// payload is opaque to the client, an argument it hands back verbatim to RESTORE.
func dumpCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	payload, ok := dumpPayload(cx, args[0])
	if !ok {
		r.Null()
		return
	}
	r.Bulk(dumpSeal(payload))
}

// dumpPayload renders the value at key to its tagged body, probing the string store
// first and then each collection keyspace in the same chain order the other
// cross-type verbs walk. ok is false when no keyspace holds a live key. A string
// body is the tag byte then the value bytes; a collection body is the M8 snapshot
// frame, whose own leading kind byte is the tag.
func dumpPayload(cx *shard.Ctx, key []byte) ([]byte, bool) {
	if v, ok := cx.St.GetString(key, cx.NowMs, cx.Val); ok {
		cx.Val = v
		out := make([]byte, 0, len(v)+1)
		out = append(out, dumpTagString)
		out = append(out, v...)
		return out, true
	}
	if row, ok := set.DumpKey(cx, key); ok {
		return akifile.AppendCollSnap(nil, row), true
	}
	if row, ok := zset.DumpKey(cx, key); ok {
		return akifile.AppendCollSnap(nil, row), true
	}
	if row, ok := hash.DumpKey(cx, key); ok {
		return akifile.AppendCollSnap(nil, row), true
	}
	if row, ok := list.DumpKey(cx, key); ok {
		return akifile.AppendCollSnap(nil, row), true
	}
	if row, ok := stream.DumpKey(cx, key); ok {
		return akifile.AppendCollSnap(nil, row), true
	}
	return nil, false
}

// dumpSeal appends the version word and the CRC64 footer, the redis-shaped envelope
// a RESTORE argument carries: body, then two little-endian version bytes, then the
// eight-byte CRC over everything before it. RESTORE peels the same layout in reverse.
func dumpSeal(payload []byte) []byte {
	out := make([]byte, 0, len(payload)+10)
	out = append(out, payload...)
	out = binary.LittleEndian.AppendUint16(out, dumpVersion)
	sum := crc64.Checksum(out, dumpTable)
	return binary.LittleEndian.AppendUint64(out, sum)
}

// dumpUnseal verifies a RESTORE payload and returns its tagged body. It refuses a
// blob too short for the ten-byte footer, one whose CRC does not cover its body, or
// one stamped with a version this build cannot read, the three checks redis folds
// into its one "version or checksum are wrong" answer.
func dumpUnseal(b []byte) ([]byte, bool) {
	if len(b) < 10 {
		return nil, false
	}
	body := b[:len(b)-8]
	if crc64.Checksum(body, dumpTable) != binary.LittleEndian.Uint64(b[len(b)-8:]) {
		return nil, false
	}
	if binary.LittleEndian.Uint16(body[len(body)-2:]) > dumpVersion {
		return nil, false
	}
	return body[:len(body)-2], true
}

// restoreCmd answers RESTORE key ttl serialized-value [REPLACE] [ABSTTL]
// [IDLETIME seconds] [FREQ frequency]. It parses and verifies the payload, guards
// an existing key with BUSYKEY unless REPLACE clears it first, resolves the key
// deadline from the ttl argument (0 for a persistent key, relative ms by default,
// absolute unix ms under ABSTTL), and installs the value. IDLETIME and FREQ are
// accepted and their integers validated, but f3 keeps no per-key LRU or LFU clock a
// RESTORE could seed, so they set nothing; the honest effect is the value landing
// with its TTL, which is what a client checks.
func restoreCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || ttl < 0 {
		r.Err("ERR Invalid TTL value, must be >= 0")
		return
	}
	replace, absttl, ok := parseRestoreOpts(args[3:])
	if !ok {
		r.Err("ERR syntax error")
		return
	}
	payload, ok := dumpUnseal(args[2])
	if !ok {
		r.Err("ERR DUMP payload version or checksum are wrong")
		return
	}
	if keyExistsAnywhere(cx, key) {
		if !replace {
			r.Err("BUSYKEY Target key name already exists.")
			return
		}
		restoreClear(cx, key)
	}
	var expireAt int64
	if ttl != 0 {
		if absttl {
			expireAt = ttl
		} else {
			expireAt = cx.NowMs + ttl
		}
	}
	if err := restorePayload(cx, key, payload, expireAt); err != nil {
		r.Err("ERR Bad data format")
		return
	}
	r.Status("OK")
}

// parseRestoreOpts walks the RESTORE option tail. REPLACE and ABSTTL are flags;
// IDLETIME and FREQ each take an integer this build validates and then ignores, so
// a malformed value is a syntax error before any key is touched. ok is false on any
// unknown token or a flag missing its value.
func parseRestoreOpts(opts [][]byte) (replace, absttl, ok bool) {
	for i := 0; i < len(opts); {
		switch {
		case tokenIs(opts[i], "REPLACE"):
			replace = true
			i++
		case tokenIs(opts[i], "ABSTTL"):
			absttl = true
			i++
		case tokenIs(opts[i], "IDLETIME"), tokenIs(opts[i], "FREQ"):
			if i+1 >= len(opts) {
				return false, false, false
			}
			if _, err := strconv.ParseInt(string(opts[i+1]), 10, 64); err != nil {
				return false, false, false
			}
			i += 2
		default:
			return false, false, false
		}
	}
	return replace, absttl, true
}

// restorePayload routes a verified payload to the right rebuild by its tag byte: the
// string tag installs the value bytes through the store, and a collection kind
// installs through that type's RestoreKey, which reuses the M8 applySnapshot rebuild
// and stamps the RESTORE deadline. expireAt is 0 for a persistent key.
func restorePayload(cx *shard.Ctx, key, payload []byte, expireAt int64) error {
	if len(payload) < 1 {
		return errBadPayload
	}
	if payload[0] == dumpTagString {
		return cx.St.SetString(key, payload[1:], cx.NowMs, expireAt, false)
	}
	row, err := akifile.ParseCollSnap(payload)
	if err != nil {
		return err
	}
	switch row.Kind {
	case akifile.CollKindSet:
		return set.RestoreKey(cx, key, row, expireAt)
	case akifile.CollKindZset:
		return zset.RestoreKey(cx, key, row, expireAt)
	case akifile.CollKindHash:
		return hash.RestoreKey(cx, key, row, expireAt)
	case akifile.CollKindList:
		return list.RestoreKey(cx, key, row, expireAt)
	case akifile.CollKindStream:
		return stream.RestoreKey(cx, key, row, expireAt)
	}
	return errBadPayload
}

// restoreClear removes any prior key across every keyspace before a REPLACE
// install, so the new value lands on a clean slot. A key lives in exactly one
// keyspace, so at most one arm removes it; the same span delCmd deletes over.
func restoreClear(cx *shard.Ctx, key []byte) {
	cx.St.Del(key, cx.NowMs)
	set.Delete(cx, key)
	zset.Delete(cx, key)
	hash.Delete(cx, key)
	list.Delete(cx, key)
	stream.Delete(cx, key)
}
