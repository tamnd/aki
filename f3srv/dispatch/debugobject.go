// DEBUG OBJECT key returns a one-line internal description of a key's value, the
// introspection line redis-cli and a few test harnesses read to check a value's
// encoding and serialized size. f3 builds it truthfully from the pieces the earlier
// slices already grew: the OBJECT ENCODING chain now has a value-returning form per
// type (encodingOf), the DUMP serialize primitive gives an honest serialized length
// (dumpPayload is exactly the bytes a checkpoint would write for this value), and
// the per-key access clock gives lru_seconds_idle. It stops short of the encoding-
// specific tail redis appends for a quicklist (ql_nodes and friends), which would
// describe an internal layout f3 does not keep; the fields a harness parses (
// encoding, serializedlength, lru_seconds_idle) are all present and exact.
package dispatch

import (
	"strconv"

	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/engine/f3/stream"
	"github.com/tamnd/aki/engine/f3/zset"
)

// encodingOf reports the OBJECT ENCODING name for whichever type holds key, walking
// the same chain the Reply-writing OBJECT verb uses (stream, hash, list, set, zset,
// then the string store) and returning the name as a value instead of a reply. Each
// probe is non-creating and honours the key deadline, so a lazily-expired key reads
// as absent and the walk leaves no residency state behind. ok is false when no
// keyspace holds key.
func encodingOf(cx *shard.Ctx, key []byte) (string, bool) {
	if name, ok := stream.Encoding(cx, key); ok {
		return name, true
	}
	if name, ok := hash.Encoding(cx, key); ok {
		return name, true
	}
	if name, ok := list.Encoding(cx, key); ok {
		return name, true
	}
	if name, ok := set.Encoding(cx, key); ok {
		return name, true
	}
	if name, ok := zset.Encoding(cx, key); ok {
		return name, true
	}
	if v, ok := cx.St.GetString(key, cx.NowMs, cx.Val); ok {
		cx.Val = v
		return strEncoding(v), true
	}
	return "", false
}

// strEncoding is the int/embstr/raw split redis reports for a string value, the
// same rule set.stringEncoding keeps: a canonical integer is int, a short value is
// embstr, and a long one is raw. The raw-sticky bit APPEND and SETRANGE set is not
// exposed by the store yet, so a short string those touched reads embstr here where
// redis would read raw, the one known gap the OBJECT ENCODING chain also carries.
func strEncoding(v []byte) string {
	if _, ok := store.ParseInt(v); ok {
		return "int"
	}
	if len(v) <= 44 {
		return "embstr"
	}
	return "raw"
}

// debugRefcount reports the reference count for a present key: a string holding a
// canonical small integer in the shared range 0..9999 reports the shared sentinel
// redis uses for its interned integers, and every other value reports one, since f3
// shares no allocations between keys. The caller has already established the key is
// present, so a missing key never reaches here.
func debugRefcount(cx *shard.Ctx, key []byte) int64 {
	if v, ok := cx.St.GetString(key, cx.NowMs, cx.Val); ok {
		cx.Val = v
		if n, isInt := store.ParseInt(v); isInt && n >= 0 && n < 10000 {
			return sharedRefcount
		}
	}
	return 1
}

// debugObject answers DEBUG OBJECT key: assemble the internal description line from
// the encoding, the serialized length (the DUMP body the value would checkpoint to),
// the reference count, and the idle seconds, or the "no such key" error redis gives
// for a key present in no keyspace. The pointer address and lru clock redis prints
// are stable placeholders here (0x0 and 0): f3 exposes no heap address for a value
// and keeps the idle seconds, not the raw lru stamp, so the honest fields are the
// ones a harness reads.
func debugObject(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args) != 2 {
		r.Err("ERR wrong number of arguments for 'debug|object' command")
		return
	}
	key := args[1]
	enc, ok := encodingOf(cx, key)
	if !ok {
		r.Err("ERR no such key")
		return
	}
	payload, _ := dumpPayload(cx, key)
	idle := int64(0)
	if cx.St.HasString(key, cx.NowMs) {
		if s, iok := cx.St.IdleSeconds(key, cx.NowMs); iok {
			idle = s
		}
	} else if s, iok := collectionIdleSeconds(cx, key); iok {
		idle = s
	}
	line := "Value at:0x0 refcount:" + strconv.FormatInt(debugRefcount(cx, key), 10) +
		" encoding:" + enc +
		" serializedlength:" + strconv.Itoa(len(payload)) +
		" lru:0 lru_seconds_idle:" + strconv.FormatInt(idle, 10)
	r.Status(line)
}
