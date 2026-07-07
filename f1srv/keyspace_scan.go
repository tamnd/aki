package f1srv

// Keyspace enumeration (spec 2064/f1_rewrite_ltm/12, "Generic / keyspace"): KEYS, SCAN,
// RANDOMKEY, and TOUCH. The first three ride one engine primitive, Store.ScanKeys, which walks
// the fixed primary bucket array and hands back the top-level keys the server's isTopKind
// predicate selects. The engine stays type-agnostic; the server owns the policy of which record
// kinds are top-level keys and filters logically-expired keys, since the engine has no TTL
// concept.
//
// KEYS is the whole-keyspace glob match, flagged O(N) in doc 12 and kept for compatibility.
// SCAN is the resumable cursor form: the primary bucket array never rehashes and a key never
// migrates between primary buckets, so a bucket index is a stable cursor and a key present for a
// whole iteration is returned at least once, which is the SCAN guarantee. RANDOMKEY returns any
// live key by probing from a random bucket. TOUCH counts the keys that exist, the same tally as
// EXISTS, since f1raw has no per-key idle clock to bump.

import (
	"math/rand/v2"
	"strconv"
	"strings"
)

// kindString is the record kind byte for a plain string value: zero, the same value stringKind
// carries inside engine/f1raw. A string record and a collection header row are the top-level
// keys; element rows, sidecar expire rows, and stream PEL rows are not.
const kindString byte = 0x00

// isTopKind reports whether a record kind names a top-level key: a plain string, or the header
// row of a hash, set, zset, list, or stream. Every other kind is an interior row (an element, a
// score sidecar, an expire row, a stream group or PEL) that a keyspace enumeration must skip so
// each logical key is reported exactly once, through its header.
func isTopKind(kind byte) bool {
	switch kind {
	case kindString, kindHashMeta, kindSetMeta, kindZsetMeta, kindListMeta, kindStreamMeta:
		return true
	}
	return false
}

// isMigratableKind reports whether the background migrator may sink a record of this kind to the
// cold region (engine SetMigratableKindFunc). The engine already migrates strings unconditionally;
// this predicate names the collection element kinds the server has proven tier-safe on top of that
// floor. A kind is tier-safe only when every one of its read, write, and delete paths follows a
// record across the tier boundary rather than trusting a cached resident address.
//
// Only the hash field row qualifies today. Its sole secondary structure is the ordered element
// index, whose nodes re-resolve their address through the tier-aware primary index on each access
// (spec 2064/21 D22 Option B), so HGET, HGETALL, HSCAN, and HRANDFIELD all resolve a migrated field
// from the cold frame with no per-node migration hook. The hash header row stays a top-level key
// and resident, so it is never offered here.
//
// The other element kinds are deliberately excluded until each is audited the same way. The zset
// carries two element kinds (a member row and a score-family row) that a range read walks together,
// the list keeps an order-statistic window, and the set member row is read through the dense member
// vector, which caches a raw arena offset and reads the member key from it (engine randvec.go), so
// it cannot re-resolve by key and needs the heavier Option A retier hook before it can migrate.
func isMigratableKind(kind byte) bool {
	return kind == kindHashField
}

// keyKindName maps a resolved key type to the Redis type name SCAN's TYPE filter compares
// against, matching the words TYPE returns.
func keyKindName(k keyKind) string {
	switch k {
	case keyString:
		return "string"
	case keyHash:
		return "hash"
	case keySet:
		return "set"
	case keyZset:
		return "zset"
	case keyList:
		return "list"
	case keyStream:
		return "stream"
	}
	return "none"
}

// cmdKeys implements KEYS pattern: every key whose name matches the glob, in one pass over the
// whole bucket array. It is O(N) over the keyspace by nature, which doc 12 flags, so it is a
// compatibility and tooling command, not a hot path. A logically-expired key is reaped and left
// out through the keyTypeOf liveness probe, which also collapses a collection to its single
// header row so a key appears once.
func (c *connState) cmdKeys(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'keys' command")
		return
	}
	pattern := argv[1]

	// Sweep the whole array to the done sentinel. A large per-call target lets ScanKeys skip the
	// empty stretches cheaply, and the loop still terminates correctly when the keyspace holds
	// more keys than buckets (overflow chains), where a single target-bounded call would stop
	// short. The keys are arena subslices stable for the store's life, so this collects them
	// without copying and the reply length is exact.
	c.kscan = c.kscan[:0]
	cursor := uint64(0)
	for {
		var keys [][]byte
		keys, cursor = c.srv.store.ScanKeys(cursor, keyScanTarget, c.kscan, isTopKind)
		c.kscan = keys
		if cursor == 0 {
			break
		}
	}
	keys := c.kscan

	matched := keys[:0]
	for _, k := range keys {
		if c.keyTypeOf(k) == keyMissing {
			continue
		}
		if !globMatch(pattern, k) {
			continue
		}
		matched = append(matched, k)
	}

	c.writeArrayHeader(len(matched))
	for _, k := range matched {
		c.writeBulk(k)
	}
}

// keyScanTarget is the per-call key target KEYS uses to sweep the array in a few large batches
// instead of one bucket at a time. It only bounds how much each ScanKeys call collects before
// returning a cursor; KEYS loops to the done sentinel regardless, so it is a batching hint, not
// a cap on the result.
const keyScanTarget = 4096

// cmdScan implements SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]. The cursor is a
// primary bucket index rendered in decimal: the wire value "0" starts an iteration and a
// returned "0" ends it. COUNT is the usual hint, here the number of buckets to visit in the
// call; a bucket is never split across calls, so a batch can exceed COUNT when a bucket's
// overflow chain is long, exactly as Redis may over-return. MATCH filters by glob and TYPE by
// resolved key type. Logically-expired keys are reaped and skipped by the keyTypeOf probe.
func (c *connState) cmdScan(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'scan' command")
		return
	}
	cursor, ok := parseCursorU64(argv[1])
	if !ok {
		c.writeErr("ERR invalid cursor")
		return
	}

	count := 10
	var pattern []byte
	var typeFilter string
	haveType := false
	for i := 2; i < len(argv); i++ {
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
			nn, err := atoi64(argv[i+1])
			if err != nil || nn <= 0 {
				c.writeErr("ERR syntax error")
				return
			}
			count = int(nn)
			i++
		case eqFold(argv[i], "TYPE"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			typeFilter = strings.ToLower(string(argv[i+1]))
			haveType = true
			i++
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	c.kscan = c.kscan[:0]
	keys, next := c.srv.store.ScanKeys(cursor, count, c.kscan, isTopKind)
	c.kscan = keys

	matched := keys[:0]
	for _, k := range keys {
		kk := c.keyTypeOf(k)
		if kk == keyMissing {
			continue
		}
		if pattern != nil && !globMatch(pattern, k) {
			continue
		}
		if haveType && keyKindName(kk) != typeFilter {
			continue
		}
		matched = append(matched, k)
	}

	c.writeArrayHeader(2)
	c.writeBulk(strconv.AppendUint(nil, next, 10))
	c.writeArrayHeader(len(matched))
	for _, k := range matched {
		c.writeBulk(k)
	}
}

// cmdRandomKey implements RANDOMKEY: return any key, or a nil bulk on an empty keyspace. It
// picks a random primary bucket and sweeps forward with wraparound, letting ScanKeys skip the
// empty stretches in one call, and returns a random live key from the first batch that holds
// one. On a populated keyspace the first sweep lands on a key almost immediately, so it is O(1)
// expected, degrading to a full sweep only when the keyspace is nearly empty or every key it
// meets has expired, the same profile as Redis's random-slot-then-linear-probe. The covered
// counter tracks how many buckets have been swept so the wrap terminates after one full circle.
func (c *connState) cmdRandomKey(argv [][]byte) {
	if len(argv) != 1 {
		c.writeErr("ERR wrong number of arguments for 'randomkey' command")
		return
	}
	n := uint64(c.srv.store.BucketCount())
	if n == 0 {
		c.writeNil()
		return
	}
	cursor := uint64(rand.IntN(int(n)))
	for covered := uint64(0); covered < n; {
		c.kscan = c.kscan[:0]
		keys, next := c.srv.store.ScanKeys(cursor, 64, c.kscan, isTopKind)
		c.kscan = keys
		live := keys[:0]
		for _, k := range keys {
			if c.keyTypeOf(k) != keyMissing {
				live = append(live, k)
			}
		}
		if len(live) > 0 {
			c.writeBulk(live[rand.IntN(len(live))])
			return
		}
		// next is zero both when the sweep reached the array end and when it stopped on the
		// last bucket, so a zero cursor means it swept through to n; otherwise it advanced to
		// next. Wrap to the front and keep going until a full circle is covered.
		if next == 0 {
			covered += n - cursor
			cursor = 0
		} else {
			covered += next - cursor
			cursor = next
		}
	}
	c.writeNil()
}

// cmdTouch implements TOUCH key [key ...]: the count of the named keys that exist, counting a
// key once per occurrence the same way EXISTS does. Redis also bumps each touched key's idle
// clock; f1raw keeps no per-key idle time, so the observable reply is the existence tally, which
// is what clients and the compatibility suites assert on.
func (c *connState) cmdTouch(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'touch' command")
		return
	}
	var n int64
	for _, k := range argv[1:] {
		if c.keyTypeOf(k) != keyMissing {
			n++
		}
	}
	c.writeInt(n)
}

// parseCursorU64 parses a SCAN cursor: a run of decimal digits into a uint64. An empty or
// non-numeric cursor is rejected the way Redis rejects it, "invalid cursor". A value past the
// bucket count is not an error; ScanKeys treats it as a finished iteration and returns nothing.
func parseCursorU64(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var v uint64
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		v = v*10 + uint64(ch-'0')
	}
	return v, true
}
