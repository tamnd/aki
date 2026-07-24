package conformance

import (
	"fmt"
	"strconv"
)

// The cold arm's working set (spec 2064/obs1 doc 10, suite conformance:
// "then against cold state through the full read path"). The hot corpus's
// collections are a handful of elements and sit in their inline bands,
// which never demote, so the cold arm builds its own subset for the O2a
// types: strings small enough to stay embedded (separated values spill to
// the value log at write time, which is not a drain), and a hash, a set,
// and a zset past the listpack thresholds so the native bands exist and the
// demoters have something to shed. The verify steps are the deterministic reads:
// points, existence both ways, and counts; order-carrying fans (HGETALL,
// SMEMBERS) stay with the fingerprint helpers, which sort.

// Cold-arm cardinalities. The inline ceilings differ per type: the hash
// converts to its native band past 512 entries (hash-max-listpack-entries)
// and the set and zset past 128 (their listpack entry ceilings), so 600
// fields and 200 members clear all three with room; 40 strings give the
// drain a spread of keys without swamping the poll.
const (
	ColdStrings   = 40
	ColdFields    = 600
	ColdMembers   = 200
	ColdZMembers  = 200
	ColdListLen   = 200
	ColdStreamLen = 200
)

// ColdStringKey names the nth cold-arm string key, exported so the arm
// can watch these exact keys reach the fold.
func ColdStringKey(i int) string { return "cold:str:" + strconv.Itoa(i) }

// ColdHashKey, ColdSetKey, ColdZsetKey, and ColdListKey name the cold-arm
// collections.
const (
	ColdHashKey   = "cold:hash"
	ColdSetKey    = "cold:set"
	ColdZsetKey   = "cold:zset"
	ColdListKey   = "cold:list"
	ColdStreamKey = "cold:stream"
)

func coldField(i int) string   { return fmt.Sprintf("cf%03d", i) }
func coldValue(i int) string   { return fmt.Sprintf("cw-%03d", i) }
func coldMember(i int) string  { return fmt.Sprintf("cm%03d", i) }
func coldZMember(i int) string { return fmt.Sprintf("zm%03d", i) }

// coldElem pads each list element to roughly 100 bytes so ColdListLen of
// them span several 4 KiB list chunks and the demote pass has a real
// interior to shed; a 200-element list of short strings would fit one
// chunk and never leave the ends-stay-hot margins.
func coldElem(i int) string {
	return fmt.Sprintf("le%03d-%094d", i, i)
}

// coldStreamID is the nth entry's explicit ID, ms climbing from 1 so
// every expectation formats plain and the entries land in ID order.
func coldStreamID(i int) string {
	return strconv.Itoa(i+1) + "-1"
}

// coldSVal pads each stream entry's value to roughly 100 bytes so
// ColdStreamLen entries seal several 4 KiB blocks and the demote pass
// has sealed front blocks past the resident tail margin to shed; short
// values would leave the whole log inside two blocks and nothing cold.
func coldSVal(i int) string {
	return fmt.Sprintf("sv%03d-%092d", i, i)
}

// ColdBuild returns the write steps that stand the cold working set up.
func ColdBuild() []Step {
	var steps []Step
	for i := 0; i < ColdStrings; i++ {
		steps = append(steps, c("OK", "SET", ColdStringKey(i), "cv-"+strconv.Itoa(i)))
	}
	// Batched well under the server's command-size cap.
	const batch = 25
	for base := 0; base < ColdFields; base += batch {
		hset := []string{"HSET", ColdHashKey}
		for i := base; i < base+batch; i++ {
			hset = append(hset, coldField(i), coldValue(i))
		}
		steps = append(steps, c(strconv.Itoa(batch), hset...))
	}
	for base := 0; base < ColdMembers; base += batch {
		sadd := []string{"SADD", ColdSetKey}
		for i := base; i < base+batch; i++ {
			sadd = append(sadd, coldMember(i))
		}
		steps = append(steps, c(strconv.Itoa(batch), sadd...))
	}
	// Integer scores equal to the member index, so the score order is the
	// member-name order and every ZSCORE expectation formats plain.
	for base := 0; base < ColdZMembers; base += batch {
		zadd := []string{"ZADD", ColdZsetKey}
		for i := base; i < base+batch; i++ {
			zadd = append(zadd, strconv.Itoa(i), coldZMember(i))
		}
		steps = append(steps, c(strconv.Itoa(batch), zadd...))
	}
	// RPUSH replies with the running length, so each batch expects its end.
	for base := 0; base < ColdListLen; base += batch {
		rpush := []string{"RPUSH", ColdListKey}
		for i := base; i < base+batch; i++ {
			rpush = append(rpush, coldElem(i))
		}
		steps = append(steps, c(strconv.Itoa(base+batch), rpush...))
	}
	// XADD replies with the entry ID, one entry per step.
	for i := 0; i < ColdStreamLen; i++ {
		steps = append(steps, c(coldStreamID(i), "XADD", ColdStreamKey, coldStreamID(i), "f", coldSVal(i)))
	}
	return steps
}

// ColdVerify returns the read steps whose expectations hold against the
// same state hot or cold: every string point, every hash field, every set
// member, the misses, and the counts.
func ColdVerify() []Step {
	var steps []Step
	for i := 0; i < ColdStrings; i++ {
		steps = append(steps, c("cv-"+strconv.Itoa(i), "GET", ColdStringKey(i)))
	}
	steps = append(steps,
		c(strconv.Itoa(len("cv-0")), "STRLEN", ColdStringKey(0)),
		c("(nil)", "GET", "cold:str:missing"),
		c(strconv.Itoa(ColdFields), "HLEN", ColdHashKey),
		c(strconv.Itoa(ColdMembers), "SCARD", ColdSetKey),
	)
	for i := 0; i < ColdFields; i++ {
		steps = append(steps, c(coldValue(i), "HGET", ColdHashKey, coldField(i)))
	}
	steps = append(steps,
		c("["+coldValue(0)+" "+coldValue(ColdFields-1)+" (nil)]",
			"HMGET", ColdHashKey, coldField(0), coldField(ColdFields-1), "cf-missing"),
		c("1", "HEXISTS", ColdHashKey, coldField(7)),
		c("0", "HEXISTS", ColdHashKey, "cf-missing"),
		c("(nil)", "HGET", ColdHashKey, "cf-missing"),
	)
	for i := 0; i < ColdMembers; i++ {
		steps = append(steps, c("1", "SISMEMBER", ColdSetKey, coldMember(i)))
	}
	steps = append(steps, c("0", "SISMEMBER", ColdSetKey, "cm-missing"))
	// The zset reads: every score point, rank spots at both ends and the
	// middle, one range window, and the misses.
	steps = append(steps, c(strconv.Itoa(ColdZMembers), "ZCARD", ColdZsetKey))
	for i := 0; i < ColdZMembers; i++ {
		steps = append(steps, c(strconv.Itoa(i), "ZSCORE", ColdZsetKey, coldZMember(i)))
	}
	steps = append(steps,
		c("0", "ZRANK", ColdZsetKey, coldZMember(0)),
		c(strconv.Itoa(ColdZMembers/2), "ZRANK", ColdZsetKey, coldZMember(ColdZMembers/2)),
		c(strconv.Itoa(ColdZMembers-1), "ZRANK", ColdZsetKey, coldZMember(ColdZMembers-1)),
		c("["+coldZMember(10)+" "+coldZMember(11)+" "+coldZMember(12)+"]",
			"ZRANGE", ColdZsetKey, "10", "12"),
		c("(nil)", "ZSCORE", ColdZsetKey, "zm-missing"),
		c("(nil)", "ZRANK", ColdZsetKey, "zm-missing"),
	)
	// The list reads: the length, index points at both ends, the interior
	// (where the demoted chunks live), negative indexes, one range window,
	// and the misses.
	steps = append(steps, c(strconv.Itoa(ColdListLen), "LLEN", ColdListKey))
	for i := 0; i < ColdListLen; i += 10 {
		steps = append(steps, c(coldElem(i), "LINDEX", ColdListKey, strconv.Itoa(i)))
	}
	steps = append(steps,
		c(coldElem(ColdListLen-1), "LINDEX", ColdListKey, "-1"),
		c(coldElem(ColdListLen-10), "LINDEX", ColdListKey, strconv.Itoa(-10)),
		c("["+coldElem(90)+" "+coldElem(91)+" "+coldElem(92)+"]",
			"LRANGE", ColdListKey, "90", "92"),
		c("(nil)", "LINDEX", ColdListKey, strconv.Itoa(ColdListLen)),
		c("0", "LLEN", "cold:list:missing"),
	)
	// The stream reads: the length, ID point reads through the interior
	// (where the demoted blocks live), a range window, both open bounds,
	// and the misses.
	steps = append(steps, c(strconv.Itoa(ColdStreamLen), "XLEN", ColdStreamKey))
	entry := func(i int) string {
		return "[" + coldStreamID(i) + " [f " + coldSVal(i) + "]]"
	}
	for i := 0; i < ColdStreamLen; i += 10 {
		steps = append(steps, c("["+entry(i)+"]", "XRANGE", ColdStreamKey, coldStreamID(i), coldStreamID(i)))
	}
	steps = append(steps,
		c("["+entry(90)+" "+entry(91)+" "+entry(92)+"]",
			"XRANGE", ColdStreamKey, coldStreamID(90), coldStreamID(92)),
		c("["+entry(0)+"]", "XRANGE", ColdStreamKey, "-", "+", "COUNT", "1"),
		c("["+entry(ColdStreamLen-1)+"]", "XREVRANGE", ColdStreamKey, "+", "-", "COUNT", "1"),
		c("[]", "XRANGE", ColdStreamKey, "9999-0", "9999-9"),
		c("0", "XLEN", "cold:stream:missing"),
	)
	return steps
}
