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
// the value log at write time, which is not a drain), and a hash and a set
// past the listpack thresholds so the native bands exist and the demoters
// have something to shed. The verify steps are the deterministic reads:
// points, existence both ways, and counts; order-carrying fans (HGETALL,
// SMEMBERS) stay with the fingerprint helpers, which sort.

// Cold-arm cardinalities. The inline ceilings differ per type: the hash
// converts to its native band past 512 entries (hash-max-listpack-entries)
// and the set past 128 (set-max-listpack-entries), so 600 fields and 200
// members clear both with room; 40 strings give the drain a spread of keys
// without swamping the poll.
const (
	ColdStrings = 40
	ColdFields  = 600
	ColdMembers = 200
)

// ColdStringKey names the nth cold-arm string key, exported so the arm
// can watch these exact keys reach the fold.
func ColdStringKey(i int) string { return "cold:str:" + strconv.Itoa(i) }

// ColdHashKey and ColdSetKey name the cold-arm collections.
const (
	ColdHashKey = "cold:hash"
	ColdSetKey  = "cold:set"
)

func coldField(i int) string  { return fmt.Sprintf("cf%03d", i) }
func coldValue(i int) string  { return fmt.Sprintf("cw-%03d", i) }
func coldMember(i int) string { return fmt.Sprintf("cm%03d", i) }

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
	return steps
}
