// Package expire holds the EXPIRE-family command semantics once, shared by every
// keyspace that can carry a key-level deadline (spec 2064/f3/16 section 2, doc 17
// rows at line 195, rollout plan Spec/2064/f3/milestones/M-expiry-generic-key-ttl-plan.md).
//
// EXPIRE, PEXPIRE, EXPIREAT, and PEXPIREAT differ only in how they name the
// target instant: PEXPIREAT is the primitive (absolute unix ms) and the other
// three convert to it. Everything after that instant is computed is keyspace
// independent: the NX/XX/GT/LT flag gate, the past-instant-deletes-and-returns-1
// quirk, and the reply. So the semantics live here, and each keyspace supplies
// only a Backend: how to read the key's current deadline, how to store a new one,
// and how to delete the key. The string store and the collection registries thus
// share one correct implementation instead of copying the flag logic per type.
package expire

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The four expiry units, one per verb. PEXPIREAT is the identity.
const (
	unitEXsec = iota // EXPIRE: relative seconds
	unitPXms         // PEXPIRE: relative milliseconds
	unitEXat         // EXPIREAT: absolute unix seconds
	unitPXat         // PEXPIREAT: absolute unix milliseconds
)

// Backend is the per-keyspace deadline plumbing the shared core drives once it
// has computed the target instant and cleared the flag gate. All three methods
// run on the shard owner goroutine, so they hold no lock.
type Backend interface {
	// Present reports whether the key holds a live value in this keyspace and its
	// current deadline. curAt is 0 when the key has no deadline, which the core
	// reads as an infinite TTL for the GT/LT comparison.
	Present() (curAt int64, present bool)
	// Store installs at as the key's deadline. It returns true on success. On a
	// failure it has already answered the client (a parked backpressure hold or an
	// out-of-memory error) and returns false, so the core stops without replying.
	Store(at int64) bool
	// Delete removes the key. A past or non-positive instant deletes the key and
	// still reports success, Redis's documented quirk, and this is that delete.
	Delete()
}

// secToMs converts seconds to milliseconds, reporting whether the multiply fit,
// so an absurd argument errors instead of wrapping to a bogus deadline.
func secToMs(sec int64) (int64, bool) {
	ms := sec * 1000
	if sec != 0 && ms/1000 != sec {
		return 0, false
	}
	return ms, true
}

// addOverflow returns a+b and whether it stayed inside int64.
func addOverflow(a, b int64) (int64, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, false
	}
	return s, true
}

// instant folds a (unit, value) pair into an absolute unix-ms deadline. Unlike
// the SET path a past or non-positive value is legal here (it deletes the key and
// returns 1); only an arithmetic overflow fails, which Redis reports as an
// invalid expire time.
func instant(nowMs int64, unit int, n int64) (int64, bool) {
	switch unit {
	case unitEXsec:
		ms, ok := secToMs(n)
		if !ok {
			return 0, false
		}
		return addOverflow(nowMs, ms)
	case unitPXms:
		return addOverflow(nowMs, n)
	case unitEXat:
		return secToMs(n)
	default: // unitPXat
		return n, true
	}
}

// eqFold reports whether b equals the ASCII option name s case-insensitively,
// without allocating. s is all-uppercase at every call site.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		x := b[i]
		if x >= 'a' && x <= 'z' {
			x -= 32
		}
		if x != s[i] {
			return false
		}
	}
	return true
}

// Apply runs one EXPIRE-family command against be. verb is the uppercase command
// name; it selects the time unit and names the command in the error text. args is
// key, time, then an optional NX|XX|GT|LT condition flag (Redis allows the
// XX-with-GT/LT pairs). The caller has already routed the key to the keyspace
// whose deadline plumbing be wraps.
func Apply(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string, be Backend) {
	var unit int
	var lname string
	switch verb {
	case "EXPIRE":
		unit, lname = unitEXsec, "expire"
	case "PEXPIRE":
		unit, lname = unitPXms, "pexpire"
	case "EXPIREAT":
		unit, lname = unitEXat, "expireat"
	default: // PEXPIREAT
		unit, lname = unitPXat, "pexpireat"
	}

	n, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	var nx, xx, gt, lt bool
	for _, f := range args[2:] {
		switch {
		case eqFold(f, "NX"):
			nx = true
		case eqFold(f, "XX"):
			xx = true
		case eqFold(f, "GT"):
			gt = true
		case eqFold(f, "LT"):
			lt = true
		default:
			r.Err("ERR Unsupported option " + string(f))
			return
		}
	}
	// NX excludes every other flag; GT and LT exclude each other. XX pairs with GT
	// or LT (only-if-exists plus the comparison), which Redis allows.
	if (nx && (xx || gt || lt)) || (gt && lt) {
		r.Err("ERR NX and XX, GT or LT options at the same time are not compatible")
		return
	}
	at, ok := instant(cx.NowMs, unit, n)
	if !ok {
		r.Err("ERR invalid expire time in '" + lname + "' command")
		return
	}

	curAt, present := be.Present()
	if !present {
		r.Int(0)
		return
	}
	// A key with no deadline is an infinite TTL for the GT/LT comparison.
	hasTTL := curAt != 0
	switch {
	case nx && hasTTL:
		r.Int(0)
		return
	case xx && !hasTTL:
		r.Int(0)
		return
	case gt && (!hasTTL || at <= curAt):
		r.Int(0)
		return
	case lt && hasTTL && at >= curAt:
		r.Int(0)
		return
	}

	// A deadline at or before now deletes the key and still reports success.
	if at <= cx.NowMs {
		be.Delete()
		r.Int(1)
		return
	}
	if !be.Store(at) {
		return
	}
	r.Int(1)
}
