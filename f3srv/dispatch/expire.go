package dispatch

// The dispatch-level expiry-option helpers, shared by the multi-key and
// bounded-increment commands that must run here rather than in a type package
// because their key-existence probe spans every keyspace (MSETEX, INCREX). They
// mirror the str package's deadline math exactly: the same EX/PX/EXAT/PXAT units
// and the same strictly-positive, overflow-checked folding Redis's
// getExpireMillisecondsOrReply performs, so an expiry parsed here behaves the
// same as one parsed on the SET path.

// Expiry units for the EX/PX/EXAT/PXAT family.
const (
	unitNone  = 0
	unitEXsec = iota // relative seconds
	unitPXms         // relative milliseconds
	unitEXat         // absolute unix seconds
	unitPXat         // absolute unix milliseconds
)

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

// secToMs converts seconds to milliseconds, reporting whether the multiply fit,
// so an absurd EX argument errors instead of wrapping to a bogus deadline.
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

// expireDeadline folds a (unit, value) pair into an absolute unix-ms deadline,
// false for a non-positive value or an overflow: the raw argument must be
// strictly positive in every unit, matching Redis's
// getExpireMillisecondsOrReply.
func expireDeadline(nowMs int64, unit int, n int64) (int64, bool) {
	if n <= 0 {
		return 0, false
	}
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
