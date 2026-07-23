package conformance

import (
	"fmt"
	"strings"
)

// Render flattens one decoded reply into the corpus's comparable form:
// status and bulk as their text, integers as digits, nil as (nil),
// arrays bracketed and space-joined. Bulk "3" and integer 3 render
// alike on purpose; reply-type exactness is the package suites' job.
func Render(v any) string {
	switch x := v.(type) {
	case nil:
		return "(nil)"
	case string:
		return x
	case int64:
		return fmt.Sprintf("%d", x)
	case []any:
		parts := make([]string, len(x))
		for i := range x {
			parts[i] = Render(x[i])
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return fmt.Sprintf("%v", v)
}

// Check compares a rendered reply against a step's want, honoring the
// "~" substring form. It reports the mismatch text, empty on a match.
func Check(s Step, got string) string { return check(s, got, s.Want) }

// CheckDurable is Check on a durability-booted node, where a step may
// carry a different expected reply. A DurableWant may hold "|"-separated
// alternatives for replies that race the flush cadence.
func CheckDurable(s Step, got string) string {
	if s.DurableWant == "" {
		return check(s, got, s.Want)
	}
	var msg string
	for _, want := range strings.Split(s.DurableWant, "|") {
		if msg = check(s, got, want); msg == "" {
			return ""
		}
	}
	return msg + " (or any of " + s.DurableWant + ")"
}

func check(s Step, got, want string) string {
	if strings.HasPrefix(want, "~") {
		if !strings.Contains(got, want[1:]) {
			return fmt.Sprintf("%v: got %q, want it to contain %q", s.Cmd, got, want[1:])
		}
		return ""
	}
	if got != want {
		return fmt.Sprintf("%v: got %q, want %q", s.Cmd, got, want)
	}
	return ""
}
