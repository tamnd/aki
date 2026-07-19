package drivers

import (
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// TestDebugSleep checks DEBUG SLEEP blocks for about the requested time and then
// answers OK, the behavior test harnesses lean on to make a command hang.
func TestDebugSleep(t *testing.T) {
	_, nc, br := startServer(t)
	start := time.Now()
	if got := sendCmd(t, br, nc, "DEBUG", "SLEEP", "0.2"); got != "OK" {
		t.Fatalf("DEBUG SLEEP = %v, want OK", got)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("DEBUG SLEEP 0.2 returned after %v, want at least ~0.2s", elapsed)
	}
}

// TestDebugSleepBadArg checks a non-float sleep argument is refused rather than
// silently treated as zero.
func TestDebugSleepBadArg(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "DEBUG", "SLEEP", "soon").(errorReply); !ok {
		t.Fatalf("DEBUG SLEEP with a non-float did not error")
	}
}

// TestDebugStubsOK checks the internal-poke subcommands f3 has no equivalent of
// acknowledge with OK, so a harness setup step does not fail on them.
func TestDebugStubsOK(t *testing.T) {
	_, nc, br := startServer(t)
	// SET-ACTIVE-EXPIRE 0 below flips a process-global toggle; restore it so this
	// test does not silently disable the active-expiry cycle for later tests.
	defer shard.SetActiveExpire(true)
	cases := [][]string{
		{"DEBUG", "JMAP"},
		{"DEBUG", "SET-ACTIVE-EXPIRE", "0"},
		{"DEBUG", "QUICKLIST-PACKED-THRESHOLD", "100"},
		{"DEBUG", "CHANGE-REPL-ID"},
	}
	for _, c := range cases {
		args := make([]string, len(c))
		copy(args, c)
		if got := sendCmd(t, br, nc, args...); got != "OK" {
			t.Fatalf("%v = %v, want OK", c, got)
		}
	}
}

// TestDebugUnknown checks a DEBUG subcommand f3 does not model errors rather than
// answering a misleading OK.
func TestDebugUnknown(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "DEBUG", "SEGFAULT").(errorReply); !ok {
		t.Fatalf("DEBUG SEGFAULT did not error")
	}
}
