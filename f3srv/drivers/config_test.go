package drivers

import (
	"testing"
)

// TestConfigGetExact checks CONFIG GET for a single known parameter answers a
// two-element name/value pair.
func TestConfigGetExact(t *testing.T) {
	_, nc, br := startServer(t)
	reply, ok := sendCmd(t, br, nc, "CONFIG", "GET", "maxmemory").([]any)
	if !ok || len(reply) != 2 {
		t.Fatalf("CONFIG GET maxmemory = %v, want one name/value pair", reply)
	}
	if reply[0] != "maxmemory" || reply[1] != "0" {
		t.Fatalf("CONFIG GET maxmemory = %v, want [maxmemory 0]", reply)
	}
}

// TestConfigGetGlob checks a glob pattern returns every matching parameter, and
// that a pattern matching nothing is the empty array rather than an error.
func TestConfigGetGlob(t *testing.T) {
	_, nc, br := startServer(t)

	reply, ok := sendCmd(t, br, nc, "CONFIG", "GET", "maxmemory*").([]any)
	if !ok || len(reply) < 4 || len(reply)%2 != 0 {
		t.Fatalf("CONFIG GET maxmemory* = %v, want even-length array of pairs", reply)
	}
	// maxmemory, maxmemory-policy, and maxmemory-samples all match.
	names := map[string]string{}
	for i := 0; i < len(reply); i += 2 {
		names[reply[i].(string)] = reply[i+1].(string)
	}
	for _, want := range []string{"maxmemory", "maxmemory-policy", "maxmemory-samples"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("CONFIG GET maxmemory* missing %q, got %v", want, names)
		}
	}

	empty, ok := sendCmd(t, br, nc, "CONFIG", "GET", "no-such-param").([]any)
	if !ok || len(empty) != 0 {
		t.Fatalf("CONFIG GET no-such-param = %v, want empty array", empty)
	}
}

// TestConfigSetRoundTrips checks CONFIG SET stores a value that a later GET
// reflects, and that an unknown parameter errors without touching the store.
func TestConfigSetRoundTrips(t *testing.T) {
	_, nc, br := startServer(t)
	// The config store is process-global, so restore the seed before returning
	// to keep the other config tests order-independent (they run sequentially).
	t.Cleanup(func() { sendCmd(t, br, nc, "CONFIG", "SET", "maxmemory", "0") })

	if got := sendCmd(t, br, nc, "CONFIG", "SET", "maxmemory", "100mb"); got != "OK" {
		t.Fatalf("CONFIG SET maxmemory = %v, want OK", got)
	}
	reply := sendCmd(t, br, nc, "CONFIG", "GET", "maxmemory").([]any)
	if reply[1] != "100mb" {
		t.Fatalf("after SET, maxmemory = %v, want 100mb", reply[1])
	}

	// An unknown parameter in a multi-pair SET errors and leaves the known pair
	// unapplied (atomic validation).
	if _, ok := sendCmd(t, br, nc, "CONFIG", "SET", "appendonly", "yes", "bogus", "1").(errorReply); !ok {
		t.Fatalf("CONFIG SET with unknown param did not error")
	}
	reply = sendCmd(t, br, nc, "CONFIG", "GET", "appendonly").([]any)
	if reply[1] != "no" {
		t.Fatalf("appendonly = %v after failed atomic SET, want unchanged 'no'", reply[1])
	}
}

// TestConfigSetArity checks an odd-length SET tail is refused for arity.
func TestConfigSetArity(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "CONFIG", "SET", "maxmemory").(errorReply); !ok {
		t.Fatalf("CONFIG SET with no value did not error")
	}
}

// TestConfigRewriteResetstat checks REWRITE reports the no-config-file case and
// RESETSTAT acks, the two contracts a client relies on.
func TestConfigRewriteResetstat(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "CONFIG", "REWRITE").(errorReply); !ok {
		t.Fatalf("CONFIG REWRITE did not error on a fileless server")
	}
	if got := sendCmd(t, br, nc, "CONFIG", "RESETSTAT"); got != "OK" {
		t.Fatalf("CONFIG RESETSTAT = %v, want OK", got)
	}
}

// TestConfigBadSubcommand checks an unknown subcommand errors.
func TestConfigBadSubcommand(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "CONFIG", "NOPE").(errorReply); !ok {
		t.Fatalf("CONFIG NOPE did not error")
	}
}
