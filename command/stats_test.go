package command

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// infoField pulls one "key:value" field out of an INFO section reply, failing the
// test when the field is absent.
func infoField(t *testing.T, r *bufio.Reader, c net.Conn, section, key string) string {
	t.Helper()
	out, ok := sendArgs(t, r, c, "INFO", section).(string)
	if !ok {
		t.Fatalf("INFO %s did not return a bulk string", section)
	}
	for _, ln := range strings.Split(out, "\r\n") {
		if name, val, found := strings.Cut(ln, ":"); found && name == key {
			return val
		}
	}
	t.Fatalf("INFO %s has no field %q in:\n%s", section, key, out)
	return ""
}

// TestStatsCommandstats checks that running commands shows up in the INFO
// commandstats section with the call count and a non-empty time figure.
func TestStatsCommandstats(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "SET", "foo", "bar")
	sendArgs(t, r, c, "GET", "foo")
	sendArgs(t, r, c, "GET", "foo")

	get := infoField(t, r, c, "commandstats", "cmdstat_get")
	if !strings.Contains(get, "calls=2") {
		t.Fatalf("cmdstat_get = %q want calls=2", get)
	}
	if !strings.Contains(get, "usec_per_call=") || !strings.Contains(get, "rejected_calls=0") || !strings.Contains(get, "failed_calls=0") {
		t.Fatalf("cmdstat_get = %q missing expected fields", get)
	}
	set := infoField(t, r, c, "commandstats", "cmdstat_set")
	if !strings.Contains(set, "calls=1") {
		t.Fatalf("cmdstat_set = %q want calls=1", set)
	}
}

// TestStatsSubcommand checks that a container command is recorded under its
// subcommand-qualified name with the "|" separator.
func TestStatsSubcommand(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "GET", "maxmemory")
	field := infoField(t, r, c, "commandstats", "cmdstat_config|get")
	if !strings.Contains(field, "calls=1") {
		t.Fatalf("cmdstat_config|get = %q want calls=1", field)
	}
}

// TestStatsFailed checks a command that returns an error bumps failed_calls and
// adds an errorstat entry under the error code.
func TestStatsFailed(t *testing.T) {
	r, c := startData(t)

	// A wrong-type operation: LPUSH onto a string key returns WRONGTYPE.
	sendArgs(t, r, c, "SET", "k", "v")
	if _, ok := sendArgs(t, r, c, "LPUSH", "k", "x").(cmdErr); !ok {
		t.Fatalf("LPUSH on a string key did not error")
	}

	lpush := infoField(t, r, c, "commandstats", "cmdstat_lpush")
	if !strings.Contains(lpush, "calls=1") || !strings.Contains(lpush, "failed_calls=1") {
		t.Fatalf("cmdstat_lpush = %q want calls=1 failed_calls=1", lpush)
	}
	if got := infoField(t, r, c, "errorstats", "errorstat_WRONGTYPE"); !strings.Contains(got, "count=1") {
		t.Fatalf("errorstat_WRONGTYPE = %q want count=1", got)
	}
}

// TestStatsRejected checks a command rejected before it runs bumps rejected_calls
// and does not count as a call.
func TestStatsRejected(t *testing.T) {
	r, c := startData(t)

	// GET with no key is an arity error, rejected before the handler runs.
	if _, ok := sendArgs(t, r, c, "GET").(cmdErr); !ok {
		t.Fatalf("GET with no args did not error")
	}
	get := infoField(t, r, c, "commandstats", "cmdstat_get")
	if !strings.Contains(get, "calls=0") || !strings.Contains(get, "rejected_calls=1") {
		t.Fatalf("cmdstat_get = %q want calls=0 rejected_calls=1", get)
	}
}

// TestStatsLatencystats checks the latencystats section reports per-command
// percentiles when latency tracking is on.
func TestStatsLatencystats(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "SET", "foo", "bar")
	field := infoField(t, r, c, "latencystats", "latency_percentiles_usec_set")
	for _, p := range []string{"p50=", "p99=", "p99.9="} {
		if !strings.Contains(field, p) {
			t.Fatalf("latency_percentiles_usec_set = %q missing %q", field, p)
		}
	}
}

// TestStatsLatencystatsOff checks the latencystats section is empty when latency
// tracking is turned off.
func TestStatsLatencystatsOff(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "SET", "latency-tracking", "no")
	sendArgs(t, r, c, "SET", "foo", "bar")
	out, ok := sendArgs(t, r, c, "INFO", "latencystats").(string)
	if !ok {
		t.Fatalf("INFO latencystats did not return a bulk string")
	}
	if strings.Contains(out, "latency_percentiles_usec_") {
		t.Fatalf("latencystats not empty with tracking off:\n%s", out)
	}
}

// TestStatsResetStat checks CONFIG RESETSTAT clears the command and error tables.
func TestStatsResetStat(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "SET", "foo", "bar")
	sendArgs(t, r, c, "GET", "foo")
	if v := sendArgs(t, r, c, "CONFIG", "RESETSTAT"); v != "OK" {
		t.Fatalf("CONFIG RESETSTAT = %v", v)
	}
	out, ok := sendArgs(t, r, c, "INFO", "commandstats").(string)
	if !ok {
		t.Fatalf("INFO commandstats did not return a bulk string")
	}
	// Only the INFO call itself should appear, and the GET and SET counters are
	// gone.
	if strings.Contains(out, "cmdstat_get") || strings.Contains(out, "cmdstat_set") {
		t.Fatalf("commandstats still has get/set after RESETSTAT:\n%s", out)
	}
}
