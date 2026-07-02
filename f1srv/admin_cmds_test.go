package f1srv

import "testing"

// SLOWLOG GET/LEN/RESET behave as an empty log: GET is the empty array, LEN is zero, RESET is OK,
// and HELP returns the fixed help array.
func TestSlowlog(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SLOWLOG", "GET")
	expect(t, rw, "*0")
	cmd(t, rw, "SLOWLOG", "GET", "10")
	expect(t, rw, "*0")
	cmd(t, rw, "SLOWLOG", "GET", "-1")
	expect(t, rw, "*0")
	cmd(t, rw, "SLOWLOG", "LEN")
	expect(t, rw, ":0")
	cmd(t, rw, "SLOWLOG", "RESET")
	expect(t, rw, "+OK")

	cmd(t, rw, "SLOWLOG", "HELP")
	expect(t, rw, "*12")
	for i := 0; i < 12; i++ {
		readReply(t, rw) // each is a simple string; presence checked by the header count
	}
}

// SLOWLOG rejects the same argument mistakes Redis does, echoing the subcommand verbatim.
func TestSlowlogErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SLOWLOG")
	expect(t, rw, "-ERR wrong number of arguments for 'slowlog' command")
	cmd(t, rw, "SLOWLOG", "GET", "-2")
	expect(t, rw, "-ERR count should be greater than or equal to -1")
	cmd(t, rw, "SLOWLOG", "GET", "abc")
	expect(t, rw, "-ERR count should be greater than or equal to -1")
	cmd(t, rw, "SLOWLOG", "GET", "1", "2")
	expect(t, rw, "-ERR unknown subcommand or wrong number of arguments for 'GET'. Try SLOWLOG HELP.")
	cmd(t, rw, "SLOWLOG", "LEN", "extra")
	expect(t, rw, "-ERR wrong number of arguments for 'slowlog|len' command")
	cmd(t, rw, "SLOWLOG", "RESET", "extra")
	expect(t, rw, "-ERR wrong number of arguments for 'slowlog|reset' command")
	cmd(t, rw, "SLOWLOG", "badsub")
	expect(t, rw, "-ERR unknown subcommand 'badsub'. Try SLOWLOG HELP.")
}

// LATENCY RESET/HISTORY/LATEST behave as an empty event log, GRAPH has no samples, DOCTOR reports
// monitoring disabled, HISTOGRAM is empty, and HELP returns its help array.
func TestLatency(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "LATENCY", "RESET")
	expect(t, rw, ":0")
	cmd(t, rw, "LATENCY", "RESET", "a", "b")
	expect(t, rw, ":0")
	cmd(t, rw, "LATENCY", "HISTORY", "event")
	expect(t, rw, "*0")
	cmd(t, rw, "LATENCY", "LATEST")
	expect(t, rw, "*0")
	cmd(t, rw, "LATENCY", "HISTOGRAM")
	expect(t, rw, "*0")

	cmd(t, rw, "LATENCY", "GRAPH", "x")
	expect(t, rw, "-ERR No samples available for event 'x'")

	cmd(t, rw, "LATENCY", "DOCTOR")
	if got := readBulk(t, rw); got != latencyDoctor {
		t.Fatalf("DOCTOR = %q, want the latency-monitoring-disabled report", got)
	}

	cmd(t, rw, "LATENCY", "HELP")
	expect(t, rw, "*17")
	for i := 0; i < 17; i++ {
		readReply(t, rw)
	}
}

// LATENCY rejects the same argument mistakes Redis does.
func TestLatencyErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "LATENCY")
	expect(t, rw, "-ERR wrong number of arguments for 'latency' command")
	cmd(t, rw, "LATENCY", "HISTORY")
	expect(t, rw, "-ERR wrong number of arguments for 'latency|history' command")
	cmd(t, rw, "LATENCY", "HISTORY", "a", "b")
	expect(t, rw, "-ERR wrong number of arguments for 'latency|history' command")
	cmd(t, rw, "LATENCY", "LATEST", "x")
	expect(t, rw, "-ERR wrong number of arguments for 'latency|latest' command")
	cmd(t, rw, "LATENCY", "badsub")
	expect(t, rw, "-ERR unknown subcommand 'badsub'. Try LATENCY HELP.")
}
