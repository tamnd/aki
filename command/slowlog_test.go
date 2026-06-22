package command

import (
	"strings"
	"testing"
)

// TestSlowlogRecords checks a slow command lands in the slow log with its
// arguments and that a fast command does not.
func TestSlowlogRecords(t *testing.T) {
	r, c := startData(t)

	// Log anything slower than 30 ms. DEBUG SLEEP for 100 ms clears it; SET does
	// not. Reset right after the CONFIG SET so only the two commands under test are
	// in the window, and use a wide margin so a contended runner does not log SET.
	sendArgs(t, r, c, "CONFIG", "SET", "slowlog-log-slower-than", "30000")
	sendArgs(t, r, c, "SLOWLOG", "RESET")
	sendArgs(t, r, c, "DEBUG", "SLEEP", "0.1")
	sendArgs(t, r, c, "SET", "foo", "bar")

	if n := sendArgs(t, r, c, "SLOWLOG", "LEN"); n != int64(1) {
		t.Fatalf("SLOWLOG LEN = %v want 1", n)
	}
	entries := asArray(t, sendArgs(t, r, c, "SLOWLOG", "GET"))
	if len(entries) != 1 {
		t.Fatalf("SLOWLOG GET returned %d entries want 1", len(entries))
	}
	row := asArray(t, entries[0])
	if len(row) != 6 {
		t.Fatalf("slowlog row has %d fields want 6", len(row))
	}
	if row[0] != int64(0) {
		t.Fatalf("first entry id = %v want 0", row[0])
	}
	if dur, _ := row[2].(int64); dur < 10000 {
		t.Fatalf("duration = %v want >= 10000", row[2])
	}
	args := asArray(t, row[3])
	if len(args) != 3 || args[0] != "DEBUG" || args[1] != "SLEEP" || args[2] != "0.1" {
		t.Fatalf("logged args = %v want [DEBUG SLEEP 0.1]", args)
	}
}

// TestSlowlogReset checks RESET empties the log.
func TestSlowlogReset(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "SET", "slowlog-log-slower-than", "0")
	sendArgs(t, r, c, "SET", "foo", "bar")
	if n := sendArgs(t, r, c, "SLOWLOG", "LEN"); n == int64(0) {
		t.Fatalf("SLOWLOG LEN = 0 after logging everything")
	}
	if v := sendArgs(t, r, c, "SLOWLOG", "RESET"); v != "OK" {
		t.Fatalf("SLOWLOG RESET = %v", v)
	}
	if n := sendArgs(t, r, c, "SLOWLOG", "LEN"); n != int64(0) {
		t.Fatalf("SLOWLOG LEN = %v after reset want 0", n)
	}
}

// TestSlowlogDisabled checks a threshold of -1 records nothing.
func TestSlowlogDisabled(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "SET", "slowlog-log-slower-than", "-1")
	sendArgs(t, r, c, "DEBUG", "SLEEP", "0.02")
	if n := sendArgs(t, r, c, "SLOWLOG", "LEN"); n != int64(0) {
		t.Fatalf("SLOWLOG LEN = %v with logging disabled want 0", n)
	}
}

// TestSlowlogMaxLen checks the ring drops the oldest entries past slowlog-max-len.
func TestSlowlogMaxLen(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "SET", "slowlog-log-slower-than", "0")
	sendArgs(t, r, c, "CONFIG", "SET", "slowlog-max-len", "3")
	for i := 0; i < 6; i++ {
		sendArgs(t, r, c, "PING")
	}
	if n := sendArgs(t, r, c, "SLOWLOG", "LEN"); n != int64(3) {
		t.Fatalf("SLOWLOG LEN = %v with max-len 3 want 3", n)
	}
}

// TestLatencyMonitor checks a command spike above latency-monitor-threshold shows
// up in LATENCY HISTORY and LATEST, and that RESET clears it.
func TestLatencyMonitor(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "SET", "latency-monitor-threshold", "10")
	sendArgs(t, r, c, "DEBUG", "SLEEP", "0.05")

	hist := asArray(t, sendArgs(t, r, c, "LATENCY", "HISTORY", "command"))
	if len(hist) == 0 {
		t.Fatalf("LATENCY HISTORY command is empty after a 50 ms command")
	}
	pair := asArray(t, hist[0])
	if len(pair) != 2 {
		t.Fatalf("history sample has %d fields want 2", len(pair))
	}
	if ms, _ := pair[1].(int64); ms < 10 {
		t.Fatalf("sample latency = %v want >= 10", pair[1])
	}

	latest := asArray(t, sendArgs(t, r, c, "LATENCY", "LATEST"))
	if len(latest) == 0 {
		t.Fatalf("LATENCY LATEST is empty")
	}
	row := asArray(t, latest[0])
	if len(row) != 4 || row[0] != "command" {
		t.Fatalf("latest row = %v want [command ...]", row)
	}

	if n := sendArgs(t, r, c, "LATENCY", "RESET"); n != int64(1) {
		t.Fatalf("LATENCY RESET = %v want 1", n)
	}
	if h := asArray(t, sendArgs(t, r, c, "LATENCY", "HISTORY", "command")); len(h) != 0 {
		t.Fatalf("history not empty after reset: %v", h)
	}
}

// TestLatencyDisabled checks a threshold of 0 records no spikes.
func TestLatencyDisabled(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "SET", "latency-monitor-threshold", "0")
	sendArgs(t, r, c, "DEBUG", "SLEEP", "0.03")
	if h := asArray(t, sendArgs(t, r, c, "LATENCY", "HISTORY", "command")); len(h) != 0 {
		t.Fatalf("history not empty with monitor disabled: %v", h)
	}
}

// TestLatencyDoctorAndGraph checks the doctor report and graph render after a
// spike, and that the graph errors for an unknown event.
func TestLatencyDoctorAndGraph(t *testing.T) {
	r, c := startData(t)

	sendArgs(t, r, c, "CONFIG", "SET", "latency-monitor-threshold", "10")
	sendArgs(t, r, c, "DEBUG", "SLEEP", "0.05")

	doctor, ok := sendArgs(t, r, c, "LATENCY", "DOCTOR").(string)
	if !ok || !strings.Contains(doctor, "command") {
		t.Fatalf("LATENCY DOCTOR = %v want a report mentioning command", doctor)
	}
	graph, ok := sendArgs(t, r, c, "LATENCY", "GRAPH", "command").(string)
	if !ok || !strings.Contains(graph, "command") {
		t.Fatalf("LATENCY GRAPH = %v want a graph for command", graph)
	}
	if _, ok := sendArgs(t, r, c, "LATENCY", "GRAPH", "nope").(cmdErr); !ok {
		t.Fatalf("LATENCY GRAPH for unknown event did not error")
	}
}
