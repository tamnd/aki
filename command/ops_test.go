package command

import (
	"strings"
	"testing"
)

func TestSlowlog(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "SLOWLOG GET"); got != "*0" {
		t.Fatalf("SLOWLOG GET = %q", got)
	}
	if got := sendLine(t, r, c, "SLOWLOG LEN"); got != ":0" {
		t.Fatalf("SLOWLOG LEN = %q", got)
	}
	if got := sendLine(t, r, c, "SLOWLOG RESET"); got != "+OK" {
		t.Fatalf("SLOWLOG RESET = %q", got)
	}
	if got := sendLine(t, r, c, "SLOWLOG HELP"); !strings.HasPrefix(got, "*") {
		t.Fatalf("SLOWLOG HELP = %q", got)
	}
}

func TestLatency(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "LATENCY HISTORY command"); got != "*0" {
		t.Fatalf("LATENCY HISTORY = %q", got)
	}
	if got := sendLine(t, r, c, "LATENCY LATEST"); got != "*0" {
		t.Fatalf("LATENCY LATEST = %q", got)
	}
	if got := sendLine(t, r, c, "LATENCY RESET"); got != ":0" {
		t.Fatalf("LATENCY RESET = %q", got)
	}
	if got := sendLine(t, r, c, "LATENCY GRAPH command"); !strings.HasPrefix(got, "-ERR No samples") {
		t.Fatalf("LATENCY GRAPH = %q", got)
	}
	header := sendLine(t, r, c, "LATENCY DOCTOR")
	body := readBulk(t, r, header)
	if body == "" {
		t.Fatal("LATENCY DOCTOR empty")
	}
}

func TestMemoryUsage(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	got := sendLine(t, r, c, "MEMORY USAGE k")
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("MEMORY USAGE = %q", got)
	}
	if got := sendLine(t, r, c, "MEMORY USAGE missing"); got != "$-1" && got != "_" {
		t.Fatalf("MEMORY USAGE missing = %q", got)
	}
	if got := sendLine(t, r, c, "MEMORY USAGE k SAMPLES 0"); !strings.HasPrefix(got, ":") {
		t.Fatalf("MEMORY USAGE SAMPLES = %q", got)
	}
	if got := sendLine(t, r, c, "MEMORY USAGE k BOGUS 0"); !strings.HasPrefix(got, "-ERR syntax error") {
		t.Fatalf("MEMORY USAGE bad opt = %q", got)
	}
}

func TestMemoryStatsAndMisc(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "MEMORY PURGE"); got != "+OK" {
		t.Fatalf("MEMORY PURGE = %q", got)
	}
	header := sendLine(t, r, c, "MEMORY MALLOC-STATS")
	body := readBulk(t, r, header)
	if !strings.Contains(body, "HeapAlloc") {
		t.Fatalf("MEMORY MALLOC-STATS = %q", body)
	}
	header = sendLine(t, r, c, "MEMORY DOCTOR")
	if readBulk(t, r, header) == "" {
		t.Fatal("MEMORY DOCTOR empty")
	}
	// MEMORY STATS is a flat array in RESP2 (two elements per pair). Check it
	// last because the body lines past the header are left unread.
	if got := sendLine(t, r, c, "MEMORY STATS"); !strings.HasPrefix(got, "*") {
		t.Fatalf("MEMORY STATS = %q", got)
	}
}

func TestAlign8(t *testing.T) {
	cases := map[int]int{0: 0, 1: 8, 8: 8, 9: 16, 15: 16, 16: 16}
	for in, want := range cases {
		if got := align8(in); got != want {
			t.Fatalf("align8(%d) = %d, want %d", in, got, want)
		}
	}
}
