package drivers

import (
	"strings"
	"testing"
)

// TestInfoUsedMemory checks the INFO memory surface end to end: used_memory
// parses out of the reply, grows when a value lands, and gives the bytes back
// when the key leaves.
func TestInfoUsedMemory(t *testing.T) {
	_, nc, br := startServer(t)

	base := readInfo(t, nc, br)
	if _, ok := base["used_memory"]; !ok {
		t.Fatalf("no used_memory in INFO: %v", base)
	}
	if base["index_bytes"] == 0 {
		t.Fatalf("no index_bytes in INFO: %v", base)
	}
	if base["used_memory"] != base["index_bytes"]+base["arena_live_bytes"] {
		t.Fatalf("used_memory %d != index %d + arena live %d",
			base["used_memory"], base["index_bytes"], base["arena_live_bytes"])
	}

	const valLen = 1 << 20
	send(t, nc, "SET", "big", strings.Repeat("x", valLen))
	expect(t, br, "+OK\r\n")

	grown := readInfo(t, nc, br)
	if grown["used_memory"] < base["used_memory"]+valLen {
		t.Fatalf("used_memory did not grow by the value: %d -> %d",
			base["used_memory"], grown["used_memory"])
	}
	if grown["chunked_bytes"] != valLen {
		t.Fatalf("chunked_bytes = %d, want %d", grown["chunked_bytes"], valLen)
	}

	send(t, nc, "DEL", "big")
	expect(t, br, ":1\r\n")
	after := readInfo(t, nc, br)
	if after["used_memory"] != base["used_memory"] {
		t.Fatalf("used_memory did not drain: base %d, after %d",
			base["used_memory"], after["used_memory"])
	}
	if after["chunked_bytes"] != 0 {
		t.Fatalf("chunked_bytes did not drain: %d", after["chunked_bytes"])
	}
}

// TestDBSize checks the keyless count fan: empty, populated across shards,
// and after a delete.
func TestDBSize(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "DBSIZE")
	expect(t, br, ":0\r\n")

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		send(t, nc, "SET", k, "v")
		expect(t, br, "+OK\r\n")
	}
	send(t, nc, "DBSIZE")
	expect(t, br, ":5\r\n")

	send(t, nc, "DEL", "c")
	expect(t, br, ":1\r\n")
	send(t, nc, "DBSIZE")
	expect(t, br, ":4\r\n")

	send(t, nc, "DBSIZE", "extra")
	expect(t, br, "-ERR wrong number of arguments for 'dbsize' command\r\n")
}
