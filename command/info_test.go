package command

import (
	"strings"
	"testing"
)

func TestInfoDefault(t *testing.T) {
	r, c := start(t, Config{})
	header := sendLine(t, r, c, "INFO")
	body := readBulk(t, r, header)
	for _, want := range []string{
		"# Server", "redis_version:7.4.0", "run_id:", "tcp_port:",
		"# Clients", "connected_clients:",
		"# Memory", "used_memory:", "maxmemory_policy:noeviction",
		"# Persistence", "loading:0",
		"# Stats", "total_commands_processed:",
		"# Replication", "role:master",
		"# CPU", "used_cpu_sys:",
		"# Cluster", "cluster_enabled:0",
		"# Keyspace",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("INFO missing %q", want)
		}
	}
	// Default output must not carry the commandstats section.
	if strings.Contains(body, "# Commandstats") {
		t.Fatal("default INFO should not include Commandstats")
	}
}

func TestInfoSection(t *testing.T) {
	r, c := start(t, Config{})
	header := sendLine(t, r, c, "INFO server")
	body := readBulk(t, r, header)
	if !strings.Contains(body, "# Server") {
		t.Fatalf("INFO server missing Server header: %q", body)
	}
	if strings.Contains(body, "# Memory") {
		t.Fatalf("INFO server should not include Memory: %q", body)
	}
}

func TestInfoEverything(t *testing.T) {
	r, c := start(t, Config{})
	header := sendLine(t, r, c, "INFO everything")
	body := readBulk(t, r, header)
	for _, want := range []string{"# Commandstats", "# Latencystats", "# Errorstats"} {
		if !strings.Contains(body, want) {
			t.Fatalf("INFO everything missing %q", want)
		}
	}
}

func TestInfoKeyspace(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SET k v"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	header := sendLine(t, r, c, "INFO keyspace")
	body := readBulk(t, r, header)
	if !strings.Contains(body, "db0:keys=1") {
		t.Fatalf("INFO keyspace = %q", body)
	}
}

func TestLolwut(t *testing.T) {
	r, c := start(t, Config{})
	header := sendLine(t, r, c, "LOLWUT")
	body := readBulk(t, r, header)
	if !strings.Contains(body, "Redis ver. 7.4.0") {
		t.Fatalf("LOLWUT footer = %q", body)
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		0:       "0B",
		512:     "512B",
		1024:    "1.00K",
		1536:    "1.50K",
		1048576: "1.00M",
	}
	for in, want := range cases {
		if got := humanBytes(in); got != want {
			t.Fatalf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}
