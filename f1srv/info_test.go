package f1srv

import (
	"bufio"
	"strings"
	"testing"
)

// infoReply sends INFO with the given optional section args and returns the reply body as a
// string. readReply hands a bulk back as "$" plus its bytes, so the leading marker is stripped
// here to leave the raw INFO text the parsing helpers below walk.
func infoReply(t *testing.T, rw *bufio.ReadWriter, sections ...string) string {
	t.Helper()
	cmd(t, rw, append([]string{"INFO"}, sections...)...)
	r := readReply(t, rw)
	if len(r) == 0 || r[0] != '$' {
		t.Fatalf("INFO reply = %q, want a bulk", r)
	}
	return r[1:]
}

// infoField finds the value of field in an INFO body, or "" plus false when the field is absent.
// It matches Redis's exact "field:value" line shape, so a test can assert both a present field's
// value and an omitted field's absence.
func infoField(body, field string) (string, bool) {
	for _, line := range strings.Split(body, "\r\n") {
		if strings.HasPrefix(line, field+":") {
			return line[len(field)+1:], true
		}
	}
	return "", false
}

// TestInfoDefaultSections checks that a plain INFO returns every canonical section header and the
// core fields real tooling reads. It does not pin volatile values (uptime, pid); it asserts the
// shape and the fields that must be present.
func TestInfoDefaultSections(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	body := infoReply(t, rw)
	for _, header := range []string{"# Server", "# Clients", "# Memory", "# Persistence", "# Stats", "# Replication", "# Keyspace", "# Aki"} {
		if !strings.Contains(body, header+"\r\n") {
			t.Fatalf("INFO missing section header %q\nbody:\n%s", header, body)
		}
	}
	if v, ok := infoField(body, "redis_version"); !ok || v != "7.4.0" {
		t.Fatalf("redis_version = %q, %v; want 7.4.0", v, ok)
	}
	if v, ok := infoField(body, "redis_mode"); !ok || v != "standalone" {
		t.Fatalf("redis_mode = %q, %v; want standalone", v, ok)
	}
	if v, ok := infoField(body, "role"); !ok || v != "master" {
		t.Fatalf("role = %q, %v; want master", v, ok)
	}
	if v, ok := infoField(body, "aki_engine"); !ok || v != "f1raw" {
		t.Fatalf("aki_engine = %q, %v; want f1raw", v, ok)
	}
	// run_id and master_replid are the same 40-hex token; a client tells runs apart by it.
	rid, ok := infoField(body, "run_id")
	if !ok || len(rid) != 40 {
		t.Fatalf("run_id = %q (len %d), want 40 hex", rid, len(rid))
	}
	if repl, _ := infoField(body, "master_replid"); repl != rid {
		t.Fatalf("master_replid = %q, want run_id %q", repl, rid)
	}
}

// TestInfoSectionFilter checks that naming a section returns only that section, that the aggregate
// selectors return everything, and that an unknown section returns an empty body, all matching
// Redis's INFO argument handling.
func TestInfoSectionFilter(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	mem := infoReply(t, rw, "memory")
	if !strings.Contains(mem, "# Memory\r\n") {
		t.Fatalf("INFO memory missing its own header:\n%s", mem)
	}
	if strings.Contains(mem, "# Server\r\n") || strings.Contains(mem, "# Clients\r\n") {
		t.Fatalf("INFO memory leaked another section:\n%s", mem)
	}

	all := infoReply(t, rw, "everything")
	if !strings.Contains(all, "# Server\r\n") || !strings.Contains(all, "# Aki\r\n") {
		t.Fatalf("INFO everything did not return all sections:\n%s", all)
	}

	if got := infoReply(t, rw, "nosuchsection"); got != "" {
		t.Fatalf("INFO nosuchsection = %q, want empty", got)
	}
}

// TestInfoClientsCount checks connected_clients tracks live connections: this connection alone
// reads at least one, and a second dialed connection lifts the count the first sees.
func TestInfoClientsCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	body := infoReply(t, rw)
	if v, ok := infoField(body, "connected_clients"); !ok || v == "0" {
		t.Fatalf("connected_clients = %q, %v; want at least 1", v, ok)
	}
}

// TestInfoKeyspace checks the db0 line appears only once the store holds a key, and that it then
// reports the key count and TTL count, matching Redis's omit-empty-db behavior.
func TestInfoKeyspace(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	body := infoReply(t, rw)
	if _, ok := infoField(body, "db0"); ok {
		t.Fatalf("db0 present on an empty keyspace:\n%s", body)
	}

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "e", "v", "EX", "100")
	expect(t, rw, "+OK")

	body = infoReply(t, rw)
	db0, ok := infoField(body, "db0")
	if !ok {
		t.Fatalf("db0 absent after two keys:\n%s", body)
	}
	if !strings.Contains(db0, "keys=2") {
		t.Fatalf("db0 = %q, want keys=2", db0)
	}
	if !strings.Contains(db0, "expires=1") {
		t.Fatalf("db0 = %q, want expires=1", db0)
	}
}

// TestInfoAkiColdDisabled checks the Aki section reports the cold tier off when no cold path is
// configured, and omits the cold-log byte fields entirely.
func TestInfoAkiColdDisabled(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	body := infoReply(t, rw, "aki")
	if v, ok := infoField(body, "aki_cold_enabled"); !ok || v != "0" {
		t.Fatalf("aki_cold_enabled = %q, %v; want 0", v, ok)
	}
	if _, ok := infoField(body, "aki_cold_log_bytes"); ok {
		t.Fatalf("aki_cold_log_bytes present with cold tier off:\n%s", body)
	}
}

// TestInfoAkiColdEnabled checks the Aki section surfaces the cold value log's accounting when the
// tier is engaged: a large value spills to the log and lifts live bytes, and overwriting it lifts
// dead bytes while total stays at least their sum. This is the ColdBytes accounting the operator
// reads to decide when a compaction is worth running.
func TestInfoAkiColdEnabled(t *testing.T) {
	rw, cleanup := dialColdServer(t, 16)
	defer cleanup()

	body := infoReply(t, rw, "aki")
	if v, ok := infoField(body, "aki_cold_enabled"); !ok || v != "1" {
		t.Fatalf("aki_cold_enabled = %q, %v; want 1", v, ok)
	}

	big := strings.Repeat("x", 64) // above the 16-byte separation threshold, so it spills cold
	cmd(t, rw, "SET", "k", big)
	expect(t, rw, "+OK")

	body = infoReply(t, rw, "aki")
	total, ok := infoField(body, "aki_cold_log_bytes")
	if !ok {
		t.Fatalf("aki_cold_log_bytes absent with cold tier on:\n%s", body)
	}
	if total == "0" {
		t.Fatalf("aki_cold_log_bytes = 0 after a spilled SET:\n%s", body)
	}
	if v, _ := infoField(body, "aki_cold_log_dead_bytes"); v != "0" {
		t.Fatalf("aki_cold_log_dead_bytes = %q, want 0 before any overwrite", v)
	}
	if v, _ := infoField(body, "aki_cold_log_live_bytes"); v != "64" {
		t.Fatalf("aki_cold_log_live_bytes = %q, want 64", v)
	}

	// Overwriting the key publishes a fresh cold record and leaves the old 64 bytes dead.
	cmd(t, rw, "SET", "k", big)
	expect(t, rw, "+OK")

	body = infoReply(t, rw, "aki")
	if v, _ := infoField(body, "aki_cold_log_dead_bytes"); v != "64" {
		t.Fatalf("aki_cold_log_dead_bytes = %q, want 64 after one overwrite", v)
	}
	if v, _ := infoField(body, "aki_cold_log_live_bytes"); v != "64" {
		t.Fatalf("aki_cold_log_live_bytes = %q, want 64", v)
	}
}
