package command

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// configGet returns the value CONFIG GET reports for a single directive, or the
// empty string when the directive does not match.
func configGet(t *testing.T, r *bufio.Reader, c net.Conn, name string) string {
	t.Helper()
	got := readArray(t, r, c, "CONFIG GET "+name)
	if len(got) == 0 {
		return ""
	}
	if len(got) != 2 {
		t.Fatalf("CONFIG GET %s = %v want one pair", name, got)
	}
	return got[1]
}

func TestConfigGetDefault(t *testing.T) {
	r, c := startData(t)
	if got := configGet(t, r, c, "maxmemory"); got != "0" {
		t.Fatalf("maxmemory default = %q want 0", got)
	}
	if got := configGet(t, r, c, "maxmemory-policy"); got != "noeviction" {
		t.Fatalf("policy default = %q", got)
	}
}

func TestConfigSetMemory(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory 100mb"); got != "+OK" {
		t.Fatalf("SET maxmemory = %q", got)
	}
	if got := configGet(t, r, c, "maxmemory"); got != "104857600" {
		t.Fatalf("maxmemory = %q want 104857600", got)
	}
}

func TestConfigSetEnum(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory-policy ALLKEYS-LRU"); got != "+OK" {
		t.Fatalf("SET policy = %q", got)
	}
	if got := configGet(t, r, c, "maxmemory-policy"); got != "allkeys-lru" {
		t.Fatalf("policy = %q", got)
	}
	if got := sendLine(t, r, c, "CONFIG SET maxmemory-policy bogus"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("SET bad policy = %q", got)
	}
}

func TestConfigSetBool(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "CONFIG SET appendonly 1")
	if got := configGet(t, r, c, "appendonly"); got != "yes" {
		t.Fatalf("appendonly = %q want yes", got)
	}
	_ = sendLine(t, r, c, "CONFIG SET appendonly false")
	if got := configGet(t, r, c, "appendonly"); got != "no" {
		t.Fatalf("appendonly = %q want no", got)
	}
}

func TestConfigSetUnknown(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "CONFIG SET no-such-directive 1")
	if !strings.HasPrefix(got, "-ERR Unknown option") {
		t.Fatalf("SET unknown = %q", got)
	}
}

func TestConfigSetImmutable(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "CONFIG SET port 7000")
	if !strings.Contains(got, "immutable") {
		t.Fatalf("SET port = %q want immutable error", got)
	}
}

func TestConfigSetAtomic(t *testing.T) {
	r, c := startData(t)
	// The second pair is invalid, so neither change applies.
	got := sendLine(t, r, c, "CONFIG SET maxmemory 50mb maxmemory-policy bogus")
	if !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("partial SET = %q want error", got)
	}
	if v := configGet(t, r, c, "maxmemory"); v != "0" {
		t.Fatalf("maxmemory changed despite failed SET: %q", v)
	}
}

func TestConfigSetMultiple(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory 1mb maxmemory-samples 10"); got != "+OK" {
		t.Fatalf("multi SET = %q", got)
	}
	if v := configGet(t, r, c, "maxmemory"); v != "1048576" {
		t.Fatalf("maxmemory = %q", v)
	}
	if v := configGet(t, r, c, "maxmemory-samples"); v != "10" {
		t.Fatalf("samples = %q", v)
	}
}

func TestConfigGetGlob(t *testing.T) {
	r, c := startData(t)
	got := readArray(t, r, c, "CONFIG GET maxmemory*")
	// At least maxmemory, maxmemory-policy, maxmemory-samples and more, each a
	// name/value pair, so the flat array length is even and well above two.
	if len(got) < 6 || len(got)%2 != 0 {
		t.Fatalf("glob len = %d", len(got))
	}
}

func TestConfigGetMultiPattern(t *testing.T) {
	r, c := startData(t)
	got := readArray(t, r, c, "CONFIG GET maxclients databases")
	if len(got) != 4 {
		t.Fatalf("multi-pattern len = %d want 4", len(got))
	}
}

func TestConfigGetUnknownEmpty(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG GET no-such-directive"); got != "*0" {
		t.Fatalf("GET unknown = %q want *0", got)
	}
}

func TestConfigResetStat(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG RESETSTAT"); got != "+OK" {
		t.Fatalf("RESETSTAT = %q", got)
	}
}

func TestConfigRewriteNoFile(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "CONFIG REWRITE")
	if !strings.HasPrefix(got, "-ERR The server is running without a config file") {
		t.Fatalf("REWRITE = %q", got)
	}
}

func TestConfigSaveCanonical(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET save \"900 1 300 10\""); got != "+OK" {
		t.Fatalf("SET save = %q", got)
	}
	if v := configGet(t, r, c, "save"); v != "900 1 300 10" {
		t.Fatalf("save = %q", v)
	}
	// An odd number of fields is rejected.
	if got := sendLine(t, r, c, "CONFIG SET save \"900\""); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("SET bad save = %q", got)
	}
}
