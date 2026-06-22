package command

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// configSet turns appendonly on and points the data dir at a temp directory, the
// setup every AOF test shares.
func enableAOF(t *testing.T, r *bufio.Reader, c net.Conn) string {
	t.Helper()
	dir := t.TempDir()
	if got := sendLine(t, r, c, "CONFIG SET dir "+dir); got != "+OK" {
		t.Fatalf("CONFIG SET dir = %q", got)
	}
	if got := sendLine(t, r, c, "CONFIG SET appendonly yes"); got != "+OK" {
		t.Fatalf("CONFIG SET appendonly = %q", got)
	}
	return dir
}

// TestBgrewriteaofCreatesLayout checks BGREWRITEAOF builds the appendonlydir with
// a base RDB, an incr file, and a manifest tying them together.
func TestBgrewriteaofCreatesLayout(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	_ = sendLine(t, r, c, "SET k v")

	got := sendLine(t, r, c, "BGREWRITEAOF")
	if got != "+Background append only file rewriting started" {
		t.Fatalf("BGREWRITEAOF = %q", got)
	}

	aofdir := filepath.Join(dir, "appendonlydir")
	entries, err := os.ReadDir(aofdir)
	if err != nil {
		t.Fatalf("read appendonlydir: %v", err)
	}
	var base, incr, manifest bool
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".base.rdb"):
			base = true
		case strings.HasSuffix(e.Name(), ".incr.aof"):
			incr = true
		case strings.HasSuffix(e.Name(), ".manifest"):
			manifest = true
		}
	}
	if !base || !incr || !manifest {
		t.Fatalf("layout incomplete: base=%v incr=%v manifest=%v", base, incr, manifest)
	}

	man, err := os.ReadFile(filepath.Join(aofdir, "appendonly.aof.manifest"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	text := string(man)
	if !strings.Contains(text, "type b") || !strings.Contains(text, "type i") {
		t.Fatalf("manifest missing base/incr lines: %q", text)
	}
}

// TestAOFPropagatesWrites checks a write after a rewrite lands in the incr file
// as a RESP command.
func TestAOFPropagatesWrites(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	if got := sendLine(t, r, c, "BGREWRITEAOF"); got != "+Background append only file rewriting started" {
		t.Fatalf("BGREWRITEAOF = %q", got)
	}

	_ = sendLine(t, r, c, "SET foo bar")
	_ = sendLine(t, r, c, "LPUSH mylist a b c")

	aofdir := filepath.Join(dir, "appendonlydir")
	incr := readIncrFile(t, aofdir)
	if !strings.Contains(incr, "SET") || !strings.Contains(incr, "foo") {
		t.Fatalf("incr missing SET foo: %q", incr)
	}
	if !strings.Contains(incr, "LPUSH") {
		t.Fatalf("incr missing LPUSH: %q", incr)
	}
	// A SELECT is emitted before the first write so the replay targets the right db.
	if !strings.Contains(incr, "SELECT") {
		t.Fatalf("incr missing SELECT: %q", incr)
	}
}

// TestAOFExpireRewritten checks EXPIRE is rewritten to an absolute PEXPIREAT.
func TestAOFExpireRewritten(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "EXPIRE k 100"); got != ":1" {
		t.Fatalf("EXPIRE = %q", got)
	}

	incr := readIncrFile(t, filepath.Join(dir, "appendonlydir"))
	if !strings.Contains(incr, "PEXPIREAT") {
		t.Fatalf("incr missing PEXPIREAT: %q", incr)
	}
	if strings.Contains(incr, "EXPIRE\r\n$1\r\nk\r\n$3\r\n100") {
		t.Fatalf("incr kept relative EXPIRE: %q", incr)
	}
}

// TestAOFReadOnlyCommandNotPropagated checks a read does not grow the incr file.
func TestAOFReadOnlyCommandNotPropagated(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")
	_ = sendLine(t, r, c, "SET k v")
	before := len(readIncrFile(t, filepath.Join(dir, "appendonlydir")))
	_ = bulk(t, r, c, "GET k")
	after := len(readIncrFile(t, filepath.Join(dir, "appendonlydir")))
	if after != before {
		t.Fatalf("read grew incr file: before=%d after=%d", before, after)
	}
}

// TestInfoAOFFields checks INFO persistence reports the AOF state.
func TestInfoAOFFields(t *testing.T) {
	r, c := startData(t)
	_ = enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")

	header := sendLine(t, r, c, "INFO persistence")
	info := readBulk(t, r, header)
	for _, want := range []string{"aof_enabled:1", "aof_last_bgrewrite_status:ok", "aof_base_size:"} {
		if !strings.Contains(info, want) {
			t.Fatalf("INFO persistence missing %q in:\n%s", want, info)
		}
	}
}

// readIncrFile returns the contents of the single incr file in an appendonlydir.
func readIncrFile(t *testing.T, aofdir string) string {
	t.Helper()
	entries, err := os.ReadDir(aofdir)
	if err != nil {
		t.Fatalf("read appendonlydir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".incr.aof") {
			blob, rerr := os.ReadFile(filepath.Join(aofdir, e.Name()))
			if rerr != nil {
				t.Fatalf("read incr: %v", rerr)
			}
			return string(blob)
		}
	}
	t.Fatalf("no incr file in %s", aofdir)
	return ""
}
