package command

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
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

// TestDebugLoadAOFReload checks DEBUG LOADAOF flushes the dataset and rebuilds it
// from the base RDB and the incremental file, including replaying overwrites in
// order.
func TestDebugLoadAOFReload(t *testing.T) {
	r, c := startData(t)
	_ = enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")
	_ = sendLine(t, r, c, "SET foo bar")
	_ = sendLine(t, r, c, "SET foo baz")
	_ = sendLine(t, r, c, "RPUSH list a b c")

	if got := sendLine(t, r, c, "DEBUG LOADAOF"); got != "+OK" {
		t.Fatalf("DEBUG LOADAOF = %q", got)
	}

	if got := bulk(t, r, c, "GET foo"); got != "baz" {
		t.Fatalf("GET foo after reload = %q want baz", got)
	}
	if got := sendLine(t, r, c, "LLEN list"); got != ":3" {
		t.Fatalf("LLEN list after reload = %q", got)
	}
}

// TestDebugLoadAOFBase checks the base RDB is loaded on reload: data folded into
// the base by a rewrite survives even though it is not in the incremental file.
func TestDebugLoadAOFBase(t *testing.T) {
	r, c := startData(t)
	_ = enableAOF(t, r, c)
	_ = sendLine(t, r, c, "SET inbase yes")
	// Fold inbase into a fresh base, then add a key that lives only in the incr.
	_ = sendLine(t, r, c, "BGREWRITEAOF")
	_ = sendLine(t, r, c, "SET inincr yes")

	if got := sendLine(t, r, c, "DEBUG LOADAOF"); got != "+OK" {
		t.Fatalf("DEBUG LOADAOF = %q", got)
	}
	if got := bulk(t, r, c, "GET inbase"); got != "yes" {
		t.Fatalf("GET inbase after reload = %q", got)
	}
	if got := bulk(t, r, c, "GET inincr"); got != "yes" {
		t.Fatalf("GET inincr after reload = %q", got)
	}
}

// TestAOFStartupLoad checks a fresh dispatcher pointed at an existing
// appendonlydir loads the dataset on initAOF, the startup path.
func TestAOFStartupLoad(t *testing.T) {
	dir := t.TempDir()

	// First dispatcher writes the AOF.
	d1 := New(Config{Engine: NewEngine(memKeyspace(t))})
	d1.conf.set("dir", dir)
	d1.conf.set("appendonly", "yes")
	d1.initAOF()
	applyAOFWrite(d1, "SET", "foo", "bar")
	applyAOFWrite(d1, "SET", "n", "42")
	d1.closeAOF()

	// Second dispatcher loads it on startup.
	d2 := New(Config{Engine: NewEngine(memKeyspace(t))})
	d2.conf.set("dir", dir)
	d2.conf.set("appendonly", "yes")
	d2.initAOF()
	defer d2.closeAOF()

	if got, ok := aofGetKey(t, d2, 0, "foo"); !ok || got != "bar" {
		t.Fatalf("foo after startup load = %q ok=%v", got, ok)
	}
	if got, ok := aofGetKey(t, d2, 0, "n"); !ok || got != "42" {
		t.Fatalf("n after startup load = %q ok=%v", got, ok)
	}
}

// applyAOFWrite dispatches one write command on an offline connection so it
// propagates into the open incr file, the way a real client write would.
func applyAOFWrite(d *Dispatcher, args ...string) {
	conn := networking.NewOfflineConn()
	sess := &session{authenticated: true}
	conn.SetSession(sess)
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	cmd, err := d.table.lookup(argv)
	if err != nil {
		return
	}
	d.runCommand(&Ctx{Conn: conn, Argv: argv, d: d, sess: sess}, cmd)
}

// aofGetKey reads a string value straight from a dispatcher's keyspace.
func aofGetKey(t *testing.T, d *Dispatcher, db int, key string) (string, bool) {
	t.Helper()
	var (
		body  []byte
		found bool
	)
	if err := d.engine.view(db, func(kdb *keyspace.DB) error {
		b, _, ok, e := kdb.Get([]byte(key))
		if e != nil {
			return e
		}
		body, found = b, ok
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
	return string(body), found
}

// memKeyspace opens an in-memory keyspace for the dispatcher-level AOF tests.
func memKeyspace(t *testing.T) *keyspace.Keyspace {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	return ks
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
