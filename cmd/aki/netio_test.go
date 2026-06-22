package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/aki/command"
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/vfs"
)

// startServer brings up an in-memory aki server and returns the address it bound
// to. It is the harness for the networked dump and import tests.
func startServer(t *testing.T) string {
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

	d := command.New(command.Config{Engine: command.NewEngine(ks)})
	ncfg := networking.Config{Addr: "127.0.0.1:0"}
	srv := networking.New(ncfg, d)
	d.SetServer(srv)
	go func() { _ = srv.ListenAndServe(ncfg) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	return srv.Addr().String()
}

// mustCall runs a command on the server and fails the test on a transport error
// or an error reply.
func mustCall(t *testing.T, cl *netClient, args ...string) {
	t.Helper()
	reply, err := cl.call(args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	if reply.Type == '-' {
		t.Fatalf("%v: %s", args, reply.Err)
	}
}

// TestDumpFromServer populates a running instance, dumps it over the wire, and
// checks the keys land in the output file.
func TestDumpFromServer(t *testing.T) {
	addr := startServer(t)
	cl, err := dialServer(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.close()

	mustCall(t, cl, "SET", "s", "hello")
	mustCall(t, cl, "RPUSH", "l", "a", "b", "c")
	mustCall(t, cl, "HSET", "h", "f", "v")
	mustCall(t, cl, "SELECT", "2")
	mustCall(t, cl, "SET", "other", "1")

	out := filepath.Join(t.TempDir(), "dump.rdb")
	if err := cmdDump([]string{"--addr", addr, "--output", out}); err != nil {
		t.Fatalf("dump --addr: %v", err)
	}

	blob, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	keys := 0
	for _, db := range snap.DBs {
		keys += len(db.Entries)
	}
	if keys != 4 {
		t.Fatalf("dumped keys = %d want 4", keys)
	}
}

// TestDumpFromServerSingleDB limits the wire dump to one database.
func TestDumpFromServerSingleDB(t *testing.T) {
	addr := startServer(t)
	cl, err := dialServer(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.close()

	mustCall(t, cl, "SET", "a", "1")
	mustCall(t, cl, "SELECT", "3")
	mustCall(t, cl, "SET", "b", "2")
	mustCall(t, cl, "SET", "c", "3")

	out := filepath.Join(t.TempDir(), "dump.rdb")
	if err := cmdDump([]string{"--addr", addr, "--db", "3", "--output", out}); err != nil {
		t.Fatalf("dump --addr --db 3: %v", err)
	}
	blob, _ := os.ReadFile(out)
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	keys := 0
	for _, db := range snap.DBs {
		keys += len(db.Entries)
	}
	if keys != 2 {
		t.Fatalf("single-db dump keys = %d want 2", keys)
	}
}

// TestImportToServer reads an RDB file and ships it to a running instance, then
// checks the keys are readable there.
func TestImportToServer(t *testing.T) {
	addr := startServer(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "in.rdb")

	snap := rdb.Snapshot{DBs: []rdb.DBData{{Index: 0, Entries: []rdb.Entry{
		{Key: []byte("x"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("11")}, ExpireMS: -1},
		{Key: []byte("y"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("22")}, ExpireMS: -1},
	}}}}
	blob, _ := rdb.MarshalFile(snap)
	if err := os.WriteFile(src, blob, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := cmdImport([]string{src, "--addr", addr}); err != nil {
		t.Fatalf("import --addr: %v", err)
	}

	cl, err := dialServer(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.close()
	for k, want := range map[string]string{"x": "11", "y": "22"} {
		reply, gerr := cl.call("GET", k)
		if gerr != nil {
			t.Fatalf("GET %s: %v", k, gerr)
		}
		if string(reply.Str) != want {
			t.Fatalf("GET %s = %q want %q", k, reply.Str, want)
		}
	}
}

// TestDumpImportRoundTripOverWire dumps one server and imports into a second,
// covering both networked directions in one pass.
func TestDumpImportRoundTripOverWire(t *testing.T) {
	srcAddr := startServer(t)
	dstAddr := startServer(t)

	sc, err := dialServer(srcAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial src: %v", err)
	}
	defer sc.close()
	mustCall(t, sc, "SET", "k1", "v1")
	mustCall(t, sc, "SET", "k2", "v2")

	out := filepath.Join(t.TempDir(), "mid.rdb")
	if err := cmdDump([]string{"--addr", srcAddr, "--output", out}); err != nil {
		t.Fatalf("dump src: %v", err)
	}
	if err := cmdImport([]string{out, "--addr", dstAddr}); err != nil {
		t.Fatalf("import dst: %v", err)
	}

	dc, err := dialServer(dstAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial dst: %v", err)
	}
	defer dc.close()
	reply, _ := dc.call("GET", "k1")
	if string(reply.Str) != "v1" {
		t.Fatalf("dst GET k1 = %q want v1", reply.Str)
	}
}
