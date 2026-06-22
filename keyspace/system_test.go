package keyspace

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// TestSystemTablePutGet covers the basic store and read back, a missing entry,
// a replace, and a delete.
func TestSystemTablePutGet(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "s.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	ks, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok, err := ks.SystemGet("acl:default"); err != nil || ok {
		t.Fatalf("get on empty table = ok %v err %v want false nil", ok, err)
	}

	if err := ks.SystemPut("acl:default", []byte("user default on nopass ~* &* +@all")); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, ok, err := ks.SystemGet("acl:default")
	if err != nil || !ok || string(v) != "user default on nopass ~* &* +@all" {
		t.Fatalf("get = %q ok %v err %v", v, ok, err)
	}

	if err := ks.SystemPut("acl:default", []byte("user default off")); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if v, _, _ := ks.SystemGet("acl:default"); string(v) != "user default off" {
		t.Fatalf("after replace = %q", v)
	}

	ok, err = ks.SystemDelete("acl:default")
	if err != nil || !ok {
		t.Fatalf("delete = ok %v err %v", ok, err)
	}
	if _, ok, _ := ks.SystemGet("acl:default"); ok {
		t.Fatalf("get after delete still present")
	}
}

// TestSystemTableList checks prefix listing returns sorted names and respects
// the prefix boundary.
func TestSystemTableList(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "s.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	ks, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}

	_ = ks.SystemPut("acl:carol", []byte("c"))
	_ = ks.SystemPut("acl:alice", []byte("a"))
	_ = ks.SystemPut("acl:bob", []byte("b"))
	_ = ks.SystemPut("fn:lib1", []byte("x"))

	got, err := ks.SystemList("acl:")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"acl:alice", "acl:bob", "acl:carol"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("list acl: = %v want %v", got, want)
	}

	all, _ := ks.SystemList("")
	if len(all) != 4 {
		t.Fatalf("list all = %v want 4 entries", all)
	}
}

// TestSystemTablePersistAcrossReopen writes entries, commits, closes the file,
// reopens it, and checks the system table came back. This is the property the
// .aki ACL persistence rests on: the table survives a plain restart with no AOF
// or RDB.
func TestSystemTablePersistAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "s.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatal(err)
	}
	ks, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	// Many entries so the table spills past a single leaf and exercises splits.
	for i := range 200 {
		if err := ks.SystemPut(fmt.Sprintf("u:%04d", i), fmt.Appendf(nil, "line-%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// A regular key too, to prove the system table and the data live side by side.
	db0 := mustDB(t, ks, 0)
	_ = db0.Set([]byte("plain"), []byte("key"), TypeString, EncRaw, -1)
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "s.aki", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := Open(p2)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}

	for i := range 200 {
		v, ok, err := ks2.SystemGet(fmt.Sprintf("u:%04d", i))
		if err != nil || !ok {
			t.Fatalf("after reopen get u:%04d ok %v err %v", i, ok, err)
		}
		if want := fmt.Sprintf("line-%d", i); string(v) != want {
			t.Fatalf("after reopen u:%04d = %q want %q", i, v, want)
		}
	}
	names, _ := ks2.SystemList("u:")
	if len(names) != 200 {
		t.Fatalf("after reopen list = %d want 200", len(names))
	}
	rdb0 := mustDB(t, ks2, 0)
	if b, _, found, _ := rdb0.Get([]byte("plain")); !found || string(b) != "key" {
		t.Fatalf("plain key gone after reopen: %q found %v", b, found)
	}
}
