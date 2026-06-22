package command

import (
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// openEngine builds a dispatcher backed by a keyspace over fs/name. The caller
// reuses the same fs across an open/close cycle to prove ACL state survives a
// plain restart with no AOF or RDB.
func openEngine(t *testing.T, fs vfs.VFS, name string, create bool) (*Dispatcher, *pager.Pager) {
	t.Helper()
	var (
		p   *pager.Pager
		err error
	)
	if create {
		p, err = pager.Create(fs, name, pager.Options{PageSize: 4096, DBCount: 16})
	} else {
		p, err = pager.Open(fs, name, pager.Options{})
	}
	if err != nil {
		t.Fatalf("pager: %v", err)
	}
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("keyspace: %v", err)
	}
	d := New(Config{Databases: 16, Engine: NewEngine(ks)})
	return d, p
}

// TestACLPersistRoundTrip adds a user, persists it, then reopens the file and
// loads the ACL back. The user and its rules must come back intact.
func TestACLPersistRoundTrip(t *testing.T) {
	fs := vfs.NewMem()

	d, p := openEngine(t, fs, "acl.aki", true)
	u := &aclUser{name: "alice"}
	if err := applyACLRules(u, []string{"on", ">secret", "~app:*", "+get", "+set"}); err != nil {
		t.Fatalf("apply rules: %v", err)
	}
	want := aclLine(u)
	d.acl.mu.Lock()
	d.acl.users["alice"] = u
	d.acl.mu.Unlock()
	d.persistACL()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, p2 := openEngine(t, fs, "acl.aki", false)
	defer func() { _ = p2.Close() }()
	if err := d2.LoadACLFromKeyspace(); err != nil {
		t.Fatalf("load: %v", err)
	}
	got := d2.acl.get("alice")
	if got == nil {
		t.Fatal("alice missing after reopen")
	}
	if line := aclLine(got); line != want {
		t.Fatalf("alice line = %q want %q", line, want)
	}
	if d2.acl.get("default") == nil {
		t.Fatal("default user missing after reopen")
	}
}

// TestACLPersistDelUserRemovesEntry persists two users, drops one, persists
// again, and checks the dropped user does not return after a reopen.
func TestACLPersistDelUserRemovesEntry(t *testing.T) {
	fs := vfs.NewMem()

	d, p := openEngine(t, fs, "acl.aki", true)
	for _, name := range []string{"bob", "carol"} {
		u := &aclUser{name: name}
		if err := applyACLRules(u, []string{"on", "nopass", "~*", "+@all"}); err != nil {
			t.Fatalf("apply rules %s: %v", name, err)
		}
		d.acl.mu.Lock()
		d.acl.users[name] = u
		d.acl.mu.Unlock()
	}
	d.persistACL()
	if !d.acl.del("bob") {
		t.Fatal("del bob reported false")
	}
	d.persistACL()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, p2 := openEngine(t, fs, "acl.aki", false)
	defer func() { _ = p2.Close() }()
	if err := d2.LoadACLFromKeyspace(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if d2.acl.get("bob") != nil {
		t.Fatal("bob still present after delete and reopen")
	}
	if d2.acl.get("carol") == nil {
		t.Fatal("carol missing after reopen")
	}
}

// TestACLPersistSkippedWithAclFile checks that an external aclfile turns the .aki
// persistence off. The aclfile stays authoritative, so nothing is written to the
// system table.
func TestACLPersistSkippedWithAclFile(t *testing.T) {
	fs := vfs.NewMem()
	d, p := openEngine(t, fs, "acl.aki", true)
	defer func() { _ = p.Close() }()
	d.acl.aclFile = "/tmp/aki-acl.conf"

	u := &aclUser{name: "dave"}
	if err := applyACLRules(u, []string{"on", "nopass", "~*", "+@all"}); err != nil {
		t.Fatalf("apply rules: %v", err)
	}
	d.acl.mu.Lock()
	d.acl.users["dave"] = u
	d.acl.mu.Unlock()
	d.persistACL()

	lines, err := d.engine.systemEntries(aclEntryPrefix)
	if err != nil {
		t.Fatalf("systemEntries: %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("system table has %d ACL entries, want 0 when aclfile is set", len(lines))
	}
}

// TestLoadLinesSynthesizesDefault confirms loadLines always leaves a default user
// even when the persisted set names none.
func TestLoadLinesSynthesizesDefault(t *testing.T) {
	a := newACLRegistry("")
	err := a.loadLines(map[string]string{
		"eve": "user eve on nopass ~* +@all",
	})
	if err != nil {
		t.Fatalf("loadLines: %v", err)
	}
	if a.get("eve") == nil {
		t.Fatal("eve missing")
	}
	def := a.get("default")
	if def == nil || !def.on || !def.nopass {
		t.Fatalf("default user not synthesized correctly: %+v", def)
	}
}
