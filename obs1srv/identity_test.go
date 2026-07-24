package obs1srv_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/obs1srv"
)

func TestNodeIdentityPersists(t *testing.T) {
	dir := t.TempDir()

	first, err := obs1srv.LoadNodeIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first.Node == 0 || first.Incarnation != 1 {
		t.Fatalf("first boot: %+v", first)
	}

	second, err := obs1srv.LoadNodeIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	if second.Node != first.Node {
		t.Fatalf("id changed across restarts: %016x then %016x", first.Node, second.Node)
	}
	if second.Incarnation != 2 {
		t.Fatalf("second boot incarnation: %d", second.Incarnation)
	}

	b, err := os.ReadFile(filepath.Join(dir, "node-id"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(b), "obs1-node ") || !strings.HasSuffix(string(b), " 2\n") {
		t.Fatalf("identity file: %q", b)
	}
}

func TestNodeIdentityEphemeral(t *testing.T) {
	a, err := obs1srv.LoadNodeIdentity("")
	if err != nil {
		t.Fatal(err)
	}
	b, err := obs1srv.LoadNodeIdentity("")
	if err != nil {
		t.Fatal(err)
	}
	if a.Node == 0 || b.Node == 0 || a.Node == b.Node {
		t.Fatalf("ephemeral ids: %016x and %016x", a.Node, b.Node)
	}
	if a.Incarnation != 1 || b.Incarnation != 1 {
		t.Fatalf("ephemeral incarnations: %d and %d", a.Incarnation, b.Incarnation)
	}
}

func TestNodeIdentityRejectsForeignFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "node-id"), []byte("not an identity\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := obs1srv.LoadNodeIdentity(dir); err == nil {
		t.Fatal("foreign file accepted")
	}
	if err := os.WriteFile(filepath.Join(dir, "node-id"), []byte("obs1-node 0000000000000000 3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := obs1srv.LoadNodeIdentity(dir); err == nil {
		t.Fatal("zero node id accepted")
	}
}

func TestIdentityRecords(t *testing.T) {
	id := obs1srv.NodeIdentity{Node: 42, Incarnation: 3}

	j := id.JoinRecord("127.0.0.1:6379", "127.0.0.1:7379", 100, "dev")
	if j.Op != obs1.MemberJoin || j.Node != 42 || j.Incarnation != 3 {
		t.Fatalf("join record: %+v", j)
	}
	if j.Resp != "127.0.0.1:6379" || j.Mesh != "127.0.0.1:7379" || j.Weight != 100 || j.Version != "dev" {
		t.Fatalf("join record: %+v", j)
	}

	l := id.LeaveRecord()
	if l.Op != obs1.MemberLeave || l.Node != 42 || l.Incarnation != 3 {
		t.Fatalf("leave record: %+v", l)
	}
}
