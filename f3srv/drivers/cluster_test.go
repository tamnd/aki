package drivers

import (
	"strings"
	"testing"
)

// TestClusterInfoStandalone checks CLUSTER INFO reports cluster support off, the
// one field a cluster-aware driver keys on to stay on its standalone path.
func TestClusterInfoStandalone(t *testing.T) {
	_, nc, br := startServer(t)
	body, ok := sendCmd(t, br, nc, "CLUSTER", "INFO").(string)
	if !ok {
		t.Fatalf("CLUSTER INFO did not return a bulk string")
	}
	if !strings.Contains(body, "cluster_enabled:0") {
		t.Fatalf("CLUSTER INFO = %q, want cluster_enabled:0", body)
	}
	if !strings.Contains(body, "cluster_known_nodes:1") {
		t.Fatalf("CLUSTER INFO = %q, want cluster_known_nodes:1", body)
	}
}

// TestClusterMyIDStable checks CLUSTER MYID is a 40-hex-character id and is the
// same across two calls, the per-run stability redis guarantees.
func TestClusterMyID(t *testing.T) {
	_, nc, br := startServer(t)
	first, ok := sendCmd(t, br, nc, "CLUSTER", "MYID").(string)
	if !ok || len(first) != 40 {
		t.Fatalf("CLUSTER MYID = %q, want a 40-char id", first)
	}
	for _, c := range first {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("CLUSTER MYID = %q, want hex only", first)
		}
	}
	if second := sendCmd(t, br, nc, "CLUSTER", "MYID"); second != first {
		t.Fatalf("CLUSTER MYID changed across calls: %v then %v", first, second)
	}
}

// TestClusterSlotsEmpty checks CLUSTER SLOTS and SHARDS are empty arrays on a
// standalone node, and CLUSTER NODES names this node as myself,master.
func TestClusterSlotsAndNodes(t *testing.T) {
	_, nc, br := startServer(t)
	if arr, ok := sendCmd(t, br, nc, "CLUSTER", "SLOTS").([]any); !ok || len(arr) != 0 {
		t.Fatalf("CLUSTER SLOTS = %v, want an empty array", arr)
	}
	if arr, ok := sendCmd(t, br, nc, "CLUSTER", "SHARDS").([]any); !ok || len(arr) != 0 {
		t.Fatalf("CLUSTER SHARDS = %v, want an empty array", arr)
	}
	id := sendCmd(t, br, nc, "CLUSTER", "MYID").(string)
	nodes, ok := sendCmd(t, br, nc, "CLUSTER", "NODES").(string)
	if !ok || !strings.Contains(nodes, "myself,master") {
		t.Fatalf("CLUSTER NODES = %q, want a myself,master self line", nodes)
	}
	if !strings.HasPrefix(nodes, id) {
		t.Fatalf("CLUSTER NODES = %q, want it to start with MYID %s", nodes, id)
	}
}

// TestClusterMutatingDisabled checks a mutating cluster subcommand is refused
// with the cluster-disabled error redis gives when cluster support is off.
func TestClusterMutatingDisabled(t *testing.T) {
	_, nc, br := startServer(t)
	reply, ok := sendCmd(t, br, nc, "CLUSTER", "ADDSLOTS", "1").(errorReply)
	if !ok || !strings.Contains(string(reply), "cluster support disabled") {
		t.Fatalf("CLUSTER ADDSLOTS = %v, want the cluster-disabled error", reply)
	}
}

// TestReplicaofNoOne checks REPLICAOF NO ONE succeeds as the promotion no-op it
// is on a standalone primary, while a host/port form is refused.
func TestReplicaofNoOne(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "REPLICAOF", "NO", "ONE"); got != "OK" {
		t.Fatalf("REPLICAOF NO ONE = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "SLAVEOF", "no", "one"); got != "OK" {
		t.Fatalf("SLAVEOF no one = %v, want OK", got)
	}
	if _, ok := sendCmd(t, br, nc, "REPLICAOF", "localhost", "6379").(errorReply); !ok {
		t.Fatalf("REPLICAOF host port did not refuse")
	}
}

// TestWaitaofZero checks WAITAOF answers [0,0] with the WAL off and no replicas,
// and refuses a non-integer argument.
func TestWaitaof(t *testing.T) {
	_, nc, br := startServer(t)
	arr, ok := sendCmd(t, br, nc, "WAITAOF", "0", "0", "100").([]any)
	if !ok || len(arr) != 2 || arr[0] != int64(0) || arr[1] != int64(0) {
		t.Fatalf("WAITAOF = %v, want [0 0]", arr)
	}
	if _, ok := sendCmd(t, br, nc, "WAITAOF", "x", "0", "0").(errorReply); !ok {
		t.Fatalf("WAITAOF with a non-integer did not error")
	}
}

// TestReplconfRefused checks the replication-protocol verbs are refused rather
// than answered, since f3 speaks no replication protocol.
func TestReplconfRefused(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "REPLCONF", "GETACK", "*").(errorReply); !ok {
		t.Fatalf("REPLCONF was not refused")
	}
}
