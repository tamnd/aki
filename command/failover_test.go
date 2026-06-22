package command

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// TestFailoverErrors checks the FAILOVER argument and state validation on a node
// with no replicas.
func TestFailoverErrors(t *testing.T) {
	r, c := startData(t)

	got := sendArgs(t, r, c, "FAILOVER")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR FAILOVER requires connected replicas." {
		t.Fatalf("FAILOVER with no replicas = %v", got)
	}

	got = sendArgs(t, r, c, "FAILOVER", "ABORT")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR No failover in progress." {
		t.Fatalf("FAILOVER ABORT with none = %v", got)
	}

	got = sendArgs(t, r, c, "FAILOVER", "ABORT", "FORCE")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR FAILOVER ABORT is not valid with other arguments." {
		t.Fatalf("FAILOVER ABORT FORCE = %v", got)
	}

	got = sendArgs(t, r, c, "FAILOVER", "BOGUS")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR syntax error" {
		t.Fatalf("FAILOVER BOGUS = %v", got)
	}
}

// TestFailoverPromotesReplica brings up a master and a replica, runs FAILOVER on
// the master, and checks the roles swap and the dataset survives on the new
// master.
func TestFailoverPromotesReplica(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	waitForInfoLine(t, mr, mc, "connected_slaves:1")

	if got := sendLine(t, mr, mc, "SET k v"); got != "+OK" {
		t.Fatalf("master SET = %q", got)
	}
	waitForBulk(t, rr, rc, "k", "v")

	if got := sendLine(t, mr, mc, "FAILOVER"); got != "+OK" {
		t.Fatalf("FAILOVER = %q want +OK", got)
	}

	// The old master demotes itself and the chosen replica becomes master.
	waitForInfoLine(t, mr, mc, "role:slave")
	waitForInfoLine(t, rr, rc, "role:master")

	// The new master holds the data and accepts writes.
	if got := bulk(t, rr, rc, "GET k"); got != "v" {
		t.Fatalf("new master GET k = %q want v", got)
	}
	if got := sendLine(t, rr, rc, "SET k2 v2"); got != "+OK" {
		t.Fatalf("write on new master = %q want +OK", got)
	}
}

// waitForInfoLine polls INFO replication until it contains want or the deadline
// passes.
func waitForInfoLine(t *testing.T, r *bufio.Reader, c net.Conn, want string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	var info string
	for time.Now().Before(deadline) {
		info, _ = sendArgs(t, r, c, "INFO", "replication").(string)
		if containsLine(info, want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("INFO replication never showed %q\n%s", want, info)
}
