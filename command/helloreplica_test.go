package command

import "testing"

// TestHelloRoleMaster checks a standalone node reports role "master" in the
// HELLO handshake map.
func TestHelloRoleMaster(t *testing.T) {
	r, c := startData(t)
	m := flatMapAny(t, sendReply(t, r, c, "HELLO"))
	if m["role"] != "master" {
		t.Fatalf("HELLO role = %v want master", m["role"])
	}
}

// TestHelloRoleReplica brings up a master and a replica and checks the replica
// reports role "replica" in HELLO, while the master still reports "master".
// Redis uses "replica" here, not "slave".
func TestHelloRoleReplica(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	waitForInfoLine(t, rr, rc, "role:slave")

	m := flatMapAny(t, sendReply(t, rr, rc, "HELLO"))
	if m["role"] != "replica" {
		t.Fatalf("replica HELLO role = %v want replica", m["role"])
	}

	mm := flatMapAny(t, sendReply(t, mr, mc, "HELLO"))
	if mm["role"] != "master" {
		t.Fatalf("master HELLO role = %v want master", mm["role"])
	}
}
