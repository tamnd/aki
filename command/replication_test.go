package command

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// waitForBulk polls GET key on a replica until it returns want or the deadline
// passes. Replication is asynchronous, so a freshly written key shows up on the
// replica a moment after the master acknowledges the write.
func waitForBulk(t *testing.T, r *bufio.Reader, c net.Conn, key, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		last = bulk(t, r, c, "GET "+key)
		if last == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replica GET %s = %q want %q", key, last, want)
}

// TestReplicationFullSyncAndStream brings up two instances, points one at the
// other with REPLICAOF, and checks that the pre-existing dataset arrives by full
// resync and that later writes arrive over the command stream.
func TestReplicationFullSyncAndStream(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	// A key written before the replica attaches must come across in the RDB snapshot.
	if got := sendLine(t, mr, mc, "SET before snap"); got != "+OK" {
		t.Fatalf("master SET before = %q", got)
	}

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q want +OK", got)
	}

	waitForBulk(t, rr, rc, "before", "snap")

	// A key written after the link is up must arrive over the live stream.
	if got := sendLine(t, mr, mc, "SET after stream"); got != "+OK" {
		t.Fatalf("master SET after = %q", got)
	}
	waitForBulk(t, rr, rc, "after", "stream")
}

// TestReplicaIsReadOnly checks a replica refuses client writes while it is
// following a master, and accepts them again after REPLICAOF NO ONE.
func TestReplicaIsReadOnly(t *testing.T) {
	_, _, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	// Give the link a moment to come up so the role is settled.
	time.Sleep(200 * time.Millisecond)

	got := sendLine(t, rr, rc, "SET k v")
	if got == "" || got[0] != '-' {
		t.Fatalf("write on replica = %q want READONLY error", got)
	}

	if got := sendLine(t, rr, rc, "REPLICAOF NO ONE"); got != "+OK" {
		t.Fatalf("REPLICAOF NO ONE = %q", got)
	}
	if got := sendLine(t, rr, rc, "SET k v"); got != "+OK" {
		t.Fatalf("write after promotion = %q want +OK", got)
	}
}

// TestInfoReplicationRoles checks INFO replication reports the master side with a
// connected slave and the replica side with its master host and link status.
func TestInfoReplicationRoles(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	// Wait until the master sees the replica attach.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, _ := sendArgs(t, mr, mc, "INFO", "replication").(string)
		if containsLine(info, "connected_slaves:1") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	minfo, _ := sendArgs(t, mr, mc, "INFO", "replication").(string)
	if !containsLine(minfo, "role:master") {
		t.Fatalf("master INFO missing role:master\n%s", minfo)
	}
	if !containsLine(minfo, "connected_slaves:1") {
		t.Fatalf("master INFO missing connected_slaves:1\n%s", minfo)
	}

	rinfo, _ := sendArgs(t, rr, rc, "INFO", "replication").(string)
	if !containsLine(rinfo, "role:slave") {
		t.Fatalf("replica INFO missing role:slave\n%s", rinfo)
	}
	if !containsLine(rinfo, "master_host:"+mHost) {
		t.Fatalf("replica INFO missing master_host:%s\n%s", mHost, rinfo)
	}
}

// TestWaitReturnsReplicaCount checks WAIT reports how many replicas acknowledged
// a write and that WAIT 0 returns at once.
func TestWaitReturnsReplicaCount(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendArgs(t, mr, mc, "WAIT", "0", "100"); got != int64(0) {
		t.Fatalf("WAIT 0 with no replicas = %v want 0", got)
	}

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	// Wait until the master sees the replica attach before issuing WAIT.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, _ := sendArgs(t, mr, mc, "INFO", "replication").(string)
		if containsLine(info, "connected_slaves:1") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := sendLine(t, mr, mc, "SET k v"); got != "+OK" {
		t.Fatalf("master SET = %q", got)
	}
	got := sendArgs(t, mr, mc, "WAIT", "1", "3000")
	if got != int64(1) {
		t.Fatalf("WAIT 1 = %v want 1", got)
	}
}

// containsLine reports whether the INFO text has a line equal to want, ignoring
// the trailing CR that INFO lines carry.
func containsLine(info, want string) bool {
	for _, ln := range splitLines(info) {
		if ln == want {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}
			out = append(out, line)
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
