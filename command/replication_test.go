package command

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
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

// TestReplBacklogCopyFrom checks the ring buffer returns the right tail for a
// resume offset, both before and after the buffer wraps and overwrites old bytes.
func TestReplBacklogCopyFrom(t *testing.T) {
	b := newReplBacklog(8, 0)
	b.feed([]byte("abcd"))
	// Offset 0 is the start, so the whole content comes back.
	if got, ok := b.copyFrom(0); !ok || string(got) != "abcd" {
		t.Fatalf("copyFrom(0) = %q %v want abcd true", got, ok)
	}
	// A mid-stream offset returns only the tail.
	if got, ok := b.copyFrom(2); !ok || string(got) != "cd" {
		t.Fatalf("copyFrom(2) = %q %v want cd true", got, ok)
	}
	// The end offset returns an empty slice, which is a valid resume with nothing
	// buffered yet.
	if got, ok := b.copyFrom(4); !ok || len(got) != 0 {
		t.Fatalf("copyFrom(4) = %q %v want empty true", got, ok)
	}
	// An offset past the end is out of range.
	if _, ok := b.copyFrom(5); ok {
		t.Fatalf("copyFrom(5) should be out of range")
	}

	// Overflow the 8-byte ring so the base offset advances and old bytes are gone.
	b.feed([]byte("efghij")) // total fed 10 bytes, ring holds the last 8 (offsets 2..9)
	if b.off != 2 {
		t.Fatalf("base offset = %d want 2 after overflow", b.off)
	}
	if _, ok := b.copyFrom(1); ok {
		t.Fatalf("copyFrom(1) should be out of range after overwrite")
	}
	if got, ok := b.copyFrom(2); !ok || string(got) != "cdefghij" {
		t.Fatalf("copyFrom(2) = %q %v want cdefghij true", got, ok)
	}
	if got, ok := b.copyFrom(6); !ok || string(got) != "ghij" {
		t.Fatalf("copyFrom(6) = %q %v want ghij true", got, ok)
	}
}

// rawCmd writes a RESP array of bulk strings to conn, the wire form a replica
// sends during the replication handshake.
func rawCmd(t *testing.T, conn net.Conn, parts ...string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(parts)) + "\r\n")
	for _, p := range parts {
		b.WriteString("$" + strconv.Itoa(len(p)) + "\r\n" + p + "\r\n")
	}
	if _, err := conn.Write([]byte(b.String())); err != nil {
		t.Fatalf("rawCmd write: %v", err)
	}
}

// readAvailable reads from br for up to d, returning everything it saw. It is for
// draining the replication stream, which has no single framed reply to parse.
func readAvailable(conn net.Conn, br *bufio.Reader, d time.Duration) string {
	var sb strings.Builder
	buf := make([]byte, 4096)
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := br.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			continue
		}
	}
	return sb.String()
}

// TestPartialResyncContinue drives the master side of partial resync over a raw
// socket. A fake replica does a full resync to learn the replid and offset, then
// disconnects. After the master takes more writes a fresh connection presents the
// cached replid and offset and the master answers +CONTINUE and streams the bytes
// the replica missed rather than a whole new snapshot.
func TestPartialResyncContinue(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	_ = mr

	// First attach: full resync to capture the replid and the stream offset.
	rb, rc := dial(t, mHost+":"+mPort)
	rawCmd(t, rc, "REPLCONF", "listening-port", "0")
	if _, err := rb.ReadString('\n'); err != nil { // +OK
		t.Fatalf("REPLCONF reply: %v", err)
	}
	rawCmd(t, rc, "PSYNC", "?", "-1")
	line, err := rb.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "+FULLRESYNC ") {
		t.Fatalf("PSYNC reply = %q err %v want +FULLRESYNC", line, err)
	}
	fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "+")))
	replid := fields[1]
	offset := fields[2]
	// Consume the RDB bulk so the socket is clean, then drop the connection.
	blobLine, err := rb.ReadString('\n')
	if err != nil || !strings.HasPrefix(blobLine, "$") {
		t.Fatalf("RDB bulk header = %q err %v", blobLine, err)
	}
	blobLen, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(blobLine, "$")))
	if _, err := io.ReadFull(rb, make([]byte, blobLen)); err != nil {
		t.Fatalf("read RDB blob: %v", err)
	}
	_ = rc.Close()

	// The master takes writes while the replica is away. These land in the backlog.
	if got := sendLine(t, mr, mc, "SET pa 1"); got != "+OK" {
		t.Fatalf("SET pa = %q", got)
	}
	if got := sendLine(t, mr, mc, "SET pb 2"); got != "+OK" {
		t.Fatalf("SET pb = %q", got)
	}

	// Reconnect and present the cached replid and offset. The master should resume.
	rb2, rc2 := dial(t, mHost+":"+mPort)
	rawCmd(t, rc2, "PSYNC", replid, offset)
	stream := readAvailable(rc2, rb2, 1500*time.Millisecond)
	if !strings.Contains(stream, "+CONTINUE") {
		t.Fatalf("partial resync reply did not start with +CONTINUE:\n%q", stream)
	}
	if !strings.Contains(stream, "pa") || !strings.Contains(stream, "pb") {
		t.Fatalf("partial resync stream missing the missed writes:\n%q", stream)
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
