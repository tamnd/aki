package command

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestAOFFunctionLoadPropagates checks a FUNCTION LOAD after a rewrite lands in
// the incr file as FUNCTION LOAD REPLACE, the idempotent form a replay needs.
func TestAOFFunctionLoadPropagates(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")

	if got := sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet); got != "mylib" {
		t.Fatalf("FUNCTION LOAD = %v", got)
	}

	incr := readIncrFile(t, filepath.Join(dir, "appendonlydir"))
	if !strings.Contains(incr, "FUNCTION") || !strings.Contains(incr, "REPLACE") {
		t.Fatalf("incr missing FUNCTION LOAD REPLACE: %q", incr)
	}
	if !strings.Contains(incr, "mylib") {
		t.Fatalf("incr missing library source: %q", incr)
	}
}

// TestAOFFunctionRewriteBaseRDB checks a rewrite folds the loaded libraries into
// the base RDB as FUNCTION2 records, so a reload restores them even though the
// fresh incr file is empty. The reload clears the in-memory registry first, so the
// function only comes back if the base carried it.
func TestAOFFunctionRewriteBaseRDB(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)

	if got := sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet); got != "mylib" {
		t.Fatalf("FUNCTION LOAD = %v", got)
	}
	// The rewrite must capture the library into the base even though it was loaded
	// before the rewrite ran.
	if got := sendLine(t, r, c, "BGREWRITEAOF"); got != "+Background append only file rewriting started" {
		t.Fatalf("BGREWRITEAOF = %q", got)
	}

	// The fresh incr file is empty: the function lives in the base RDB now, not in a
	// command preamble.
	incr := readIncrFile(t, filepath.Join(dir, "appendonlydir"))
	if strings.Contains(incr, "FUNCTION") {
		t.Fatalf("incr should not carry a FUNCTION preamble: %q", incr)
	}

	if got := sendLine(t, r, c, "DEBUG LOADAOF"); got != "+OK" {
		t.Fatalf("DEBUG LOADAOF = %q", got)
	}

	// The function survives the reload and still runs.
	if got := sendArgs(t, r, c, "FCALL", "myset", "1", "k", "hello"); got != "OK" {
		t.Fatalf("FCALL myset after reload = %v", got)
	}
	if got := sendArgs(t, r, c, "FCALL", "myget", "1", "k"); got != "hello" {
		t.Fatalf("FCALL myget after reload = %v", got)
	}
}

// TestReplicaFullSyncCarriesFunctions checks a replica that attaches AFTER the
// master loaded a function gets it through the full sync RDB, not just over the
// live command stream. The function comes in the snapshot's FUNCTION2 records.
func TestReplicaFullSyncCarriesFunctions(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	// Load the function on the master first, then attach the replica. A full sync
	// has to carry the function for the replica to call it.
	if got := sendArgs(t, mr, mc, "FUNCTION", "LOAD", libGetSet); got != "mylib" {
		t.Fatalf("master FUNCTION LOAD = %v", got)
	}
	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}

	// The replica reports the library through FUNCTION LIST once the full sync RDB
	// has been applied and its functions registered.
	deadline := time.Now().Add(3 * time.Second)
	var last any
	for time.Now().Before(deadline) {
		last = sendArgs(t, rr, rc, "FUNCTION", "LIST")
		if arr, ok := last.([]any); ok && len(arr) == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replica FUNCTION LIST = %v want one library", last)
}

// TestReplicationStreamsFunctionLoad checks a FUNCTION LOAD on the master reaches
// a connected replica over the command stream, so the replica can run the
// function too.
func TestReplicationStreamsFunctionLoad(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	// A key written after the link is up arrives over the stream.
	if got := sendLine(t, mr, mc, "SET k hello"); got != "+OK" {
		t.Fatalf("master SET = %q", got)
	}
	waitForBulk(t, rr, rc, "k", "hello")

	// The function loaded on the master must replicate, then the replica can call it.
	if got := sendArgs(t, mr, mc, "FUNCTION", "LOAD", libGetSet); got != "mylib" {
		t.Fatalf("master FUNCTION LOAD = %v", got)
	}

	deadline := time.Now().Add(3 * time.Second)
	var last any
	for time.Now().Before(deadline) {
		last = sendArgs(t, rr, rc, "FCALL", "myget", "1", "k")
		if last == "hello" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replica FCALL myget = %v want hello", last)
}
