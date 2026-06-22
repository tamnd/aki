package command

import (
	"strings"
	"testing"
	"time"
)

// flatMap turns a RESP2 map reply, which arrives as a flat array of alternating
// key and value bulk strings, into a Go map for assertions.
func flatMap(t *testing.T, v any) map[string]string {
	t.Helper()
	arr := asArray(t, v)
	if len(arr)%2 != 0 {
		t.Fatalf("map reply has odd length %d", len(arr))
	}
	m := map[string]string{}
	for i := 0; i < len(arr); i += 2 {
		k, _ := arr[i].(string)
		val, _ := arr[i+1].(string)
		m[k] = val
	}
	return m
}

// TestSentinelGetMasterAddrStandalone checks a standalone instance reports its
// own address for the configured master name and rejects an unknown name.
func TestSentinelGetMasterAddrStandalone(t *testing.T) {
	r, c, host, port := startDataAddr(t)

	addr := asArray(t, sendArgs(t, r, c, "SENTINEL", "get-master-addr-by-name", "mymaster"))
	if len(addr) != 2 {
		t.Fatalf("get-master-addr = %v want [ip port]", addr)
	}
	if addr[0] != host || addr[1] != port {
		t.Fatalf("get-master-addr = %v want [%s %s]", addr, host, port)
	}

	got := sendArgs(t, r, c, "SENTINEL", "get-master-addr-by-name", "nosuch")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "No such master") {
		t.Fatalf("unknown name = %v want No such master error", got)
	}
}

// TestSentinelMasters checks the masters array carries one entry whose map names
// the configured master and flags it as a master.
func TestSentinelMasters(t *testing.T) {
	r, c := startData(t)

	masters := asArray(t, sendArgs(t, r, c, "SENTINEL", "masters"))
	if len(masters) != 1 {
		t.Fatalf("SENTINEL masters = %d entries want 1", len(masters))
	}
	m := flatMap(t, masters[0])
	if m["name"] != "mymaster" {
		t.Fatalf("master name = %q want mymaster", m["name"])
	}
	if m["flags"] != "master" {
		t.Fatalf("master flags = %q want master", m["flags"])
	}
	if m["num-slaves"] != "0" {
		t.Fatalf("num-slaves = %q want 0", m["num-slaves"])
	}
	if m["quorum"] != "1" || m["num-other-sentinels"] != "0" {
		t.Fatalf("quorum/sentinels = %q/%q want 1/0", m["quorum"], m["num-other-sentinels"])
	}
}

// TestSentinelMasterByName checks SENTINEL master returns the map for a known
// name and an error for an unknown one.
func TestSentinelMasterByName(t *testing.T) {
	r, c := startData(t)

	m := flatMap(t, sendArgs(t, r, c, "SENTINEL", "master", "mymaster"))
	if m["name"] != "mymaster" {
		t.Fatalf("master map name = %q want mymaster", m["name"])
	}
	if len(m["runid"]) != 40 {
		t.Fatalf("runid = %q want 40-hex", m["runid"])
	}

	got := sendArgs(t, r, c, "SENTINEL", "master", "nope")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "No such master") {
		t.Fatalf("unknown master = %v want No such master error", got)
	}
}

// TestSentinelMyID checks SENTINEL myid returns the 40-hex sentinel id.
func TestSentinelMyID(t *testing.T) {
	r, c := startData(t)
	id, _ := sendArgs(t, r, c, "SENTINEL", "myid").(string)
	if len(id) != 40 {
		t.Fatalf("SENTINEL myid = %q want 40-hex", id)
	}
}

// TestSentinelIsMasterDownAbstains checks the quorum vote always abstains.
func TestSentinelIsMasterDownAbstains(t *testing.T) {
	r, c := startData(t)
	got := asArray(t, sendArgs(t, r, c, "SENTINEL", "is-master-down-by-addr", "127.0.0.1", "6379", "0", "*"))
	if len(got) != 3 || got[0] != int64(0) || got[1] != "*" || got[2] != int64(0) {
		t.Fatalf("is-master-down = %v want [0 * 0]", got)
	}
}

// TestSentinelDisabled checks the whole family is refused when compat mode is off.
func TestSentinelDisabled(t *testing.T) {
	r, c := startData(t)
	if got := sendArgs(t, r, c, "CONFIG", "SET", "sentinel-compat-mode", "no"); got != "OK" {
		t.Fatalf("CONFIG SET sentinel-compat-mode = %v", got)
	}
	got := sendArgs(t, r, c, "SENTINEL", "masters")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "SENTINEL command disabled") {
		t.Fatalf("SENTINEL while disabled = %v want disabled error", got)
	}
}

// TestSentinelReplicasReflectsAttached brings up a replica and checks SENTINEL
// replicas on the master lists it, and that a replica answers
// get-master-addr-by-name with its upstream master.
func TestSentinelReplicasReflectsAttached(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	// Wait until the master sees the replica attach.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		reps := asArray(t, sendArgs(t, mr, mc, "SENTINEL", "replicas", "mymaster"))
		if len(reps) == 1 {
			rep := flatMap(t, reps[0])
			if rep["flags"] != "slave" {
				t.Fatalf("replica flags = %q want slave", rep["flags"])
			}
			if rep["master-link-status"] != "ok" {
				t.Fatalf("master-link-status = %q want ok", rep["master-link-status"])
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	reps := asArray(t, sendArgs(t, mr, mc, "SENTINEL", "replicas", "mymaster"))
	if len(reps) != 1 {
		t.Fatalf("SENTINEL replicas = %d want 1", len(reps))
	}

	// The replica resolves the master name to its upstream master address.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		addr := asArray(t, sendArgs(t, rr, rc, "SENTINEL", "get-master-addr-by-name", "mymaster"))
		if len(addr) == 2 && addr[0] == mHost && addr[1] == mPort {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replica get-master-addr never resolved to %s:%s", mHost, mPort)
}
