package command

import (
	"testing"
	"time"
)

// TestEnoughGoodReplicas checks the min-replicas-to-write gate logic: off by
// default, off when either knob is zero, counting only replicas that acked within
// the lag window, and always open on a replica.
func TestEnoughGoodReplicas(t *testing.T) {
	d := New(Config{})

	// Default config has min-replicas-to-write 0, so the gate is open.
	if !d.enoughGoodReplicas() {
		t.Fatal("gate should be open with min-replicas-to-write 0")
	}

	d.conf.set("min-replicas-to-write", "1")
	d.conf.set("min-replicas-max-lag", "10")

	// A master with no replicas falls short.
	if d.enoughGoodReplicas() {
		t.Fatal("gate should be closed with no replicas")
	}

	// A replica that acked just now counts as good.
	d.repl.replicas[1] = &replicaHandle{ackTime: time.Now()}
	if !d.enoughGoodReplicas() {
		t.Fatal("gate should open with one fresh replica")
	}

	// A replica that has gone quiet past the lag window does not count.
	d.repl.replicas[1].ackTime = time.Now().Add(-30 * time.Second)
	if d.enoughGoodReplicas() {
		t.Fatal("gate should be closed when the only replica is stale")
	}

	// A zero lag window disables the gate even with a stale replica.
	d.conf.set("min-replicas-max-lag", "0")
	if !d.enoughGoodReplicas() {
		t.Fatal("gate should be open with min-replicas-max-lag 0")
	}

	// A replica instance never applies the gate; its master already did.
	d.conf.set("min-replicas-max-lag", "10")
	d.repl.role = "slave"
	if !d.enoughGoodReplicas() {
		t.Fatal("gate should be open on a replica")
	}
}

// TestMinReplicasToWriteGate checks the NOREPLICAS rejection on the wire: with
// the gate on and no replicas, writes are refused but reads still work, and
// turning the gate off restores writes.
func TestMinReplicasToWriteGate(t *testing.T) {
	r, c := startData(t)

	if got := sendLine(t, r, c, "SET k v"); got != "+OK" {
		t.Fatalf("SET before gate = %q", got)
	}

	if got := sendLine(t, r, c, "CONFIG SET min-replicas-to-write 1"); got != "+OK" {
		t.Fatalf("CONFIG SET min-replicas-to-write = %q", got)
	}
	if got := sendLine(t, r, c, "CONFIG SET min-replicas-max-lag 10"); got != "+OK" {
		t.Fatalf("CONFIG SET min-replicas-max-lag = %q", got)
	}

	// A write is refused now that no good replica is connected.
	if got := sendLine(t, r, c, "SET k v2"); got != "-NOREPLICAS Not enough good replicas to write." {
		t.Fatalf("SET under gate = %q want NOREPLICAS", got)
	}

	// Reads are unaffected.
	h := sendLine(t, r, c, "GET k")
	if v := readBulk(t, r, h); v != "v" {
		t.Fatalf("GET under gate = %q want v", v)
	}

	// Turning the gate off restores writes.
	if got := sendLine(t, r, c, "CONFIG SET min-replicas-to-write 0"); got != "+OK" {
		t.Fatalf("CONFIG SET min-replicas-to-write 0 = %q", got)
	}
	if got := sendLine(t, r, c, "SET k v3"); got != "+OK" {
		t.Fatalf("SET after gate off = %q", got)
	}
}
