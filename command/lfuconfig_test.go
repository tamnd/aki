package command

import "testing"

// TestLFULogFactorConfig checks that CONFIG SET lfu-log-factor reaches the
// keyspace. With a log factor of 0 the LFU counter climbs on every access, so a
// run of reads raises OBJECT FREQ by one each time. Under the default factor of
// 10 the same reads would almost never bump the counter, so a deterministic climb
// proves the knob is wired through.
func TestLFULogFactorConfig(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "CONFIG SET maxmemory-policy allkeys-lfu")
	_ = sendLine(t, r, c, "CONFIG SET lfu-log-factor 0")
	_ = sendLine(t, r, c, "SET k v")

	// The fresh key seeds at the init value of 5.
	if got := sendLine(t, r, c, "OBJECT FREQ k"); got != ":5" {
		t.Fatalf("seeded freq = %q want :5", got)
	}

	// Ten reads, each an access that increments with probability 1 at factor 0.
	// GET replies with a bulk string, so consume the value line too.
	for range 10 {
		h := sendLine(t, r, c, "GET k")
		_ = readBulk(t, r, h)
	}
	if got := sendLine(t, r, c, "OBJECT FREQ k"); got != ":15" {
		t.Fatalf("freq after 10 reads at factor 0 = %q want :15", got)
	}
}
