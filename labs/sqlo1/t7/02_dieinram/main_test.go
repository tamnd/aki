package main

import "testing"

// A small run keeps the accounting honest: every volatile record lands
// in exactly one bucket, and with TTL far past the drain interval the
// drained records are overwhelmingly alive.
func TestAccountingAddsUp(t *testing.T) {
	r, err := runOne(config{
		dir:       t.TempDir(),
		val:       1024,
		mbps:      8,
		intervals: 4,
		ttlMs:     60_000,
		volPct:    100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.written == 0 {
		t.Fatal("no volatile records written")
	}
	if got := r.drainedAlive + r.drainedDead + r.diedInRAM + r.pending; got != r.written {
		t.Fatalf("buckets sum to %d, written %d", got, r.written)
	}
	if r.drainedDead > r.drainedAlive/10 {
		t.Fatalf("ttl 60x the interval yet %d of %d drains were dead", r.drainedDead, r.drainedAlive+r.drainedDead)
	}
	if r.drainedAlive == 0 {
		t.Fatal("nothing drained: the dirty threshold never tripped")
	}
}
