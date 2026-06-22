package keyspace

import "testing"

func TestCheckCounts(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	now := nowMillis()
	if err := db.Set([]byte("a"), []byte("1"), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := db.Set([]byte("b"), []byte("2"), TypeString, EncRaw, now+3600_000); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	checks, err := ks.Check()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	c := checks[0]
	if c.Entries != 2 {
		t.Errorf("Entries = %d, want 2", c.Entries)
	}
	if c.Live != 2 {
		t.Errorf("Live = %d, want 2", c.Live)
	}
	if c.Expires != 1 {
		t.Errorf("Expires = %d, want 1", c.Expires)
	}
	if c.OrderErrors != 0 {
		t.Errorf("OrderErrors = %d, want 0", c.OrderErrors)
	}
	if c.BadHeaders != 0 {
		t.Errorf("BadHeaders = %d, want 0", c.BadHeaders)
	}
	if c.FutureTTL != 0 {
		t.Errorf("FutureTTL = %d, want 0", c.FutureTTL)
	}
}

func TestFixFutureTTLs(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	now := nowMillis()
	if err := db.Set([]byte("good"), []byte("x"), TypeString, EncRaw, now+3600_000); err != nil {
		t.Fatalf("set good: %v", err)
	}
	if err := db.Set([]byte("bad"), []byte("y"), TypeString, EncRaw, now+farFutureMs*2); err != nil {
		t.Fatalf("set bad: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	checks, err := ks.Check()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if checks[0].FutureTTL != 1 {
		t.Fatalf("FutureTTL = %d, want 1", checks[0].FutureTTL)
	}

	n, err := ks.FixFutureTTLs()
	if err != nil {
		t.Fatalf("fix: %v", err)
	}
	if n != 1 {
		t.Fatalf("fixed = %d, want 1", n)
	}

	checks, err = ks.Check()
	if err != nil {
		t.Fatalf("recheck: %v", err)
	}
	if checks[0].FutureTTL != 0 {
		t.Errorf("FutureTTL after fix = %d, want 0", checks[0].FutureTTL)
	}

	// The good key keeps its TTL, the bad key loses it.
	_, h, found, err := db.Peek([]byte("good"))
	if err != nil || !found {
		t.Fatalf("peek good: found=%v err=%v", found, err)
	}
	if !h.HasTTL() {
		t.Errorf("good key lost its TTL")
	}
	_, h2, found, err := db.Peek([]byte("bad"))
	if err != nil || !found {
		t.Fatalf("peek bad: found=%v err=%v", found, err)
	}
	if h2.HasTTL() {
		t.Errorf("bad key still has a TTL after fix")
	}
}
