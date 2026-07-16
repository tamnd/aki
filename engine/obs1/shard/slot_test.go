package shard

import "testing"

// TestCRC16Check pins the CRC-16/XMODEM parameters with the doc 02
// section 2.1 check value.
func TestCRC16Check(t *testing.T) {
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("crc16 check value = %#04x, want 0x31C3", got)
	}
}

// TestHashSlotPins pins byte-exact slots for a few well-known keys so a
// table or rule regression cannot hide behind self-consistency.
func TestHashSlotPins(t *testing.T) {
	for _, c := range []struct {
		key  string
		slot int
	}{
		{"foo", 12182},
		{"bar", 5061},
		{"", 0},
	} {
		if got := HashSlot([]byte(c.key)); got != c.slot {
			t.Fatalf("HashSlot(%q) = %d, want %d", c.key, got, c.slot)
		}
	}
}

// TestHashTagRule walks the Redis hash-tag rule: only the bytes between
// the first '{' and the first '}' after it are hashed, and only when at
// least one byte sits between them.
func TestHashTagRule(t *testing.T) {
	for _, c := range []struct {
		key, covers string
	}{
		{"{user1000}.following", "user1000"},
		{"{user1000}.followers", "user1000"},
		{"foo{}{bar}", "foo{}{bar}"}, // first {} is empty: whole key
		{"foo{{bar}}zap", "{bar"},    // first { to the first } after it
		{"foo{bar}{zap}", "bar"},     // only the first pair counts
		{"foo{bar", "foo{bar"},       // no } after {: whole key
		{"plain", "plain"},
		{"{x}", "x"},
	} {
		if got := string(hashTagged([]byte(c.key))); got != c.covers {
			t.Fatalf("hashTagged(%q) covers %q, want %q", c.key, got, c.covers)
		}
	}
	if HashSlot([]byte("{user1000}.following")) != HashSlot([]byte("{user1000}.followers")) {
		t.Fatal("keys sharing a hash tag must share a slot")
	}
	if HashSlot([]byte("{user1000}.following")) != HashSlot([]byte("user1000")) {
		t.Fatal("a tagged key must hash as its bare tag")
	}
}

// TestGroupOfSlot pins the doc 02 section 1.2 formula at the default G
// and the cap that keeps a non-divisor G total.
func TestGroupOfSlot(t *testing.T) {
	for _, c := range []struct {
		slot, g, group int
	}{
		{0, 128, 0},
		{127, 128, 0},
		{128, 128, 1},
		{16383, 128, 127},
		{0, 8, 0},
		{2047, 8, 0},
		{2048, 8, 1},
		{16383, 8, 7},
		{16383, 100, 99}, // non-divisor G: capped, last group wider
	} {
		if got := groupOfSlot(c.slot, c.g); got != c.group {
			t.Fatalf("groupOfSlot(%d, %d) = %d, want %d", c.slot, c.g, got, c.group)
		}
	}
}

// TestSlotRouting checks the runtime route end to end: keys sharing a
// hash tag land on one shard, GroupOf agrees with the formula, and every
// shard index stays in range across the whole slot space.
func TestSlotRouting(t *testing.T) {
	rt := New(4, testArena, testSeg)
	if rt.Groups() != DefaultSlotGroups {
		t.Fatalf("Groups() = %d, want %d", rt.Groups(), DefaultSlotGroups)
	}
	a := rt.ShardOf([]byte("{acct:77}:balance"))
	b := rt.ShardOf([]byte("{acct:77}:history"))
	if a != b {
		t.Fatalf("tagged keys split: shard %d vs %d", a, b)
	}
	if g := rt.GroupOf([]byte("foo")); g != groupOfSlot(12182, DefaultSlotGroups) {
		t.Fatalf("GroupOf(foo) = %d, want %d", g, groupOfSlot(12182, DefaultSlotGroups))
	}
	seen := make(map[int]bool)
	for slot := 0; slot < totalSlots; slot++ {
		g := groupOfSlot(slot, DefaultSlotGroups)
		sh := g % rt.Shards()
		if sh < 0 || sh >= rt.Shards() {
			t.Fatalf("slot %d routes to shard %d of %d", slot, sh, rt.Shards())
		}
		seen[sh] = true
	}
	if len(seen) != rt.Shards() {
		t.Fatalf("route reaches %d of %d shards", len(seen), rt.Shards())
	}
}

// TestOpenSlotGroups checks the Config knob and its default.
func TestOpenSlotGroups(t *testing.T) {
	rt, err := Open(Config{Shards: 2, ArenaBytes: testArena, SegBytes: testSeg, SlotGroups: 8})
	if err != nil {
		t.Fatal(err)
	}
	if rt.Groups() != 8 {
		t.Fatalf("Groups() = %d, want 8", rt.Groups())
	}
	rt2, err := Open(Config{Shards: 2, ArenaBytes: testArena, SegBytes: testSeg})
	if err != nil {
		t.Fatal(err)
	}
	if rt2.Groups() != DefaultSlotGroups {
		t.Fatalf("default Groups() = %d, want %d", rt2.Groups(), DefaultSlotGroups)
	}
}
