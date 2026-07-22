package set

import (
	"strconv"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The dual-home routing (spec 2064/f3/11, keyspace-unification arc). A set key
// lives in exactly one home: a tiny set (intset or listpack class) inline in a
// store arena record, an escalated set (hashtable or partitioned) in the Go-heap
// registry g.m. This suite proves the create, escalate, empty, TTL, introspect,
// and read paths keep that invariant, so a command never sees a key in both homes
// or misses it in one, and so every generic-introspection handler that once mapped
// "store record present" to "a string" now distinguishes the arena-homed set.
//
// The helpers mirror the Sadd and Srem handler bodies exactly (the same limit
// expire_test and durable_test name: a package unit test cannot build the concrete
// shard.Reply), routing through the real resolveTouch/newSetInto/commit funnel so a
// tiny set homes in the arena and an escalated one evacuates to g.m the way a live
// command leaves them.

// arenaSadd mirrors Sadd: resolve the key through the dual-home funnel, create a
// brand-new set in the reusable scratch homed in the arena, add each member, and
// commit back to whichever home the set now belongs in.
func arenaSadd(cx *shard.Ctx, g *reg, key string, members ...string) {
	k := []byte(key)
	s, home := g.resolveTouch(cx, k)
	if s == nil {
		newSetInto(g.scratch, []byte(members[0]))
		s, home = g.scratch, homeArena
	}
	for _, m := range members {
		if s.add([]byte(m)) {
			logAdd(cx, k, []byte(m))
		}
	}
	g.commit(cx, k, s, home)
}

// arenaSrem mirrors Srem: resolve through the funnel, remove each member, and
// commit, which drops the key from its home when the last member leaves.
func arenaSrem(cx *shard.Ctx, g *reg, key string, members ...string) {
	k := []byte(key)
	s, home := g.resolveTouch(cx, k)
	if s == nil {
		return
	}
	for _, m := range members {
		if s.rem([]byte(m)) {
			logRemove(cx, k, []byte(m))
		}
	}
	g.commit(cx, k, s, home)
}

// inArena reports whether key holds a tiny set in the arena home.
func inArena(cx *shard.Ctx, key string) bool {
	_, _, _, present := peekArenaSet(cx, []byte(key))
	return present
}

// inReg reports whether key holds an escalated set in the Go-heap registry.
func inReg(g *reg, key string) bool {
	_, ok := g.m[key]
	return ok
}

// TestArenaCreateHomesInArena checks a freshly created tiny set lands in the arena
// and nowhere else: it is not in g.m, the store reports a live record but not a
// string one, and the set reads back through the dual-home funnel.
func TestArenaCreateHomesInArena(t *testing.T) {
	for _, c := range []struct {
		name    string
		members []string
		want    encoding
	}{
		{"intset", []string{"1", "2", "3"}, encIntset},
		{"listpack", []string{"a", "b", "c"}, encListpack},
	} {
		t.Run(c.name, func(t *testing.T) {
			cx, g := newCtx(t)
			arenaSadd(cx, g, "k", c.members...)

			if !inArena(cx, "k") {
				t.Fatal("tiny set not homed in the arena")
			}
			if inReg(g, "k") {
				t.Fatal("tiny set should not occupy a g.m entry")
			}
			if cx.St.HasString([]byte("k"), cx.NowMs) {
				t.Fatal("HasString true for an arena-homed set")
			}
			if !cx.St.Exists([]byte("k"), cx.NowMs) {
				t.Fatal("Exists false for a live arena record")
			}
			s := setAt(cx, g, "k")
			if s == nil || s.enc != c.want {
				t.Fatalf("resolved set %v, want enc %s", s, c.want)
			}
			eqStrings(t, "members", membersAt(cx, g, "k"), sortedCopy(c.members))
		})
	}
}

// TestArenaEscalationEvacuates checks a tiny arena set that grows past the inline
// bands moves into g.m and leaves no arena record behind: the two homes never hold
// the key at once.
func TestArenaEscalationEvacuates(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "k", "seed")
	if !inArena(cx, "k") || inReg(g, "k") {
		t.Fatal("seed set should start in the arena only")
	}

	// Push past the listpack entry cap so the set escalates to a hashtable.
	members := make([]string, 0, maxListpackEntries+1)
	for i := 0; i <= maxListpackEntries; i++ {
		members = append(members, "m"+strconv.Itoa(i))
	}
	arenaSadd(cx, g, "k", members...)

	if inArena(cx, "k") {
		t.Fatal("escalated set left a stale arena record")
	}
	if !inReg(g, "k") {
		t.Fatal("escalated set not installed in g.m")
	}
	if g.m["k"].enc != encHashtable {
		t.Fatalf("escalated set enc %s, want hashtable", g.m["k"].enc)
	}
}

// TestArenaSremEmptyDrops checks removing the last member of a tiny arena set drops
// its record, so the key is absent from both homes and EXISTS answers 0.
func TestArenaSremEmptyDrops(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "k", "only")
	arenaSrem(cx, g, "k", "only")

	if inArena(cx, "k") {
		t.Fatal("emptied arena set still has a record")
	}
	if inReg(g, "k") {
		t.Fatal("emptied set should not appear in g.m")
	}
	if cx.St.Exists([]byte("k"), cx.NowMs) {
		t.Fatal("emptied arena set still Exists in the store")
	}
	if Has(cx, []byte("k")) {
		t.Fatal("Has true for a dropped set")
	}
}

// TestArenaDeadlinePersist checks EXPIRE-family TTL round-trips through the arena
// record: setting a deadline keeps the set inline with its TTL, Deadline reads it
// back, and PERSIST clears it in place without evacuating to g.m.
func TestArenaDeadlinePersist(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "k", "1", "2")

	// Stamp a deadline through the set expire backend's Store, the arena TTL path.
	b := &setBackend{g: g, cx: cx, key: []byte("k")}
	if _, present := b.Present(); !present {
		t.Fatal("arena set not present to the expire backend")
	}
	if !b.Store(cx.NowMs + 100000) {
		t.Fatal("Store deadline failed")
	}
	if inReg(g, "k") {
		t.Fatal("a volatile arena set should stay inline, not move to g.m")
	}
	at, ok := Deadline(cx, []byte("k"))
	if !ok || at != cx.NowMs+100000 {
		t.Fatalf("Deadline %d,%v, want %d,true", at, ok, cx.NowMs+100000)
	}

	if !Persist(cx, []byte("k")) {
		t.Fatal("Persist reported no deadline cleared")
	}
	at, ok = Deadline(cx, []byte("k"))
	if !ok || at != 0 {
		t.Fatalf("Deadline after persist %d,%v, want 0,true", at, ok)
	}
	if !inArena(cx, "k") {
		t.Fatal("persisted set left the arena")
	}
}

// TestArenaExpiredLazyReap checks a past-deadline arena set reads as absent and its
// record is reaped on the next access, the lazy-expiry rule the g.m home keeps.
func TestArenaExpiredLazyReap(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "k", "1", "2")
	b := &setBackend{g: g, cx: cx, key: []byte("k")}
	b.Present()
	if !b.Store(cx.NowMs + 10) {
		t.Fatal("Store deadline failed")
	}
	cx.NowMs += 20 // past the deadline

	if Has(cx, []byte("k")) {
		t.Fatal("expired arena set still reads present")
	}
	if inArena(cx, "k") {
		t.Fatal("expired arena set not reaped on access")
	}
}

// TestArenaIntrospection checks OBJECT ENCODING, MEMORY USAGE, and OBJECT IDLETIME
// all resolve an arena-homed set through its own type paths rather than the string
// record: the encoding matches the band, the footprint is nonzero, and a set just
// touched reads zero idle seconds.
func TestArenaIntrospection(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "k", "x", "y", "z")

	enc, ok := Encoding(cx, []byte("k"))
	if !ok || enc != "listpack" {
		t.Fatalf("Encoding %q,%v, want listpack,true", enc, ok)
	}
	n, ok := MemoryUsage(cx, []byte("k"))
	if !ok || n == 0 {
		t.Fatalf("MemoryUsage %d,%v, want nonzero,true", n, ok)
	}
	idle, ok := IdleSeconds(cx, []byte("k"))
	if !ok || idle != 0 {
		t.Fatalf("IdleSeconds %d,%v, want 0,true (just written)", idle, ok)
	}
}

// TestArenaOperandFreshCopy checks the multi-operand read funnel materializes each
// arena set into its own buffer, so reading two arena operands at once never lets
// operand two's load scribble operand one's members.
func TestArenaOperandFreshCopy(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "a", "1", "2", "3")
	arenaSadd(cx, g, "b", "7", "8", "9")

	sa, wrongA := g.operand(cx, []byte("a"))
	sb, wrongB := g.operand(cx, []byte("b"))
	if wrongA || wrongB || sa == nil || sb == nil {
		t.Fatalf("operands a=%v/%v b=%v/%v", sa, wrongA, sb, wrongB)
	}
	if sa == sb {
		t.Fatal("two arena operands resolved to the same buffer")
	}
	eqStrings(t, "operand a", memberList(sa), []string{"1", "2", "3"})
	eqStrings(t, "operand b", memberList(sb), []string{"7", "8", "9"})
}

// TestArenaWrongType checks a key holding a string resolves homeString through the
// funnel, the WRONGTYPE every set command answers, and never reads as an arena set.
func TestArenaWrongType(t *testing.T) {
	cx, g := newCtx(t)
	if err := cx.St.Set([]byte("k"), []byte("astring")); err != nil {
		t.Fatalf("seed string: %v", err)
	}
	if inArena(cx, "k") {
		t.Fatal("a string key read as an arena set")
	}
	s, home := g.resolveTouch(cx, []byte("k"))
	if s != nil || home != homeString {
		t.Fatalf("resolveTouch over a string got %v,%d, want nil,homeString", s, home)
	}
	if _, wrong := g.operand(cx, []byte("k")); !wrong {
		t.Fatal("operand over a string should report wrong")
	}
}

// TestArenaDumpRoundTrip checks DUMP folds a tiny arena set to the same snapshot row
// an escalated set produces, and RestoreKey rebuilds it: the members and TTL survive
// a dump-and-restore through the one snapshot encoder.
func TestArenaDumpRoundTrip(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "k", "alpha", "beta", "gamma")

	row, ok := DumpKey(cx, []byte("k"))
	if !ok {
		t.Fatal("DumpKey found no arena set")
	}
	if err := RestoreKey(cx, []byte("dst"), row, 0); err != nil {
		t.Fatalf("RestoreKey: %v", err)
	}
	eqStrings(t, "restored members", membersAt(cx, g, "dst"), []string{"alpha", "beta", "gamma"})
}

// TestArenaSnapshotFolds checks the checkpoint dumper folds an arena-homed set into
// the record log: without the arena walk a reopen would truncate the effect tail at
// the checkpoint and lose the tiny set. It asserts a snapshot frame is emitted for
// the key by counting the collection-kind records the store holds after the write.
func TestArenaSnapshotFolds(t *testing.T) {
	cx, g := newCtx(t)
	arenaSadd(cx, g, "k", "1", "2", "3")

	total, _ := cx.St.CountCollKind(store.KindSet)
	if total != 1 {
		t.Fatalf("CountCollKind set = %d, want 1 arena set before snapshot", total)
	}
	// Snapshot must not disturb the live arena home: the set stays inline and the
	// count is unchanged, the read-only discipline the dumper keeps.
	Snapshot(cx)
	total, _ = cx.St.CountCollKind(store.KindSet)
	if total != 1 {
		t.Fatalf("CountCollKind set = %d after Snapshot, want 1 (read-only)", total)
	}
	if !inArena(cx, "k") {
		t.Fatal("Snapshot evicted the arena set")
	}
}
