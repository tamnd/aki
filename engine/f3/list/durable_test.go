package list

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The list effect-log round trip (spec 2064/f3/M8-collection-durability-plan, the list
// arm of slices 2 and 3): run a mix of pushes at both ends, pops, an LSET, an LTRIM, an
// LREM, and an LINSERT against a list store backed by a real .aki, close the file so only
// its durable bytes remain, reopen into a fresh store and an empty registry, replay the
// effect log, and assert every list reads back in the exact order the first run left it.
// It is the list sibling of the set vertical's TestSetEffectLogRecovers: a crash leaves
// nothing in the registry, and the ordered effect log alone rebuilds the lists.
//
// The command handlers write their reply through a shard.Reply a package unit test cannot
// build, so the pushD, popD, lsetD, ltrimD, lremD, and linsertD helpers mirror the
// handlers' mutate-and-log bodies exactly (the saddD convention the set vertical set). The
// delete goes through the real exported Delete, which logs its own key-delete effect.

// pushD mirrors pushCmd with create set: create the list on first push, push each value
// to the named end, and cut a push effect for each. It runs no waiter serve, the deferred
// blocking arc a unit test never exercises.
func pushD(cx *shard.Ctx, g *reg, key string, front bool, vals ...string) {
	k := []byte(key)
	l := g.live(cx, k)
	if l == nil {
		l = newList()
		g.m[key] = l
	}
	for _, v := range vals {
		if front {
			l.pushFront([]byte(v))
		} else {
			l.pushBack([]byte(v))
		}
		logPush(cx, k, []byte(v), front)
	}
	if l.length() == 0 {
		g.drop(k)
	} else {
		g.note(l)
	}
}

// popD mirrors popCmd's single-element form: pop the named end, cut a pop effect, drop the
// key on the last element, and return the popped value.
func popD(cx *shard.Ctx, g *reg, key string, front bool) []byte {
	k := []byte(key)
	l := g.live(cx, k)
	if l == nil {
		return nil
	}
	v := append([]byte(nil), popOne(l, front)...)
	logPop(cx, k, front)
	if l.length() == 0 {
		g.drop(k)
	} else {
		g.note(l)
	}
	return v
}

// lsetD mirrors Lset: overwrite the element at a signed index and cut a set effect for the
// resolved position.
func lsetD(cx *shard.Ctx, g *reg, key string, index int, v string) {
	k := []byte(key)
	l := g.live(cx, k)
	i := normIndex(index, l.length())
	l.setAt(i, []byte(v))
	logSet(cx, k, i, []byte(v))
	g.note(l)
}

// ltrimD mirrors Ltrim: keep the resolved inclusive window, cut a trim effect for the
// bounds passed, and drop the key when the trim clears it.
func ltrimD(cx *shard.Ctx, g *reg, key string, start, stop int) {
	k := []byte(key)
	l := g.live(cx, k)
	lo, hi, ok := clampRange(start, stop, l.length())
	if !ok {
		lo, hi = 1, 0
	}
	l.trim(lo, hi)
	logTrim(cx, k, lo, hi)
	if l.length() == 0 {
		g.drop(k)
	} else {
		g.note(l)
	}
}

// lremD mirrors Lrem: remove matches under the count-sign rule, cut a rem effect when it
// removed one, drop the key on empty, and return the count removed.
func lremD(cx *shard.Ctx, g *reg, key string, count int, v string) int {
	k := []byte(key)
	l := g.live(cx, k)
	removed := l.remove(count, []byte(v))
	if removed > 0 {
		logRem(cx, k, count, []byte(v))
	}
	if l.length() == 0 {
		g.drop(k)
	} else if removed > 0 {
		g.note(l)
	}
	return removed
}

// linsertD mirrors Linsert: place a value before or after the first pivot match, cut an
// insert effect on success, and report whether the pivot was found.
func linsertD(cx *shard.Ctx, g *reg, key string, before bool, pivot, v string) bool {
	k := []byte(key)
	l := g.live(cx, k)
	if !l.insert(before, []byte(pivot), []byte(v)) {
		return false
	}
	logInsert(cx, k, []byte(pivot), []byte(v), before)
	g.note(l)
	return true
}

// elements reads a list back in head-to-tail order, the order every reply and the
// snapshot run preserve. A nil list reads as no elements.
func elements(l *list) []string {
	if l == nil {
		return nil
	}
	var out []string
	l.each(func(v []byte) { out = append(out, string(v)) })
	return out
}

func listDurStore(t *testing.T, path string, create bool) (*akifile.File, *store.Store) {
	t.Helper()
	var f *akifile.File
	var err error
	if create {
		f, err = akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	} else {
		f, err = akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	}
	if err != nil {
		t.Fatalf("open aki (create=%v): %v", create, err)
	}
	s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	return f, s
}

func TestListEffectLogRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "listdur.aki")

	// First run: build lists at both ends, pop, set, trim, rem, insert, promote one to
	// the native band, and delete a key, then close so only durable bytes survive.
	f, s := listDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	pushD(cx, g, "q", false, "a", "b", "c", "d") // RPUSH -> [a b c d]
	pushD(cx, g, "q", true, "z")                 // LPUSH -> [z a b c d]
	popD(cx, g, "q", true)                       // LPOP  -> [a b c d]
	popD(cx, g, "q", false)                      // RPOP  -> [a b c]
	lsetD(cx, g, "q", 1, "B")                    // -> [a B c]
	pushD(cx, g, "q", false, "d", "e", "f")      // -> [a B c d e f]
	ltrimD(cx, g, "q", 1, 4)                     // -> [B c d e]
	linsertD(cx, g, "q", true, "c", "X")         // insert before c -> [B X c d e]

	pushD(cx, g, "dups", false, "x", "y", "x", "z", "x") // [x y x z x]
	lremD(cx, g, "dups", 2, "x")                         // head->tail drop 2 x -> [y z x]

	// Promote a list past the ~8 KiB listpack budget so it recovers on the native band.
	big := make([]string, 0, 80)
	for i := 0; i < 80; i++ {
		big = append(big, strings.Repeat(fmt.Sprintf("e%02d", i), 40)) // ~120 bytes each
	}
	pushD(cx, g, "big", false, big...)
	if g.m["big"].encoding() != encQuicklist {
		t.Fatalf("big list stayed %s, expected quicklist for the native-band round trip", g.m["big"].encoding())
	}
	popD(cx, g, "big", true)         // drop the head off the native band
	lsetD(cx, g, "big", 5, "REWRIT") // rewrite an interior element on the native band

	pushD(cx, g, "gone", false, "1", "2")
	if !Delete(cx, []byte("gone")) {
		t.Fatal("delete of a live list reported absent")
	}

	wantQ := elements(g.m["q"])
	wantDups := elements(g.m["dups"])
	wantBig := elements(g.m["big"])
	if !reflect.DeepEqual(wantQ, []string{"B", "X", "c", "d", "e"}) {
		t.Fatalf("first-run q = %v", wantQ)
	}
	if !reflect.DeepEqual(wantDups, []string{"y", "z", "x"}) {
		t.Fatalf("first-run dups = %v", wantDups)
	}
	if _, ok := g.m["gone"]; ok {
		t.Fatal("gone should be deleted in the first run")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen into a fresh store and empty registry, rebuild from the log.
	f2, s2 := listDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := elements(g2.m["q"]); !reflect.DeepEqual(got, wantQ) {
		t.Fatalf("q after recovery = %v, want %v", got, wantQ)
	}
	if got := elements(g2.m["dups"]); !reflect.DeepEqual(got, wantDups) {
		t.Fatalf("dups after recovery = %v, want %v", got, wantDups)
	}
	if got := elements(g2.m["big"]); !reflect.DeepEqual(got, wantBig) {
		t.Fatalf("big after recovery mismatch: len got=%d want=%d", len(got), len(wantBig))
	}
	if g2.m["big"].encoding() != encQuicklist {
		t.Fatalf("big recovered as %s, want quicklist", g2.m["big"].encoding())
	}
	if _, ok := g2.m["gone"]; ok {
		t.Fatal("deleted list gone came back after recovery")
	}
}

// TestListSnapshotRecovers is the slice-3 round trip across a checkpoint boundary: build
// lists with effects, fold them to snapshot frames the way the checkpoint dumper does,
// give one a key TTL, then mutate past the snapshot, close, reopen, and recover. The
// reopen must rebuild each list from its snapshot and replay only the effect tail cut
// after it, so the recovered state is the composition, and a key TTL taken at snapshot
// time survives. It also proves a snapshot-restored key an effect empties is dropped and
// the pre-snapshot effects do not leak past the snapshot reset.
func TestListSnapshotRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "listsnap.aki")
	f, s := listDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	pushD(cx, g, "q", false, "a", "b", "c")
	pushD(cx, g, "r", false, "1", "2", "3")
	pushD(cx, g, "gone", false, "x", "y")
	const ttl = int64(5_000_000) // far past NowMs, so the list is live at snapshot and recovery
	g.m["q"].expireAt = ttl

	Snapshot(cx) // fold every live list to a snapshot frame

	// Effects after the snapshot: q trims and gains, r swaps ends, gone drains to empty so
	// its snapshot-restored form must drop on replay.
	ltrimD(cx, g, "q", 0, 1)      // [a b]
	pushD(cx, g, "q", false, "d") // [a b d]
	popD(cx, g, "r", true)        // [2 3]
	pushD(cx, g, "r", false, "9") // [2 3 9]
	popD(cx, g, "gone", true)     // [y]
	popD(cx, g, "gone", true)     // empty -> dropped

	wantQ := elements(g.m["q"]) // [a b d]
	wantR := elements(g.m["r"]) // [2 3 9]
	if _, ok := g.m["gone"]; ok {
		t.Fatal("gone should be empty in the first run")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := listDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := elements(g2.m["q"]); !reflect.DeepEqual(got, wantQ) {
		t.Fatalf("q after recovery = %v, want %v", got, wantQ)
	}
	if got := elements(g2.m["r"]); !reflect.DeepEqual(got, wantR) {
		t.Fatalf("r after recovery = %v, want %v", got, wantR)
	}
	if _, ok := g2.m["gone"]; ok {
		t.Fatal("gone came back after recovery, snapshot restored a key the tail emptied")
	}
	if got := g2.m["q"].expireAt; got != ttl {
		t.Fatalf("q TTL after recovery = %d, want %d (snapshot header must carry it)", got, ttl)
	}
}

// TestListSnapshotOnlyRecovers proves the snapshot alone rebuilds a list with no effect
// tail after it, the bounded path a clean shutdown then a reopen takes: the checkpoint
// holds the whole list, the effect log past it is empty, and recovery reads the list back
// from the snapshot frame only, in order.
func TestListSnapshotOnlyRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "listsnaponly.aki")
	f, s := listDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)
	pushD(cx, g, "l", false, "a", "b", "c", "d")
	Snapshot(cx)
	want := elements(g.m["l"])
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := listDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := elements(registry(cx2).m["l"]); !reflect.DeepEqual(got, want) {
		t.Fatalf("list after snapshot-only recovery = %v, want %v", got, want)
	}
}

// TestListEffectLogNoopWithoutFile proves the log helpers and Recover are inert on a plain
// in-memory store with no .aki handle: the mutations still land in the registry, nothing
// is logged, and Recover walks nothing and rebuilds nothing.
func TestListEffectLogNoopWithoutFile(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	pushD(cx, g, "l", false, "a", "b")
	if got := elements(g.m["l"]); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("in-memory list = %v, want [a b]", got)
	}
	cx2 := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover on a no-file store: %v", err)
	}
	if Len(cx2) != 0 {
		t.Fatalf("recover over a no-file store built %d lists, want 0", Len(cx2))
	}
}
