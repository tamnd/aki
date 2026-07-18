package stream

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The stream effect-log and snapshot round trips (spec 2064/f3/M8-collection-durability-
// plan, the stream arm of slices 2 and 3): run adds at climbing IDs, deletes, a trim, and
// an XSETID against a stream store backed by a real .aki, close the file so only its
// durable bytes remain, reopen into a fresh store and an empty registry, replay the log,
// and assert every stream reads back with the exact entries and counters the first run
// left it. It is the stream sibling of the set, hash, zset, and list verticals.
//
// The command handlers write their reply through a shard.Reply a package unit test cannot
// build, so the xaddD, xdelD, xtrimD, and xsetidD helpers mirror the handlers' mutate-and-
// log bodies exactly (the driver convention the earlier verticals set). The delete goes
// through the real exported Delete, which logs its own key-delete effect. IDs are explicit
// so the log content is deterministic.

// fields builds a field run from name/value string pairs.
func fields(pairs ...string) []field {
	fs := make([]field, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		fs = append(fs, field{name: []byte(pairs[i]), value: []byte(pairs[i+1])})
	}
	return fs
}

// xaddD mirrors Xadd for an explicit ID: create the stream on first add, append the entry,
// cut an add effect, and reconcile the footprint.
func xaddD(cx *shard.Ctx, g *reg, key string, ms, seq uint64, pairs ...string) {
	k := []byte(key)
	s := g.live(cx, k)
	newKey := s == nil
	if newKey {
		s = newStream()
	}
	id := streamID{ms: ms, seq: seq}
	fs := fields(pairs...)
	s.appendEntry(id, fs)
	if newKey {
		g.m[key] = s
	}
	logAdd(cx, k, id, fs)
	g.note(s)
}

// xdelD mirrors Xdel: tombstone each ID, cut a delete effect per removal, and reconcile.
func xdelD(cx *shard.Ctx, g *reg, key string, ids ...streamID) int {
	k := []byte(key)
	s := g.live(cx, k)
	if s == nil {
		return 0
	}
	n := 0
	for _, id := range ids {
		if s.delete(id) {
			n++
			logDel(cx, k, id)
		}
	}
	g.note(s)
	return n
}

// xtrimD mirrors Xtrim MAXLEN exact: trim to n live entries, cut the boundary effect when
// it removed one, and reconcile.
func xtrimD(cx *shard.Ctx, g *reg, key string, n uint64) int {
	k := []byte(key)
	s := g.live(cx, k)
	if s == nil {
		return 0
	}
	removed := s.trim(trimSpec{kind: trimMaxlen, maxlen: n})
	if removed > 0 {
		logTrimBoundary(cx, k, s)
	}
	g.note(s)
	return removed
}

// xsetidD mirrors Xsetid with both options: set the counters and cut a set-id effect.
func xsetidD(cx *shard.Ctx, g *reg, key string, id streamID, entriesAdded uint64, maxDeleted streamID) {
	k := []byte(key)
	s := g.live(cx, k)
	s.lastID = id
	s.entriesAdded = entriesAdded
	s.maxDeletedID = maxDeleted
	logSetID(cx, k, s)
	g.note(s)
}

// dumpEntry is one live entry rendered for comparison: its ID string and its flat field
// run.
type dumpEntry struct {
	id     string
	fields []string
}

// entriesOf reads a stream back as its live entries in ID order, the order every read and
// the snapshot run preserve. A nil or empty stream reads as no entries.
func entriesOf(s *stream) []dumpEntry {
	if s == nil {
		return nil
	}
	es := s.collectRange(bound{id: minID}, bound{id: maxID}, false, -1)
	out := make([]dumpEntry, len(es))
	for i := range es {
		fs := make([]string, 0, len(es[i].fields)*2)
		for _, f := range es[i].fields {
			fs = append(fs, string(f.name), string(f.value))
		}
		out[i] = dumpEntry{id: es[i].id.String(), fields: fs}
	}
	return out
}

// counters reads the four per-stream counters recovery must restore.
func counters(s *stream) (last, maxDel string, added, length uint64) {
	return s.lastID.String(), s.maxDeletedID.String(), s.entriesAdded, s.length
}

func streamDurStore(t *testing.T, path string, create bool) (*akifile.File, *store.Store) {
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

func TestStreamEffectLogRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streamdur.aki")

	// First run: build streams with adds, deletes, a trim, an XSETID, promote one to the
	// native band, and delete a key, then close so only durable bytes survive.
	f, s := streamDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	// q: add five entries, delete the newest so lastID outlives the live tail, and delete
	// an interior one.
	xaddD(cx, g, "q", 1, 1, "f", "a")
	xaddD(cx, g, "q", 1, 2, "f", "b")
	xaddD(cx, g, "q", 1, 3, "f", "c")
	xaddD(cx, g, "q", 1, 4, "f", "d")
	xaddD(cx, g, "q", 1, 5, "f", "e")
	xdelD(cx, g, "q", streamID{1, 2}) // interior
	xdelD(cx, g, "q", streamID{1, 5}) // newest: lastID stays 1-5, maxDeletedID -> 1-5
	if got := entriesOf(g.m["q"]); len(got) != 3 {
		t.Fatalf("first-run q live = %d, want 3", len(got))
	}

	// r: add six then trim to the newest three, exercising the boundary effect.
	for i := uint64(1); i <= 6; i++ {
		xaddD(cx, g, "r", 2, i, "n", string(rune('0'+i)))
	}
	if removed := xtrimD(cx, g, "r", 3); removed != 3 {
		t.Fatalf("first-run r trim removed = %d, want 3", removed)
	}

	// big: enough entries to break the inline cap and land on the native band.
	for i := uint64(1); i <= 40; i++ {
		xaddD(cx, g, "big", 3, i, "k", "v")
	}
	if g.m["big"].kind != bandNative {
		t.Fatalf("big stayed inline, expected native for the round trip")
	}
	xdelD(cx, g, "big", streamID{3, 10})

	// graft: an XSETID grafts counters onto a stream.
	xaddD(cx, g, "graft", 5, 1, "x", "y")
	xsetidD(cx, g, "graft", streamID{9, 9}, 100, streamID{7, 7})

	// gone: delete the whole key so it does not come back.
	xaddD(cx, g, "gone", 4, 1, "z", "z")
	if !Delete(cx, []byte("gone")) {
		t.Fatal("delete of a live stream reported absent")
	}

	wantQ := entriesOf(g.m["q"])
	wantR := entriesOf(g.m["r"])
	wantBig := entriesOf(g.m["big"])
	qLast, qMaxDel, qAdded, qLen := counters(g.m["q"])
	gLast, gMaxDel, gAdded, _ := counters(g.m["graft"])
	if qLast != "1-5" || qMaxDel != "1-5" || qAdded != 5 || qLen != 3 {
		t.Fatalf("first-run q counters last=%s maxDel=%s added=%d len=%d", qLast, qMaxDel, qAdded, qLen)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen into a fresh store and empty registry, rebuild from the log.
	f2, s2 := streamDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := entriesOf(g2.m["q"]); !reflect.DeepEqual(got, wantQ) {
		t.Fatalf("q after recovery = %v, want %v", got, wantQ)
	}
	if got := entriesOf(g2.m["r"]); !reflect.DeepEqual(got, wantR) {
		t.Fatalf("r after recovery = %v, want %v", got, wantR)
	}
	if got := entriesOf(g2.m["big"]); !reflect.DeepEqual(got, wantBig) {
		t.Fatalf("big after recovery mismatch: len got=%d want=%d", len(got), len(wantBig))
	}
	if g2.m["big"].kind != bandNative {
		t.Fatalf("big recovered inline, want native")
	}
	if last, maxDel, added, length := counters(g2.m["q"]); last != qLast || maxDel != qMaxDel || added != qAdded || length != qLen {
		t.Fatalf("q counters after recovery last=%s maxDel=%s added=%d len=%d, want %s %s %d %d",
			last, maxDel, added, length, qLast, qMaxDel, qAdded, qLen)
	}
	if last, maxDel, added, _ := counters(g2.m["graft"]); last != gLast || maxDel != gMaxDel || added != gAdded {
		t.Fatalf("graft counters after recovery last=%s maxDel=%s added=%d, want %s %s %d",
			last, maxDel, added, gLast, gMaxDel, gAdded)
	}
	if _, ok := g2.m["gone"]; ok {
		t.Fatal("deleted stream gone came back after recovery")
	}
}

// TestStreamSnapshotRecovers is the slice-3 round trip across a checkpoint boundary: build
// streams with effects, fold them to snapshot frames the way the checkpoint dumper does,
// give one a key TTL, then mutate past the snapshot, close, reopen, and recover. The reopen
// must rebuild each stream from its snapshot and replay only the effect tail cut after it,
// so the recovered state is the composition, a key TTL taken at snapshot time survives, an
// emptied-but-kept stream stays present with its counters, and a key deleted after the
// snapshot does not leak back.
// sexpireD mirrors streamBackend.Store for a future instant: set the live stream's
// deadline and cut the expire effect that carries it, the store arm of an EXPIRE to a
// future instant a unit test cannot drive through the reply-writing Expire.
func sexpireD(cx *shard.Ctx, g *reg, key string, at int64) {
	k := []byte(key)
	s := g.live(cx, k)
	if s == nil {
		return
	}
	s.expireAt = at
	logExpire(cx, k, at)
}

// sexpirePastD mirrors streamBackend.Delete: an EXPIRE to a past instant drops the key on
// the spot and logs the key-delete so replay does not resurrect the entries.
func sexpirePastD(cx *shard.Ctx, g *reg, key string) {
	k := []byte(key)
	logDeleteKey(cx, k)
	g.drop(k)
}

// TestStreamKeyExpireRecovers is the stream arm of the key-expire round trip: a deadline
// set or cleared after the snapshot must survive a reopen on the effect tail alone. It
// covers EXPIRE to a future instant, PERSIST of a snapshot-carried deadline, and EXPIRE to
// a past instant, mirroring the set vertical's TestSetKeyExpireRecovers.
func TestStreamKeyExpireRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streamkeyexpire.aki")
	f, s := streamDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	xaddD(cx, g, "future", 1, 1, "f", "a")
	xaddD(cx, g, "persist", 2, 1, "f", "b")
	xaddD(cx, g, "past", 3, 1, "f", "c")
	const carried = int64(5_000_000)
	g.m["persist"].expireAt = carried

	Snapshot(cx)

	const future = int64(9_000_000)
	sexpireD(cx, g, "future", future)
	if !Persist(cx, []byte("persist")) {
		t.Fatal("persist of a stream with a deadline reported none removed")
	}
	sexpirePastD(cx, g, "past")

	wantFuture := entriesOf(g.m["future"])
	wantPersist := entriesOf(g.m["persist"])
	if g.m["future"].expireAt != future {
		t.Fatalf("first-run future TTL = %d, want %d", g.m["future"].expireAt, future)
	}
	if g.m["persist"].expireAt != 0 {
		t.Fatalf("first-run persist TTL = %d, want 0", g.m["persist"].expireAt)
	}
	if _, ok := g.m["past"]; ok {
		t.Fatal("past should be gone in the first run")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := streamDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := entriesOf(g2.m["future"]); !reflect.DeepEqual(got, wantFuture) {
		t.Fatalf("future entries after recovery = %v, want %v", got, wantFuture)
	}
	if got := g2.m["future"].expireAt; got != future {
		t.Fatalf("future TTL after recovery = %d, want %d (post-snapshot expire effect must survive)", got, future)
	}
	if got := entriesOf(g2.m["persist"]); !reflect.DeepEqual(got, wantPersist) {
		t.Fatalf("persist entries after recovery = %v, want %v", got, wantPersist)
	}
	if got := g2.m["persist"].expireAt; got != 0 {
		t.Fatalf("persist TTL after recovery = %d, want 0 (post-snapshot PERSIST effect must survive)", got)
	}
	if _, ok := g2.m["past"]; ok {
		t.Fatal("past came back after recovery, the expire-past delete effect was lost")
	}
}

func TestStreamSnapshotRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streamsnap.aki")
	f, s := streamDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	// q gains a TTL and entries; drained will be emptied by XDEL but kept; gone will be
	// deleted after the snapshot.
	xaddD(cx, g, "q", 1, 1, "f", "a")
	xaddD(cx, g, "q", 1, 2, "f", "b")
	xaddD(cx, g, "drained", 8, 1, "d", "1")
	xaddD(cx, g, "drained", 8, 2, "d", "2")
	xdelD(cx, g, "drained", streamID{8, 1}, streamID{8, 2}) // emptied, lastID stays 8-2
	xaddD(cx, g, "gone", 9, 1, "g", "g")
	const ttl = int64(5_000_000) // far past NowMs, so the stream is live at snapshot and recovery
	g.m["q"].expireAt = ttl

	Snapshot(cx) // fold every live stream to a snapshot frame

	// Effects after the snapshot: q trims to one and gains one; drained gains an entry; gone
	// is deleted so its snapshot-restored form must not survive.
	xaddD(cx, g, "q", 1, 3, "f", "c")
	xtrimD(cx, g, "q", 2) // keep newest two: 1-2, 1-3
	xaddD(cx, g, "drained", 8, 3, "d", "3")
	if !Delete(cx, []byte("gone")) {
		t.Fatal("delete of gone reported absent")
	}

	wantQ := entriesOf(g.m["q"])
	wantDrained := entriesOf(g.m["drained"])
	drLast, drMaxDel, drAdded, drLen := counters(g.m["drained"])

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := streamDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := entriesOf(g2.m["q"]); !reflect.DeepEqual(got, wantQ) {
		t.Fatalf("q after recovery = %v, want %v", got, wantQ)
	}
	if got := entriesOf(g2.m["drained"]); !reflect.DeepEqual(got, wantDrained) {
		t.Fatalf("drained after recovery = %v, want %v", got, wantDrained)
	}
	if last, maxDel, added, length := counters(g2.m["drained"]); last != drLast || maxDel != drMaxDel || added != drAdded || length != drLen {
		t.Fatalf("drained counters after recovery last=%s maxDel=%s added=%d len=%d, want %s %s %d %d",
			last, maxDel, added, length, drLast, drMaxDel, drAdded, drLen)
	}
	if got := g2.m["q"].expireAt; got != ttl {
		t.Fatalf("q TTL after recovery = %d, want %d (snapshot header must carry it)", got, ttl)
	}
	if _, ok := g2.m["gone"]; ok {
		t.Fatal("gone came back after recovery, snapshot restored a key the tail deleted")
	}
}

// TestStreamSnapshotOnlyRecovers proves the snapshot alone rebuilds a stream with no effect
// tail after it, the bounded path a clean shutdown then a reopen takes, including an
// emptied-but-kept stream whose only durable trace is its snapshot counters.
func TestStreamSnapshotOnlyRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streamsnaponly.aki")
	f, s := streamDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	xaddD(cx, g, "s", 1, 1, "a", "1")
	xaddD(cx, g, "s", 1, 2, "b", "2")
	xaddD(cx, g, "s", 1, 3, "c", "3")
	// empty: added then fully deleted, kept as an empty stream with a live lastID.
	xaddD(cx, g, "empty", 4, 1, "x", "x")
	xdelD(cx, g, "empty", streamID{4, 1})
	Snapshot(cx)

	wantS := entriesOf(g.m["s"])
	eLast, eMaxDel, eAdded, eLen := counters(g.m["empty"])

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := streamDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := entriesOf(g2.m["s"]); !reflect.DeepEqual(got, wantS) {
		t.Fatalf("s after snapshot-only recovery = %v, want %v", got, wantS)
	}
	es, ok := g2.m["empty"]
	if !ok {
		t.Fatal("empty stream absent after recovery, an emptied stream is kept")
	}
	if last, maxDel, added, length := counters(es); last != eLast || maxDel != eMaxDel || added != eAdded || length != eLen {
		t.Fatalf("empty counters after recovery last=%s maxDel=%s added=%d len=%d, want %s %s %d %d",
			last, maxDel, added, length, eLast, eMaxDel, eAdded, eLen)
	}
}

// TestStreamEffectLogNoopWithoutFile proves the log helpers and Recover are inert on a plain
// in-memory store with no .aki handle: the mutations still land in the registry, nothing is
// logged, and Recover walks nothing and rebuilds nothing.
func TestStreamEffectLogNoopWithoutFile(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	xaddD(cx, g, "s", 1, 1, "a", "b")
	if got := entriesOf(g.m["s"]); len(got) != 1 {
		t.Fatalf("in-memory stream live = %d, want 1", len(got))
	}
	cx2 := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover on a no-file store: %v", err)
	}
	if Len(cx2) != 0 {
		t.Fatalf("recover over a no-file store built %d streams, want 0", Len(cx2))
	}
}
