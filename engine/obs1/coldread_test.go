package obs1_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
)

// coldFixture folds a couple of string records through the directory
// fixture and returns the reader wired over the same store and
// directory, plus the keymap the locators come from.
func coldFixture(t *testing.T, st obs1.Store, extra ...string) (*foldFixture, *obs1.Keymap, *obs1.Directory, *obs1.ColdReader) {
	t.Helper()
	fx, km, dir := newFoldDirFixture(t)
	kv := append([]string{"k1", "v1", "k2", "v2"}, extra...)
	fx.folder.Add(frames(kv...))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	if st == nil {
		st = fx.sim
	}
	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: st,
		Dir:   func(group uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cr.Close)
	return fx, km, dir, cr
}

// fetchWait runs one Fetch and blocks for its completion.
func fetchWait(t *testing.T, cr *obs1.ColdReader, key string, loc obs1.KeyLoc) (obs1.ColdRecord, error) {
	t.Helper()
	var (
		rec  obs1.ColdRecord
		rerr error
		done = make(chan struct{})
	)
	cr.Fetch(3, []byte(key), loc, func(r obs1.ColdRecord, err error) {
		rec, rerr = r, err
		close(done)
	})
	<-done
	return rec, rerr
}

// TestColdReaderFetch serves folded records back out of the bucket: a
// keymap locator, one ranged GET, the record's value.
func TestColdReaderFetch(t *testing.T) {
	_, km, _, cr := coldFixture(t, nil)
	for key, want := range map[string]string{"k1": "v1", "k2": "v2"} {
		loc, ok := km.Lookup(obs1.Fingerprint([]byte(key)))
		if !ok {
			t.Fatalf("%s not in keymap", key)
		}
		rec, err := fetchWait(t, cr, key, loc)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if !rec.Found || rec.Tombstone || string(rec.Value) != want {
			t.Fatalf("%s came back %+v", key, rec)
		}
		if rec.Kind != kindString {
			t.Fatalf("%s kind 0x%02x", key, rec.Kind)
		}
	}
	st := cr.Stats()
	if st.Fetches != 2 || st.Errs != 0 || st.Misses != 0 || st.Unresolved != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// TestColdReaderTombstone reads a delete claim back: the record rides a
// tombstone run and comes back Tombstone, never a value.
func TestColdReaderTombstone(t *testing.T) {
	fx, km, _, cr := coldFixture(t, nil)
	fx.folder.Delete([]byte("k1"))
	fx.folder.Flush()
	waitFor(t, "tombstone publish", func() bool { return len(fx.folder.Ledger()) == 2 })

	// The delete removed k1 from the keymap, so point at the tombstone
	// segment by hand: seg 2, its only chunk.
	rec, err := fetchWait(t, cr, "k1", obs1.KeyLoc{Seg: 2, Chunk: 0})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Found || !rec.Tombstone || len(rec.Value) != 0 {
		t.Fatalf("tombstone came back %+v", rec)
	}
	if _, ok := km.Lookup(obs1.Fingerprint([]byte("k1"))); ok {
		t.Fatal("k1 still in keymap after the delete")
	}
}

// TestColdReaderMissAndUnresolved: a key absent from a resolved chunk is
// a counted definitive miss, and a locator the directory does not know
// is the retry-after-refresh error, never a miss.
func TestColdReaderMissAndUnresolved(t *testing.T) {
	_, km, _, cr := coldFixture(t, nil)
	loc, ok := km.Lookup(obs1.Fingerprint([]byte("k1")))
	if !ok {
		t.Fatal("k1 not in keymap")
	}
	rec, err := fetchWait(t, cr, "nope", loc)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Found {
		t.Fatalf("phantom record %+v", rec)
	}

	if _, err := fetchWait(t, cr, "k1", obs1.KeyLoc{Seg: 999, Chunk: 0}); !errors.Is(err, obs1.ErrColdUnresolved) {
		t.Fatalf("unknown segment returned %v", err)
	}
	if _, err := fetchWait(t, cr, "k1", obs1.KeyLoc{Seg: loc.Seg, Chunk: loc.Chunk, Tier: 1}); !errors.Is(err, obs1.ErrColdUnresolved) {
		t.Fatalf("foreign tier returned %v", err)
	}
	st := cr.Stats()
	if st.Misses != 1 || st.Unresolved != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// gateStore blocks GetRange until the test releases it, which pins the
// single-flight window open deterministically.
type gateStore struct {
	obs1.Store
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	gets    int
}

func newGateStore(inner obs1.Store) *gateStore {
	return &gateStore{Store: inner, entered: make(chan struct{}, 16), release: make(chan struct{})}
}

func (g *gateStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, obs1.ObjectInfo, error) {
	g.mu.Lock()
	g.gets++
	g.mu.Unlock()
	g.entered <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, obs1.ObjectInfo{}, ctx.Err()
	}
	return g.Store.GetRange(ctx, key, off, n)
}

// TestColdReaderSingleFlight: two intents on records in the same block
// share one GET; the second attaches to the first's flight.
func TestColdReaderSingleFlight(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	fx.folder.Add(frames("k1", "v1", "k2", "v2"))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	gs := newGateStore(fx.sim)
	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: gs,
		Dir:   func(group uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cr.Close)

	loc1, _ := km.Lookup(obs1.Fingerprint([]byte("k1")))
	loc2, _ := km.Lookup(obs1.Fingerprint([]byte("k2")))

	var wg sync.WaitGroup
	got := make([]obs1.ColdRecord, 2)
	errs := make([]error, 2)
	fetch := func(i int, key string, loc obs1.KeyLoc) {
		wg.Add(1)
		cr.Fetch(3, []byte(key), loc, func(r obs1.ColdRecord, err error) {
			got[i], errs[i] = r, err
			wg.Done()
		})
	}
	fetch(0, "k1", loc1)
	<-gs.entered // the first flight is inside its GET
	fetch(1, "k2", loc2)
	waitFor(t, "second intent attaches", func() bool { return cr.Stats().Attached == 1 })
	close(gs.release)
	wg.Wait()

	for i, want := range []string{"v1", "v2"} {
		if errs[i] != nil {
			t.Fatalf("intent %d: %v", i, errs[i])
		}
		if !got[i].Found || string(got[i].Value) != want {
			t.Fatalf("intent %d came back %+v", i, got[i])
		}
	}
	g := func() int { g := gs; g.mu.Lock(); defer g.mu.Unlock(); return g.gets }()
	if g != 1 {
		t.Fatalf("%d GETs for one block", g)
	}
	st := cr.Stats()
	if st.BlockGETs != 1 || st.Attached != 1 || st.Fetches != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestColdReaderClose: a close mid-flight cancels the GET and still
// completes the waiter, with the cancellation error, and later fetches
// refuse loudly.
func TestColdReaderClose(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	fx.folder.Add(frames("k1", "v1"))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	gs := newGateStore(fx.sim)
	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: gs,
		Dir:   func(group uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		t.Fatal(err)
	}
	loc, _ := km.Lookup(obs1.Fingerprint([]byte("k1")))

	done := make(chan error, 1)
	cr.Fetch(3, []byte("k1"), loc, func(r obs1.ColdRecord, err error) { done <- err })
	<-gs.entered
	cr.Close()
	if err := <-done; err == nil {
		t.Fatal("cancelled flight completed without an error")
	}

	cr.Fetch(3, []byte("k1"), loc, func(r obs1.ColdRecord, err error) { done <- err })
	if err := <-done; err == nil {
		t.Fatal("fetch after close did not refuse")
	}
}
