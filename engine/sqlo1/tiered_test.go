package sqlo1

import (
	"context"
	"fmt"
	"hash/maphash"
	"math/rand"
	"testing"
)

// countingStore wraps a Store and counts BatchGet rounds, so the tests
// can see coalescing and definitive misses instead of trusting counters
// the code under test maintains itself.
type countingStore struct {
	Store
	batchGets int
	batchKeys int
}

func (c *countingStore) BatchGet(ctx context.Context, keys [][]byte) ([]Record, error) {
	c.batchGets++
	c.batchKeys += len(keys)
	return c.Store.BatchGet(ctx, keys)
}

// tieredRig is a Tiered over a counting MemStore with an owned clock and
// generous defaults; tests shrink what they need through the fields.
type tieredRig struct {
	t   *Tiered
	cs  *countingStore
	ms  *MemStore
	now int64
}

func newTieredRig(t *testing.T, entries int, promoteP float64, seed uint64) *tieredRig {
	t.Helper()
	ms := NewMemStore()
	cs := &countingStore{Store: ms}
	r := &tieredRig{ms: ms, cs: cs, now: 1 << 41} // an arbitrary modern wall clock
	r.t = NewTiered(cs, TieredConfig{
		Budget:   Budget{Entries: entries, Arenas: 64 << 20},
		PromoteP: promoteP,
		Seed:     seed,
		NowMs:    func() int64 { return r.now },
	})
	return r
}

// preload puts records straight into the store, below the tier.
func (r *tieredRig) preload(t *testing.T, recs ...Record) {
	t.Helper()
	b := &DrainBatch{Seq: r.ms.Stats().HighWater + 1}
	for _, rec := range recs {
		b.Ops = append(b.Ops, Op{Rec: rec})
	}
	if err := r.ms.ApplyBatch(context.Background(), b); err != nil {
		t.Fatalf("preload: %v", err)
	}
	// The drainer numbers its batches from the store's high water at
	// construction time; preloading behind its back would collide, so
	// only preload before the first write through the tier or accept
	// that this test never drains.
	r.t.dr.seq = b.Seq
}

func (r *tieredRig) get(t *testing.T, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := r.t.Get(context.Background(), []byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v, ok
}

func (r *tieredRig) set(t *testing.T, key, val string) {
	t.Helper()
	if err := r.t.Set(context.Background(), []byte(key), []byte(val), TagString); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
}

func (r *tieredRig) del(t *testing.T, key string) bool {
	t.Helper()
	ok, err := r.t.Del(context.Background(), []byte(key))
	if err != nil {
		t.Fatalf("Del(%q): %v", key, err)
	}
	return ok
}

func (r *tieredRig) flush(t *testing.T) {
	t.Helper()
	if err := r.t.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func (r *tieredRig) state(t *testing.T, key string) uint8 {
	t.Helper()
	s, ok := r.t.ht.lookup(maphash.Bytes(r.t.ht.seed, []byte(key)), []byte(key))
	if !ok {
		return 0
	}
	return r.t.ht.hdrs[s].state
}

func TestTieredColdMissPromotesAndServes(t *testing.T) {
	r := newTieredRig(t, 64, 1.0, 1)
	r.preload(t, Record{Key: []byte("k"), Value: []byte("v"), Gen: 7})

	v, ok := r.get(t, "k")
	if !ok || string(v) != "v" {
		t.Fatalf("cold get = %q %v", v, ok)
	}
	if st := r.t.Stats(); st.ColdHits != 1 || st.Promotions != 1 || st.BatchReads != 1 {
		t.Fatalf("after cold hit: %+v", st)
	}
	if got := r.state(t, "k"); got != stateResident {
		t.Fatalf("promoted state = %d, want resident", got)
	}
	s, _ := r.t.ht.lookup(maphash.Bytes(r.t.ht.seed, []byte("k")), []byte("k"))
	if hd := &r.t.ht.hdrs[s]; hd.gen != 7 || hd.vptr == 0 {
		t.Fatalf("promoted header gen %d vptr %d", hd.gen, hd.vptr)
	}
	if r.t.ht.dirtyBytes != 0 || r.t.ht.dirtyN != 0 {
		t.Fatal("promotion must not dirty anything")
	}

	// The second read is a hot hit and touches the store not at all.
	if v, ok := r.get(t, "k"); !ok || string(v) != "v" {
		t.Fatalf("hot get = %q %v", v, ok)
	}
	if st := r.t.Stats(); st.HotHits != 1 || r.cs.batchGets != 1 {
		t.Fatalf("after hot hit: %+v, store rounds %d", st, r.cs.batchGets)
	}

	// A promoted record never re-drains: flush moves nothing.
	r.flush(t)
	if hw := r.ms.Stats().HighWater; hw != 1 {
		t.Fatalf("flush after promotion moved the high water to %d", hw)
	}
}

func TestTieredPromotionCoinAndGhostOverride(t *testing.T) {
	// Coin disabled: cold hits serve from disk and never enter the tier.
	r := newTieredRig(t, 64, -1, 2)
	r.preload(t, Record{Key: []byte("k"), Value: []byte("v")})
	for range 10 {
		if v, ok := r.get(t, "k"); !ok || string(v) != "v" {
			t.Fatalf("cold get = %q %v", v, ok)
		}
	}
	if st := r.t.Stats(); st.Promotions != 0 || st.HotKeys != 0 || st.ColdHits != 10 {
		t.Fatalf("disabled coin still promoted: %+v", st)
	}

	// A ghost hit promotes even with the coin disabled (the hotclock
	// lab's D=0 pin), and the stamps come back with it.
	r.set(t, "g", "old")
	r.t.ht.SetTick(r.t.ht.tick + 5)
	r.get(t, "g") // stamp a read at the later tick
	r.flush(t)
	s, _ := r.t.ht.lookup(maphash.Bytes(r.t.ht.seed, []byte("g")), []byte("g"))
	wantRead := r.t.ht.hdrs[s].lastRead
	if !r.t.ht.evict(s, true) {
		t.Fatal("evict refused a drained resident")
	}
	// Move the clock so the restored stamp is distinguishable from the
	// promotion's own read touch.
	r.t.ht.SetTick(r.t.ht.tick + 1)
	if _, ok := r.get(t, "g"); !ok {
		t.Fatal("cold read after evict missed")
	}
	st := r.t.Stats()
	if st.Promotions != 1 || st.GhostPromotions != 1 {
		t.Fatalf("ghost promotion: %+v", st)
	}
	s, ok := r.t.ht.lookup(maphash.Bytes(r.t.ht.seed, []byte("g")), []byte("g"))
	if !ok || r.t.ht.hdrs[s].state != stateResident {
		t.Fatal("ghost promotion left no resident header")
	}
	if r.t.ht.hdrs[s].prevRead != wantRead {
		t.Fatalf("ghost stamps lost: prevRead %d want %d", r.t.ht.hdrs[s].prevRead, wantRead)
	}
}

func TestTieredTombstoneIsDefinitiveMiss(t *testing.T) {
	r := newTieredRig(t, 64, 1.0, 3)
	r.preload(t, Record{Key: []byte("k"), Value: []byte("v")})

	if !r.del(t, "k") {
		t.Fatal("Del of a cold live key reported nothing to delete")
	}
	rounds := r.cs.batchGets // Del checked cold existence

	// The store still holds k until drain, but the tombstone must answer
	// without asking it.
	if _, err := r.ms.Get(context.Background(), []byte("k")); err != nil {
		t.Fatal("undrained deletion already reached the store")
	}
	if _, ok := r.get(t, "k"); ok {
		t.Fatal("tombstoned key still readable")
	}
	if r.cs.batchGets != rounds {
		t.Fatal("a tombstone miss went cold")
	}
	if r.del(t, "k") {
		t.Fatal("second Del found something")
	}
	if r.cs.batchGets != rounds {
		t.Fatal("a tombstone Del went cold")
	}

	r.flush(t)
	if _, err := r.ms.Get(context.Background(), []byte("k")); err != ErrNotFound {
		t.Fatalf("drained deletion left the store record: %v", err)
	}
	if r.t.ht.Len() != 0 || len(r.t.ht.index) != 0 {
		t.Fatal("drained tombstone left a header behind")
	}
}

func TestTieredBatchGetCoalesces(t *testing.T) {
	r := newTieredRig(t, 64, 1.0, 4)
	var keys [][]byte
	for i := range 5 {
		k, v := fmt.Sprintf("cold%d", i), fmt.Sprintf("val%d", i)
		r.preload(t, Record{Key: []byte(k), Value: []byte(v)})
	}
	r.set(t, "hot0", "hv0")
	r.set(t, "hot1", "hv1")
	keys = append(keys,
		[]byte("hot0"), []byte("cold0"), []byte("absent0"),
		[]byte("cold1"), []byte("hot1"), []byte("cold2"),
		[]byte("absent1"), []byte("cold3"), []byte("cold4"),
	)

	rounds := r.cs.batchGets
	out, err := r.t.BatchGet(context.Background(), keys, nil)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if r.cs.batchGets != rounds+1 {
		t.Fatalf("7 cold probes took %d store rounds, want 1", r.cs.batchGets-rounds)
	}
	want := []string{"hv0", "val0", "", "val1", "hv1", "val2", "", "val3", "val4"}
	for i, w := range want {
		switch {
		case w == "" && out[i] != nil:
			t.Fatalf("out[%d] = %q, want miss", i, out[i])
		case w != "" && (out[i] == nil || string(out[i]) != w):
			t.Fatalf("out[%d] = %q, want %q", i, out[i], w)
		}
	}
	st := r.t.Stats()
	if st.HotHits != 2 || st.ColdHits != 5 || st.Misses != 2 || st.Promotions != 5 {
		t.Fatalf("batch stats: %+v", st)
	}
}

func TestTieredBatchPromotionNeverEvicts(t *testing.T) {
	// A tier packed with residents, then a batch whose cold hits would
	// all love to promote: none may evict, because earlier entries in
	// out alias arena bytes an eviction would recycle.
	r := newTieredRig(t, 4, 1.0, 5)
	for i := range 4 {
		r.set(t, fmt.Sprintf("hot%d", i), "hhhh")
	}
	r.flush(t) // all four cool to resident, table full
	r.preload(t, Record{Key: []byte("cold0"), Value: []byte("cv0")})

	out, err := r.t.BatchGet(context.Background(),
		[][]byte{[]byte("hot0"), []byte("cold0")}, nil)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if string(out[0]) != "hhhh" || string(out[1]) != "cv0" {
		t.Fatalf("out = %q %q", out[0], out[1])
	}
	st := r.t.Stats()
	if st.Promotions != 0 || st.PromoteSkips != 1 {
		t.Fatalf("full-tier batch promotion: %+v", st)
	}
	if r.t.ht.Len() != 4 {
		t.Fatal("batch promotion evicted a resident")
	}

	// The same cold hit through a single Get may evict to make room.
	if _, ok := r.get(t, "cold0"); !ok {
		t.Fatal("single get missed")
	}
	st = r.t.Stats()
	if st.Promotions != 1 {
		t.Fatalf("single-get promotion: %+v", st)
	}
}

func TestTieredCooledOnDrainThenEvictionIsFree(t *testing.T) {
	r := newTieredRig(t, 8, -1, 6)
	r.set(t, "k", "v1")
	if got := r.state(t, "k"); got != stateDirty {
		t.Fatalf("state after write = %d", got)
	}
	if r.t.ht.dirtyBytes == 0 {
		t.Fatal("write did not count dirty bytes")
	}

	r.flush(t)
	if got := r.state(t, "k"); got != stateResident {
		t.Fatalf("state after drain = %d, want resident (cooled)", got)
	}
	if r.t.ht.dirtyBytes != 0 {
		t.Fatalf("cooled table still counts %d dirty bytes", r.t.ht.dirtyBytes)
	}
	if v, ok := r.get(t, "k"); !ok || string(v) != "v1" {
		t.Fatalf("cooled read = %q %v (must still be RAM-served)", v, ok)
	}
	rounds := r.cs.batchGets

	// Cooling is what makes eviction free: no drain, no IO, just drop.
	freed := r.t.ev.evict(1)
	if freed == 0 {
		t.Fatal("evicting a cooled record freed nothing")
	}
	if r.cs.batchGets != rounds {
		t.Fatal("eviction touched the store")
	}
	if v, ok := r.get(t, "k"); !ok || string(v) != "v1" {
		t.Fatalf("post-evict cold read = %q %v", v, ok)
	}
}

func TestTieredSetMakesRoomWhenFull(t *testing.T) {
	r := newTieredRig(t, 4, -1, 7)
	for i := range 4 {
		r.set(t, fmt.Sprintf("k%d", i), "vvvv")
	}
	r.flush(t)
	// Table full of residents: the fifth write must evict, not fail,
	// and everything is still readable through the tiers afterward.
	r.set(t, "k4", "v4")
	for i := range 5 {
		k := fmt.Sprintf("k%d", i)
		want := "vvvv"
		if i == 4 {
			want = "v4"
		}
		if v, ok := r.get(t, k); !ok || string(v) != want {
			t.Fatalf("get %s = %q %v", k, v, ok)
		}
	}
}

func TestTieredExpiryAcrossTheTiers(t *testing.T) {
	r := newTieredRig(t, 64, 1.0, 8)

	// A cold record past due is a miss and never promotes.
	r.preload(t,
		Record{Key: []byte("dead"), Value: []byte("x"), ExpireMs: r.now - 1},
		Record{Key: []byte("live"), Value: []byte("y"), ExpireMs: r.now + 10_000},
	)
	if _, ok := r.get(t, "dead"); ok {
		t.Fatal("expired cold record served")
	}
	if st := r.t.Stats(); st.Promotions != 0 {
		t.Fatal("expired cold record promoted")
	}

	// A volatile record promotes with its expiry projected, and the hot
	// copy dies on schedule without a cold read.
	if _, ok := r.get(t, "live"); !ok {
		t.Fatal("live volatile record missed")
	}
	if got := r.state(t, "live"); got != stateResident {
		t.Fatalf("volatile promotion state = %d", got)
	}
	rounds := r.cs.batchGets
	r.now += 20_000
	if err := r.t.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if _, ok := r.get(t, "live"); ok {
		t.Fatal("hot copy outlived its expiry")
	}
	if r.cs.batchGets != rounds {
		t.Fatal("expired hot key went cold instead of answering definitively")
	}
	if r.del(t, "live") {
		t.Fatal("Del of an expired key reported a kill")
	}
}

func TestTieredDrainCarriesExpiry(t *testing.T) {
	r := newTieredRig(t, 64, 1.0, 9)
	r.set(t, "k", "v")
	at := uint32(uint64(r.now+60_000+1023) >> 10)
	if _, changed, ok := r.t.ht.setExpire([]byte("k"), at); !ok || !changed {
		t.Fatal("setExpire refused a live dirty key")
	}
	r.flush(t)

	rec, err := r.ms.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("store get: %v", err)
	}
	if want := int64(at) << 10; rec.ExpireMs != want {
		t.Fatalf("drained ExpireMs = %d, want %d", rec.ExpireMs, want)
	}
	if rec.ExpireMs < r.now+60_000 {
		t.Fatal("ceil projection let the record expire early")
	}

	// The round trip is a fixed point: evict, re-promote, re-stamp.
	s, _ := r.t.ht.lookup(maphash.Bytes(r.t.ht.seed, []byte("k")), []byte("k"))
	r.t.ht.evict(s, false)
	if _, ok := r.get(t, "k"); !ok {
		t.Fatal("cold read of volatile key missed")
	}
	s, _ = r.t.ht.lookup(maphash.Bytes(r.t.ht.seed, []byte("k")), []byte("k"))
	if got := r.t.ht.hdrs[s].expireLo; got != at {
		t.Fatalf("re-promoted expireLo = %d, want %d", got, at)
	}
}

func TestTieredEmptyValueIsNotAMiss(t *testing.T) {
	r := newTieredRig(t, 64, 1.0, 10)
	r.preload(t, Record{Key: []byte("e"), Value: []byte{}})
	v, ok := r.get(t, "e")
	if !ok || v == nil || len(v) != 0 {
		t.Fatalf("empty-value get = %v %v", v, ok)
	}
	out, err := r.t.BatchGet(context.Background(), [][]byte{[]byte("e")}, nil)
	if err != nil || out[0] == nil || len(out[0]) != 0 {
		t.Fatalf("empty-value batch get = %v %v", out, err)
	}
}

// TestTieredMixedTrafficShadow runs random traffic through a small tier
// over the placeholder store against a map shadow: whatever the tiers do
// internally (drain, evict, promote, tombstone), reads must agree with
// the shadow at every step.
func TestTieredMixedTrafficShadow(t *testing.T) {
	r := newTieredRig(t, 32, 0.5, 11)
	rng := rand.New(rand.NewSource(11))
	shadow := map[string]string{}
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%03d", i)
	}

	for op := range 8000 {
		k := keys[rng.Intn(len(keys))]
		switch rng.Intn(10) {
		case 0:
			if got := r.del(t, k); got != (shadow[k] != "") {
				t.Fatalf("op %d: Del(%s) = %v, shadow has %q", op, k, got, shadow[k])
			}
			delete(shadow, k)
		case 1, 2, 3:
			v := fmt.Sprintf("v%d", op)
			r.set(t, k, v)
			shadow[k] = v
		default:
			v, ok := r.get(t, k)
			want, wantOK := shadow[k]
			if ok != wantOK || (ok && string(v) != want) {
				t.Fatalf("op %d: Get(%s) = %q %v, want %q %v", op, k, v, ok, want, wantOK)
			}
		}
		switch {
		case op%997 == 0:
			r.flush(t)
		case op%251 == 0:
			r.t.ht.SetTick(r.t.ht.tick + 1)
			r.t.ev.evict(64)
		}
	}
	r.flush(t)
	for _, k := range keys {
		v, ok := r.get(t, k)
		want, wantOK := shadow[k]
		if ok != wantOK || (ok && string(v) != want) {
			t.Fatalf("final: Get(%s) = %q %v, want %q %v", k, v, ok, want, wantOK)
		}
	}
}
