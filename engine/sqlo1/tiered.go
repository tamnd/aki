package sqlo1

import (
	"context"
	"errors"
	"hash/maphash"
	"math/rand/v2"
	"time"
)

// promoteDefaultP is the cold-read promotion probability, doc 04 section 4:
// the hotclock lab's D=0.125 verdict (labs/sqlo1/s1/01_hotclock), on the
// gate box for re-verdict against real cold-read costs in
// labs/sqlo1/b3/02_hotclock. A ghost hit bypasses the coin entirely: the
// ring proved the key was hot recently, so the coin's one job, filtering
// one-hit wonders, does not apply.
const promoteDefaultP = 0.125

// errHotFull is a write the hot tier could not house even after eviction
// and a forced drain: everything left is dirty and the store refused to
// take it, or the tier is sized to nothing. This is a RAM-side refusal;
// the disk-side refusal is ErrShed at the write door.
var errHotFull = errors.New("sqlo1: hot tier full")

// TieredConfig sizes a Tiered runtime. Budget rows the hot tier consumes
// come from ComputeBudget in production; tests build small Budget literals
// directly.
type TieredConfig struct {
	Budget Budget
	// PromoteP overrides the promotion probability: 0 means the doc 04
	// default, negative disables the coin (ghost hits still promote,
	// which is the hotclock lab's D=0 pin).
	PromoteP float64
	Seed     uint64
	// NowMs is the wall clock in milliseconds; nil means time.Now. The
	// coarse tick the stamps and expiry checks use is NowMs>>10, advanced
	// by Tick.
	NowMs func() int64
}

// TieredStats counts what the read path did; INFO wiring arrives with the
// observability work, the tests use them to see through the tiers.
type TieredStats struct {
	HotHits         int64
	ColdHits        int64
	Misses          int64
	Promotions      int64
	GhostPromotions int64
	// PromoteSkips counts promotions passed up because the tier had no
	// room it was allowed to make; promotion is opportunistic and never
	// applies pressure.
	PromoteSkips int64
	// BatchReads counts Store.BatchGet rounds, the coalescing proof: an
	// N-key read with M cold misses is one round, not M.
	BatchReads int64
	// Evictions and EvictedBytes count eviction policy victims. Only
	// clean residents are candidates (R-I3), so none of these cost a
	// store write or any other IO (R-I5).
	Evictions    int64
	EvictedBytes int64
	HotKeys      int
	DirtyBytes   int
}

// Tiered is the shard runtime composite, doc 04 sections 4 through 8 wired
// over one Store: the hot tier answers first, cold misses coalesce into
// Store.BatchGet rounds, cold hits promote on the sampled clock, writes
// dirty the hot tier and drain cools them to resident, and eviction under
// pressure only ever touches clean residents. Single-owner discipline is
// assumed, not enforced: exactly one goroutine calls into a Tiered, per R1.
type Tiered struct {
	ht    *HotTable
	st    Store
	dr    *drainer
	ev    *evictor
	lad   *ladder
	coin  *rand.Rand
	prob  float64
	nowMs func() int64

	// Reusable read-path buffers, so a steady-state BatchGet allocates
	// nothing of its own (R2; the alloczero lab gates the full path once
	// the runtime is wired to the server).
	missKeys [][]byte
	missPos  []int

	stats TieredStats
}

// NewTiered builds the composite over st and stamps the coarse clock from
// the configured wall clock.
func NewTiered(st Store, cfg TieredConfig) *Tiered {
	prob := cfg.PromoteP
	switch {
	case prob == 0:
		prob = promoteDefaultP
	case prob < 0:
		prob = 0
	}
	now := cfg.NowMs
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	ht := NewBudgetedHotTable(cfg.Budget)
	dr := newDrainer(ht, st)
	ev := newEvictor(ht, cfg.Seed)
	// A store that feels disk-side pressure exposes Maintainer and the
	// ladder's WAL and free-extent rungs come alive; MemStore does not,
	// and those rungs read zero.
	mt, _ := st.(Maintainer)
	t := &Tiered{
		ht:    ht,
		st:    st,
		dr:    dr,
		ev:    ev,
		lad:   newLadder(ht, dr, ev, mt),
		coin:  rand.New(rand.NewPCG(cfg.Seed^0xa5a5a5a5a5a5a5a5, cfg.Seed+1)),
		prob:  prob,
		nowMs: now,
	}
	t.ht.SetTick(uint32(uint64(now()) >> 10))
	return t
}

// Tick advances the coarse clock, spends drain quanta if dirty pressure
// asks for them, and runs the timer half of the maintenance rungs: a due
// checkpoint and any foreground compaction happen here, off the command
// path. The server calls it about once a second.
func (t *Tiered) Tick(ctx context.Context) error {
	t.ht.SetTick(uint32(uint64(t.nowMs()) >> 10))
	if _, err := t.lad.step(ctx); err != nil {
		return err
	}
	return t.lad.tick(ctx)
}

// Get reads one key through the tiers. The value aliases hot-tier arenas
// or the store's read buffers and is valid until the next call on this
// Tiered; ok is false for a missing, deleted, or expired key. A cold miss
// goes through BatchGet even alone, because the seam's batch door is the
// only cold-read door (R3).
func (t *Tiered) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if v, hit, definitive := t.ht.probeRead(key); definitive {
		if hit {
			t.stats.HotHits++
			return v, true, nil
		}
		t.stats.Misses++
		return nil, false, nil
	}
	t.missKeys = append(t.missKeys[:0], key)
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return nil, false, err
	}
	rec := recs[0]
	if rec.Key == nil || t.expiredRec(rec) {
		t.stats.Misses++
		return nil, false, nil
	}
	t.stats.ColdHits++
	t.maybePromote(key, rec, true)
	return nonNilValue(rec.Value), true, nil
}

// BatchGet reads keys in order, appending one entry per key to out (reuse
// the returned slice across calls): the value for a hit, nil for a miss.
// All cold misses coalesce into a single Store.BatchGet round. Values
// alias hot-tier arenas or store buffers and are valid until the next call
// on this Tiered; promotion during a batch never evicts, precisely so a
// hot hit earlier in out cannot have its arena bytes recycled under it.
func (t *Tiered) BatchGet(ctx context.Context, keys [][]byte, out [][]byte) ([][]byte, error) {
	out = out[:0]
	t.missKeys = t.missKeys[:0]
	t.missPos = t.missPos[:0]
	for i, k := range keys {
		v, hit, definitive := t.ht.probeRead(k)
		switch {
		case hit:
			t.stats.HotHits++
			out = append(out, v)
		case definitive:
			t.stats.Misses++
			out = append(out, nil)
		default:
			out = append(out, nil)
			t.missKeys = append(t.missKeys, k)
			t.missPos = append(t.missPos, i)
		}
	}
	if len(t.missKeys) == 0 {
		return out, nil
	}
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return out, err
	}
	for j, rec := range recs {
		if rec.Key == nil || t.expiredRec(rec) {
			t.stats.Misses++
			continue
		}
		t.stats.ColdHits++
		out[t.missPos[j]] = nonNilValue(rec.Value)
		t.maybePromote(t.missKeys[j], rec, false)
	}
	return out, nil
}

// Set writes key through the hot tier: the record is dirty until drain
// cools it. A refused write makes room by evicting clean residents, then
// by forcing a drain cycle; only a tier with nothing left to give errors.
// A store at its disk hard minimum bounces the write with ErrShed before
// it dirties anything, after one repair pass; Del stays exempt because
// deletions are how the user creates the garbage compaction reclaims,
// and shedding them would wedge a full store for good.
func (t *Tiered) Set(ctx context.Context, key, val []byte, tag uint8) error {
	if shed, err := t.lad.shed(ctx); err != nil {
		return err
	} else if shed {
		return ErrShed
	}
	if !t.ht.Put(key, val, tag) {
		if err := t.makeRoomFor(ctx, len(key)+len(val)); err != nil {
			return err
		}
		if !t.ht.Put(key, val, tag) {
			return errHotFull
		}
	}
	_, err := t.lad.step(ctx)
	return err
}

// Del removes key and reports whether it existed. A hot live key gets a
// dirty tombstone; a hot tombstone or expired key is already gone; a key
// the tier does not hold is checked cold through the batch door, and a
// live cold key gets a tombstone header so the deletion drains like any
// other write.
func (t *Tiered) Del(ctx context.Context, key []byte) (bool, error) {
	if _, hit, definitive := t.ht.probeRead(key); definitive {
		if !hit {
			return false, nil
		}
		t.ht.Del(key)
		_, err := t.lad.step(ctx)
		return true, err
	}
	t.missKeys = append(t.missKeys[:0], key)
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return false, err
	}
	if recs[0].Key == nil || t.expiredRec(recs[0]) {
		return false, nil
	}
	if !t.ht.delCold(key) {
		if err := t.makeRoomFor(ctx, len(key)); err != nil {
			return false, err
		}
		if !t.ht.delCold(key) {
			return false, errHotFull
		}
	}
	_, err = t.lad.step(ctx)
	return true, err
}

// Flush drains until nothing is dirty; shutdown and tests use it.
func (t *Tiered) Flush(ctx context.Context) error {
	for {
		n, err := t.dr.drain(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// Stats snapshots the counters plus the tier's live shape.
func (t *Tiered) Stats() TieredStats {
	s := t.stats
	s.Evictions = t.ev.evictions
	s.EvictedBytes = t.ev.evictedBytes
	s.HotKeys = t.ht.Len()
	s.DirtyBytes = t.ht.dirtyBytes
	return s
}

// maybePromote runs the doc 04 promotion decision for a cold hit: a ghost
// hit promotes unconditionally (the hotclock lab's D=0 pin), everything
// else flips the coin. Promotion inserts the record as resident, so a
// later eviction is free and the record never re-drains. It is strictly
// opportunistic: when the tier is full it evicts clean residents only if
// the caller allows it (single reads do, batches do not, see BatchGet),
// and if room still cannot be made the promotion is skipped, never forced.
func (t *Tiered) maybePromote(key []byte, rec Record, canEvict bool) {
	ghost := t.ht.ghosts.peek(maphash.Bytes(t.ht.seed, key))
	if !ghost && t.coin.Float64() >= t.prob {
		return
	}
	var expireLo uint32
	if rec.ExpireMs > 0 {
		// Ceil, so the hot copy never expires before the record; the
		// drain-side reconstruction below the seam is expireLo<<10, and
		// ceil makes that round trip a fixed point.
		expireLo = uint32(uint64(rec.ExpireMs+1023) >> 10)
	}
	// TagString is the whole surface until the per-type docs plug in;
	// the seam's Record carries no type tag, and the per-type
	// integration re-derives it from the record encoding when that
	// lands. The root bit does cross the seam, and the header keeps it
	// so the type layer can tell a promoted root payload from a plain
	// value without decoding.
	tag := TagString
	if rec.Root {
		tag |= TagRoot
	}
	val := nonNilValue(rec.Value)
	if !t.ht.promote(key, val, tag, rec.Gen, expireLo) {
		if canEvict {
			t.ev.evict(hdrSize + len(key) + len(val))
		}
		if !canEvict || !t.ht.promote(key, val, tag, rec.Gen, expireLo) {
			t.stats.PromoteSkips++
			return
		}
	}
	t.stats.Promotions++
	if ghost {
		t.stats.GhostPromotions++
	}
}

// makeRoomFor frees space for a refused insert of payload bytes; freed
// short of the need means the tier genuinely has nothing to give.
func (t *Tiered) makeRoomFor(ctx context.Context, payload int) error {
	_, err := t.lad.makeRoom(ctx, hdrSize+payload)
	return err
}

// expiredRec applies lazy expiry to a cold record: past-due is a miss and
// never promotes. The record itself dies later by the tombstone path (the
// reaper) or as compaction garbage; visibility does not wait for either.
func (t *Tiered) expiredRec(rec Record) bool {
	return rec.ExpireMs > 0 && rec.ExpireMs <= t.nowMs()
}

// nonNilValue keeps a found record's empty value distinguishable from the
// nil that marks a miss in BatchGet results.
func nonNilValue(v []byte) []byte {
	if v == nil {
		return []byte{}
	}
	return v
}
