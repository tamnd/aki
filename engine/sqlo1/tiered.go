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
	// ChunkVacates counts class-migration deadlock breaks: passes that
	// force-evicted one value chunk's clean residents so its bytes could
	// rejoin the arena budget and serve a class the freelists had never
	// seen. A steadily climbing count means the budget is too tight for
	// the workload's value-size drift.
	ChunkVacates int64
	// Reaped counts keys the sampling reaper tombstoned; ReapSkips
	// counts candidates it passed up, either because the hot tier held
	// a newer copy or because there was no room for the tombstone.
	Reaped    int64
	ReapSkips int64
	// ReapCancels counts dirty puts that expired in the queue and
	// drained as tombstones instead of value bytes; VolDefers counts
	// the queue laps volatile-near records sat out to get there (doc
	// 11 section 6, the die-in-RAM path).
	ReapCancels int64
	VolDefers   int64
	HotKeys     int
	DirtyBytes  int
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
	t.ht.SetNow(now())
	return t
}

// Now re-stamps the hot tier's exact clock and returns the millisecond.
// The server calls it at the top of every command, which is what makes
// lazy expiry millisecond-exact (doc 11 section 2): the hot probe's
// confirm check compares against this stamp, and Tick alone only moves
// it about once a second.
func (t *Tiered) Now() int64 {
	ms := t.nowMs()
	t.ht.SetNow(ms)
	return ms
}

// Tick advances the coarse clock, spends drain quanta if dirty pressure
// asks for them, and runs the timer half of the maintenance rungs: a due
// checkpoint and any foreground compaction happen here, off the command
// path. The server calls it about once a second.
func (t *Tiered) Tick(ctx context.Context) error {
	t.ht.SetNow(t.nowMs())
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

// Lookup is Get plus the root bit: the type layer's read door. The
// seam's Record carries no type tag, but the root bit crosses it, so
// this is how per-type code tells a root payload from a plain value
// without decoding anything (the hot header keeps the bit for
// promoted records). Aliasing rules match Get.
func (t *Tiered) Lookup(ctx context.Context, key []byte) (val []byte, root, ok bool, err error) {
	val, root, _, ok, err = t.LookupEntry(ctx, key)
	return val, root, ok, err
}

// LookupEntry is Lookup plus the exact expire_ms (0 for none): the
// command layer's read door when it needs the expiry beside the value,
// which is TTL answers, KEEPTTL, and GETEX. Aliasing rules match Get.
func (t *Tiered) LookupEntry(ctx context.Context, key []byte) (val []byte, root bool, expMs int64, ok bool, err error) {
	if v, tag, exp, hit, definitive := t.ht.probeEntry(key); definitive {
		if hit {
			t.stats.HotHits++
			return v, tag&TagRoot != 0, exp, true, nil
		}
		t.stats.Misses++
		return nil, false, 0, false, nil
	}
	t.missKeys = append(t.missKeys[:0], key)
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return nil, false, 0, false, err
	}
	rec := recs[0]
	if rec.Key == nil || t.expiredRec(rec) {
		t.stats.Misses++
		return nil, false, 0, false, nil
	}
	t.stats.ColdHits++
	t.maybePromote(key, rec, true)
	return nonNilValue(rec.Value), rec.Root, rec.ExpireMs, true, nil
}

// ExpireAt stamps an absolute expire_ms on key and reports whether the
// key was there to stamp; atMs 0 is PERSIST, and a past atMs is legal
// (the key goes invisible lazily, like Redis storing an already-dead
// expiry). The stamp is durable by the re-dirty rule: a resident header
// re-dirties inside setExpireMs so the edit drains with the record
// image, and a cold key is pulled in as a dirty copy first. Like Del,
// ExpireAt skips the shed gate: TTL edits are how the user creates the
// garbage that relieves disk pressure.
func (t *Tiered) ExpireAt(ctx context.Context, key []byte, atMs int64) (bool, error) {
	if _, _, _, hit, definitive := t.ht.probeEntry(key); definitive {
		if !hit {
			return false, nil
		}
		t.ht.setExpireMs(key, atMs)
		_, err := t.lad.step(ctx)
		return true, err
	}
	t.missKeys = append(t.missKeys[:0], key)
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return false, err
	}
	rec := recs[0]
	if rec.Key == nil || t.expiredRec(rec) {
		return false, nil
	}
	tag := TagString
	if rec.Root {
		tag |= TagRoot
	}
	val := nonNilValue(rec.Value)
	if err := t.putPressured(ctx, key, val, tag, rec.Gen); err != nil {
		return false, err
	}
	t.ht.setExpireMs(key, atMs)
	_, err = t.lad.step(ctx)
	return true, err
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

// LookupBatch is BatchGet with the root bit and exact expire_ms
// beside each value: the type layer's batch read door, so multi-key
// commands can tell a root payload from a plain value and preserve
// expiries without decoding anything. vals gets one entry per key
// (nil for a miss), roots and exps the matching flags and stamps;
// reuse all three returned slices across calls. Coalescing and
// aliasing rules match BatchGet: all cold misses share one
// Store.BatchGet round, and promotion during the round never evicts,
// so an earlier hot hit's arena bytes cannot be recycled under it.
func (t *Tiered) LookupBatch(ctx context.Context, keys [][]byte, vals [][]byte, roots []bool, exps []int64) ([][]byte, []bool, []int64, error) {
	vals = vals[:0]
	roots = roots[:0]
	exps = exps[:0]
	t.missKeys = t.missKeys[:0]
	t.missPos = t.missPos[:0]
	for i, k := range keys {
		v, tag, exp, hit, definitive := t.ht.probeEntry(k)
		switch {
		case hit:
			t.stats.HotHits++
			vals = append(vals, v)
			roots = append(roots, tag&TagRoot != 0)
			exps = append(exps, exp)
		case definitive:
			t.stats.Misses++
			vals = append(vals, nil)
			roots = append(roots, false)
			exps = append(exps, 0)
		default:
			vals = append(vals, nil)
			roots = append(roots, false)
			exps = append(exps, 0)
			t.missKeys = append(t.missKeys, k)
			t.missPos = append(t.missPos, i)
		}
	}
	if len(t.missKeys) == 0 {
		return vals, roots, exps, nil
	}
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return vals, roots, exps, err
	}
	for j, rec := range recs {
		if rec.Key == nil || t.expiredRec(rec) {
			t.stats.Misses++
			continue
		}
		t.stats.ColdHits++
		vals[t.missPos[j]] = nonNilValue(rec.Value)
		roots[t.missPos[j]] = rec.Root
		exps[t.missPos[j]] = rec.ExpireMs
		t.maybePromote(t.missKeys[j], rec, false)
	}
	return vals, roots, exps, nil
}

// Set writes key through the hot tier: the record is dirty until drain
// cools it. A refused write makes room by evicting clean residents, then
// by forcing a drain cycle; only a tier with nothing left to give errors.
// A store at its disk hard minimum bounces the write with ErrShed before
// it dirties anything, after one repair pass; Del stays exempt because
// deletions are how the user creates the garbage compaction reclaims,
// and shedding them would wedge a full store for good.
func (t *Tiered) Set(ctx context.Context, key, val []byte, tag uint8) error {
	return t.SetGen(ctx, key, val, tag, 0)
}

// SetGen is Set for segment records, which carry their root's
// generation so the store's liveness probe can retire them wholesale
// on a bump. User records and roots write through Set: their gen is
// zero at the seam (a root's own generation lives inside its
// payload, doc 03 section 6.3).
func (t *Tiered) SetGen(ctx context.Context, key, val []byte, tag uint8, gen uint32) error {
	if shed, err := t.lad.shed(ctx); err != nil {
		return err
	} else if shed {
		return ErrShed
	}
	if err := t.putPressured(ctx, key, val, tag, gen); err != nil {
		return err
	}
	_, err := t.lad.step(ctx)
	return err
}

// putPressured is PutGen with the full pressure ladder behind a
// refusal: byte-counted room first (evict, then drain), and when the
// write is still refused because it needs a size class the saturated
// arena has never served, the chunk-vacate stage rebudgets whole value
// chunks until the write's exact needs fit. Only a tier with truly
// nothing to give returns errHotFull.
func (t *Tiered) putPressured(ctx context.Context, key, val []byte, tag uint8, gen uint32) error {
	if t.ht.PutGen(key, val, tag, gen) {
		return nil
	}
	if err := t.makeRoomFor(ctx, len(key)+len(val)); err != nil {
		return err
	}
	if t.ht.PutGen(key, val, tag, gen) {
		return nil
	}
	if err := t.lad.vacateFor(ctx, key, val, false); err != nil {
		return err
	}
	if !t.ht.PutGen(key, val, tag, gen) {
		return errHotFull
	}
	return nil
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
			if err := t.lad.vacateFor(ctx, key, nil, true); err != nil {
				return false, err
			}
			if !t.ht.delCold(key) {
				return false, errHotFull
			}
		}
	}
	_, err = t.lad.step(ctx)
	return true, err
}

// Bump schedules a root-generation bump to ride the drain batch that
// carries key's next op. The type layer calls this before the mutation
// that retires the generation (the collection DEL or type overwrite),
// so the bump and the root's post-image can never land in different
// batches; the store applies them in one durability unit.
func (t *Tiered) Bump(key []byte, rooth uint64, newgen uint32) {
	t.dr.addBump(key, rooth, newgen)
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
// StoreStats polls the cold store's own accounting, the INFO feed.
func (t *Tiered) StoreStats() StoreStats {
	return t.st.Stats()
}

func (t *Tiered) Stats() TieredStats {
	s := t.stats
	s.Evictions = t.ev.evictions
	s.EvictedBytes = t.ev.evictedBytes
	s.ChunkVacates = t.ht.vacates
	s.ReapCancels = t.dr.cancels
	s.VolDefers = t.dr.volDefers
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
	if !t.ht.promote(key, val, tag, rec.Gen, rec.ExpireMs) {
		if canEvict {
			t.ev.evict(hdrSize + len(key) + len(val))
		}
		if !canEvict || !t.ht.promote(key, val, tag, rec.Gen, rec.ExpireMs) {
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
