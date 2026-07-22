package set

import (
	"math/bits"
	"math/rand/v2"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The set type keeps its per-key structures in an owner-local registry hung off
// the shard's Ctx.Coll (spec 2064/f3/11): one map from key to the inline set,
// touched only by the shard goroutine, so it holds no lock. The string store
// and this registry are separate keyspaces for now; the WRONGTYPE guard below
// keeps a set command off a key the string store owns. TYPE, single-key EXISTS,
// and single-key DEL have moved on to span every type (their unified handlers
// consult set.Has and set.Delete here); full cross-type unification (a SET
// overwriting a set, multi-key DEL over sets) lands with the keyspace slice;
// this slice keeps the set surface self-consistent and refuses the cross-type
// collisions it cannot yet resolve.

// reg is the shard's set registry plus its draw state. The PRNG is owner-local
// (doc 11 section 5.6): SPOP and SRANDMEMBER draw from a per-shard PCG that is
// never shared and never locked, so the draw path takes no atomic and touches no
// global rand state. The two scratch slices back the count-form draws and are
// reused across commands, so a steady-state count draw allocates nothing.
type reg struct {
	m   map[string]*set
	rng rand.PCG

	// idxScratch is the full index permutation the large-sample distinct draw
	// (SRANDMEMBER positive count above the crossover) partial-shuffles in place.
	idxScratch []uint32
	// pickScratch holds the indexes already chosen by the small-sample distinct
	// draw so its rejection loop can skip repeats without a map.
	pickScratch []int

	// scratch is the single reusable set the single-key funnel materializes an
	// arena-homed tiny set into (resolveTouch), so a command over a tiny set
	// backed by an inline arena record (inline.go) loads, mutates, and commits it
	// back with no per-command heap allocation. It is owner-local and valid only
	// within one command: the next resolveTouch reloads it. Multi-key paths that
	// must hold more than one operand at once (gather, smove) materialize into
	// fresh sets instead, since they cannot share this one buffer.
	scratch *set

	// resident is the running sum of every live set's resident-byte footprint
	// (set.residentBytes), the figure the shard reads to weigh a collection's RAM
	// against the store's arena under memory pressure (spec 2064/f3/06 section 6).
	// It is kept exact by note and drop across the map's inserts and removes, so
	// the shard never walks the registry to size it. Maintained only when acctOn.
	resident uint64
	// acctOn gates the accounting: it is true only when the shard's store runs the
	// cold tier (ColdConfigured). With no cold region to demote a set into the
	// figure would drive nothing, so the registry keeps none and note is one bool
	// load, holding the L9 zero-delta contract for a store with no resident cap.
	acctOn bool
}

// setSeed hands each shard's registry a distinct PCG stream. The counter is
// touched once per registry, at first use, never on the draw path, so the "never
// locked" contract of doc 11 section 5.6 holds where it matters. The stream is
// deterministic given creation order, which keeps a shard's draws reproducible
// for a replay without a shared global generator.
var setSeed atomic.Uint64

func freshPCG() rand.PCG {
	n := setSeed.Add(1)
	return *rand.NewPCG(n*0x9e3779b97f4a7c15+0x243f6a8885a308d3, n*0xbf58476d1ce4e5b9+0x13198a2e03707344)
}

// registry returns the shard's set registry, building it on first use. The
// Ctx and thus the registry live for the worker's whole life, so a set added
// on one command is there for the next.
func registry(cx *shard.Ctx) *reg {
	if cx.Coll == nil {
		cx.Coll = &reg{
			m:       make(map[string]*set),
			rng:     freshPCG(),
			acctOn:  cx.St != nil && cx.St.ColdConfigured(),
			scratch: &set{},
		}
	}
	return cx.Coll.(*reg)
}

// next returns a uniform integer in [0, n) with no modulo bias (F15 exact
// uniformity): Lemire's multiply-shift with rejection, the same unbiased bound
// math/rand/v2 uses, over the owner-local PCG. n is always positive at the call
// sites, since a draw only runs on a non-empty set.
func (g *reg) next(n int) int {
	un := uint64(n)
	hi, lo := bits.Mul64(g.rng.Uint64(), un)
	if lo < un {
		// Only the low tail of the 2^64 space can bias the result; reject it.
		thresh := -un % un
		for lo < thresh {
			hi, lo = bits.Mul64(g.rng.Uint64(), un)
		}
	}
	return int(hi)
}

// Has reports whether key holds a set on this shard, without building the
// registry when none exists yet. It is the presence probe the unified TYPE
// consults across the collection types: a string value or another collection at
// key is not a set, so Has reads false for those and leaves the type to the
// caller's other probes.
func Has(cx *shard.Ctx, key []byte) bool {
	if cx.Coll != nil && cx.Coll.(*reg).peek(cx, key) != nil {
		return true
	}
	_, _, _, present := peekArenaSet(cx, key)
	return present
}

// Delete removes key when it holds a set on this shard and reports whether it
// did: the set arm of the unified single-key DEL. It builds no registry when
// none exists, so a DEL over a key of another type touches nothing here. Cold
// chunks a demoted set left behind are not reclaimed yet, the same deferral
// every collection carries until the cold-reclamation slice threads DEL.
func Delete(cx *shard.Ctx, key []byte) bool {
	if cx.Coll == nil {
		return false
	}
	g := cx.Coll.(*reg)
	if g.live(cx, key) != nil {
		logDeleteKey(cx, key)
		g.drop(key)
		return true
	}
	if _, _, _, present := peekArenaSet(cx, key); present {
		logDeleteKey(cx, key)
		cx.St.DropCollBlob(key)
		return true
	}
	return false
}

// Flush drops every set on this shard, the set arm of FLUSHALL and FLUSHDB. It
// clears the map and zeroes the resident-byte total, so a flush leaves the
// registry empty and weighing nothing, matching the store the flush just reset.
// The draw PRNG and the scratch slices are kept, since a flush replaces the keys,
// not the shard's registry object. It builds no registry when none exists.
func Flush(cx *shard.Ctx) {
	if cx.Coll == nil {
		return
	}
	g := cx.Coll.(*reg)
	g.m = make(map[string]*set)
	g.resident = 0
}

// Len is the number of sets this shard holds, the set contribution to DBSIZE. A
// dropped set leaves the map, so the map size is the live count; it reads zero
// before any set command has built a registry on this shard.
func Len(cx *shard.Ctx) int {
	n := 0
	if cx.Coll != nil {
		n = len(cx.Coll.(*reg).m)
	}
	if cx.St != nil {
		total, _ := cx.St.CountCollKind(store.KindSet)
		n += int(total)
	}
	return n
}

// VolatileLen counts the sets on this shard carrying a key-level TTL, the set
// contribution to INFO's Keyspace expires field. It walks the registry map
// counting a non-zero deadline whether or not it has passed, matching the
// map-size basis of Len (a lazily-expired-but-unreaped set still shows in both
// totals until a read drops it). INFO is a cold path, so the O(keys) walk is off
// every command's critical path. It builds no registry when none exists.
func VolatileLen(cx *shard.Ctx) uint64 {
	if cx.Coll == nil {
		return 0
	}
	var n uint64
	for _, s := range cx.Coll.(*reg).m {
		if s.expireAt != 0 {
			n++
		}
	}
	if cx.St != nil {
		_, withTTL := cx.St.CountCollKind(store.KindSet)
		n += withTTL
	}
	return n
}

// RangeKeys calls fn with every set key on this shard, the set contribution to
// the unified KEYS and SCAN walk. It builds no registry when none exists, so a
// shard that ran no set command yields nothing. It returns false when fn asked
// to stop, halting the outer walk for a bounded scan. The slice fn receives is
// the map key's bytes, valid only for that call; fn copies what it keeps.
func RangeKeys(cx *shard.Ctx, fn func(key []byte) bool) bool {
	now := cx.NowMs
	if cx.Coll != nil {
		for k, s := range cx.Coll.(*reg).m {
			// Skip a lazily-expired set so KEYS and SCAN never surface a key EXISTS
			// would report absent. The skip is read-only (no drop) to match the string
			// store's expiry-aware walk, which reaps nothing during a scan.
			if s.expireAt != 0 && s.expireAt <= now {
				continue
			}
			if !fn([]byte(k)) {
				return false
			}
		}
	}
	// The arena home: every tiny set inline in a store record, walked the same
	// expiry-aware, read-only way (RangeCollKind skips a past-deadline record when
	// now is non-zero and reaps nothing), so KEYS and SCAN union the two homes.
	if cx.St != nil {
		cont := true
		cx.St.RangeCollKind(store.KindSet, now, func(key []byte) bool {
			cont = fn(key)
			return cont
		})
		if !cont {
			return false
		}
	}
	return true
}

// live returns the set at key, or nil when none exists or the set has lazily
// expired. A set whose deadline has passed is dropped here and treated as absent,
// so an expired set is dead to this command and every later one in the epoch
// (spec 2064/f3/16 section 2). This is the one funnel every read, mutate, create,
// and probe path routes through, so the expiry check lives in exactly one place.
// The deadline compare is a single field load against cx.NowMs, predicted away
// for the common set that carries no TTL.
func (g *reg) live(cx *shard.Ctx, key []byte) *set {
	s := g.peek(cx, key)
	if s != nil {
		// Record the access the way Redis stamps robj.lru on every lookup: live is
		// the read, mutate, and create funnel, so one stamp here clocks every real
		// command. The read-only probes (peek) skip it, so OBJECT IDLETIME, OBJECT
		// ENCODING, MEMORY USAGE, EXISTS, and TYPE are NOTOUCH, matching Redis.
		s.clock = store.LRUClock(cx.NowMs)
	}
	return s
}

// peek returns the live set at key without recording an access, the NOTOUCH
// resolve the read-only introspection and presence probes use so a query does
// not reset the key's idle clock. It still reaps a lazily-expired set, since an
// expired set is absent to a probe just as it is to a command.
func (g *reg) peek(cx *shard.Ctx, key []byte) *set {
	s := g.m[string(key)]
	if s == nil {
		return nil
	}
	if s.expireAt != 0 && s.expireAt <= cx.NowMs {
		// A lazily-expired set publishes the expired event on its way out, the same
		// notification the active cycle sends. Gated on the notify mask, so it costs
		// one atomic load only when a set actually expires here.
		cx.NotifyKeyspaceEvent(shard.NotifyExpired, "expired", key)
		g.drop(key)
		return nil
	}
	return s
}

// install puts a freshly built set under key and stamps its access clock, so a
// brand-new key reads idle zero and then accrues idle from creation the way Redis
// stamps robj.lru in createObject. Every path that first places a set in the map
// (SADD, the *STORE result, SMOVE's destination, WAL replay) routes through here,
// so no create path leaves the clock at zero, which the idle read would otherwise
// misreport as a near-full wrap of idle time. It does not touch the resident
// total; the caller's note posts the new set's footprint.
func (g *reg) install(cx *shard.Ctx, key []byte, s *set) {
	s.clock = store.LRUClock(cx.NowMs)
	g.m[string(key)] = s
}

// IdleSeconds reports seconds since the set at key was last accessed by a
// command, the set arm of OBJECT IDLETIME, read back from the per-key access
// clock without touching it (NOTOUCH). ok is false when no set lives at key, so
// the dispatcher can fall through to the other keyspaces.
func (g *reg) IdleSeconds(cx *shard.Ctx, key []byte) (int64, bool) {
	s := g.peek(cx, key)
	if s == nil {
		return 0, false
	}
	return store.IdleSecondsFrom(s.clock, cx.NowMs), true
}

// IdleSeconds is the package entry the dispatcher calls for OBJECT IDLETIME on a
// set key. It builds no registry when none exists, the read-only discipline
// every probe keeps.
func IdleSeconds(cx *shard.Ctx, key []byte) (int64, bool) {
	if g, ok := cx.Coll.(*reg); ok {
		if v, present := g.IdleSeconds(cx, key); present {
			return v, true
		}
	}
	// The arena home: a tiny set's idle clock rides its record bits word, read back
	// through inlineIdleSeconds without materializing the set (inline.go).
	if _, bits, _, present := peekArenaSet(cx, key); present {
		return inlineIdleSeconds(bits, cx.NowMs), true
	}
	return 0, false
}

// lookup finds the set for key. present is false when no set exists or it has
// lazily expired; wrong is true when the key instead holds a value in the string
// store, which every set command answers with WRONGTYPE.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (s *set, wrong bool) {
	if s = g.live(cx, key); s != nil {
		return s, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// A set key lives in exactly one of two homes (spec 2064/f3, keyspace-unification
// arc). A tiny set (intset or listpack class) lives inline in one store arena
// record via the CollBlob primitives (collblob.go), the memory-bar form that
// spends no Go-heap header, no separate data slice, and no registry map entry on
// it. A set that has escalated past the inline bands (hashtable or partitioned)
// lives in the Go-heap registry map g.m, where its native table already outweighs
// the map overhead. The invariant the whole routing layer keeps is that a key is
// never in both homes at once: a create writes the arena, an escalation moves the
// set out of the arena into g.m, and every read resolves the two homes in one
// funnel so a command never sees a key twice or misses it.
const (
	homeAbsent = iota // no set at key (and no other-type value)
	homeString        // key holds a string or another collection type: WRONGTYPE
	homeReg           // an escalated set living in the Go-heap registry map g.m
	homeArena         // a tiny set living inline in a store arena record
)

// peekArenaSet resolves the tiny arena-homed set at key, reaping and reporting
// absent for a lazily-expired one exactly as the registry's peek does for a
// g.m-homed set: it fires the expired keyspace event and drops the record so the
// set is absent to this command and every later one in the epoch. present is
// false for a missing key, a key that holds a string or another collection type,
// or an expired set. The returned blob and bits alias the arena and are valid
// only until the next store write that could republish the record.
func peekArenaSet(cx *shard.Ctx, key []byte) (blob []byte, bits uint16, at int64, present bool) {
	if cx.St == nil {
		return nil, 0, 0, false
	}
	b, kind, bt, a, ok := cx.St.PeekCollBlob(key)
	if !ok || kind != store.KindSet {
		return nil, 0, 0, false
	}
	if a != 0 && a <= cx.NowMs {
		cx.NotifyKeyspaceEvent(shard.NotifyExpired, "expired", key)
		cx.St.DropCollBlob(key)
		return nil, 0, 0, false
	}
	return b, bt, a, true
}

// resolveInto is the dual-home resolve every single-key command funnels through:
// it returns the live set at key and which home it lives in, reaping a lazily-
// expired set from either home first. An escalated set in g.m is returned in
// place and clocked the way live does (homeReg). A tiny arena set is materialized
// into dst, a copy the caller may mutate freely before commit writes it back
// (homeArena); dst is the reusable scratch for the single-key funnel or a fresh
// set for the multi-operand paths that cannot share one buffer. A key holding a
// string or another collection type resolves homeString, which every set command
// answers WRONGTYPE. A missing key resolves homeAbsent. The arena materialization
// does not re-stamp the record's idle clock: a read leaves it NOTOUCH, and a
// mutating command re-stamps it through commit, which rewrites the record anyway.
func (g *reg) resolveInto(cx *shard.Ctx, key []byte, dst *set) (s *set, home int) {
	if s = g.live(cx, key); s != nil {
		return s, homeReg
	}
	if blob, bits, at, present := peekArenaSet(cx, key); present {
		loadInline(dst, blob, bits, at)
		return dst, homeArena
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, homeString
	}
	return nil, homeAbsent
}

// resolveTouch is the single-key funnel's resolve: it loads a tiny arena set into
// the registry's reusable scratch, so the common single-key command over a tiny
// set allocates nothing. The scratch is valid only until the next resolveTouch,
// so a command reads and commits it within its own body.
func (g *reg) resolveTouch(cx *shard.Ctx, key []byte) (s *set, home int) {
	return g.resolveInto(cx, key, g.scratch)
}

// newSetInto resets dst to a fresh empty set whose first member chooses the band
// (an integer first member opens an intset, matching newSet), the zero-alloc
// create the arena funnel uses so a brand-new tiny set is built in the reusable
// scratch rather than a heap allocation. commit then embeds it in the arena.
func newSetInto(dst *set, first []byte) {
	dst.clock = 0
	dst.expireAt = 0
	dst.ht = nil
	dst.part = nil
	dst.cold = nil
	dst.acct = 0
	dst.n = 0
	dst.data = dst.data[:0]
	if _, ok := store.ParseInt(first); ok {
		dst.enc = encIntset
	} else {
		dst.enc = encListpack
	}
}

// evacuate moves a set out of the reusable scratch into a fresh heap set, the
// homeArena-to-homeReg transition an escalation triggers: a tiny set that grew
// past the inline bands mid-command is no longer a single packed blob, so it
// leaves the arena for the Go-heap registry. It is called only for an escalated
// set (hashtable or partitioned), whose data slice is already nil and whose table
// lives in ht/part, so the shallow struct copy hands those pointers to the new
// set and the scratch is detached so its reuse never aliases the escaped set.
func evacuate(s *set) *set {
	ns := &set{}
	*ns = *s
	s.ht = nil
	s.part = nil
	s.cold = nil
	s.data = nil
	return ns
}

// commit writes a single-key command's resolved set back to its home after the
// command mutated it. home is what resolveTouch returned (or homeArena for a set
// freshly built with newSetInto). An emptied set is dropped from its home, since
// the registry and the arena both keep the last-member-leaves rule. A set still
// in the inline bands re-embeds in the arena; one that escalated mid-command
// evacuates to g.m. An escalated set already in g.m (homeReg) is mutated in
// place, so commit only reconciles its footprint. It stamps the arena record's
// idle clock on the re-embed, the access this write records.
func (g *reg) commit(cx *shard.Ctx, key []byte, s *set, home int) {
	if s.card() == 0 {
		switch home {
		case homeReg:
			g.drop(key)
		case homeArena:
			cx.St.DropCollBlob(key)
		}
		return
	}
	if home == homeReg {
		g.note(s)
		return
	}
	if inlineEligible(s) {
		bits := packBits(s, inlineClock(cx.NowMs))
		// A tiny set is far under collInlineMax, so PutCollBlob only errors on an
		// arena that cannot grow, the same out-of-room condition the string SET path
		// surfaces; the set funnel has no reply channel here, so the write is best
		// effort, matching the in-memory paths that never fail.
		_ = cx.St.PutCollBlob(key, store.KindSet, bits, s.data, s.expireAt, cx.NowMs)
		return
	}
	// Escalated past the inline bands mid-command: leave the arena for the Go-heap
	// registry. DropCollBlob is a no-op when the key was never in the arena (a
	// freshly created set that escalated before its first commit), so a brand-new
	// large set installs cleanly.
	cx.St.DropCollBlob(key)
	ns := evacuate(s)
	g.install(cx, key, ns)
	g.note(ns)
}

// operand resolves one set-algebra operand for the multi-key read paths (gather),
// which hold every operand at once and so cannot share the single-key scratch: a
// tiny arena set is materialized into a FRESH heap set, so operand two's load
// never scribbles operand one's buffer. An escalated set in g.m is returned in
// place (the drivers only read it). wrong is true when the key holds a string or
// another collection type; a missing set resolves to a nil operand the drivers
// read as empty. The materialized operand is read-only and never committed, so
// the arena record is untouched.
func (g *reg) operand(cx *shard.Ctx, key []byte) (s *set, wrong bool) {
	if s = g.live(cx, key); s != nil {
		return s, false
	}
	if blob, bits, at, present := peekArenaSet(cx, key); present {
		ns := &set{}
		loadInline(ns, blob, bits, at)
		return ns, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

// note reconciles s's footprint into the running total: it posts the delta since
// the last note, so the total stays the exact sum of every live set's footprint.
// A mutating command calls it before returning on any set that survives the
// command (an emptied set goes through drop instead), which keeps the total exact
// at every command boundary, the only point the shard reads it. It is a single
// bool load when accounting is off. Owner goroutine only.
func (g *reg) note(s *set) {
	if !g.acctOn {
		return
	}
	nb := s.residentBytes()
	g.resident += nb - s.acct
	s.acct = nb
}

// drop removes a set from the registry: Redis deletes a set the moment its last
// member leaves, and the STORE and DEL paths drop a replaced or deleted key. It
// takes the set's last-posted footprint back out of the running total, so the
// total never carries a gone set's bytes.
func (g *reg) drop(key []byte) {
	if g.acctOn {
		if s := g.m[string(key)]; s != nil {
			g.resident -= s.acct
		}
	}
	delete(g.m, string(key))
}

// ResidentBytes is the running sum of every live set's resident-byte footprint on
// this shard, the collection half of the store's memory-pressure figure (spec
// 2064/f3/06 section 6). It is zero when the store runs no cold tier. The shard
// reads it at a demote boundary; the trigger that consumes it lands with the set
// demotion slice. Owner goroutine only.
func (g *reg) ResidentBytes() uint64 { return g.resident }

// ResidentBytes exposes the shard's set-registry resident-byte total to the
// worker's demote loop through the owner-local Coll slot, without the shard
// package reaching into the set registry's shape. It is zero before any set
// command has built a registry on this shard, or when the store runs no cold
// tier. Owner goroutine only.
func ResidentBytes(cx *shard.Ctx) uint64 {
	if g, ok := cx.Coll.(*reg); ok {
		return g.ResidentBytes()
	}
	return 0
}
