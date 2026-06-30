package keyspace

import (
	"math/rand/v2"
	"sync/atomic"
)

// LFU tuning constants, matching the Redis defaults. The counter is 8 bits and
// climbs slowly so it can span a wide range of access rates, and it decays over
// time so a key that was hot once does not stay hot forever. The log factor and
// decay time are the defaults Open seeds onto the keyspace; lfu-log-factor and
// lfu-decay-time override them through SetLFUParams.
const (
	lfuInitVal   = 5  // counter a brand new key starts at
	lfuLogFactor = 10 // higher slows the climb, so 255 means very hot
	lfuDecayTime = 1  // minutes of no access that drop the counter by one
)

// keyAccess is the in-memory recency and frequency bookkeeping for one live key.
// It is not stored in the value header or the file: a reopened database rebuilds
// it as keys are accessed, the same way Redis loses approximate LRU and LFU state
// across a restart unless it is carried in the RDB.
type keyAccess struct {
	atime uint32 // unix seconds of the last access, for LRU and OBJECT IDLETIME
	freq  uint8  // LFU counter, for LFU eviction and OBJECT FREQ
	decr  uint32 // unix minutes at the last LFU decay step
}

// coarseMillis caches the wall clock in whole milliseconds. The server cron
// refreshes it once per tick through RefreshClock, so the hot path can read a
// recent timestamp from this atomic instead of paying a time.Now syscall on
// every operation. This is the same trick Redis uses with server.unixtime, which
// serverCron updates at the configured hz and the data commands read instead of
// calling gettimeofday. A clock at most one cron tick stale (100ms at the default
// hz of 10) is exact enough for the LRU and LFU bookkeeping it feeds, which
// itself is approximate and second-granular.
var coarseMillis atomic.Int64

// coarseActive is set the first time RefreshClock runs, which only the cron does.
// Until then (tests that drive the keyspace directly without StartBackground, and
// the window before the first tick), coarseSeconds reads the real clock so a test
// that stubs nowMillis still sees its overridden time. Once the cron is live the
// hot path reads the cached atomic.
//
// All keyAccess bookkeeping (atime and the LFU decr/freq) reads this same coarse
// clock: the hot path stamps it on every access, so the seeders and the
// introspection readers (Idle, Freq, accessMetrics) must read the same clock or a
// coarse-stamped field gets compared against the real clock. The two diverge
// whenever the cron lags a tick across a minute boundary, and they diverge for the
// whole run once a background server stops and freezes the cache, which would
// fire a spurious decay or idle step on the next read.
var coarseActive atomic.Bool

// RefreshClock samples the wall clock into the coarse cache. The server cron calls
// it once per tick, and StartBackground calls it once before serving so the first
// reads after startup are already warm.
func RefreshClock() {
	coarseMillis.Store(nowMillis())
	coarseActive.Store(true)
}

// coarseSeconds returns the cached wall clock in whole seconds for the hot-cache
// recency stamp. Before the cron is live it falls back to the real clock so test
// stubs of nowMillis still take effect; this keeps the hot cache's atime behaviour
// identical to nowSeconds outside a running server.
func coarseSeconds() uint32 {
	if coarseActive.Load() {
		return uint32(coarseMillis.Load() / 1000)
	}
	return uint32(nowMillis() / 1000)
}

// coarseMinutes returns the cached wall clock in whole minutes for the LFU decay
// stamp. Like coarseSeconds it reads the cron-cached atomic once the server is
// live and falls back to the real clock before the first tick, so the read hot
// path never pays a time.Now syscall to nudge the minute-granular LFU bookkeeping.
func coarseMinutes() uint32 {
	if coarseActive.Load() {
		return uint32(coarseMillis.Load() / 60000)
	}
	return uint32(nowMillis() / 60000)
}

// recordAccess updates a key's recency and frequency after it is read or written.
// isNew marks the first write of a key, which seeds the LFU counter above zero so
// a fresh key is not the instant eviction victim.
// accessMu guards the access map so concurrent reads can update eviction
// bookkeeping without blocking on the engine write lock.
//
// db.access stores *keyAccess so updates to existing entries can go through the
// pointer without a map assignment. This keeps the steady-state hot-key path
// at zero heap allocations: string(key) is used as a temporary for map lookup
// (compiler optimization, no alloc), and the value is updated via the returned
// pointer without touching the map again.
func (db *DB) recordAccess(key []byte, isNew bool) {
	if isNew {
		// A brand-new key must be seeded or the eviction sampler treats it as
		// maximally idle and evicts it first, so its first write always takes the
		// lock. New keys are rare next to repeat writes, so this does not contend.
		db.accessMu.Lock()
		if db.access == nil {
			db.access = make(map[string]*keyAccess)
		}
		db.access[string(key)] = &keyAccess{atime: coarseSeconds(), freq: lfuInitVal, decr: coarseMinutes()}
		db.accessMu.Unlock()
		return
	}
	// A repeat access only bumps recency and frequency, both of which are
	// approximate. Under a write storm to one key, fifty goroutines all want this
	// one entry, and blocking on the lock to nudge an approximate counter parks the
	// write path for no real gain. So try the lock and drop the sample if it is
	// held, the same way Redis's sampled LRU tolerates a missed bump. Recency is
	// not lost on the write path even when the sample drops: the hot-value cache
	// stamps the entry's atime atomically on every put, and accessMetrics reads the
	// fresher of the two. Only the probabilistic LFU bump is skipped.
	if !db.accessMu.TryLock() {
		return
	}
	defer db.accessMu.Unlock()
	if db.access == nil {
		db.access = make(map[string]*keyAccess)
	}
	// map index with string([]byte) is a compiler-optimized temporary: 0 alloc.
	a := db.access[string(key)]
	if a == nil {
		// First read of a key that was never written to this DB (e.g., a key that
		// arrived via replication without a local write). Seed the entry.
		k := string(key)
		a = &keyAccess{freq: lfuInitVal, decr: coarseMinutes()}
		db.access[k] = a
	}
	// Update through the pointer: no map assignment, no string allocation. The
	// recency and decay stamps read the coarse cron-cached clock rather than
	// time.Now, since this runs on every read and the bookkeeping it feeds is
	// approximate and second-granular; a stamp at most one cron tick stale is
	// exact enough and saves two syscalls per hot read.
	now := coarseMinutes()
	a.atime = coarseSeconds()
	decayed := db.lfuDecay(*a, now)
	*a = db.lfuIncr(decayed)
}

// dropAccess forgets a key's bookkeeping when it is deleted or evicted.
func (db *DB) dropAccess(key []byte) {
	db.accessMu.Lock()
	if db.access != nil {
		delete(db.access, string(key))
	}
	db.accessMu.Unlock()
}

// lfuDecay lowers the counter by one for every decay period that passed since the
// last step, then stamps the current time. A key untouched for a long time loses
// frequency, so an old burst does not protect it forever. A decay time of zero
// turns decay off, the lfu-decay-time 0 case, so the counter holds its value.
// now is the current wall clock in whole minutes, passed in so the read hot path
// can hand it the coarse cron-cached clock while introspection callers pass the
// exact clock.
func (db *DB) lfuDecay(a keyAccess, now uint32) keyAccess {
	decayTime := db.ks.lfuDecayTime
	if decayTime > 0 && a.decr != 0 && now > a.decr {
		periods := (now - a.decr) / uint32(decayTime)
		if periods > uint32(a.freq) {
			a.freq = 0
		} else {
			a.freq -= uint8(periods)
		}
	}
	a.decr = now
	return a
}

// lfuIncr raises the counter probabilistically. The chance of a bump shrinks as
// the counter grows, so common keys saturate slowly and the 8-bit field still
// separates a key hit thousands of times from one hit a handful. A log factor of
// zero makes the chance one, so the counter climbs on every access.
func (db *DB) lfuIncr(a keyAccess) keyAccess {
	if a.freq == 255 {
		return a
	}
	base := 0.0
	if a.freq > lfuInitVal {
		base = float64(a.freq - lfuInitVal)
	}
	p := 1.0 / (base*float64(db.ks.lfuLogFactor) + 1.0)
	if rand.Float64() < p {
		a.freq++
	}
	return a
}

// The hot engine carries a key's recency and frequency in a single atomic word on
// the entry instead of in db.access under a global mutex, the F2/Redis principle
// that per-record metadata lives in the record (note 354). The word packs the same
// three fields keyAccess holds: the LFU counter in the low byte, the decay minute
// (low 16 bits of the wall clock in minutes, the way Redis stores ldt) in the next
// two bytes, and the last-access second in the high 32 bits. freq and decr are
// 16-bit minute arithmetic exactly like Redis's LFUTimeElapsed, so an old burst
// still decays correctly across the minute wrap.
const (
	accFreqShift = 0
	accDecrShift = 8
	accTimeShift = 32
	accDecrMask  = 0xFFFF
)

func packAccessWord(atime uint32, decrMin uint16, freq uint8) uint64 {
	return uint64(freq)<<accFreqShift | uint64(decrMin)<<accDecrShift | uint64(atime)<<accTimeShift
}

func accWordTime(w uint64) uint32 { return uint32(w >> accTimeShift) }
func accWordDecr(w uint64) uint16 { return uint16(w >> accDecrShift) }
func accWordFreq(w uint64) uint8  { return uint8(w >> accFreqShift) }

// lfuElapsedMinutes is the minutes between the decay stamp and now in 16-bit
// wrapping arithmetic, matching Redis's LFUTimeElapsed: a now at or after the stamp
// is a plain difference, and a wrap (now below the stamp) counts forward through
// 65536. A key untouched longer than the 16-bit minute range is maximally decayed
// either way, which is correct.
func lfuElapsedMinutes(now, decr uint16) uint16 {
	if now >= decr {
		return now - decr
	}
	return (65535 - decr) + now + 1
}

// lfuDecayWord lowers the counter by one for each decay period elapsed since the
// decay stamp, the word-path twin of lfuDecay. It does not restamp: the caller that
// writes the word back stamps the current minute, and the read-only introspection
// callers (Freq, accessMetrics) want the decayed value without recording an access.
func (db *DB) lfuDecayWord(freq uint8, decr, now uint16) uint8 {
	decayTime := db.ks.lfuDecayTime
	if decayTime <= 0 || decr == 0 {
		return freq
	}
	periods := lfuElapsedMinutes(now, decr) / uint16(decayTime)
	if periods == 0 {
		return freq
	}
	if periods >= uint16(freq) {
		return 0
	}
	return freq - uint8(periods)
}

// lfuIncrWord is lfuIncr on a bare counter: the same probabilistic climb whose
// chance shrinks as the counter grows, so the 8-bit field separates a key hit
// thousands of times from one hit a handful.
func (db *DB) lfuIncrWord(freq uint8) uint8 {
	if freq == 255 {
		return 255
	}
	base := 0.0
	if freq > lfuInitVal {
		base = float64(freq - lfuInitVal)
	}
	p := 1.0 / (base*float64(db.ks.lfuLogFactor) + 1.0)
	if rand.Float64() < p {
		freq++
	}
	return freq
}

// hotAccessUpdate is the policy the hot engine runs in place on every read and
// write. It is the lock-free, side-map-free twin of recordAccess: a new key seeds
// the counter so it is not the instant eviction victim, and a repeat access decays
// then bumps the counter and restamps recency, all on the word the engine already
// holds. The engine skips the store when this returns the word unchanged, which is
// the common case for a key already touched this second whose bump did not fire.
func (db *DB) hotAccessUpdate(old uint64, isNew bool) uint64 {
	nowMin := uint16(coarseMinutes())
	if isNew {
		return packAccessWord(coarseSeconds(), nowMin, lfuInitVal)
	}
	freq := db.lfuDecayWord(accWordFreq(old), accWordDecr(old), nowMin)
	freq = db.lfuIncrWord(freq)
	return packAccessWord(coarseSeconds(), nowMin, freq)
}

// Idle returns whole seconds since the key was last accessed, the OBJECT IDLETIME
// answer. A key with no recorded access yet reports zero.
func (db *DB) Idle(key []byte) uint32 {
	if hs := db.hotAcc.Load(); hs != nil {
		if w, ok := hs.AccessWord(key); ok {
			now := coarseSeconds()
			if at := accWordTime(w); now > at {
				return now - at
			}
			return 0
		}
	}
	db.accessMu.Lock()
	a := db.access[string(key)]
	if a == nil {
		db.accessMu.Unlock()
		return 0
	}
	atime := a.atime
	db.accessMu.Unlock()
	now := coarseSeconds()
	if now < atime {
		return 0
	}
	return now - atime
}

// Freq returns the decayed LFU counter, the OBJECT FREQ answer. The decay is
// computed for the read but not stored, since reading frequency is not itself an
// access.
func (db *DB) Freq(key []byte) uint8 {
	if hs := db.hotAcc.Load(); hs != nil {
		if w, ok := hs.AccessWord(key); ok {
			return db.lfuDecayWord(accWordFreq(w), accWordDecr(w), uint16(coarseMinutes()))
		}
	}
	db.accessMu.Lock()
	a := db.access[string(key)]
	if a == nil {
		db.accessMu.Unlock()
		return 0
	}
	cp := *a
	db.accessMu.Unlock()
	return db.lfuDecay(cp, coarseMinutes()).freq
}

// SetIdle seeds a key's last-access time to idle seconds in the past, which is how
// RESTORE IDLETIME reconstructs the LRU clock of a dumped key.
func (db *DB) SetIdle(key []byte, idle uint32) {
	now := coarseSeconds()
	at := uint32(0)
	if idle < now {
		at = now - idle
	}
	// On the hot engine a just-RESTORE'd string key already lives on the engine with
	// a seeded access word; rewrite that word's recency in place rather than seeding
	// a shadow map entry the engine-first readers would never consult.
	if hs := db.hotAcc.Load(); hs != nil {
		if w, ok := hs.AccessWord(key); ok {
			decr := accWordDecr(w)
			if decr == 0 {
				decr = uint16(coarseMinutes())
			}
			hs.SetAccessWord(key, packAccessWord(at, decr, accWordFreq(w)))
			return
		}
	}
	db.accessMu.Lock()
	defer db.accessMu.Unlock()
	if db.access == nil {
		db.access = make(map[string]*keyAccess)
	}
	a := db.access[string(key)]
	if a == nil {
		k := string(key)
		a = &keyAccess{decr: coarseMinutes()}
		db.access[k] = a
	}
	a.atime = at
	if a.decr == 0 {
		a.decr = coarseMinutes()
	}
}

// SetFreq seeds a key's LFU counter, which is how RESTORE FREQ reconstructs the
// frequency of a dumped key.
func (db *DB) SetFreq(key []byte, freq uint8) {
	// On the hot engine the key already carries its access word; overwrite the
	// counter in place and restamp the decay minute, the word twin of the map seed
	// below, so the engine-first readers see the restored frequency.
	if hs := db.hotAcc.Load(); hs != nil {
		if w, ok := hs.AccessWord(key); ok {
			at := accWordTime(w)
			if at == 0 {
				at = coarseSeconds()
			}
			hs.SetAccessWord(key, packAccessWord(at, uint16(coarseMinutes()), freq))
			return
		}
	}
	db.accessMu.Lock()
	defer db.accessMu.Unlock()
	if db.access == nil {
		db.access = make(map[string]*keyAccess)
	}
	a := db.access[string(key)]
	if a == nil {
		k := string(key)
		a = &keyAccess{}
		db.access[k] = a
	}
	a.freq = freq
	a.decr = coarseMinutes()
	if a.atime == 0 {
		a.atime = coarseSeconds()
	}
}

// accessMetrics returns the recency timestamp and the decayed frequency the
// eviction sampler sorts on. A key with no record yet looks maximally idle and
// minimally frequent, so an un-accessed key is evicted before a tracked one.
//
// HotGet updates the hot-cache entry's atime atomically on each hit without
// calling recordAccess, so we check both the hot cache and the access map and
// take the more recent atime. LFU frequency still comes from the access map.
func (db *DB) accessMetrics(key []byte) (atime uint32, freq uint8) {
	// On the hot engine the key's recency and frequency live inline on its entry, so
	// the eviction sampler reads them straight off the word with no lock and no map
	// probe. This is the F2/Redis principle the side map could not give the engine.
	if hs := db.hotAcc.Load(); hs != nil {
		if w, ok := hs.AccessWord(key); ok {
			return accWordTime(w), db.lfuDecayWord(accWordFreq(w), accWordDecr(w), uint16(coarseMinutes()))
		}
	}
	hotAtime, inCache := db.hc.Load().cgetAtime(key)

	db.accessMu.Lock()
	a := db.access[string(key)]
	db.accessMu.Unlock()

	if a == nil {
		return hotAtime, 0
	}
	at := a.atime
	if inCache && hotAtime > at {
		at = hotAtime
	}
	return at, db.lfuDecay(*a, coarseMinutes()).freq
}
