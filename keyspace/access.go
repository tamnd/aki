package keyspace

import "math/rand/v2"

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

// nowSeconds and nowMinutes read the same clock the rest of the keyspace uses, so
// tests that stub nowMillis move the access clock too.
func nowSeconds() uint32 { return uint32(nowMillis() / 1000) }
func nowMinutes() uint32 { return uint32(nowMillis() / 60000) }

// recordAccess updates a key's recency and frequency after it is read or written.
// isNew marks the first write of a key, which seeds the LFU counter above zero so
// a fresh key is not the instant eviction victim.
func (db *DB) recordAccess(key []byte, isNew bool) {
	if db.access == nil {
		db.access = make(map[string]keyAccess)
	}
	k := string(key)
	if isNew {
		db.access[k] = keyAccess{atime: nowSeconds(), freq: lfuInitVal, decr: nowMinutes()}
		return
	}
	a, ok := db.access[k]
	if !ok {
		a = keyAccess{freq: lfuInitVal, decr: nowMinutes()}
	}
	a.atime = nowSeconds()
	a = db.lfuIncr(db.lfuDecay(a))
	db.access[k] = a
}

// dropAccess forgets a key's bookkeeping when it is deleted or evicted.
func (db *DB) dropAccess(key []byte) {
	if db.access != nil {
		delete(db.access, string(key))
	}
}

// lfuDecay lowers the counter by one for every decay period that passed since the
// last step, then stamps the current time. A key untouched for a long time loses
// frequency, so an old burst does not protect it forever. A decay time of zero
// turns decay off, the lfu-decay-time 0 case, so the counter holds its value.
func (db *DB) lfuDecay(a keyAccess) keyAccess {
	now := nowMinutes()
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

// Idle returns whole seconds since the key was last accessed, the OBJECT IDLETIME
// answer. A key with no recorded access yet reports zero.
func (db *DB) Idle(key []byte) uint32 {
	a, ok := db.access[string(key)]
	if !ok {
		return 0
	}
	now := nowSeconds()
	if now < a.atime {
		return 0
	}
	return now - a.atime
}

// Freq returns the decayed LFU counter, the OBJECT FREQ answer. The decay is
// computed for the read but not stored, since reading frequency is not itself an
// access.
func (db *DB) Freq(key []byte) uint8 {
	a, ok := db.access[string(key)]
	if !ok {
		return 0
	}
	return db.lfuDecay(a).freq
}

// SetIdle seeds a key's last-access time to idle seconds in the past, which is how
// RESTORE IDLETIME reconstructs the LRU clock of a dumped key.
func (db *DB) SetIdle(key []byte, idle uint32) {
	if db.access == nil {
		db.access = make(map[string]keyAccess)
	}
	now := nowSeconds()
	at := uint32(0)
	if idle < now {
		at = now - idle
	}
	k := string(key)
	a := db.access[k]
	a.atime = at
	if a.decr == 0 {
		a.decr = nowMinutes()
	}
	db.access[k] = a
}

// SetFreq seeds a key's LFU counter, which is how RESTORE FREQ reconstructs the
// frequency of a dumped key.
func (db *DB) SetFreq(key []byte, freq uint8) {
	if db.access == nil {
		db.access = make(map[string]keyAccess)
	}
	k := string(key)
	a := db.access[k]
	a.freq = freq
	a.decr = nowMinutes()
	if a.atime == 0 {
		a.atime = nowSeconds()
	}
	db.access[k] = a
}

// accessMetrics returns the recency timestamp and the decayed frequency the
// eviction sampler sorts on. A key with no record yet looks maximally idle and
// minimally frequent, so an un-accessed key is evicted before a tracked one.
func (db *DB) accessMetrics(key []byte) (atime uint32, freq uint8) {
	a, ok := db.access[string(key)]
	if !ok {
		return 0, 0
	}
	return a.atime, db.lfuDecay(a).freq
}
