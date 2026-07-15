package sqlo1

// ghostEntry is doc 04's 16-byte ghost: the hash of a recently evicted
// key plus its last stamps, enough to recognize a returning key as warm
// and hand its history back.
type ghostEntry struct {
	hash      uint64
	lastRead  uint32
	lastWrite uint32
}

// ghostRing holds ghosts in a direct-mapped table of capacity/16 slots
// (doc 04 section 3). Direct-mapped rather than a FIFO with an index
// because the doc's budget is 16 bytes per ghost and any exact-recency
// structure would bust it; a slot collision simply replaces the older
// ghost, which costs recall on a sliver of keys and nothing else.
type ghostRing struct {
	slots []ghostEntry
}

func newGhostRing(n int) ghostRing {
	if n < 1 {
		n = 1
	}
	return ghostRing{slots: make([]ghostEntry, n)}
}

// put files a ghost. Hash zero is the empty-slot sentinel, so the one
// key in 2^64 that hashes to zero is never remembered; it re-enters cold
// like any ghost lost to a collision.
func (g *ghostRing) put(h uint64, lastRead, lastWrite uint32) {
	if h == 0 {
		return
	}
	g.slots[h%uint64(len(g.slots))] = ghostEntry{hash: h, lastRead: lastRead, lastWrite: lastWrite}
}

// peek reports whether a ghost for h survives, without clearing it; the
// promotion decision looks before the insert path takes.
func (g *ghostRing) peek(h uint64) bool {
	return h != 0 && g.slots[h%uint64(len(g.slots))].hash == h
}

// take returns and clears the ghost for h, if one survived.
func (g *ghostRing) take(h uint64) (ghostEntry, bool) {
	if h == 0 {
		return ghostEntry{}, false
	}
	s := &g.slots[h%uint64(len(g.slots))]
	if s.hash != h {
		return ghostEntry{}, false
	}
	e := *s
	*s = ghostEntry{}
	return e, true
}
