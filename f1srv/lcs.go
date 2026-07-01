package f1srv

// LCS (spec 2064/f1_rewrite_ltm/04 and /12): the longest common subsequence of two string
// values. It is the one heavy string command, an O(n*m) dynamic program, so doc 04 flags it
// risk-noted with a size cap in the LTM regime; here it is the exact arithmetic and the exact
// Redis reply shape on top of two string reads. A missing key reads as an empty string and a
// non-string key is the LCS-specific type error, not the generic WRONGTYPE.
//
//	LCS key1 key2 [LEN] [IDX [MINMATCHLEN n] [WITHMATCHLEN]]
//
// The default form returns the subsequence as a bulk string, LEN returns its length, and IDX
// returns the match blocks (a map, flattened to a four-element array on RESP2) from the end of
// the strings toward the start, optionally filtered by MINMATCHLEN and tagged WITHMATCHLEN.

// lcsMatch is one contiguous block shared by the two strings, with inclusive byte ranges into
// each and the block length.
type lcsMatch struct {
	aStart, aEnd int
	bStart, bEnd int
	length       int
}

// cmdLCS implements LCS. The validation order mirrors Redis exactly: arity first, then the key
// types (both must be string or missing, checked before any option is parsed), then the option
// words, and only once the whole option list parses cleanly does the LEN-with-IDX conflict fire.
func (c *connState) cmdLCS(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'lcs' command")
		return
	}
	k1, k2 := argv[1], argv[2]

	// Snapshot both values under both key stripes, reaping an expired key first so it reads as
	// an empty string. Redis reads and type-checks the keys before it parses the options, so a
	// non-string key is rejected here regardless of what follows in the argument list. The copy
	// is taken under the lock and the DP runs after the unlock, so the O(n*m) work never holds a
	// stripe lock.
	unlock := c.lockStripes([][]byte{k1, k2})
	a, okA := c.lcsSnapshot(k1)
	b, okB := c.lcsSnapshot(k2)
	unlock()
	if !okA || !okB {
		c.writeErr("ERR The specified keys must contain string values")
		return
	}

	var wantLen, wantIdx, withMatchLen bool
	var minMatchLen int64
	for i := 3; i < len(argv); i++ {
		switch {
		case eqFold(argv[i], "LEN"):
			wantLen = true
		case eqFold(argv[i], "IDX"):
			wantIdx = true
		case eqFold(argv[i], "WITHMATCHLEN"):
			withMatchLen = true
		case eqFold(argv[i], "MINMATCHLEN"):
			i++
			if i >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			// Redis parses this with string2ll, so a non-integer or an out-of-range value is
			// "value is not an integer or out of range", not a syntax error, and a negative value
			// is accepted (it simply filters nothing).
			v, ok := strictInt64(argv[i])
			if !ok {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			minMatchLen = v
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	if wantLen && wantIdx {
		c.writeErr("ERR If you want both the length and indexes, please just use IDX.")
		return
	}

	length, lcsStr, matches := computeLCS(a, b)
	switch {
	case wantLen:
		c.writeInt(int64(length))
	case wantIdx:
		c.writeLCSMatches(matches, length, minMatchLen, withMatchLen)
	default:
		// The empty-LCS reply is an empty bulk string, not a nil, matching Redis.
		c.writeBulk(lcsStr)
	}
}

// lcsSnapshot reaps key if it has expired, then returns an owned copy of its string value and
// true, an empty value and true for a missing key, or nil and false for a key held by another
// type. The value is copied into a fresh slice (not the shared vbuf) so both snapshots survive
// the DP that follows without clobbering each other. It runs under the caller's stripe lock, so
// it reaps with the locked primitives rather than expireIfNeeded, which would re-enter the lock.
func (c *connState) lcsSnapshot(key []byte) ([]byte, bool) {
	if c.srv.volatile.Load() != 0 {
		if at, ok := c.getExpiry(key); ok && at <= c.nowMs {
			c.dropKeyLocked(key)
		}
	}
	switch c.resolveType(key) {
	case keyMissing:
		return nil, true
	case keyString:
		v, _ := c.srv.store.Get(key, nil)
		return v, true
	default:
		return nil, false
	}
}

// computeLCS runs the standard O(m*n) dynamic program and backtracks to produce the LCS length,
// the subsequence string, and the contiguous match blocks in the order Redis emits them, from
// the end of the strings toward the start.
func computeLCS(a, b []byte) (length int, lcsStr []byte, matches []lcsMatch) {
	m, n := len(a), len(b)
	if m == 0 || n == 0 {
		return 0, nil, nil
	}
	// dp is a flat (m+1) x (n+1) grid; dp[i*(n+1)+j] is the LCS length of the first i bytes of a
	// and the first j bytes of b.
	dp := make([]int, (m+1)*(n+1))
	at := func(i, j int) int { return dp[i*(n+1)+j] }
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if a[i-1] == b[j-1] {
				dp[i*(n+1)+j] = at(i-1, j-1) + 1
			} else if at(i-1, j) >= at(i, j-1) {
				dp[i*(n+1)+j] = at(i-1, j)
			} else {
				dp[i*(n+1)+j] = at(i, j-1)
			}
		}
	}
	length = at(m, n)

	lcsStr = make([]byte, length)
	idx := length
	i, j := m, n
	prevAI, prevBI := -1, -1
	for i > 0 && j > 0 {
		if a[i-1] == b[j-1] {
			idx--
			lcsStr[idx] = a[i-1]
			ai, bi := i-1, j-1
			// A new block starts whenever this character is not diagonally adjacent to the
			// previous matched character.
			if prevAI != ai+1 || prevBI != bi+1 {
				matches = append(matches, lcsMatch{aStart: ai, aEnd: ai, bStart: bi, bEnd: bi, length: 1})
			} else {
				last := &matches[len(matches)-1]
				last.aStart = ai
				last.bStart = bi
				last.length++
			}
			prevAI, prevBI = ai, bi
			i--
			j--
		} else if at(i-1, j) >= at(i, j-1) {
			i--
		} else {
			j--
		}
	}
	return length, lcsStr, matches
}

// writeLCSMatches writes the IDX reply: on RESP2 the map is a flat four-element array of the
// "matches" key, the block list (longest-first, filtered by MINMATCHLEN and tagged WITHMATCHLEN),
// the "len" key, and the total LCS length. A match block whose length is below MINMATCHLEN is
// dropped, but the reported total length is always the full LCS length, before the filter.
func (c *connState) writeLCSMatches(matches []lcsMatch, length int, minMatchLen int64, withMatchLen bool) {
	var kept []lcsMatch
	for _, m := range matches {
		if int64(m.length) < minMatchLen {
			continue
		}
		kept = append(kept, m)
	}

	c.writeArrayHeader(4)
	c.writeBulk([]byte("matches"))
	c.writeArrayHeader(len(kept))
	for _, m := range kept {
		if withMatchLen {
			c.writeArrayHeader(3)
		} else {
			c.writeArrayHeader(2)
		}
		c.writeArrayHeader(2)
		c.writeInt(int64(m.aStart))
		c.writeInt(int64(m.aEnd))
		c.writeArrayHeader(2)
		c.writeInt(int64(m.bStart))
		c.writeInt(int64(m.bEnd))
		if withMatchLen {
			c.writeInt(int64(m.length))
		}
	}
	c.writeBulk([]byte("len"))
	c.writeInt(int64(length))
}
