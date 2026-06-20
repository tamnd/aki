package command

import (
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// lcsMatch is one contiguous block shared by the two strings, with inclusive
// byte ranges into each and the block length.
type lcsMatch struct {
	aStart, aEnd int
	bStart, bEnd int
	length       int
}

// lcsOptions is the parsed LCS request.
type lcsOptions struct {
	idx          bool
	length       bool
	minMatchLen  int
	withMatchLen bool
}

// handleLCS implements LCS key1 key2 [LEN] [IDX [MINMATCHLEN n] [WITHMATCHLEN]]
// (doc 08 §3.23). It computes the longest common subsequence of the two string
// values, treating a missing key as an empty string and rejecting a non-string
// key with WRONGTYPE.
func handleLCS(ctx *Ctx) {
	opts, errMsg, ok := parseLCSOptions(ctx.Argv[3:])
	if !ok {
		ctx.enc().WriteError(errMsg)
		return
	}
	if opts.length && opts.idx {
		ctx.enc().WriteError("ERR If you want both the length and indexes, please just use IDX.")
		return
	}

	var (
		wrongTyp bool
		a, b     []byte
	)
	if !ctx.view(func(db *keyspace.DB) error {
		av, wrongA, err := lcsLoad(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		bv, wrongB, err := lcsLoad(db, ctx.Argv[2])
		if err != nil {
			return err
		}
		if wrongA || wrongB {
			wrongTyp = true
			return nil
		}
		a, b = av, bv
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	length, lcsStr, matches := computeLCS(a, b)
	enc := ctx.enc()
	switch {
	case opts.length:
		enc.WriteInteger(int64(length))
	case opts.idx:
		writeLCSMatches(ctx, matches, length, opts)
	default:
		enc.WriteBulkString(lcsStr)
	}
}

// lcsLoad reads a key's string value for LCS. A missing key is an empty string;
// a non-string key sets the wrong-type flag.
func lcsLoad(db *keyspace.DB, key []byte) (val []byte, wrongType bool, err error) {
	b, hdr, found, err := db.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if hdr.Type != keyspace.TypeString {
		return nil, true, nil
	}
	return b, false, nil
}

// parseLCSOptions parses the words after the two keys.
func parseLCSOptions(args [][]byte) (lcsOptions, string, bool) {
	opts := lcsOptions{}
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "LEN":
			opts.length = true
		case "IDX":
			opts.idx = true
		case "WITHMATCHLEN":
			opts.withMatchLen = true
		case "MINMATCHLEN":
			i++
			if i >= len(args) {
				return opts, "ERR syntax error", false
			}
			v, ok := parseInteger(args[i])
			if !ok || v < 0 {
				return opts, "ERR syntax error", false
			}
			opts.minMatchLen = int(v)
		default:
			return opts, "ERR syntax error", false
		}
	}
	return opts, "", true
}

// computeLCS runs the standard O(m*n) dynamic program and backtracks to produce
// the LCS length, the subsequence string, and the contiguous match blocks in the
// order Redis emits them (from the end of the strings toward the start).
func computeLCS(a, b []byte) (length int, lcsStr []byte, matches []lcsMatch) {
	m, n := len(a), len(b)
	if m == 0 || n == 0 {
		return 0, nil, nil
	}
	// dp is a flat (m+1) x (n+1) grid; dp[i*(n+1)+j] is the LCS length of the
	// first i bytes of a and the first j bytes of b.
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
			// A new block starts whenever this character is not diagonally
			// adjacent to the previous matched character.
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

// writeLCSMatches writes the IDX reply: a map of "matches" to the list of blocks
// (longest-first, optionally filtered by MINMATCHLEN and tagged WITHMATCHLEN) and
// "len" to the total LCS length.
func writeLCSMatches(ctx *Ctx, matches []lcsMatch, length int, opts lcsOptions) {
	enc := ctx.enc()
	kept := matches[:0:0]
	for _, m := range matches {
		if opts.minMatchLen > 0 && m.length < opts.minMatchLen {
			continue
		}
		kept = append(kept, m)
	}

	enc.WriteMapLen(2)
	enc.WriteBulkStringStr("matches")
	enc.WriteArrayLen(len(kept))
	for _, m := range kept {
		if opts.withMatchLen {
			enc.WriteArrayLen(3)
		} else {
			enc.WriteArrayLen(2)
		}
		enc.WriteArrayLen(2)
		enc.WriteInteger(int64(m.aStart))
		enc.WriteInteger(int64(m.aEnd))
		enc.WriteArrayLen(2)
		enc.WriteInteger(int64(m.bStart))
		enc.WriteInteger(int64(m.bEnd))
		if opts.withMatchLen {
			enc.WriteInteger(int64(m.length))
		}
	}
	enc.WriteBulkStringStr("len")
	enc.WriteInteger(int64(length))
}
