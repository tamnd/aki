package command

import (
	"math/rand/v2"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// zsetRankCommands returns the rank lookups, the score-ordered pops, and the
// random-member command (doc 11 §7.9, §7.21, §7.22).
func zsetRankCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "zrank", Group: GroupSortedSet, Since: "2.0.0",
			Arity: -3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZRank(ctx, false) }},
		{Name: "zrevrank", Group: GroupSortedSet, Since: "2.0.0",
			Arity: -3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZRank(ctx, true) }},
		{Name: "zpopmin", Group: GroupSortedSet, Since: "5.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZPop(ctx, false) }},
		{Name: "zpopmax", Group: GroupSortedSet, Since: "5.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZPop(ctx, true) }},
		{Name: "zrandmember", Group: GroupSortedSet, Since: "6.2.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZRandMember},
	}
}

// writeScoredPairs writes a member/score list with the protocol-aware shape that
// ZPOPMIN/MAX, ZRANDMEMBER and the WITHSCORES ranges share: a flat array of
// alternating member and score in RESP2, an array of two-element pairs in RESP3.
func writeScoredPairs(enc *resp.Encoder, pairs []zmember) {
	if enc.Proto() == 3 {
		enc.WriteArrayLen(len(pairs))
		for _, p := range pairs {
			enc.WriteArrayLen(2)
			enc.WriteBulkString(p.member)
			enc.WriteDouble(p.score)
		}
		return
	}
	enc.WriteArrayLen(len(pairs) * 2)
	for _, p := range pairs {
		enc.WriteBulkString(p.member)
		enc.WriteDouble(p.score)
	}
}

// handleZRank implements ZRANK and ZREVRANK with the optional WITHSCORE flag.
// The rank is the member's 0-based position in ascending order, or descending
// order for ZREVRANK.
func handleZRank(ctx *Ctx, rev bool) {
	withScore := false
	if len(ctx.Argv) == 4 {
		if !strings.EqualFold(string(ctx.Argv[3]), "WITHSCORE") {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		withScore = true
	} else if len(ctx.Argv) != 3 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	member := ctx.Argv[2]
	var (
		wrongTyp bool
		rank     int64 = -1
		score    float64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		members, hdr, ok, err := getZSet(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if ok && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		idx := zsetFind(members, member)
		if idx < 0 {
			return nil
		}
		score = members[idx].score
		if rev {
			rank = int64(len(members) - 1 - idx)
		} else {
			rank = int64(idx)
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	if rank < 0 {
		if withScore {
			enc.WriteNullArray()
		} else {
			enc.WriteNull()
		}
		return
	}
	if withScore {
		enc.WriteArrayLen(2)
		enc.WriteInteger(rank)
		enc.WriteDouble(score)
		return
	}
	enc.WriteInteger(rank)
}

// handleZPop implements ZPOPMIN and ZPOPMAX. Without a count it pops one pair
// and replies a flat two-element array; with a count it pops up to that many and
// replies the protocol-aware pair shape.
func handleZPop(ctx *Ctx, fromMax bool) {
	hasCount := false
	var count int64 = 1
	if len(ctx.Argv) == 3 {
		c, ok := parseInteger(ctx.Argv[2])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		if c < 0 {
			ctx.enc().WriteError("ERR value is out of range, must be positive")
			return
		}
		hasCount = true
		count = c
	} else if len(ctx.Argv) != 2 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}

	var (
		wrongTyp bool
		popped   []zmember
	)
	done := ctx.update(func(db *keyspace.DB) error {
		members, hdr, found, err := getZSet(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		n := int(min(count, int64(len(members))))
		if n == 0 {
			return nil
		}
		var kept []zmember
		if fromMax {
			// Highest scores sit at the tail; return them highest first.
			popped = make([]zmember, n)
			for i := range n {
				popped[i] = members[len(members)-1-i]
			}
			kept = members[:len(members)-n]
		} else {
			popped = append(popped, members[:n]...)
			kept = members[n:]
		}
		if len(kept) == 0 {
			_, err := db.Delete(ctx.Argv[1])
			return err
		}
		return db.Set(ctx.Argv[1], zsetEncode(kept), keyspace.TypeZSet, zsetEncoding(kept, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	if !hasCount {
		if len(popped) == 0 {
			enc.WriteArrayLen(0)
			return
		}
		enc.WriteArrayLen(2)
		enc.WriteBulkString(popped[0].member)
		enc.WriteDouble(popped[0].score)
		return
	}
	writeScoredPairs(enc, popped)
}

// handleZRandMember implements ZRANDMEMBER key [count [WITHSCORES]].
func handleZRandMember(ctx *Ctx) {
	hasCount := false
	var count int64
	withScores := false
	if len(ctx.Argv) >= 3 {
		c, ok := parseInteger(ctx.Argv[2])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		hasCount = true
		count = c
		if len(ctx.Argv) == 4 {
			if !strings.EqualFold(string(ctx.Argv[3]), "WITHSCORES") {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			withScores = true
		} else if len(ctx.Argv) > 4 {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	var (
		wrongTyp bool
		members  []zmember
	)
	if !ctx.view(func(db *keyspace.DB) error {
		ms, hdr, ok, err := getZSet(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if ok && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		members = ms
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	if !hasCount {
		if len(members) == 0 {
			enc.WriteNull()
			return
		}
		enc.WriteBulkString(members[rand.IntN(len(members))].member)
		return
	}
	picks := hashRandIndices(len(members), count)
	chosen := make([]zmember, len(picks))
	for i, idx := range picks {
		chosen[i] = members[idx]
	}
	if withScores {
		writeScoredPairs(enc, chosen)
		return
	}
	enc.WriteArrayLen(len(chosen))
	for _, zm := range chosen {
		enc.WriteBulkString(zm.member)
	}
}
