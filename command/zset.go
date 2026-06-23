package command

import (
	"math"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// zsetCommands returns the core sorted-set write and lookup commands: ZADD,
// ZINCRBY, ZSCORE, ZMSCORE, ZCARD and ZREM (doc 11 §7).
func zsetCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "zadd", Group: GroupSortedSet, Since: "1.2.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZAdd},
		{Name: "zincrby", Group: GroupSortedSet, Since: "1.2.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZIncrBy},
		{Name: "zscore", Group: GroupSortedSet, Since: "1.2.0",
			Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZScore},
		{Name: "zmscore", Group: GroupSortedSet, Since: "6.2.0",
			Arity: -3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZMScore},
		{Name: "zcard", Group: GroupSortedSet, Since: "1.2.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZCard},
		{Name: "zrem", Group: GroupSortedSet, Since: "1.2.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZRem},
	}
}

// zaddPair is one (score, member) input to ZADD.
type zaddPair struct {
	score  float64
	member []byte
}

// handleZAdd implements ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...
func handleZAdd(ctx *Ctx) {
	args := ctx.Argv[2:]
	var nx, xx, gt, lt, ch, incr bool
	i := 0
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		case "CH":
			ch = true
		case "INCR":
			incr = true
		default:
			goto parsed
		}
	}
parsed:
	if nx && xx {
		ctx.enc().WriteError("ERR XX and NX options at the same time are not compatible")
		return
	}
	if (gt && lt) || (gt && nx) || (lt && nx) {
		ctx.enc().WriteError("ERR GT, LT, and NX options at the same time are not compatible")
		return
	}
	rest := args[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	if incr && len(rest) != 2 {
		ctx.enc().WriteError("ERR INCR option supports a single increment-element pair")
		return
	}
	pairs := make([]zaddPair, 0, len(rest)/2)
	for j := 0; j < len(rest); j += 2 {
		score, ok := parseScore(rest[j])
		if !ok {
			ctx.enc().WriteError("ERR value is not a valid float")
			return
		}
		pairs = append(pairs, zaddPair{score: score, member: rest[j+1]})
	}

	var (
		wrongTyp    bool
		nanResult   bool
		added       int64
		changed     int64
		incrResult  float64
		incrBlocked bool
	)
	done := ctx.updateShard(ctx.Argv[1], func(db *keyspace.DB) error {
		members, hdr, found, err := getZSet(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		floor := keyspace.EncListpack
		if found {
			floor = hdr.Encoding
		}
		for _, p := range pairs {
			idx := zsetFind(members, p.member)
			if idx >= 0 {
				cur := members[idx].score
				newScore := p.score
				if incr {
					newScore = cur + p.score
					if math.IsNaN(newScore) {
						nanResult = true
						return nil
					}
				}
				if nx {
					incrBlocked = true
					continue
				}
				if gt && !(newScore > cur) {
					incrBlocked = true
					continue
				}
				if lt && !(newScore < cur) {
					incrBlocked = true
					continue
				}
				if newScore != cur {
					members[idx].score = newScore
					changed++
				}
				incrResult = newScore
				continue
			}
			if xx {
				incrBlocked = true
				continue
			}
			members = append(members, zmember{member: p.member, score: p.score})
			added++
			incrResult = p.score
		}
		if len(members) == 0 {
			return nil
		}
		zsetSort(members)
		return db.Set(ctx.Argv[1], zsetEncode(members), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), members, floor), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if nanResult {
		ctx.enc().WriteError("ERR resulting score is not a number (NaN)")
		return
	}
	if added > 0 || changed > 0 {
		if incr {
			ctx.notify(notifyZset, "zincr", ctx.Argv[1])
		} else {
			ctx.notify(notifyZset, "zadd", ctx.Argv[1])
		}
	}
	if added > 0 {
		ctx.signalReady(ctx.Argv[1])
	}
	if incr {
		if incrBlocked {
			ctx.enc().WriteNull()
			return
		}
		ctx.enc().WriteDouble(incrResult)
		return
	}
	if ch {
		ctx.enc().WriteInteger(added + changed)
		return
	}
	ctx.enc().WriteInteger(added)
}

// handleZIncrBy implements ZINCRBY key increment member. It adds the increment
// to the member's score, creating the member at the increment when absent.
func handleZIncrBy(ctx *Ctx) {
	inc, ok := parseScore(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not a valid float")
		return
	}
	member := ctx.Argv[3]
	var (
		wrongTyp  bool
		nanResult bool
		result    float64
	)
	done := ctx.updateShard(ctx.Argv[1], func(db *keyspace.DB) error {
		members, hdr, found, err := getZSet(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		floor := keyspace.EncListpack
		if found {
			floor = hdr.Encoding
		}
		idx := zsetFind(members, member)
		if idx >= 0 {
			result = members[idx].score + inc
			if math.IsNaN(result) {
				nanResult = true
				return nil
			}
			members[idx].score = result
		} else {
			result = inc
			members = append(members, zmember{member: member, score: inc})
		}
		zsetSort(members)
		return db.Set(ctx.Argv[1], zsetEncode(members), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), members, floor), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if nanResult {
		ctx.enc().WriteError("ERR resulting score is not a number (NaN)")
		return
	}
	ctx.notify(notifyZset, "zincr", ctx.Argv[1])
	ctx.signalReady(ctx.Argv[1])
	ctx.enc().WriteDouble(result)
}

// handleZScore implements ZSCORE key member.
func handleZScore(ctx *Ctx) {
	var (
		wrongTyp bool
		score    float64
		found    bool
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
		if idx := zsetFind(members, ctx.Argv[2]); idx >= 0 {
			score = members[idx].score
			found = true
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if !found {
		ctx.enc().WriteNull()
		return
	}
	ctx.enc().WriteDouble(score)
}

// handleZMScore implements ZMSCORE key member [member ...].
func handleZMScore(ctx *Ctx) {
	queries := ctx.Argv[2:]
	var (
		wrongTyp bool
		scores   []float64
		present  []bool
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
		scores = make([]float64, len(queries))
		present = make([]bool, len(queries))
		for i, q := range queries {
			if idx := zsetFind(members, q); idx >= 0 {
				scores[i] = members[idx].score
				present[i] = true
			}
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
	enc.WriteArrayLen(len(queries))
	for i := range queries {
		if present[i] {
			enc.WriteDouble(scores[i])
		} else {
			enc.WriteNull()
		}
	}
}

// handleZCard implements ZCARD key.
func handleZCard(ctx *Ctx) {
	var (
		wrongTyp bool
		n        int64
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
		n = int64(len(members))
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// handleZRem implements ZREM key member [member ...]. It removes the named
// members and deletes the key when the last member goes.
func handleZRem(ctx *Ctx) {
	targets := ctx.Argv[2:]
	var (
		wrongTyp bool
		emptied  bool
		removed  int64
	)
	done := ctx.updateShard(ctx.Argv[1], func(db *keyspace.DB) error {
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
		drop := make(map[string]struct{}, len(targets))
		for _, t := range targets {
			drop[string(t)] = struct{}{}
		}
		kept := members[:0]
		for _, zm := range members {
			if _, gone := drop[string(zm.member)]; gone {
				removed++
				continue
			}
			kept = append(kept, zm)
		}
		if removed == 0 {
			return nil
		}
		if len(kept) == 0 {
			emptied = true
			_, err := db.Delete(ctx.Argv[1])
			return err
		}
		return db.Set(ctx.Argv[1], zsetEncode(kept), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), kept, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if removed > 0 {
		ctx.notify(notifyZset, "zrem", ctx.Argv[1])
		if emptied {
			ctx.notify(notifyGeneric, "del", ctx.Argv[1])
		}
	}
	ctx.enc().WriteInteger(removed)
}
