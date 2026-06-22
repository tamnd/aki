package command

import (
	"math/rand/v2"

	"github.com/tamnd/aki/keyspace"
)

// setCommands returns the core set commands: add, remove, membership and the
// random pop family (doc 10 §9). The set algebra commands are a separate slice,
// and SSCAN waits for the generic SCAN cursor machinery.
func setCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "sadd", Group: GroupSet, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSAdd},
		{Name: "srem", Group: GroupSet, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSRem},
		{Name: "smembers", Group: GroupSet, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSMembers},
		{Name: "sismember", Group: GroupSet, Since: "1.0.0",
			Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSIsMember},
		{Name: "smismember", Group: GroupSet, Since: "6.2.0",
			Arity: -3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSMIsMember},
		{Name: "scard", Group: GroupSet, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSCard},
		{Name: "spop", Group: GroupSet, Since: "1.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSPop},
		{Name: "srandmember", Group: GroupSet, Since: "1.0.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSRandMember},
		{Name: "smove", Group: GroupSet, Since: "1.0.0",
			Arity: 4, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleSMove},
	}
}

// handleSAdd implements SADD: add members, ignoring ones already present, and
// reply how many were new.
func handleSAdd(ctx *Ctx) {
	key := ctx.Argv[1]
	toAdd := ctx.Argv[2:]
	var (
		wrongTyp bool
		added    int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		members, hdr, found, err := getSet(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		for _, m := range toAdd {
			if setFind(members, m) < 0 {
				members = append(members, m)
				added++
			}
		}
		if added == 0 {
			return nil
		}
		prev := uint8(keyspace.EncIntset)
		if found {
			prev = hdr.Encoding
		}
		return db.Set(key, setEncode(members), keyspace.TypeSet, setEncoding(ctx.encLimits(), members, prev), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if added > 0 {
		ctx.notify(notifySet, "sadd", key)
	}
	ctx.enc().WriteInteger(added)
}

// handleSRem implements SREM: remove members, reply how many were removed, and
// delete the key when its last member goes.
func handleSRem(ctx *Ctx) {
	key := ctx.Argv[1]
	toRemove := ctx.Argv[2:]
	var (
		wrongTyp bool
		emptied  bool
		removed  int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		members, hdr, found, err := getSet(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		for _, m := range toRemove {
			if idx := setFind(members, m); idx >= 0 {
				members = append(members[:idx], members[idx+1:]...)
				removed++
			}
		}
		if removed == 0 {
			return nil
		}
		if len(members) == 0 {
			emptied = true
			_, err := db.Delete(key)
			return err
		}
		return db.Set(key, setEncode(members), keyspace.TypeSet, setEncoding(ctx.encLimits(), members, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if removed > 0 {
		ctx.notify(notifySet, "srem", key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	ctx.enc().WriteInteger(removed)
}

// handleSMembers implements SMEMBERS: every member as a set reply.
func handleSMembers(ctx *Ctx) {
	key := ctx.Argv[1]
	members, wrongTyp, ok := readSet(ctx, key)
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	enc.WriteSetLen(len(members))
	for _, m := range members {
		enc.WriteBulkString(m)
	}
}

// handleSIsMember implements SISMEMBER: 1 when the member is present, else 0.
func handleSIsMember(ctx *Ctx) {
	key, member := ctx.Argv[1], ctx.Argv[2]
	members, wrongTyp, ok := readSet(ctx, key)
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if setFind(members, member) >= 0 {
		ctx.enc().WriteInteger(1)
	} else {
		ctx.enc().WriteInteger(0)
	}
}

// handleSMIsMember implements SMISMEMBER: a 0/1 per queried member.
func handleSMIsMember(ctx *Ctx) {
	key := ctx.Argv[1]
	want := ctx.Argv[2:]
	members, wrongTyp, ok := readSet(ctx, key)
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(want))
	for _, m := range want {
		if setFind(members, m) >= 0 {
			enc.WriteInteger(1)
		} else {
			enc.WriteInteger(0)
		}
	}
}

// handleSCard implements SCARD: the member count, or 0 when the key is absent.
func handleSCard(ctx *Ctx) {
	key := ctx.Argv[1]
	members, wrongTyp, ok := readSet(ctx, key)
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(int64(len(members)))
}

// handleSPop implements SPOP key [count]: remove and return random members.
func handleSPop(ctx *Ctx) {
	key := ctx.Argv[1]
	hasCount := len(ctx.Argv) == 3
	var count int64
	if hasCount {
		c, ok := parseInteger(ctx.Argv[2])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		if c < 0 {
			ctx.enc().WriteError("ERR value is out of range, must be positive")
			return
		}
		count = c
	} else if len(ctx.Argv) > 3 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'spop' command")
		return
	}

	var (
		wrongTyp bool
		absent   bool
		emptied  bool
		popped   [][]byte
	)
	done := ctx.update(func(db *keyspace.DB) error {
		members, hdr, found, err := getSet(db, key)
		if err != nil {
			return err
		}
		if !found {
			absent = true
			return nil
		}
		if hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		n := 1
		if hasCount {
			n = int(min(count, int64(len(members))))
		}
		if n == 0 {
			return nil
		}
		picks := rand.Perm(len(members))[:n]
		drop := make(map[int]bool, n)
		for _, i := range picks {
			popped = append(popped, members[i])
			drop[i] = true
		}
		rest := make([][]byte, 0, len(members)-n)
		for i, m := range members {
			if !drop[i] {
				rest = append(rest, m)
			}
		}
		if len(rest) == 0 {
			emptied = true
			_, err := db.Delete(key)
			return err
		}
		return db.Set(key, setEncode(rest), keyspace.TypeSet, setEncoding(ctx.encLimits(), rest, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if len(popped) > 0 {
		ctx.notify(notifySet, "spop", key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	enc := ctx.enc()
	if !hasCount {
		if absent || len(popped) == 0 {
			enc.WriteNull()
			return
		}
		enc.WriteBulkString(popped[0])
		return
	}
	enc.WriteSetLen(len(popped))
	for _, m := range popped {
		enc.WriteBulkString(m)
	}
}

// handleSRandMember implements SRANDMEMBER key [count]: random members without
// removing them.
func handleSRandMember(ctx *Ctx) {
	key := ctx.Argv[1]
	hasCount := len(ctx.Argv) == 3
	var count int64
	if hasCount {
		c, ok := parseInteger(ctx.Argv[2])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		count = c
	} else if len(ctx.Argv) > 3 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'srandmember' command")
		return
	}

	members, wrongTyp, ok := readSet(ctx, key)
	if !ok {
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
		enc.WriteBulkString(members[rand.IntN(len(members))])
		return
	}

	if count < 0 {
		m := int(-count)
		enc.WriteArrayLen(m)
		if len(members) == 0 {
			return
		}
		for range m {
			enc.WriteBulkString(members[rand.IntN(len(members))])
		}
		return
	}
	n := int(min(count, int64(len(members))))
	picks := rand.Perm(len(members))[:n]
	enc.WriteSetLen(len(picks))
	for _, i := range picks {
		enc.WriteBulkString(members[i])
	}
}

// handleSMove implements SMOVE source destination member: atomically move a
// member from one set to another. It replies 1 when the member was in the
// source, else 0.
func handleSMove(ctx *Ctx) {
	src, dst, member := ctx.Argv[1], ctx.Argv[2], ctx.Argv[3]
	var (
		wrongTyp   bool
		moved      bool
		srcEmptied bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		srcMembers, srcHdr, srcFound, err := getSet(db, src)
		if err != nil {
			return err
		}
		if srcFound && srcHdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		dstMembers, dstHdr, dstFound, err := getSet(db, dst)
		if err != nil {
			return err
		}
		if dstFound && dstHdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		idx := setFind(srcMembers, member)
		if idx < 0 {
			return nil
		}
		moved = true
		if string(src) == string(dst) {
			return nil
		}
		srcMembers = append(srcMembers[:idx], srcMembers[idx+1:]...)
		if len(srcMembers) == 0 {
			srcEmptied = true
			if _, err := db.Delete(src); err != nil {
				return err
			}
		} else if err := db.Set(src, setEncode(srcMembers), keyspace.TypeSet,
			setEncoding(ctx.encLimits(), srcMembers, srcHdr.Encoding), keepTTL(srcHdr, srcFound)); err != nil {
			return err
		}
		if setFind(dstMembers, member) < 0 {
			dstMembers = append(dstMembers, member)
		}
		dstPrev := uint8(keyspace.EncIntset)
		if dstFound {
			dstPrev = dstHdr.Encoding
		}
		return db.Set(dst, setEncode(dstMembers), keyspace.TypeSet,
			setEncoding(ctx.encLimits(), dstMembers, dstPrev), keepTTL(dstHdr, dstFound))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if moved && string(src) != string(dst) {
		ctx.notify(notifySet, "srem", src)
		ctx.notify(notifySet, "sadd", dst)
		if srcEmptied {
			ctx.notify(notifyGeneric, "del", src)
		}
	}
	if moved {
		ctx.enc().WriteInteger(1)
	} else {
		ctx.enc().WriteInteger(0)
	}
}

// readSet is the shared read path for the read-only set commands. It returns the
// members, whether the key held a non-set, and whether the view succeeded. A
// missing key yields no members and no wrong-type flag.
func readSet(ctx *Ctx, key []byte) (members [][]byte, wrongTyp bool, ok bool) {
	ok = ctx.view(func(db *keyspace.DB) error {
		ms, hdr, found, err := getSet(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		members = ms
		return nil
	})
	return members, wrongTyp, ok
}
