package command

import (
	"math/rand/v2"

	"github.com/tamnd/aki/keyspace"
)

// setDedupMapThreshold is the member count at or above which SADD/SREM build a
// membership map (O(N+M)) instead of repeated linear scans (O(N*M)). Below it the
// linear path wins because it avoids the map allocation, which would otherwise
// dominate the common single-member call.
const setDedupMapThreshold = 8

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
	lim := ctx.encLimits()
	// applyAdd runs the dedup-and-append over a decoded member set, bumping added
	// for each genuinely new member. It is shared by the sync and fast paths so the
	// two stay byte-for-byte identical.
	applyAdd := func(members [][]byte) [][]byte {
		if len(toAdd) >= setDedupMapThreshold {
			// Many members at once: a one-pass membership map makes dedup O(N+M)
			// instead of the O(N*M) repeated linear scan below.
			seen := make(map[string]struct{}, len(members)+len(toAdd))
			for _, m := range members {
				seen[string(m)] = struct{}{}
			}
			for _, m := range toAdd {
				if _, ok := seen[string(m)]; !ok {
					seen[string(m)] = struct{}{}
					members = append(members, m)
					added++
				}
			}
		} else {
			// Few members: the linear scan avoids the map allocation, which would
			// otherwise dominate the common single-member SADD.
			for _, m := range toAdd {
				if setFind(members, m) < 0 {
					members = append(members, m)
					added++
				}
			}
		}
		return members
	}
	// sync is the full synchronous closure: the btree-backed point write, the
	// promotion to the hashtable form, and the plain blob rewrite. It is the
	// fallback for a coll-form set, a promotion, or when no workers run.
	sync := func(db *keyspace.DB) error {
		hdr, found, err := setHeader(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		// A set already in the btree-backed form takes the point-write path: one
		// sub-tree row per member, no whole-blob rewrite.
		if found && hdr.IsColl() {
			added = 0
			return db.CollUpdate(key, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
				for _, m := range toAdd {
					created, e := w.Put(m, nil)
					if e != nil {
						return e
					}
					if created {
						w.SetCount(w.Count() + 1)
						added++
					}
				}
				return nil
			})
		}
		members, _, _, err := getSet(db, key)
		if err != nil {
			return err
		}
		added = 0
		members = applyAdd(members)
		if added == 0 {
			return nil
		}
		prev := uint8(keyspace.EncIntset)
		if found {
			prev = hdr.Encoding
		}
		if setWantsTree(lim, members, prev) {
			return setPromote(db, key, members)
		}
		return db.Set(key, setEncode(members), keyspace.TypeSet, setEncoding(lim, members, prev), keepTTL(hdr, found))
	}
	// compute is the write-behind fast path for an intset or listpack form set (or
	// a fresh key) whose new blob still fits inline: decode the current body, add
	// the members, and report the new blob to stage. It falls back to sync for the
	// btree-backed form, a promotion to hashtable, or a codec error.
	compute := func(cur []byte, hdr keyspace.ValueHeader, found bool) rmwResult {
		if found && hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return rmwResult{}
		}
		if found && hdr.IsColl() {
			return rmwResult{fallback: true}
		}
		members, err := setDecode(cur)
		if err != nil {
			return rmwResult{fallback: true}
		}
		added = 0
		members = applyAdd(members)
		if added == 0 {
			return rmwResult{}
		}
		prev := uint8(keyspace.EncIntset)
		if found {
			prev = hdr.Encoding
		}
		if setWantsTree(lim, members, prev) {
			return rmwResult{fallback: true}
		}
		return rmwResult{body: setEncode(members), typ: keyspace.TypeSet, enc: setEncoding(lim, members, prev), ttlMs: keepTTL(hdr, found), write: true}
	}
	if !ctx.rmwWriteBehind(key, compute, sync) {
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
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		hdr, found, err := setHeader(db, key)
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
		if hdr.IsColl() {
			removed = 0
			return db.CollUpdate(key, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
				for _, m := range toRemove {
					existed, e := w.Delete(m)
					if e != nil {
						return e
					}
					if existed {
						w.SetCount(w.Count() - 1)
						removed++
					}
				}
				emptied = w.Count() == 0
				return nil
			})
		}
		members, _, _, err := getSet(db, key)
		if err != nil {
			return err
		}
		if len(toRemove) >= setDedupMapThreshold {
			// Many members at once: filter in one O(N+M) pass against a set of the
			// members to drop, instead of an O(N*M) sequence of slice splices.
			rm := make(map[string]struct{}, len(toRemove))
			for _, m := range toRemove {
				rm[string(m)] = struct{}{}
			}
			kept := members[:0]
			for _, m := range members {
				if _, drop := rm[string(m)]; drop {
					removed++
				} else {
					kept = append(kept, m)
				}
			}
			members = kept
		} else {
			for _, m := range toRemove {
				if idx := setFind(members, m); idx >= 0 {
					members = append(members[:idx], members[idx+1:]...)
					removed++
				}
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
// For a btree-backed set the count comes from the metadata in O(1), so SCARD does
// not walk the members.
func handleSCard(ctx *Ctx) {
	key := ctx.Argv[1]
	var (
		n        int64
		wrongTyp bool
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		card, hdr, found, err := setCard(db, key)
		if err != nil || !found {
			return err
		}
		if hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		n = card
		return nil
	})
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
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
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		hdr, found, err := setHeader(db, key)
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
		// A btree-backed set pops by sampling its sub-tree rows and deleting the
		// picks, instead of decoding and rewriting the whole blob.
		if hdr.IsColl() {
			return db.CollUpdate(key, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
				total := int(w.Count())
				n := 1
				if hasCount {
					n = int(min(count, int64(total)))
				}
				if n == 0 {
					return nil
				}
				all := make([][]byte, 0, total)
				c := w.Cursor()
				if e := c.First(); e != nil {
					return e
				}
				for c.Valid() {
					all = append(all, append([]byte(nil), c.Key()...))
					if e := c.Next(); e != nil {
						return e
					}
				}
				for _, i := range rand.Perm(len(all))[:n] {
					popped = append(popped, all[i])
					if _, e := w.Delete(all[i]); e != nil {
						return e
					}
				}
				w.SetCount(w.Count() - uint64(n))
				emptied = w.Count() == 0
				return nil
			})
		}
		members, _, _, err := getSet(db, key)
		if err != nil {
			return err
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
	lim := ctx.encLimits()
	done := ctx.update(func(db *keyspace.DB) error {
		srcHdr, srcFound, err := setHeader(db, src)
		if err != nil {
			return err
		}
		if srcFound && srcHdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		dstHdr, dstFound, err := setHeader(db, dst)
		if err != nil {
			return err
		}
		if dstFound && dstHdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		if !srcFound {
			return nil
		}
		in, err := setMemberIn(db, src, member, srcHdr)
		if err != nil {
			return err
		}
		if !in {
			return nil
		}
		moved = true
		if string(src) == string(dst) {
			return nil
		}
		srcEmptied, err = setDelOne(lim, db, src, member, srcHdr)
		if err != nil {
			return err
		}
		return setAddOne(lim, db, dst, member, dstHdr, dstFound)
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
	if ms, hit := hotGetSet(ctx, key); hit {
		return ms, false, true
	}
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
