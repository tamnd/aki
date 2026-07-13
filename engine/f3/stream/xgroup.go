package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
)

// XGROUP, the consumer-group lifecycle (spec 2064/f3/14 section 7.8). CREATE and
// SETID move the group cursor, DESTROY drops a group wholesale, CREATECONSUMER
// and DELCONSUMER manage consumer records. Every subcommand but CREATE requires
// the key and the group to exist; CREATE creates the group and, with MKSTREAM, an
// empty native stream. Creating a group upgrades an inline stream to native
// (section 4.4), because the ledger has no packed-blob form.
//
// The delivery and ack machinery that fills the PEL a group owns is XREADGROUP's
// (slice 6); this surface stands the groups up and tears them down.

// Redis error texts the XGROUP surface reproduces exactly.
const (
	errXgroupKey = "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."
	busyGroup    = "BUSYGROUP Consumer Group name already exists"
)

// nogroup builds the NOGROUP error naming the group and key, the reply every
// XGROUP subcommand gives when the named group is absent.
func nogroup(name, key []byte) string {
	return "NOGROUP No such consumer group '" + string(name) + "' for key name '" + string(key) + "'"
}

// nogroupGeneric builds the NOGROUP error XACK and XPENDING give for a missing key
// or group, key first then group, the plain form Redis uses outside XREADGROUP.
func nogroupGeneric(key, name []byte) string {
	return "NOGROUP No such key '" + string(key) + "' or consumer group '" + string(name) + "'"
}

// Xgroup dispatches the XGROUP subcommands. args[0] is the subcommand token; the
// key, when the subcommand takes one, is args[1], the index the dispatcher routes
// on. An unknown subcommand or a wrong argument count answers Redis's shared
// XGROUP error.
func Xgroup(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch {
	case eqFold(args[0], "CREATE") && len(args) >= 4:
		xgroupCreate(cx, args, r)
	case eqFold(args[0], "SETID") && len(args) >= 4:
		xgroupSetID(cx, args, r)
	case eqFold(args[0], "DESTROY") && len(args) == 3:
		xgroupDestroy(cx, args, r)
	case eqFold(args[0], "CREATECONSUMER") && len(args) == 4:
		xgroupCreateConsumer(cx, args, r)
	case eqFold(args[0], "DELCONSUMER") && len(args) == 4:
		xgroupDelConsumer(cx, args, r)
	default:
		r.Err("ERR Unknown XGROUP subcommand or wrong number of arguments for '" + string(args[0]) + "'")
	}
}

// xgroupCreate answers XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n].
// A missing key errors unless MKSTREAM, which creates an empty native stream. An
// existing group by that name is BUSYGROUP. The start cursor resolves through
// groupStartID; an ENTRIESREAD clause overrides the lag basis.
func xgroupCreate(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name, idArg := args[1], args[2], args[3]
	mkstream, entriesRead, hasEntriesRead, msg := parseGroupOpts(args[4:], true)
	if msg != "" {
		r.Err(msg)
		return
	}

	g := registry(cx)
	s, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	created := false
	if s == nil {
		if !mkstream {
			r.Err(errXgroupKey)
			return
		}
		s = newStream()
		created = true
	}
	if s.group(name) != nil {
		r.Err(busyGroup)
		return
	}
	start, read, valid, idok := groupStartID(s, idArg)
	if !idok {
		r.Err(errInvalidID)
		return
	}
	if hasEntriesRead {
		read, valid = entriesRead, true
	}
	s.addGroup(name, newGroup(start, read, valid))
	if created {
		g.m[string(key)] = s
	}
	r.Status("OK")
}

// xgroupSetID answers XGROUP SETID key group id|$ [ENTRIESREAD n]: reposition an
// existing group's cursor, refreshing the lag basis. The key and group must
// exist.
func xgroupSetID(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name, idArg := args[1], args[2], args[3]
	_, entriesRead, hasEntriesRead, msg := parseGroupOpts(args[4:], false)
	if msg != "" {
		r.Err(msg)
		return
	}
	s, grp, ok := resolveGroup(cx, registry(cx), key, name, r)
	if !ok {
		return
	}
	start, read, valid, idok := groupStartID(s, idArg)
	if !idok {
		r.Err(errInvalidID)
		return
	}
	if hasEntriesRead {
		read, valid = entriesRead, true
	}
	grp.lastDeliveredID = start
	grp.entriesRead = read
	grp.readValid = valid
	r.Status("OK")
}

// xgroupDestroy answers XGROUP DESTROY key group: drop the group and its ledger
// wholesale, replying 1 when a group was removed, 0 when none by that name
// existed. The key must exist.
func xgroupDestroy(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name := args[1], args[2]
	s, wrong := registry(cx).lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Err(errXgroupKey)
		return
	}
	if s.group(name) == nil {
		r.Int(0)
		return
	}
	delete(s.groups, string(name))
	r.Int(1)
}

// xgroupCreateConsumer answers XGROUP CREATECONSUMER key group consumer: add the
// consumer if absent, replying 1 when it created one and 0 when it already
// existed. The key and group must exist.
func xgroupCreateConsumer(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name, con := args[1], args[2], args[3]
	_, grp, ok := resolveGroup(cx, registry(cx), key, name, r)
	if !ok {
		return
	}
	if grp.createConsumer(con) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// xgroupDelConsumer answers XGROUP DELCONSUMER key group consumer: remove the
// consumer and reply the number of pending entries it owned. The key and group
// must exist; a missing consumer removes nothing and replies 0.
func xgroupDelConsumer(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name, con := args[1], args[2], args[3]
	_, grp, ok := resolveGroup(cx, registry(cx), key, name, r)
	if !ok {
		return
	}
	r.Int(grp.delConsumer(con))
}

// resolveGroup looks up the stream and its named group for a subcommand that
// requires both, writing the exact Redis error into r and returning ok=false when
// the key is missing or wrong-typed, or the group is absent.
func resolveGroup(cx *shard.Ctx, g *reg, key, name []byte, r shard.Reply) (*stream, *streamGroup, bool) {
	s, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return nil, nil, false
	}
	if s == nil {
		r.Err(errXgroupKey)
		return nil, nil, false
	}
	grp := s.group(name)
	if grp == nil {
		r.Err(nogroup(name, key))
		return nil, nil, false
	}
	return s, grp, true
}

// groupStartID resolves an XGROUP CREATE or SETID id argument to the group's
// start cursor and its lag basis. "$" starts at the current tail, having read
// every existing entry; an explicit 0-0 starts at the head having read nothing;
// an explicit ID at or past the tail is caught up; an explicit ID mid-stream has
// an entries-read the directory-priced cursor of slice 6 must resolve, so valid
// is false until then and XINFO reports entries-read and lag as nil. idok is
// false only on a malformed explicit ID.
func groupStartID(s *stream, idArg []byte) (start streamID, entriesRead uint64, valid, idok bool) {
	if len(idArg) == 1 && idArg[0] == '$' {
		return s.lastID, s.entriesAdded, true, true
	}
	id, ok := parseStreamID(idArg)
	if !ok {
		return streamID{}, 0, false, false
	}
	switch {
	case id == (streamID{}):
		return id, 0, true, true
	case id.cmp(s.lastID) >= 0:
		return id, s.entriesAdded, true, true
	default:
		return id, 0, false, true
	}
}

// parseGroupOpts reads the trailing XGROUP option tokens: MKSTREAM (CREATE only)
// and ENTRIESREAD n. It returns whether MKSTREAM was given, the ENTRIESREAD value
// and whether it was set, and a Redis error text (empty on success): a non-integer
// ENTRIESREAD is the integer error, any other stray or misplaced token the syntax
// error.
func parseGroupOpts(rest [][]byte, allowMkstream bool) (mkstream bool, entriesRead uint64, hasEntriesRead bool, msg string) {
	for i := 0; i < len(rest); {
		switch {
		case allowMkstream && eqFold(rest[i], "MKSTREAM"):
			mkstream = true
			i++
		case eqFold(rest[i], "ENTRIESREAD") && i+1 < len(rest):
			n, nok := parseUint(rest[i+1])
			if !nok {
				return false, 0, false, "ERR value is not an integer or out of range"
			}
			entriesRead, hasEntriesRead = n, true
			i += 2
		default:
			return false, 0, false, "ERR syntax error"
		}
	}
	return mkstream, entriesRead, hasEntriesRead, ""
}
