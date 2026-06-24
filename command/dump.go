package command

import (
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/rdb"
)

// dumpCommands returns DUMP and RESTORE, the serialize and deserialize pair that
// moves a single value in and out of RDB wire form (spec 2064 doc 17 section 4).
func dumpCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "dump", Group: GroupGeneric, Since: "2.6.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleDump},
		{Name: "restore", Group: GroupGeneric, Since: "2.6.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleRestore},
	}
}

// handleDump serializes the value at the key into a DUMP payload, or replies with
// a nil bulk when the key is missing.
func handleDump(ctx *Ctx) {
	key := ctx.Argv[1]
	var (
		payload []byte
		found   bool
		unsupp  bool
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		v, f, err := readDumpValue(db, key)
		if err != nil {
			if err == errDumpUnsupported {
				unsupp = true
				return nil
			}
			return err
		}
		if !f {
			return nil
		}
		found = true
		p, merr := rdb.Marshal(v)
		if merr != nil {
			unsupp = true
			return nil
		}
		payload = p
		return nil
	})
	if !ok {
		return
	}
	if unsupp {
		ctx.enc().WriteError("ERR DUMP of this type is not supported yet")
		return
	}
	if !found {
		ctx.enc().WriteNull()
		return
	}
	ctx.enc().WriteBulkString(payload)
}

// errDumpUnsupported marks a value type DUMP cannot serialize yet. Every keyspace
// type aki stores has an encoder now, so this only guards against a future type
// byte the codec does not know.
var errDumpUnsupported = errStr("rdb: dump unsupported type")

// errStr is a tiny error type so the dump path can compare against a sentinel
// without importing errors just for one value.
type errStr string

func (e errStr) Error() string { return string(e) }

// readDumpValue reads the key and builds the logical value DUMP serializes. found
// is false for a missing key.
func readDumpValue(db *keyspace.DB, key []byte) (rdb.Value, bool, error) {
	_, hdr, found, err := db.Peek(key)
	if err != nil || !found {
		return rdb.Value{}, false, err
	}
	switch hdr.Type {
	case keyspace.TypeString:
		body, _, _, gerr := db.Get(key)
		return rdb.Value{Kind: rdb.KindString, Str: body}, true, gerr
	case keyspace.TypeList:
		body, _, _, gerr := db.Get(key)
		if gerr != nil {
			return rdb.Value{}, true, gerr
		}
		elems, derr := listDecode(body)
		return rdb.Value{Kind: rdb.KindList, List: elems}, true, derr
	case keyspace.TypeSet:
		ms, _, _, gerr := getSet(db, key)
		return rdb.Value{Kind: rdb.KindSet, Set: ms}, true, gerr
	case keyspace.TypeHash:
		fields, _, _, gerr := hashMaterialize(db, key)
		if gerr != nil {
			return rdb.Value{}, true, gerr
		}
		out := make([]rdb.Field, len(fields))
		for i, f := range fields {
			out[i] = rdb.Field{Field: f.field, Value: f.value}
		}
		return rdb.Value{Kind: rdb.KindHash, Hash: out}, true, nil
	case keyspace.TypeZSet:
		zm, _, _, gerr := getZSet(db, key)
		if gerr != nil {
			return rdb.Value{}, true, gerr
		}
		out := make([]rdb.Member, len(zm))
		for i, m := range zm {
			out[i] = rdb.Member{Member: m.member, Score: m.score}
		}
		return rdb.Value{Kind: rdb.KindZSet, ZSet: out}, true, nil
	case keyspace.TypeStream:
		s, _, _, gerr := getStream(db, key)
		if gerr != nil {
			return rdb.Value{}, true, gerr
		}
		return streamToRDB(s), true, nil
	default:
		return rdb.Value{}, true, errDumpUnsupported
	}
}

// debugReload serializes the whole dataset to an in-memory RDB file, reads it back,
// and replaces the live keyspace with the result. This is the no-fork equivalent of
// what Redis does for DEBUG RELOAD: it proves the snapshot codec round-trips and
// leaves the data observably identical, with encodings re-derived from the reloaded
// values. A value type the codec cannot serialize yet aborts the reload before any
// key is touched, so nothing is lost.
func debugReload(ctx *Ctx) {
	var unsupp bool
	ok := ctx.updateKeyspace(func(ks *keyspace.Keyspace) error {
		snap := rdb.Snapshot{}
		for i := range ks.DBCount() {
			db, err := ks.DB(i)
			if err != nil {
				return err
			}
			entries, derr := reloadEntries(db)
			if derr != nil {
				if derr == errDumpUnsupported {
					unsupp = true
					return nil
				}
				return derr
			}
			if len(entries) > 0 {
				snap.DBs = append(snap.DBs, rdb.DBData{Index: i, Entries: entries})
			}
		}
		if unsupp {
			return nil
		}

		blob, merr := rdb.MarshalFile(snap)
		if merr != nil {
			return merr
		}
		loaded, uerr := rdb.UnmarshalFile(blob)
		if uerr != nil {
			return uerr
		}

		for i := range ks.DBCount() {
			db, err := ks.DB(i)
			if err != nil {
				return err
			}
			if err := db.Flush(); err != nil {
				return err
			}
		}
		for _, dbData := range loaded.DBs {
			db, err := ks.DB(dbData.Index)
			if err != nil {
				return err
			}
			for _, e := range dbData.Entries {
				if serr := storeRestored(ctx.encLimits(), db, e.Key, e.Value, e.ExpireMS); serr != nil {
					return serr
				}
				if e.HasIdle {
					db.SetIdle(e.Key, e.Idle)
				}
				if e.HasFreq {
					db.SetFreq(e.Key, e.Freq)
				}
			}
		}
		return nil
	})
	if !ok {
		return
	}
	if unsupp {
		ctx.enc().WriteError("ERR DEBUG RELOAD of a value type that is not supported yet")
		return
	}
	ctx.enc().WriteStatus("OK")
}

// reloadEntries reads every live key in a database into snapshot entries, capturing
// the absolute TTL and the LRU and LFU access state so a reload preserves them.
func reloadEntries(db *keyspace.DB) ([]rdb.Entry, error) {
	keys, err := db.Keys()
	if err != nil {
		return nil, err
	}
	entries := make([]rdb.Entry, 0, len(keys))
	for _, k := range keys {
		_, hdr, found, perr := db.Peek(k.Key)
		if perr != nil {
			return nil, perr
		}
		if !found {
			continue
		}
		idle := db.Idle(k.Key)
		freq := db.Freq(k.Key)
		v, ok, verr := readDumpValue(db, k.Key)
		if verr != nil {
			return nil, verr
		}
		if !ok {
			continue
		}
		entries = append(entries, rdb.Entry{
			Key:      k.Key,
			Value:    v,
			ExpireMS: hdr.TTLms,
			Idle:     idle,
			HasIdle:  true,
			Freq:     freq,
			HasFreq:  true,
		})
	}
	return entries, nil
}

// restoreOpts holds the parsed RESTORE flags after the payload.
type restoreOpts struct {
	replace bool
	absttl  bool
	idle    uint32
	hasIdle bool
	freq    uint8
	hasFreq bool
}

// handleRestore deserializes a DUMP payload and stores it at the key, applying the
// TTL and the optional REPLACE, ABSTTL, IDLETIME and FREQ clauses.
func handleRestore(ctx *Ctx) {
	key := ctx.Argv[1]
	ttl, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	if ttl < 0 {
		ctx.enc().WriteError("ERR Invalid argument: ttl must be a positive integer or zero")
		return
	}
	opts, errMsg := parseRestoreOpts(ctx.Argv[4:])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}

	val, derr := rdb.Unmarshal(ctx.Argv[3])
	if derr != nil {
		ctx.enc().WriteError("ERR DUMP payload version or checksum are wrong")
		return
	}

	var busy bool
	stored := ctx.updateShard(key, func(db *keyspace.DB) error {
		exists, eerr := db.Exists(key)
		if eerr != nil {
			return eerr
		}
		if exists && !opts.replace {
			busy = true
			return nil
		}
		if exists {
			if _, derr := db.Delete(key); derr != nil {
				return derr
			}
		}
		ttlMs := int64(-1)
		if ttl > 0 {
			if opts.absttl {
				ttlMs = ttl
			} else {
				ttlMs = keyspace.NowMillis() + ttl
			}
		}
		if serr := storeRestored(ctx.encLimits(), db, key, val, ttlMs); serr != nil {
			return serr
		}
		if opts.hasIdle {
			db.SetIdle(key, opts.idle)
		}
		if opts.hasFreq {
			db.SetFreq(key, opts.freq)
		}
		return nil
	})
	if !stored {
		return
	}
	if busy {
		ctx.enc().WriteError("BUSYKEY Target key name already exists")
		return
	}
	ctx.notify(notifyGeneric, "restore", key)
	ctx.enc().WriteStatus("OK")
}

// storeRestored writes a decoded value into the keyspace using the same body
// encoders the data commands use, so a restored key is indistinguishable from one
// built command by command.
func storeRestored(lim encLimits, db *keyspace.DB, key []byte, v rdb.Value, ttlMs int64) error {
	switch v.Kind {
	case rdb.KindString:
		return db.Set(key, v.Str, keyspace.TypeString, stringEncoding(v.Str), ttlMs)
	case rdb.KindList:
		return db.Set(key, listEncode(v.List), keyspace.TypeList,
			listEncoding(lim, v.List, keyspace.EncListpack), ttlMs)
	case rdb.KindSet:
		// A restored set large enough to report hashtable lands in the btree-backed
		// form, the same as one built member by member. Promotion needs the key's TTL
		// stamped first, since setPromote preserves the existing header's TTL rather
		// than taking one as an argument.
		if setWantsTree(lim, v.Set, keyspace.EncListpack) {
			if err := db.Set(key, nil, keyspace.TypeSet, keyspace.EncHashtable, ttlMs); err != nil {
				return err
			}
			return setPromote(db, key, v.Set)
		}
		return db.Set(key, setEncode(v.Set), keyspace.TypeSet,
			setEncoding(lim, v.Set, keyspace.EncListpack), ttlMs)
	case rdb.KindHash:
		fields := make([]hashField, len(v.Hash))
		for i, f := range v.Hash {
			fields[i] = hashField{field: f.Field, value: f.Value}
		}
		// A restored hash large enough to report hashtable lands in the
		// btree-backed form, the same as one built field by field. Promotion needs
		// the key's TTL stamped first, since hashPromote preserves the existing
		// header's TTL rather than taking one as an argument.
		if hashWantsTree(lim, fields, keyspace.EncListpack) {
			if err := db.Set(key, nil, keyspace.TypeHash, keyspace.EncHashtable, ttlMs); err != nil {
				return err
			}
			return hashPromote(db, key, fields)
		}
		return db.Set(key, hashEncode(fields), keyspace.TypeHash,
			hashEncoding(lim, fields, keyspace.EncListpack), ttlMs)
	case rdb.KindZSet:
		rdb.SortMembers(v.ZSet)
		members := make([]zmember, len(v.ZSet))
		for i, m := range v.ZSet {
			members[i] = zmember{member: m.Member, score: m.Score}
		}
		// A restored sorted set large enough to report skiplist lands in the
		// btree-backed form, the same as one built member by member. Promotion needs
		// the key's TTL stamped first, since zsetPromote preserves the existing
		// header's TTL rather than taking one as an argument.
		if zsetWantsTree(lim, members, keyspace.EncListpack) {
			if err := db.Set(key, nil, keyspace.TypeZSet, keyspace.EncSkiplist, ttlMs); err != nil {
				return err
			}
			return zsetPromote(db, key, members)
		}
		return db.Set(key, zsetEncode(members), keyspace.TypeZSet,
			zsetEncoding(lim, members, keyspace.EncListpack), ttlMs)
	case rdb.KindStream:
		return storeStream(db, key, rdbToStream(v.Stream), ttlMs)
	default:
		return errDumpUnsupported
	}
}

// parseRestoreOpts reads the optional clauses after the payload.
func parseRestoreOpts(args [][]byte) (restoreOpts, string) {
	var opts restoreOpts
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "REPLACE":
			opts.replace = true
		case "ABSTTL":
			opts.absttl = true
		case "IDLETIME":
			if i+1 >= len(args) {
				return opts, "ERR syntax error"
			}
			n, ok := parseInteger(args[i+1])
			if !ok || n < 0 {
				return opts, "ERR Invalid IDLETIME value, must be >= 0"
			}
			opts.idle, opts.hasIdle = uint32(n), true
			i++
		case "FREQ":
			if i+1 >= len(args) {
				return opts, "ERR syntax error"
			}
			n, ok := parseInteger(args[i+1])
			if !ok || n < 0 || n > 255 {
				return opts, "ERR Invalid FREQ value, must be >= 0 and <= 255"
			}
			opts.freq, opts.hasFreq = uint8(n), true
			i++
		default:
			return opts, "ERR syntax error"
		}
	}
	return opts, ""
}
