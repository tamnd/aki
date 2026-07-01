package f1srv

// This file implements the RESP transaction verbs (MULTI, EXEC, DISCARD, WATCH,
// UNWATCH) plus RESET, the M12 transactions slice of the f1_rewrite_ltm spec. A
// transaction on f1srv is what it is on Redis: MULTI opens a block, every later command
// is queued and answered +QUEUED, and EXEC runs the whole block back to back and frames
// the replies as one array. There is no rollback, because a queued command that turns
// out to be wrong at run time (a WRONGTYPE, say) still runs and its error is one element
// of the array; the only pre-run guards are an unknown command at queue time (which turns
// EXEC into an EXECABORT) and WATCH (optimistic locking, which turns EXEC into a null
// array when a watched key moved).
//
// WATCH is backed by the server watch table (server.go): each watched key carries a
// monotonic version, WATCH snapshots it, any command that writes the key bumps it (done
// in execCommand under the watching gate), and EXEC compares the snapshots. The table
// holds only currently-watched keys, refcounted so it empties as clients UNWATCH or EXEC.

// watchedKey is one entry in a connection's optimistic-lock set: the key it watches and
// the version that key held when WATCH ran. EXEC aborts if any entry's key has since
// moved to a different version.
type watchedKey struct {
	key string
	ver uint64
}

// cmdMulti opens a transaction. Nested MULTI is an error, matching Redis; the queue and
// the abort flag are reset so a fresh block starts clean.
func (c *connState) cmdMulti(argv [][]byte) {
	if len(argv) != 1 {
		c.writeErr("ERR wrong number of arguments for 'multi' command")
		return
	}
	if c.inMulti {
		c.writeErr("ERR MULTI calls can not be nested")
		return
	}
	c.inMulti = true
	c.multiAbort = false
	c.multiQueue = c.multiQueue[:0]
	c.writeSimple("OK")
}

// cmdExec runs a queued transaction. EXEC ends the MULTI whatever happens, so the state is
// cleared up front. A queue that hit an unknown command aborts with EXECABORT; a watched
// key that moved makes EXEC a null array; otherwise every queued command runs in order and
// the replies are framed as one array. WATCH is consumed either way.
func (c *connState) cmdExec(argv [][]byte) {
	if len(argv) != 1 {
		c.writeErr("ERR wrong number of arguments for 'exec' command")
		return
	}
	if !c.inMulti {
		c.writeErr("ERR EXEC without MULTI")
		return
	}
	aborted := c.multiAbort
	queue := c.multiQueue
	c.inMulti = false
	c.multiAbort = false

	if aborted {
		c.unwatchAll()
		c.multiQueue = queue[:0]
		c.writeErr("EXECABORT Transaction discarded because of previous errors.")
		return
	}
	if c.watchDirty() {
		c.unwatchAll()
		c.multiQueue = queue[:0]
		c.writeNilArray()
		return
	}
	// The watch snapshot passed; consume it before running so the block's own writes do not
	// count against it. Then run every queued command back to back and let each append its
	// own reply under the array header, so a command that itself replies an array nests
	// naturally, the way Redis serializes an EXEC.
	c.unwatchAll()
	c.writeArrayHeader(len(queue))
	for _, q := range queue {
		c.execCommand(q)
	}
	c.multiQueue = queue[:0]
}

// cmdDiscard throws away a queued transaction without running it, and drops any watches,
// matching Redis. DISCARD outside a transaction is an error.
func (c *connState) cmdDiscard(argv [][]byte) {
	if len(argv) != 1 {
		c.writeErr("ERR wrong number of arguments for 'discard' command")
		return
	}
	if !c.inMulti {
		c.writeErr("ERR DISCARD without MULTI")
		return
	}
	c.discardTx()
	c.writeSimple("OK")
}

// discardTx clears the queued transaction and any watches. It is the shared teardown for
// DISCARD, RESET, and a disconnect.
func (c *connState) discardTx() {
	c.inMulti = false
	c.multiAbort = false
	c.multiQueue = c.multiQueue[:0]
	c.unwatchAll()
}

// cmdWatch adds keys to this connection's optimistic-lock set. WATCH inside a MULTI is an
// error, since the point of WATCH is to guard the window before MULTI. Each key is
// snapshotted at its current version.
func (c *connState) cmdWatch(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'watch' command")
		return
	}
	if c.inMulti {
		c.writeErr("ERR WATCH inside MULTI is not allowed")
		return
	}
	for _, k := range argv[1:] {
		c.watchKey(k)
	}
	c.writeSimple("OK")
}

// cmdUnwatch drops every watch on this connection. It is always allowed, even with nothing
// watched, and always replies OK.
func (c *connState) cmdUnwatch(argv [][]byte) {
	if len(argv) != 1 {
		c.writeErr("ERR wrong number of arguments for 'unwatch' command")
		return
	}
	c.unwatchAll()
	c.writeSimple("OK")
}

// cmdReset returns the connection to a clean state and replies +RESET. On this single-db
// server the state a client can carry is an open transaction and a watch set; the other
// duties of Redis RESET (deauth, reselect db 0, unsubscribe) do not apply here.
func (c *connState) cmdReset(argv [][]byte) {
	c.inMulti = false
	c.multiAbort = false
	c.multiQueue = c.multiQueue[:0]
	c.unwatchAll()
	c.writeSimple("RESET")
}

// queueCommand copies a command into the transaction queue and answers +QUEUED. An unknown
// command cannot be queued: it flags the transaction so EXEC aborts with EXECABORT, the way
// Redis rejects a bad command at queue time. The copy is a deep copy because the parsed argv
// slices point into the connection read buffer, which is overwritten by the next batch long
// before EXEC runs the queue.
func (c *connState) queueCommand(argv [][]byte) {
	if len(argv) == 0 {
		return
	}
	if !isKnownCommand(argv[0]) {
		c.multiAbort = true
		c.writeErr("ERR unknown command '" + string(argv[0]) + "'")
		return
	}
	cp := make([][]byte, len(argv))
	for i, a := range argv {
		b := make([]byte, len(a))
		copy(b, a)
		cp[i] = b
	}
	c.multiQueue = append(c.multiQueue, cp)
	c.writeSimple("QUEUED")
}

// --- WATCH table plumbing (the server-side optimistic-lock versions) ---

// watchKey adds one key to this connection's watch set, snapshotting its current version.
// A key already watched by this connection is skipped, matching Redis's per-client dedup.
// The server table entry is refcounted so it is created on first watcher and removed on
// last unwatch, keeping the map bounded to keys under active WATCH.
func (c *connState) watchKey(key []byte) {
	ks := string(key)
	for _, w := range c.watched {
		if w.key == ks {
			return
		}
	}
	s := c.srv
	s.watchMu.Lock()
	e := s.watchVer[ks]
	if e == nil {
		e = &watchEntry{}
		s.watchVer[ks] = e
	}
	e.refs++
	ver := e.ver
	s.watchMu.Unlock()
	s.watching.Add(1)
	c.watched = append(c.watched, watchedKey{key: ks, ver: ver})
}

// unwatchAll releases every watch this connection holds, dropping each key's refcount and
// removing the table entry when the last watcher leaves. It is the teardown behind UNWATCH,
// EXEC, DISCARD, RESET, and a disconnect.
func (c *connState) unwatchAll() {
	if len(c.watched) == 0 {
		return
	}
	s := c.srv
	s.watchMu.Lock()
	for _, w := range c.watched {
		if e := s.watchVer[w.key]; e != nil {
			e.refs--
			if e.refs <= 0 {
				delete(s.watchVer, w.key)
			}
		}
	}
	s.watchMu.Unlock()
	s.watching.Add(int64(-len(c.watched)))
	c.watched = c.watched[:0]
}

// watchDirty reports whether any watched key has moved since it was snapshotted, which is
// what turns EXEC into a null array. A missing table entry is treated as dirty (defensive:
// while this connection watches a key it holds a refcount, so the entry should exist).
func (c *connState) watchDirty() bool {
	if len(c.watched) == 0 {
		return false
	}
	s := c.srv
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, w := range c.watched {
		e := s.watchVer[w.key]
		if e == nil || e.ver != w.ver {
			return true
		}
	}
	return false
}

// signalWrites bumps the watch version of every key a command writes, so a concurrent EXEC
// that watched one of them aborts. It runs in execCommand only when some client is watching
// (the atomic gate), so a watch-free workload never reaches here. A pure read bumps nothing;
// a flush dirties every watched key; every other command bumps the keys writeKeys names.
// The classification is deliberately safe in one direction: only the read set skips the
// bump, so a command missing from every table still bumps (a spurious abort is acceptable,
// a missed bump would let a stale EXEC commit over a concurrent write).
func (c *connState) signalWrites(cmd []byte, argv [][]byte) {
	if isReadOnly(cmd) {
		return
	}
	if eqFold(cmd, "FLUSHALL") || eqFold(cmd, "FLUSHDB") {
		c.bumpAllWatched()
		return
	}
	for _, k := range c.writeKeys(cmd, argv) {
		c.bumpWatch(k)
	}
}

// writeKeys returns the keys a write command modifies, into the reused wkeys scratch. The
// default is the single key at argv[1]; the multi-key and offset-key writers (MSET, DEL,
// the move family, BITOP, the numkeys pops, the stream group verbs) have explicit rules.
// Over-inclusion here is safe (it only widens which watched keys abort), so where a command
// reads some of its key arguments and writes others, naming a read key too is fine.
func (c *connState) writeKeys(cmd []byte, argv [][]byte) [][]byte {
	ks := c.wkeys[:0]
	switch {
	case eqFold(cmd, "MSET") || eqFold(cmd, "MSETNX"):
		for i := 1; i+1 < len(argv); i += 2 {
			ks = append(ks, argv[i])
		}
	case eqFold(cmd, "DEL") || eqFold(cmd, "UNLINK"):
		ks = append(ks, argv[1:]...)
	case eqFold(cmd, "RENAME") || eqFold(cmd, "RENAMENX") || eqFold(cmd, "COPY") ||
		eqFold(cmd, "SMOVE") || eqFold(cmd, "LMOVE") || eqFold(cmd, "RPOPLPUSH") ||
		eqFold(cmd, "BLMOVE") || eqFold(cmd, "BRPOPLPUSH"):
		if len(argv) > 1 {
			ks = append(ks, argv[1])
		}
		if len(argv) > 2 {
			ks = append(ks, argv[2])
		}
	case eqFold(cmd, "BITOP"):
		// BITOP op destkey srckey [srckey ...]: the destination it writes is argv[2].
		if len(argv) > 2 {
			ks = append(ks, argv[2])
		}
	case eqFold(cmd, "LMPOP") || eqFold(cmd, "ZMPOP"):
		// numkeys is argv[1]; the keys it may pop from follow it.
		ks = appendNumkeyKeys(ks, argv, 1)
	case eqFold(cmd, "BLMPOP"):
		// timeout is argv[1], numkeys is argv[2], the keys follow.
		ks = appendNumkeyKeys(ks, argv, 2)
	case eqFold(cmd, "XGROUP"):
		// XGROUP subcommand key ...: the stream key is argv[2].
		if len(argv) > 2 {
			ks = append(ks, argv[2])
		}
	case eqFold(cmd, "XREADGROUP"):
		// XREADGROUP ... STREAMS key [key ...] id [id ...]: it writes the group PEL of each
		// stream. The keys are the first half of the args after STREAMS.
		ks = appendStreamsKeys(ks, argv)
	default:
		if len(argv) > 1 {
			ks = append(ks, argv[1])
		}
	}
	c.wkeys = ks
	return ks
}

// appendNumkeyKeys appends the keys of a numkeys-prefixed command, where argv[idx] is the
// key count and the keys follow it. A malformed count contributes nothing.
func appendNumkeyKeys(ks [][]byte, argv [][]byte, idx int) [][]byte {
	if idx >= len(argv) {
		return ks
	}
	n, err := atoi64(argv[idx])
	if err != nil || n <= 0 {
		return ks
	}
	for i := 0; i < int(n) && idx+1+i < len(argv); i++ {
		ks = append(ks, argv[idx+1+i])
	}
	return ks
}

// appendStreamsKeys appends the stream keys of an XREADGROUP: the first half of the
// arguments after the STREAMS token (the second half are the ids). No STREAMS token means
// no keys.
func appendStreamsKeys(ks [][]byte, argv [][]byte) [][]byte {
	si := -1
	for i := 1; i < len(argv); i++ {
		if eqFold(argv[i], "STREAMS") {
			si = i
			break
		}
	}
	if si < 0 {
		return ks
	}
	rest := argv[si+1:]
	for i := 0; i < len(rest)/2; i++ {
		ks = append(ks, rest[i])
	}
	return ks
}

// bumpWatch advances one key's watch version if the key is currently watched. A key nobody
// watches has no table entry, so the bump is a single guarded map miss.
func (c *connState) bumpWatch(key []byte) {
	s := c.srv
	s.watchMu.Lock()
	if e := s.watchVer[string(key)]; e != nil {
		e.ver++
	}
	s.watchMu.Unlock()
}

// bumpAllWatched advances every watched key's version, the effect a FLUSHALL/FLUSHDB has on
// optimistic locks: it wipes the keyspace, so every outstanding WATCH must abort.
func (c *connState) bumpAllWatched() {
	s := c.srv
	s.watchMu.Lock()
	for _, e := range s.watchVer {
		e.ver++
	}
	s.watchMu.Unlock()
}

// isReadOnly reports whether a command never modifies the keyspace, so signalWrites can skip
// the version bump for it. This is the ONLY list that suppresses a bump, so a command absent
// from it is always treated as a write; keep pure reads here and nothing else.
func isReadOnly(cmd []byte) bool {
	return cmdIn(readOnlyCommands, cmd)
}

// isKnownCommand reports whether a command name is one this server dispatches, used to reject
// an unknown command at MULTI queue time (which turns EXEC into an EXECABORT).
func isKnownCommand(cmd []byte) bool {
	return cmdIn(knownCommands, cmd)
}

// cmdIn looks a command name up in a set case-insensitively without allocating, by folding
// the name to upper case in a stack buffer and using the compiler's alloc-free
// map[string([]byte)] lookup.
func cmdIn(m map[string]struct{}, cmd []byte) bool {
	if len(cmd) == 0 || len(cmd) > 24 {
		return false
	}
	var buf [24]byte
	for i := 0; i < len(cmd); i++ {
		b := cmd[i]
		if b >= 'a' && b <= 'z' {
			b -= 32
		}
		buf[i] = b
	}
	_, ok := m[string(buf[:len(cmd)])]
	return ok
}

// readOnlyCommands is the set of commands that never modify the keyspace. It gates the WATCH
// version bump: only these skip it. Anything not here is treated as a write.
var readOnlyCommands = map[string]struct{}{
	"GET": {}, "GETRANGE": {}, "SUBSTR": {}, "STRLEN": {}, "EXISTS": {}, "MGET": {},
	"LCS": {}, "KEYS": {}, "SCAN": {}, "RANDOMKEY": {}, "TOUCH": {}, "WAIT": {},
	"WAITAOF": {}, "MEMORY": {}, "DUMP": {},
	"HGET": {}, "HMGET": {}, "HEXISTS": {}, "HLEN": {}, "HSTRLEN": {}, "HGETALL": {},
	"HKEYS": {}, "HVALS": {}, "HSCAN": {}, "HRANDFIELD": {},
	"SISMEMBER": {}, "SMISMEMBER": {}, "SCARD": {}, "SMEMBERS": {}, "SSCAN": {},
	"SRANDMEMBER": {}, "SINTER": {}, "SUNION": {}, "SDIFF": {}, "SINTERCARD": {},
	"ZSCORE": {}, "ZMSCORE": {}, "ZCARD": {}, "ZRANK": {}, "ZREVRANK": {}, "ZRANGE": {},
	"ZREVRANGE": {}, "ZRANGEBYSCORE": {}, "ZREVRANGEBYSCORE": {}, "ZRANGEBYLEX": {},
	"ZREVRANGEBYLEX": {}, "ZCOUNT": {}, "ZLEXCOUNT": {}, "ZRANDMEMBER": {}, "ZUNION": {},
	"ZINTER": {}, "ZDIFF": {}, "ZINTERCARD": {},
	"LLEN": {}, "LINDEX": {}, "LRANGE": {}, "LPOS": {},
	"XLEN": {}, "XRANGE": {}, "XREVRANGE": {}, "XREAD": {}, "XPENDING": {}, "XINFO": {},
	"GETBIT": {}, "BITCOUNT": {}, "BITPOS": {}, "BITFIELD_RO": {},
	"PFCOUNT": {}, "PFSELFTEST": {},
	"GEOPOS": {}, "GEODIST": {}, "GEOHASH": {}, "GEOSEARCH": {}, "GEORADIUS_RO": {},
	"GEORADIUSBYMEMBER_RO": {},
	"TYPE": {}, "OBJECT": {}, "PING": {}, "ECHO": {}, "TTL": {}, "PTTL": {},
	"EXPIRETIME": {}, "PEXPIRETIME": {}, "DBSIZE": {}, "SELECT": {}, "CLIENT": {},
	"CONFIG": {}, "COMMAND": {}, "INFO": {},
}

// knownCommands is every command this server dispatches, used to reject an unknown command
// at MULTI queue time. It mirrors the execCommand switch plus the transaction verbs.
var knownCommands = map[string]struct{}{
	"GET": {}, "SET": {}, "GETEX": {}, "SETEX": {}, "PSETEX": {}, "SETNX": {},
	"GETDEL": {}, "GETSET": {}, "STRLEN": {}, "APPEND": {}, "GETRANGE": {}, "SUBSTR": {},
	"SETRANGE": {}, "INCR": {}, "DECR": {}, "INCRBY": {}, "DECRBY": {}, "INCRBYFLOAT": {},
	"DEL": {}, "UNLINK": {}, "EXISTS": {}, "MSET": {}, "MSETNX": {}, "MGET": {}, "LCS": {},
	"KEYS": {}, "SCAN": {}, "RANDOMKEY": {}, "TOUCH": {}, "RENAME": {}, "RENAMENX": {},
	"COPY": {}, "WAIT": {}, "WAITAOF": {}, "MEMORY": {}, "DUMP": {}, "RESTORE": {},
	"HSET": {}, "HMSET": {}, "HSETNX": {}, "HGET": {}, "HMGET": {}, "HDEL": {},
	"HEXISTS": {}, "HLEN": {}, "HSTRLEN": {}, "HGETALL": {}, "HKEYS": {}, "HVALS": {},
	"HSCAN": {}, "HINCRBY": {}, "HINCRBYFLOAT": {}, "HRANDFIELD": {},
	"SADD": {}, "SREM": {}, "SISMEMBER": {}, "SMISMEMBER": {}, "SCARD": {}, "SMEMBERS": {},
	"SSCAN": {}, "SRANDMEMBER": {}, "SPOP": {}, "SMOVE": {}, "SINTER": {}, "SUNION": {},
	"SDIFF": {}, "SINTERCARD": {}, "SINTERSTORE": {}, "SUNIONSTORE": {}, "SDIFFSTORE": {},
	"ZADD": {}, "ZINCRBY": {}, "ZSCORE": {}, "ZMSCORE": {}, "ZCARD": {}, "ZREM": {},
	"ZRANK": {}, "ZREVRANK": {}, "ZRANGE": {}, "ZRANGESTORE": {}, "ZREVRANGE": {},
	"ZRANGEBYSCORE": {}, "ZREVRANGEBYSCORE": {}, "ZRANGEBYLEX": {}, "ZREVRANGEBYLEX": {},
	"ZCOUNT": {}, "ZLEXCOUNT": {}, "ZPOPMIN": {}, "ZPOPMAX": {}, "ZREMRANGEBYRANK": {},
	"ZREMRANGEBYSCORE": {}, "ZREMRANGEBYLEX": {}, "ZRANDMEMBER": {}, "ZMPOP": {},
	"ZUNION": {}, "ZINTER": {}, "ZDIFF": {}, "ZINTERCARD": {}, "ZUNIONSTORE": {},
	"ZINTERSTORE": {}, "ZDIFFSTORE": {},
	"LPUSH": {}, "RPUSH": {}, "LPOP": {}, "RPOP": {}, "LLEN": {}, "LINDEX": {},
	"LRANGE": {}, "LSET": {}, "LPOS": {}, "LPUSHX": {}, "RPUSHX": {}, "LTRIM": {},
	"LINSERT": {}, "LREM": {}, "LMOVE": {}, "RPOPLPUSH": {}, "LMPOP": {}, "BLPOP": {},
	"BRPOP": {}, "BLMOVE": {}, "BRPOPLPUSH": {}, "BLMPOP": {},
	"XADD": {}, "XLEN": {}, "XRANGE": {}, "XREVRANGE": {}, "XREAD": {}, "XDEL": {},
	"XTRIM": {}, "XSETID": {}, "XGROUP": {}, "XREADGROUP": {}, "XACK": {}, "XPENDING": {},
	"XCLAIM": {}, "XAUTOCLAIM": {}, "XINFO": {},
	"SETBIT": {}, "GETBIT": {}, "BITCOUNT": {}, "BITPOS": {}, "BITOP": {}, "BITFIELD": {},
	"BITFIELD_RO": {},
	"PFADD": {}, "PFCOUNT": {}, "PFMERGE": {}, "PFDEBUG": {}, "PFSELFTEST": {},
	"GEOADD": {}, "GEOPOS": {}, "GEODIST": {}, "GEOHASH": {}, "GEORADIUS": {},
	"GEORADIUS_RO": {}, "GEORADIUSBYMEMBER": {}, "GEORADIUSBYMEMBER_RO": {},
	"GEOSEARCH": {}, "GEOSEARCHSTORE": {},
	"TYPE": {}, "OBJECT": {}, "PING": {}, "ECHO": {},
	"EXPIRE": {}, "PEXPIRE": {}, "EXPIREAT": {}, "PEXPIREAT": {}, "TTL": {}, "PTTL": {},
	"EXPIRETIME": {}, "PEXPIRETIME": {}, "PERSIST": {},
	"FLUSHALL": {}, "FLUSHDB": {}, "DBSIZE": {}, "SELECT": {}, "CLIENT": {}, "CONFIG": {},
	"COMMAND": {}, "INFO": {}, "QUIT": {},
	"MULTI": {}, "EXEC": {}, "DISCARD": {}, "WATCH": {}, "UNWATCH": {}, "RESET": {},
}
