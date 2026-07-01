package f1srv

import (
	"github.com/tamnd/aki/engine/f1raw"
)

// dispatch routes one parsed command to its handler and writes a reply. Unknown
// commands get a Redis-shaped error so a misconfigured client fails loudly rather
// than hanging. The hot string verbs (GET, SET, INCR) are checked first.
func (c *connState) dispatch(argv [][]byte) {
	if len(argv) == 0 {
		return
	}
	cmd := argv[0]
	switch {
	case eqFold(cmd, "GET"):
		c.cmdGet(argv)
	case eqFold(cmd, "SET"):
		c.cmdSet(argv)
	case eqFold(cmd, "INCR"):
		c.cmdIncrBy(argv, 1, false)
	case eqFold(cmd, "DECR"):
		c.cmdIncrBy(argv, -1, false)
	case eqFold(cmd, "INCRBY"):
		c.cmdIncrBy(argv, 0, true)
	case eqFold(cmd, "DECRBY"):
		c.cmdIncrBy(argv, 0, true)
	case eqFold(cmd, "DEL") || eqFold(cmd, "UNLINK"):
		c.cmdDel(argv)
	case eqFold(cmd, "EXISTS"):
		c.cmdExists(argv)
	case eqFold(cmd, "MSET"):
		c.cmdMSet(argv)
	case eqFold(cmd, "MGET"):
		c.cmdMGet(argv)
	case eqFold(cmd, "HSET"):
		c.cmdHSet(argv)
	case eqFold(cmd, "HMSET"):
		c.cmdHMSet(argv)
	case eqFold(cmd, "HSETNX"):
		c.cmdHSetNX(argv)
	case eqFold(cmd, "HGET"):
		c.cmdHGet(argv)
	case eqFold(cmd, "HMGET"):
		c.cmdHMGet(argv)
	case eqFold(cmd, "HDEL"):
		c.cmdHDel(argv)
	case eqFold(cmd, "HEXISTS"):
		c.cmdHExists(argv)
	case eqFold(cmd, "HLEN"):
		c.cmdHLen(argv)
	case eqFold(cmd, "HSTRLEN"):
		c.cmdHStrlen(argv)
	case eqFold(cmd, "HGETALL"):
		c.cmdHGetAll(argv)
	case eqFold(cmd, "HKEYS"):
		c.cmdHKeys(argv)
	case eqFold(cmd, "HVALS"):
		c.cmdHVals(argv)
	case eqFold(cmd, "HSCAN"):
		c.cmdHScan(argv)
	case eqFold(cmd, "SADD"):
		c.cmdSAdd(argv)
	case eqFold(cmd, "SREM"):
		c.cmdSRem(argv)
	case eqFold(cmd, "SISMEMBER"):
		c.cmdSIsMember(argv)
	case eqFold(cmd, "SMISMEMBER"):
		c.cmdSMIsMember(argv)
	case eqFold(cmd, "SCARD"):
		c.cmdSCard(argv)
	case eqFold(cmd, "SMEMBERS"):
		c.cmdSMembers(argv)
	case eqFold(cmd, "SSCAN"):
		c.cmdSScan(argv)
	case eqFold(cmd, "SRANDMEMBER"):
		c.cmdSRandMember(argv)
	case eqFold(cmd, "SPOP"):
		c.cmdSPop(argv)
	case eqFold(cmd, "SMOVE"):
		c.cmdSMove(argv)
	case eqFold(cmd, "SINTER"):
		c.cmdSInter(argv)
	case eqFold(cmd, "SUNION"):
		c.cmdSUnion(argv)
	case eqFold(cmd, "SDIFF"):
		c.cmdSDiff(argv)
	case eqFold(cmd, "SINTERCARD"):
		c.cmdSInterCard(argv)
	case eqFold(cmd, "SINTERSTORE"):
		c.cmdSInterStore(argv)
	case eqFold(cmd, "SUNIONSTORE"):
		c.cmdSUnionStore(argv)
	case eqFold(cmd, "SDIFFSTORE"):
		c.cmdSDiffStore(argv)
	case eqFold(cmd, "ZADD"):
		c.cmdZAdd(argv)
	case eqFold(cmd, "ZINCRBY"):
		c.cmdZIncrBy(argv)
	case eqFold(cmd, "ZSCORE"):
		c.cmdZScore(argv)
	case eqFold(cmd, "ZMSCORE"):
		c.cmdZMScore(argv)
	case eqFold(cmd, "ZCARD"):
		c.cmdZCard(argv)
	case eqFold(cmd, "ZREM"):
		c.cmdZRem(argv)
	case eqFold(cmd, "ZRANK"):
		c.cmdZRank(argv, false)
	case eqFold(cmd, "ZREVRANK"):
		c.cmdZRank(argv, true)
	case eqFold(cmd, "ZRANGE"):
		c.cmdZRange(argv)
	case eqFold(cmd, "ZRANGESTORE"):
		c.cmdZRangeStore(argv)
	case eqFold(cmd, "ZREVRANGE"):
		c.cmdZRevRange(argv)
	case eqFold(cmd, "ZRANGEBYSCORE"):
		c.cmdZRangeByScore(argv, false)
	case eqFold(cmd, "ZREVRANGEBYSCORE"):
		c.cmdZRangeByScore(argv, true)
	case eqFold(cmd, "ZRANGEBYLEX"):
		c.cmdZRangeByLex(argv, false)
	case eqFold(cmd, "ZREVRANGEBYLEX"):
		c.cmdZRangeByLex(argv, true)
	case eqFold(cmd, "ZCOUNT"):
		c.cmdZCount(argv)
	case eqFold(cmd, "ZLEXCOUNT"):
		c.cmdZLexCount(argv)
	case eqFold(cmd, "ZPOPMIN"):
		c.cmdZPop(argv, false)
	case eqFold(cmd, "ZPOPMAX"):
		c.cmdZPop(argv, true)
	case eqFold(cmd, "ZREMRANGEBYRANK"):
		c.cmdZRemRangeByRank(argv)
	case eqFold(cmd, "ZREMRANGEBYSCORE"):
		c.cmdZRemRangeByScore(argv)
	case eqFold(cmd, "ZREMRANGEBYLEX"):
		c.cmdZRemRangeByLex(argv)
	case eqFold(cmd, "ZRANDMEMBER"):
		c.cmdZRandMember(argv)
	case eqFold(cmd, "ZMPOP"):
		c.cmdZMPop(argv)
	case eqFold(cmd, "ZUNION"):
		c.cmdZUnion(argv)
	case eqFold(cmd, "ZINTER"):
		c.cmdZInter(argv)
	case eqFold(cmd, "ZDIFF"):
		c.cmdZDiff(argv)
	case eqFold(cmd, "ZINTERCARD"):
		c.cmdZInterCard(argv)
	case eqFold(cmd, "ZUNIONSTORE"):
		c.cmdZUnionStore(argv)
	case eqFold(cmd, "ZINTERSTORE"):
		c.cmdZInterStore(argv)
	case eqFold(cmd, "ZDIFFSTORE"):
		c.cmdZDiffStore(argv)
	case eqFold(cmd, "LPUSH"):
		c.cmdLPush(argv)
	case eqFold(cmd, "RPUSH"):
		c.cmdRPush(argv)
	case eqFold(cmd, "LPOP"):
		c.cmdLPop(argv)
	case eqFold(cmd, "RPOP"):
		c.cmdRPop(argv)
	case eqFold(cmd, "LLEN"):
		c.cmdLLen(argv)
	case eqFold(cmd, "LINDEX"):
		c.cmdLIndex(argv)
	case eqFold(cmd, "LRANGE"):
		c.cmdLRange(argv)
	case eqFold(cmd, "LSET"):
		c.cmdLSet(argv)
	case eqFold(cmd, "LPOS"):
		c.cmdLPos(argv)
	case eqFold(cmd, "LPUSHX"):
		c.cmdLPushX(argv)
	case eqFold(cmd, "RPUSHX"):
		c.cmdRPushX(argv)
	case eqFold(cmd, "LTRIM"):
		c.cmdLTrim(argv)
	case eqFold(cmd, "LINSERT"):
		c.cmdLInsert(argv)
	case eqFold(cmd, "LREM"):
		c.cmdLRem(argv)
	case eqFold(cmd, "LMOVE"):
		c.cmdLMove(argv)
	case eqFold(cmd, "RPOPLPUSH"):
		c.cmdRPopLPush(argv)
	case eqFold(cmd, "LMPOP"):
		c.cmdLMPop(argv)
	case eqFold(cmd, "BLPOP"):
		c.cmdBLPop(argv)
	case eqFold(cmd, "BRPOP"):
		c.cmdBRPop(argv)
	case eqFold(cmd, "BLMOVE"):
		c.cmdBLMove(argv)
	case eqFold(cmd, "BRPOPLPUSH"):
		c.cmdBRPopLPush(argv)
	case eqFold(cmd, "BLMPOP"):
		c.cmdBLMPop(argv)
	case eqFold(cmd, "XADD"):
		c.cmdXAdd(argv)
	case eqFold(cmd, "XLEN"):
		c.cmdXLen(argv)
	case eqFold(cmd, "XRANGE"):
		c.cmdXRange(argv, false)
	case eqFold(cmd, "XREVRANGE"):
		c.cmdXRange(argv, true)
	case eqFold(cmd, "XREAD"):
		c.cmdXRead(argv)
	case eqFold(cmd, "XDEL"):
		c.cmdXDel(argv)
	case eqFold(cmd, "XTRIM"):
		c.cmdXTrim(argv)
	case eqFold(cmd, "XSETID"):
		c.cmdXSetID(argv)
	case eqFold(cmd, "XGROUP"):
		c.cmdXGroup(argv)
	case eqFold(cmd, "XREADGROUP"):
		c.cmdXReadGroup(argv)
	case eqFold(cmd, "XACK"):
		c.cmdXAck(argv)
	case eqFold(cmd, "XPENDING"):
		c.cmdXPending(argv)
	case eqFold(cmd, "XCLAIM"):
		c.cmdXClaim(argv)
	case eqFold(cmd, "XAUTOCLAIM"):
		c.cmdXAutoClaim(argv)
	case eqFold(cmd, "XINFO"):
		c.cmdXInfo(argv)
	case eqFold(cmd, "SETBIT"):
		c.cmdSetBit(argv)
	case eqFold(cmd, "GETBIT"):
		c.cmdGetBit(argv)
	case eqFold(cmd, "BITCOUNT"):
		c.cmdBitCount(argv)
	case eqFold(cmd, "BITPOS"):
		c.cmdBitPos(argv)
	case eqFold(cmd, "BITOP"):
		c.cmdBitOp(argv)
	case eqFold(cmd, "BITFIELD"):
		c.cmdBitField(argv, false)
	case eqFold(cmd, "BITFIELD_RO"):
		c.cmdBitField(argv, true)
	case eqFold(cmd, "PFADD"):
		c.cmdPfAdd(argv)
	case eqFold(cmd, "PFCOUNT"):
		c.cmdPfCount(argv)
	case eqFold(cmd, "PFMERGE"):
		c.cmdPfMerge(argv)
	case eqFold(cmd, "PFDEBUG"):
		c.cmdPfDebug(argv)
	case eqFold(cmd, "PFSELFTEST"):
		c.cmdPfSelfTest(argv)
	case eqFold(cmd, "TYPE"):
		c.cmdType(argv)
	case eqFold(cmd, "OBJECT"):
		c.cmdObject(argv)
	case eqFold(cmd, "PING"):
		c.cmdPing(argv)
	case eqFold(cmd, "ECHO"):
		c.cmdEcho(argv)
	case eqFold(cmd, "EXPIRE") || eqFold(cmd, "PEXPIRE") ||
		eqFold(cmd, "EXPIREAT") || eqFold(cmd, "PEXPIREAT"):
		c.cmdExpire(argv)
	case eqFold(cmd, "TTL") || eqFold(cmd, "PTTL"):
		c.cmdTTL(argv)
	case eqFold(cmd, "PERSIST"):
		c.cmdPersist(argv)
	case eqFold(cmd, "FLUSHALL") || eqFold(cmd, "FLUSHDB"):
		c.srv.store.Reset()
		c.writeSimple("OK")
	case eqFold(cmd, "DBSIZE"):
		c.writeInt(int64(c.srv.store.Len()))
	case eqFold(cmd, "SELECT") || eqFold(cmd, "CLIENT") || eqFold(cmd, "CONFIG") ||
		eqFold(cmd, "RESET"):
		c.writeSimple("OK")
	case eqFold(cmd, "COMMAND"):
		c.writeArrayHeader(0)
	case eqFold(cmd, "INFO"):
		c.writeBulk([]byte("# Server\r\nredis_version:7.4.0\r\n"))
	case eqFold(cmd, "QUIT"):
		// Reply, then ask the driver to flush and close. Draining stops after this
		// command so a pipeline queued behind QUIT is discarded, matching Redis.
		c.writeSimple("OK")
		c.wantClose = true
	default:
		c.writeErr("ERR unknown command '" + string(cmd) + "'")
	}
}

func (c *connState) cmdGet(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'get' command")
		return
	}
	v, ok := c.srv.store.Get(argv[1], c.vbuf)
	c.vbuf = v
	if !ok {
		c.writeNil()
		return
	}
	c.writeBulk(v)
}

func (c *connState) cmdSet(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'set' command")
		return
	}
	// Options (EX/PX/NX/XX/GET/KEEPTTL) are accepted but only the bare set is
	// honored for now; TTL lands with the .aki durability work. A plain bench SET
	// sends no options, so this never affects the measured path.
	if err := c.srv.store.Set(argv[1], argv[2]); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeSimple("OK")
}

func (c *connState) cmdIncrBy(argv [][]byte, fixed int64, hasArg bool) {
	var delta int64
	var key []byte
	if hasArg {
		if len(argv) != 3 {
			c.writeErr("ERR wrong number of arguments")
			return
		}
		n, err := atoi64(argv[2])
		if err != nil {
			c.writeErr("ERR value is not an integer or out of range")
			return
		}
		key = argv[1]
		if eqFold(argv[0], "DECRBY") {
			delta = -n
		} else {
			delta = n
		}
	} else {
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments")
			return
		}
		key = argv[1]
		delta = fixed
	}
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	n, err := c.srv.store.Incr(key, delta)
	mu.Unlock()
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	c.writeInt(n)
}

func (c *connState) cmdDel(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'del' command")
		return
	}
	var n int64
	for _, k := range argv[1:] {
		if c.dropKey(k) {
			n++
		}
	}
	c.writeInt(n)
}

func (c *connState) cmdExists(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'exists' command")
		return
	}
	var n int64
	for _, k := range argv[1:] {
		if c.keyTypeOf(k) != keyMissing {
			n++
		}
	}
	c.writeInt(n)
}

// dropKey removes a key of any type in full and reports whether it existed. A string is a
// single record, but a collection is a header row plus every element row it owns, so DEL and
// UNLINK route through here instead of the string-only store.Delete they used to call, which
// left a collection's element rows (and its header) orphaned in the arena and reported nothing
// removed. It resolves the type once, then drops the string record or cascades the collection.
// The stripe lock serializes the cascade against concurrent writers to the same key, the same
// lock every collection write already takes.
func (c *connState) dropKey(key []byte) bool {
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	return c.dropKeyLocked(key)
}

// dropKeyLocked is the cascade body of dropKey without the stripe lock, for callers that already
// hold the relevant stripe lock (BITOP holds every key's stripe under lockStripes before it
// overwrites the destination).
func (c *connState) dropKeyLocked(key []byte) bool {
	switch c.keyTypeOf(key) {
	case keyString:
		return c.srv.store.Delete(key)
	case keyHash:
		c.dropCollIndex(c.hashPrefix(key), kindHashField)
		c.srv.store.DeleteKind(key, kindHashMeta)
		return true
	case keySet:
		c.dropCollIndex(c.setPrefix(key), kindSetMember)
		c.srv.store.DeleteKind(key, kindSetMeta)
		return true
	case keyZset:
		// A zset carries two element indexes (member-family and score-family rows), so both
		// are drained before the header. The member prefix is fully consumed before the score
		// prefix is built, so the shared pbuf they both borrow is never live for both at once.
		c.dropCollIndex(c.zmemberPrefix(key), kindZsetMember)
		c.dropCollIndex(c.zscorePrefix(key), kindZsetScore)
		c.srv.store.DeleteKind(key, kindZsetMeta)
		return true
	case keyList:
		c.dropList(key)
		return true
	case keyStream:
		// A stream is the one type whose header outlives an empty entry range, so DEL is the
		// path that clears it: drop every entry row, then the header. Later slices extend
		// dropStream to the group, consumer, and PEL sibling families.
		c.dropStream(key)
		return true
	}
	return false
}

// dropCollIndex deletes every element row under prefix in the given kind and unlinks each from
// the ordered element index, in bounded batches so a huge collection never holds the index lock
// across the whole set. It re-scans from the start of the prefix each round because the previous
// round deleted the batch it returned, so the next scan-from-start surfaces the next survivors;
// the loop ends when a scan comes back empty. It is the shared drop body for the single-index
// collections (hash fields, set members) and for each of a zset's two indexes.
func (c *connState) dropCollIndex(prefix []byte, kind byte) {
	var scan [][]byte
	for {
		keys, _ := c.srv.store.CollScan(prefix, nil, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			return
		}
		for _, k := range keys {
			c.srv.store.DeleteKind(k, kind)
			c.srv.store.CollRemove(k)
		}
		scan = keys
	}
}

// dropList deletes a list's element rows and header. A list is not carried in the ordered index
// (positional access is direct window arithmetic), so its elements are walked straight off the
// header window [head, tail) rather than a prefix scan: each position's row is a point delete,
// then the header row goes. A missing header is a no-op, which dropKey never reaches since it
// only calls here after keyTypeOf confirmed the list.
func (c *connState) dropList(lkey []byte) {
	head, tail, _, _, ok := c.listHeader(lkey)
	if !ok {
		return
	}
	for p := head; p < tail; p++ {
		c.srv.store.DeleteKind(c.listElemKey(lkey, p), kindListElem)
	}
	c.srv.store.DeleteKind(lkey, kindListMeta)
}

func (c *connState) cmdMSet(argv [][]byte) {
	if len(argv) < 3 || len(argv)%2 != 1 {
		c.writeErr("ERR wrong number of arguments for 'mset' command")
		return
	}
	for i := 1; i+1 < len(argv); i += 2 {
		if err := c.srv.store.Set(argv[i], argv[i+1]); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	c.writeSimple("OK")
}

func (c *connState) cmdMGet(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'mget' command")
		return
	}
	c.writeArrayHeader(len(argv) - 1)
	for _, k := range argv[1:] {
		v, ok := c.srv.store.Get(k, c.vbuf)
		c.vbuf = v
		if !ok {
			c.writeNil()
			continue
		}
		c.writeBulk(v)
	}
}

func (c *connState) cmdPing(argv [][]byte) {
	if len(argv) >= 2 {
		c.writeBulk(argv[1])
		return
	}
	c.writeSimple("PONG")
}

func (c *connState) cmdEcho(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'echo' command")
		return
	}
	c.writeBulk(argv[1])
}

// cmdExpire is a presence-reporting stub: it reports 1 when the key exists and 0
// otherwise, which is enough for the bench smoke check and the in-memory workloads.
// Real key expiry arrives with the durability and .aki file work, where a record can
// carry a deadline; it is deliberately not bolted onto the lock-free hot path here.
func (c *connState) cmdExpire(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	if _, ok := c.srv.store.Get(argv[1], c.vbuf[:0]); ok {
		c.writeInt(1)
		return
	}
	c.writeInt(0)
}

func (c *connState) cmdTTL(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	if _, ok := c.srv.store.Get(argv[1], c.vbuf[:0]); ok {
		c.writeInt(-1) // exists, no expiry tracked yet
		return
	}
	c.writeInt(-2) // missing
}

func (c *connState) cmdPersist(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	c.writeInt(0) // no expiry was set, so nothing to persist
}

// stripe maps a key to one of the INCR-family RMW lock stripes with a cheap
// word-at-a-time mix. It only gates INCR/DECR, never GET or SET.
func (s *Server) stripe(key []byte) uint32 {
	var h uint64 = 1469598103934665603
	for _, b := range key {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return uint32(h) & s.incrMask
}

// eqFold reports whether b equals the ASCII command name s case-insensitively,
// without allocating.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		x := b[i]
		if x >= 'a' && x <= 'z' {
			x -= 32
		}
		y := s[i]
		if y >= 'a' && y <= 'z' {
			y -= 32
		}
		if x != y {
			return false
		}
	}
	return true
}

// atoi64 parses a base-10 signed integer from bytes with no allocation and strict
// form, matching the integer arguments Redis accepts for INCRBY and friends.
func atoi64(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, f1raw.ErrNotInt
	}
	neg := false
	i := 0
	if b[0] == '-' || b[0] == '+' {
		neg = b[0] == '-'
		i = 1
		if i == len(b) {
			return 0, f1raw.ErrNotInt
		}
	}
	var n int64
	for ; i < len(b); i++ {
		d := b[i]
		if d < '0' || d > '9' {
			return 0, f1raw.ErrNotInt
		}
		n = n*10 + int64(d-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}
