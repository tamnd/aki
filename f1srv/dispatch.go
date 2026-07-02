package f1srv

import (
	"github.com/tamnd/aki/engine/f1raw"
)

// dispatch is the transaction control layer in front of the command table. The five
// transaction verbs (MULTI, EXEC, DISCARD, WATCH, UNWATCH) plus RESET are always handled
// here so they work the same whether or not a transaction is open. When a transaction is
// open every other command is queued and answered +QUEUED instead of run, matching Redis:
// a MULTI block is dispatched as one unit at EXEC, not incrementally. Outside a transaction
// the command runs immediately through execCommand.
func (c *connState) dispatch(argv [][]byte) {
	if len(argv) == 0 {
		return
	}
	cmd := argv[0]
	// While a connection is in subscribe context (RESP2), Redis allows only a small set of
	// commands; every other command is refused with a fixed error naming the lowercased verb.
	// This gate runs before the transaction verbs so an EXEC or a data command issued while
	// subscribed is refused the same way. A connection that never subscribes keeps psMode
	// false and skips the test.
	if c.psMode && !allowedInSubscribeMode(cmd) {
		c.writeErr("ERR Can't execute '" + lowerName(cmd) + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
		return
	}
	switch {
	case eqFold(cmd, "MULTI"):
		c.cmdMulti(argv)
		return
	case eqFold(cmd, "EXEC"):
		c.cmdExec(argv)
		return
	case eqFold(cmd, "DISCARD"):
		c.cmdDiscard(argv)
		return
	case eqFold(cmd, "WATCH"):
		c.cmdWatch(argv)
		return
	case eqFold(cmd, "UNWATCH"):
		c.cmdUnwatch(argv)
		return
	case eqFold(cmd, "RESET"):
		c.cmdReset(argv)
		return
	}
	// The subscribe verbs change per-connection state directly and are never queued in a
	// transaction; Redis refuses them inside MULTI, which doSubscribe/doUnsubscribe enforce.
	// PUBLISH, SPUBLISH, and PUBSUB are ordinary commands handled in execCommand, so they may
	// be queued in a transaction like any other write.
	switch {
	case eqFold(cmd, "SUBSCRIBE"):
		c.doSubscribe(argv, psKindChannel)
		return
	case eqFold(cmd, "UNSUBSCRIBE"):
		c.doUnsubscribe(argv, psKindChannel)
		return
	case eqFold(cmd, "PSUBSCRIBE"):
		c.doSubscribe(argv, psKindPattern)
		return
	case eqFold(cmd, "PUNSUBSCRIBE"):
		c.doUnsubscribe(argv, psKindPattern)
		return
	case eqFold(cmd, "SSUBSCRIBE"):
		c.doSubscribe(argv, psKindShard)
		return
	case eqFold(cmd, "SUNSUBSCRIBE"):
		c.doUnsubscribe(argv, psKindShard)
		return
	}
	if c.inMulti {
		c.queueCommand(argv)
		return
	}
	c.execCommand(argv)
}

// execCommand routes one parsed command to its handler and writes a reply. Unknown
// commands get a Redis-shaped error so a misconfigured client fails loudly rather
// than hanging. The hot string verbs (GET, SET, INCR) are checked first.
func (c *connState) execCommand(argv [][]byte) {
	if len(argv) == 0 {
		return
	}
	cmd := argv[0]
	// Reap the command's key if it has expired, before the handler runs. The volatile gate
	// inside makes this one atomic load when no key carries a TTL, so the hot path is
	// untouched; when TTLs exist it is what makes a typed read (HGET, ZSCORE, ...) see an
	// expired key as absent, matching Redis's per-lookup expireIfNeeded.
	c.reapExpiredKeys(cmd, argv)
	// When any client is watching a key, bump the watch version of every key this command
	// writes before it runs, so a concurrent EXEC that watched one of them aborts. The
	// atomic gate keeps this off the path entirely when nothing is watched, the same
	// zero-cost-when-unused pattern the volatile TTL gate uses.
	if c.srv.watching.Load() != 0 {
		c.signalWrites(cmd, argv)
	}
	switch {
	case eqFold(cmd, "GET"):
		c.cmdGet(argv)
	case eqFold(cmd, "SET"):
		c.cmdSet(argv)
	case eqFold(cmd, "GETEX"):
		c.cmdGetEx(argv)
	case eqFold(cmd, "SETEX"):
		c.cmdSetEx(argv, false)
	case eqFold(cmd, "PSETEX"):
		c.cmdSetEx(argv, true)
	case eqFold(cmd, "SETNX"):
		c.cmdSetNX(argv)
	case eqFold(cmd, "GETDEL"):
		c.cmdGetDel(argv)
	case eqFold(cmd, "GETSET"):
		c.cmdGetSet(argv)
	case eqFold(cmd, "STRLEN"):
		c.cmdStrlen(argv)
	case eqFold(cmd, "APPEND"):
		c.cmdAppend(argv)
	case eqFold(cmd, "GETRANGE"):
		c.cmdGetRange(argv, "getrange")
	case eqFold(cmd, "SUBSTR"):
		c.cmdGetRange(argv, "substr")
	case eqFold(cmd, "SETRANGE"):
		c.cmdSetRange(argv)
	case eqFold(cmd, "INCR"):
		c.cmdIncrBy(argv, 1, false)
	case eqFold(cmd, "DECR"):
		c.cmdIncrBy(argv, -1, false)
	case eqFold(cmd, "INCRBY"):
		c.cmdIncrBy(argv, 0, true)
	case eqFold(cmd, "DECRBY"):
		c.cmdIncrBy(argv, 0, true)
	case eqFold(cmd, "INCRBYFLOAT"):
		c.cmdIncrByFloat(argv)
	case eqFold(cmd, "DEL") || eqFold(cmd, "UNLINK"):
		c.cmdDel(argv)
	case eqFold(cmd, "EXISTS"):
		c.cmdExists(argv)
	case eqFold(cmd, "MSET"):
		c.cmdMSet(argv)
	case eqFold(cmd, "MSETNX"):
		c.cmdMSetNX(argv)
	case eqFold(cmd, "MGET"):
		c.cmdMGet(argv)
	case eqFold(cmd, "LCS"):
		c.cmdLCS(argv)
	case eqFold(cmd, "KEYS"):
		c.cmdKeys(argv)
	case eqFold(cmd, "SCAN"):
		c.cmdScan(argv)
	case eqFold(cmd, "RANDOMKEY"):
		c.cmdRandomKey(argv)
	case eqFold(cmd, "TOUCH"):
		c.cmdTouch(argv)
	case eqFold(cmd, "RENAME"):
		c.cmdRename(argv)
	case eqFold(cmd, "RENAMENX"):
		c.cmdRenameNx(argv)
	case eqFold(cmd, "COPY"):
		c.cmdCopy(argv)
	case eqFold(cmd, "SORT"):
		c.cmdSort(argv)
	case eqFold(cmd, "SORT_RO"):
		c.cmdSortRO(argv)
	case eqFold(cmd, "WAIT"):
		c.cmdWait(argv)
	case eqFold(cmd, "WAITAOF"):
		c.cmdWaitAOF(argv)
	case eqFold(cmd, "MEMORY"):
		c.cmdMemory(argv)
	case eqFold(cmd, "DUMP"):
		c.cmdDump(argv)
	case eqFold(cmd, "RESTORE"):
		c.cmdRestore(argv)
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
	case eqFold(cmd, "HINCRBY"):
		c.cmdHIncrBy(argv)
	case eqFold(cmd, "HINCRBYFLOAT"):
		c.cmdHIncrByFloat(argv)
	case eqFold(cmd, "HRANDFIELD"):
		c.cmdHRandField(argv)
	case eqFold(cmd, "HEXPIRE"):
		c.cmdHExpire(argv)
	case eqFold(cmd, "HPEXPIRE"):
		c.cmdHPExpire(argv)
	case eqFold(cmd, "HEXPIREAT"):
		c.cmdHExpireAt(argv)
	case eqFold(cmd, "HPEXPIREAT"):
		c.cmdHPExpireAt(argv)
	case eqFold(cmd, "HTTL"):
		c.cmdHTTL(argv)
	case eqFold(cmd, "HPTTL"):
		c.cmdHPTTL(argv)
	case eqFold(cmd, "HEXPIRETIME"):
		c.cmdHExpireTime(argv)
	case eqFold(cmd, "HPEXPIRETIME"):
		c.cmdHPExpireTime(argv)
	case eqFold(cmd, "HPERSIST"):
		c.cmdHPersist(argv)
	case eqFold(cmd, "HGETEX"):
		c.cmdHGetEx(argv)
	case eqFold(cmd, "HGETDEL"):
		c.cmdHGetDel(argv)
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
	case eqFold(cmd, "ZSCAN"):
		c.cmdZScan(argv)
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
	case eqFold(cmd, "BZPOPMIN"):
		c.cmdBZPopMin(argv)
	case eqFold(cmd, "BZPOPMAX"):
		c.cmdBZPopMax(argv)
	case eqFold(cmd, "BZMPOP"):
		c.cmdBZMPop(argv)
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
	case eqFold(cmd, "GEOADD"):
		c.cmdGeoAdd(argv)
	case eqFold(cmd, "GEOPOS"):
		c.cmdGeoPos(argv)
	case eqFold(cmd, "GEODIST"):
		c.cmdGeoDist(argv)
	case eqFold(cmd, "GEOHASH"):
		c.cmdGeoHash(argv)
	case eqFold(cmd, "GEORADIUS"):
		c.cmdGeoRadius(argv, false)
	case eqFold(cmd, "GEORADIUS_RO"):
		c.cmdGeoRadius(argv, true)
	case eqFold(cmd, "GEORADIUSBYMEMBER"):
		c.cmdGeoRadiusByMember(argv, false)
	case eqFold(cmd, "GEORADIUSBYMEMBER_RO"):
		c.cmdGeoRadiusByMember(argv, true)
	case eqFold(cmd, "GEOSEARCH"):
		c.cmdGeoSearch(argv)
	case eqFold(cmd, "GEOSEARCHSTORE"):
		c.cmdGeoSearchStore(argv)
	case eqFold(cmd, "PUBLISH"):
		c.cmdPublish(argv, false)
	case eqFold(cmd, "SPUBLISH"):
		c.cmdPublish(argv, true)
	case eqFold(cmd, "PUBSUB"):
		c.cmdPubSub(argv)
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
	case eqFold(cmd, "EXPIRETIME") || eqFold(cmd, "PEXPIRETIME"):
		c.cmdExpireTime(argv)
	case eqFold(cmd, "PERSIST"):
		c.cmdPersist(argv)
	case eqFold(cmd, "FLUSHALL") || eqFold(cmd, "FLUSHDB"):
		c.srv.store.Reset()
		c.writeSimple("OK")
	case eqFold(cmd, "DBSIZE"):
		c.writeInt(int64(c.srv.store.TopLen()))
	case eqFold(cmd, "SELECT") || eqFold(cmd, "CLIENT") || eqFold(cmd, "CONFIG"):
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
	c.expireIfNeeded(argv[1])
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
	// Bare SET (no options) is the benchmark hot path and stays exactly the lock-free
	// store.Set it always was. A fresh SET replaces the value object, so it clears any
	// existing TTL (spec 11 section 2.5); the volatile gate keeps the clear off the path
	// entirely when no key in the store carries a TTL.
	if len(argv) == 3 {
		if err := c.srv.store.Set(argv[1], argv[2]); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
		if c.srv.volatile.Load() != 0 {
			c.clearExpiry(argv[1])
		}
		c.writeSimple("OK")
		return
	}
	c.cmdSetOptions(argv)
}

// set option flags parsed off SET's trailing arguments.
const (
	setNX      = 1 << iota // set only if the key does not exist
	setXX                  // set only if the key already exists
	setGet                 // return the old value (nil if absent), as GETSET does
	setKeepTTL             // preserve the key's existing TTL instead of clearing it
)

// expiry time units for the EX/PX/EXAT/PXAT family, shared by SET and GETEX.
const (
	unitNone  = 0
	unitEXsec = iota // EX: relative seconds
	unitPXms         // PX: relative milliseconds
	unitEXat         // EXAT: absolute unix seconds
	unitPXat         // PXAT: absolute unix milliseconds
)

// cmdSetOptions is the full SET with NX/XX/GET and the EX/PX/EXAT/PXAT/KEEPTTL expiry
// options (spec 11 section 2.5). It takes the key's stripe lock so the existence check, the
// conditional write, and the TTL update are one atomic step against a concurrent writer.
// The bare-SET fast path in cmdSet handles the no-option case without the lock.
func (c *connState) cmdSetOptions(argv [][]byte) {
	key, val := argv[1], argv[2]
	var flags, unit int
	var timeArg int64
	for i := 3; i < len(argv); i++ {
		opt := argv[i]
		switch {
		case eqFold(opt, "NX"):
			if flags&setXX != 0 {
				c.writeErr("ERR syntax error")
				return
			}
			flags |= setNX
		case eqFold(opt, "XX"):
			if flags&setNX != 0 {
				c.writeErr("ERR syntax error")
				return
			}
			flags |= setXX
		case eqFold(opt, "GET"):
			flags |= setGet
		case eqFold(opt, "KEEPTTL"):
			if unit != unitNone {
				c.writeErr("ERR syntax error")
				return
			}
			flags |= setKeepTTL
		case eqFold(opt, "EX"), eqFold(opt, "PX"), eqFold(opt, "EXAT"), eqFold(opt, "PXAT"):
			// Only one expiry option is allowed, and KEEPTTL is an expiry option too.
			if unit != unitNone || flags&setKeepTTL != 0 {
				c.writeErr("ERR syntax error")
				return
			}
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, err := atoi64(argv[i+1])
			if err != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			i++
			timeArg = n
			switch {
			case eqFold(opt, "EX"):
				unit = unitEXsec
			case eqFold(opt, "PX"):
				unit = unitPXms
			case eqFold(opt, "EXAT"):
				unit = unitEXat
			default:
				unit = unitPXat
			}
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	// Compute the absolute deadline before touching the key, so a bad expire time errors
	// without having written anything.
	var atMs int64
	if unit != unitNone {
		var ok bool
		if atMs, ok = c.expiryDeadline(unit, timeArg); !ok {
			c.writeErr("ERR invalid expire time in 'set' command")
			return
		}
	}

	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	// Reap an expired key under the lock so NX/XX and GET see it as absent.
	if c.srv.volatile.Load() != 0 {
		if at, ok := c.getExpiry(key); ok && at <= c.nowMs {
			c.dropKeyLocked(key)
		}
	}
	kt := c.resolveType(key)
	// With GET, the old value must be a string; any other existing type is WRONGTYPE and
	// nothing is written, exactly as Redis checks before applying NX/XX.
	if flags&setGet != 0 && kt != keyMissing && kt != keyString {
		c.writeErr(wrongType)
		return
	}
	exists := kt != keyMissing

	// Capture the old value for the GET reply before the write overwrites it.
	var oldVal []byte
	var haveOld bool
	if flags&setGet != 0 && kt == keyString {
		oldVal, haveOld = c.srv.store.Get(key, c.vbuf[:0])
		c.vbuf = oldVal
	}

	// The NX/XX guard decides whether the write happens; GET still returns the old value.
	if (flags&setNX != 0 && exists) || (flags&setXX != 0 && !exists) {
		if flags&setGet != 0 {
			c.replyOldValue(oldVal, haveOld)
		} else {
			c.writeNil()
		}
		return
	}

	if err := c.srv.store.Set(key, val); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	// Apply the TTL: an explicit expiry sets it (a past deadline is stored and reaped on the
	// next touch), KEEPTTL leaves the existing expire row alone, and neither present clears it.
	switch {
	case unit != unitNone:
		c.setExpiryLocked(key, atMs)
	case flags&setKeepTTL == 0:
		if c.srv.volatile.Load() != 0 {
			c.clearExpiryLocked(key)
		}
	}

	if flags&setGet != 0 {
		c.replyOldValue(oldVal, haveOld)
		return
	}
	c.writeSimple("OK")
}

// replyOldValue writes the SET/GETEX GET reply: the captured old string value, or a nil bulk
// when the key was absent.
func (c *connState) replyOldValue(oldVal []byte, haveOld bool) {
	if haveOld {
		c.writeBulk(oldVal)
		return
	}
	c.writeNil()
}

// expiryDeadline folds an EX/PX/EXAT/PXAT (unit, value) pair into an absolute unix-ms
// deadline, reporting false for a non-positive value or an overflow. The relative units add
// the batch's cached now; the *AT units are absolute already. This is the shared time
// arithmetic behind SET and GETEX, matching Redis's getExpireMillisecondsOrReply: the raw
// argument must be strictly positive in every unit.
func (c *connState) expiryDeadline(unit int, n int64) (int64, bool) {
	if n <= 0 {
		return 0, false
	}
	switch unit {
	case unitEXsec:
		ms, ok := secToMs(n)
		if !ok {
			return 0, false
		}
		return addOverflow(c.nowMs, ms)
	case unitPXms:
		return addOverflow(c.nowMs, n)
	case unitEXat:
		return secToMs(n)
	default: // unitPXat
		return n, true
	}
}

// cmdGetEx implements GETEX: read a key's value and optionally change its TTL in the same
// step (spec 11 section 2.5). With no option it is a pure read that leaves the TTL untouched;
// EX/PX/EXAT/PXAT set a new expiry and PERSIST removes one. It never writes the value, so a
// missing key is a nil reply and no key is created.
func (c *connState) cmdGetEx(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'getex' command")
		return
	}
	key := argv[1]
	var unit int
	var timeArg int64
	persist := false
	for i := 2; i < len(argv); i++ {
		opt := argv[i]
		switch {
		case eqFold(opt, "PERSIST"):
			if unit != unitNone || persist {
				c.writeErr("ERR syntax error")
				return
			}
			persist = true
		case eqFold(opt, "EX"), eqFold(opt, "PX"), eqFold(opt, "EXAT"), eqFold(opt, "PXAT"):
			if unit != unitNone || persist {
				c.writeErr("ERR syntax error")
				return
			}
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, err := atoi64(argv[i+1])
			if err != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			i++
			timeArg = n
			switch {
			case eqFold(opt, "EX"):
				unit = unitEXsec
			case eqFold(opt, "PX"):
				unit = unitPXms
			case eqFold(opt, "EXAT"):
				unit = unitEXat
			default:
				unit = unitPXat
			}
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	var atMs int64
	if unit != unitNone {
		var ok bool
		if atMs, ok = c.expiryDeadline(unit, timeArg); !ok {
			c.writeErr("ERR invalid expire time in 'getex' command")
			return
		}
	}

	c.expireIfNeeded(key)
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	kt := c.resolveType(key)
	if kt == keyMissing {
		c.writeNil()
		return
	}
	if kt != keyString {
		c.writeErr(wrongType)
		return
	}
	v, _ := c.srv.store.Get(key, c.vbuf[:0])
	c.vbuf = v
	// Apply the TTL change, if any, then reply with the value. A no-option GETEX leaves the
	// expiry as it is.
	switch {
	case unit != unitNone:
		if atMs <= c.nowMs {
			// A past deadline deletes the key now, the same as SET/EXPIRE to the past. The
			// value already captured above is still what GETEX returns.
			c.dropKeyLocked(key)
		} else {
			c.setExpiryLocked(key, atMs)
		}
	case persist:
		if c.srv.volatile.Load() != 0 {
			c.clearExpiryLocked(key)
		}
	}
	c.writeBulk(v)
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
	c.expireIfNeeded(key)
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
	// A key that carries a TTL owns an expire sibling row; drop it alongside the value so
	// DEL, UNLINK, and the expiry reap all release the volatile counter. Gated on the
	// counter so a TTL-free keyspace never probes for a row that cannot exist.
	if c.srv.volatile.Load() != 0 {
		c.clearExpiryLocked(key)
	}
	switch c.resolveType(key) {
	case keyString:
		return c.srv.store.Delete(key)
	case keyHash:
		// Clear any field-TTL sibling rows and the per-hash TTL hint before the field rows go,
		// since the sweep finds the TTL rows by scanning the still-present field prefix. Gated,
		// so a TTL-free hash pays only one atomic load. The prefix is rebuilt after because the
		// sweep and dropCollIndex both borrow the shared pbuf.
		if c.srv.hfe.Load() != 0 {
			c.dropHashFieldTTLsLocked(key, c.hashPrefix(key))
		}
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
		c.expireIfNeeded(k)
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
	// In subscribe context (RESP2) PING replies with a two-element array ["pong", <arg-or-empty>]
	// rather than +PONG, so a subscribed client can tell the reply apart from a pushed message
	// frame. Outside subscribe context PING is the plain +PONG or a bulk echo of its argument.
	if c.psMode {
		c.writeArrayHeader(2)
		c.writeBulk([]byte("pong"))
		if len(argv) >= 2 {
			c.writeBulk(argv[1])
		} else {
			c.writeBulk(nil)
		}
		return
	}
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

// expire condition flags parsed off the EXPIRE family's optional trailing argument.
const (
	expNX = 1 << iota // set only if the key has no current expiry
	expXX             // set only if the key has a current expiry
	expGT             // set only if the new expiry is greater than the current (extend only)
	expLT             // set only if the new expiry is less than the current (shorten only)
)

// cmdExpire implements EXPIRE, PEXPIRE, EXPIREAT, and PEXPIREAT with the NX/XX/GT/LT
// conditions (spec 11 section 2.1-2.2). All four compute an absolute deadline in unix
// milliseconds and store it on the key's expire row; they differ only in how the client
// expresses the time. PEXPIREAT is the primitive and the other three are arithmetic in
// front of it. A computed deadline already in the past deletes the key immediately and
// still returns 1, matching Redis's lazy-delete-on-set.
func (c *connState) cmdExpire(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	n, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	// Parse the optional trailing NX/XX/GT/LT conditions. Redis accepts them in any order
	// and any count (a repeat folds to the same bit and is harmless), then rejects the two
	// incompatible pairings: NX with any of XX/GT/LT, and GT with LT. An unrecognized option
	// is reported before the compatibility check, matching Redis's left-to-right parse.
	var flags int
	for _, opt := range argv[3:] {
		switch {
		case eqFold(opt, "NX"):
			flags |= expNX
		case eqFold(opt, "XX"):
			flags |= expXX
		case eqFold(opt, "GT"):
			flags |= expGT
		case eqFold(opt, "LT"):
			flags |= expLT
		default:
			c.writeErr("ERR Unsupported option " + string(opt))
			return
		}
	}
	if flags&expNX != 0 && flags&(expXX|expGT|expLT) != 0 {
		c.writeErr("ERR NX and XX, GT or LT options at the same time are not compatible")
		return
	}
	if flags&expGT != 0 && flags&expLT != 0 {
		c.writeErr("ERR GT and LT options at the same time are not compatible")
		return
	}
	// Fold the client's time expression into an absolute deadline in ms. The *AT forms are
	// absolute already; the relative forms add to the batch's cached now. cmd is the verb.
	cmd := argv[0]
	var atMs int64
	var ok bool
	switch {
	case eqFold(cmd, "EXPIRE"):
		var ms int64
		if ms, ok = secToMs(n); ok {
			atMs, ok = addOverflow(c.nowMs, ms)
		}
	case eqFold(cmd, "PEXPIRE"):
		atMs, ok = addOverflow(c.nowMs, n)
	case eqFold(cmd, "EXPIREAT"):
		atMs, ok = secToMs(n)
	default: // PEXPIREAT
		atMs, ok = n, true
	}
	if !ok {
		c.writeErr("ERR invalid expire time in '" + string(cmd) + "' command")
		return
	}

	key := argv[1]
	// A key already past its TTL is reaped first, so EXPIRE on it reports 0 (nothing there
	// to set an expiry on), matching a missing key.
	c.expireIfNeeded(key)
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	if c.resolveType(key) == keyMissing {
		c.writeInt(0)
		return
	}
	cur, hasCur := c.getExpiry(key)
	if !c.expireConditionMet(flags, atMs, cur, hasCur) {
		c.writeInt(0)
		return
	}
	if atMs <= c.nowMs {
		// The deadline is in the past: delete now rather than store a dead expiry, and still
		// report 1 because the TTL was accepted (setting it to the past means delete now).
		c.dropKeyLocked(key)
		c.writeInt(1)
		return
	}
	c.setExpiryLocked(key, atMs)
	c.writeInt(1)
}

// expireConditionMet evaluates the NX/XX/GT/LT guard against the current expiry. The
// flags that survive validation are NX alone, or any AND-combination of XX/GT/LT, so this
// evaluates each present bit as a conjunct. A key with no current expiry is treated as
// infinite for GT/LT: GT never fires against infinity (no finite time is greater), LT
// always fires (any finite time is less). With no flag the write always happens.
func (c *connState) expireConditionMet(flags int, newAt, cur int64, hasCur bool) bool {
	if flags&expNX != 0 {
		// NX is exclusive of the others (validated in cmdExpire): set only if no current expiry.
		return !hasCur
	}
	if flags&expXX != 0 && !hasCur {
		return false
	}
	if flags&expGT != 0 && (!hasCur || newAt <= cur) {
		return false
	}
	if flags&expLT != 0 && hasCur && newAt >= cur {
		return false
	}
	return true
}

// cmdTTL implements TTL and PTTL: remaining time to live, -1 when the key exists with no
// expiry, -2 when the key is missing or already expired (spec 11 section 2.3). TTL rounds
// to the nearest second the Redis way, adding 500 ms before the integer-second divide.
func (c *connState) cmdTTL(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	key := argv[1]
	c.expireIfNeeded(key)
	if c.resolveType(key) == keyMissing {
		c.writeInt(-2)
		return
	}
	at, ok := c.getExpiry(key)
	if !ok {
		c.writeInt(-1)
		return
	}
	ms := at - c.nowMs
	if ms < 0 {
		ms = 0
	}
	if eqFold(argv[0], "PTTL") {
		c.writeInt(ms)
		return
	}
	c.writeInt((ms + 500) / 1000)
}

// cmdExpireTime implements EXPIRETIME and PEXPIRETIME: the absolute deadline as unix
// seconds or unix milliseconds, -1 with no expiry, -2 missing (spec 11 section 2.3).
// These read the stored field directly, so they are the cheapest of the TTL readers.
func (c *connState) cmdExpireTime(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	key := argv[1]
	c.expireIfNeeded(key)
	if c.resolveType(key) == keyMissing {
		c.writeInt(-2)
		return
	}
	at, ok := c.getExpiry(key)
	if !ok {
		c.writeInt(-1)
		return
	}
	if eqFold(argv[0], "PEXPIRETIME") {
		c.writeInt(at)
		return
	}
	c.writeInt(at / 1000)
}

// cmdPersist removes a key's expiry, making it immortal again. It returns 1 if an expiry
// was removed, 0 if the key has no expiry or does not exist (spec 11 section 2.4).
func (c *connState) cmdPersist(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	key := argv[1]
	c.expireIfNeeded(key)
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	if c.resolveType(key) == keyMissing {
		c.writeInt(0)
		return
	}
	if c.clearExpiryLocked(key) {
		c.writeInt(1)
		return
	}
	c.writeInt(0)
}

// secToMs converts a seconds count to milliseconds and reports whether the multiply fit
// in int64, so an EXPIRE or EXPIREAT with an absurd seconds value errors instead of
// wrapping to a bogus deadline.
func secToMs(sec int64) (int64, bool) {
	ms := sec * 1000
	if sec != 0 && ms/1000 != sec {
		return 0, false
	}
	return ms, true
}

// addOverflow returns a+b and whether it overflowed int64, so a relative EXPIRE that
// would push the absolute deadline past the representable range errors instead of
// wrapping to a past time.
func addOverflow(a, b int64) (int64, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, false
	}
	return s, true
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
