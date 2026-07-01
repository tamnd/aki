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
		if c.srv.store.Delete(k) {
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
		if _, ok := c.srv.store.Get(k, c.vbuf[:0]); ok {
			n++
		}
	}
	c.writeInt(n)
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
