package dispatch

import "testing"

// IsWrite is the classifier CLIENT PAUSE WRITE consults (markWrites): a write is
// held under a WRITE-mode pause, a read flows. The contract worth locking is that
// the obvious writes across every type read true, the obvious reads read false,
// and the read/write pairs that only differ by whether they set or report a TTL
// land on the right side, since that is the easy one to get wrong.
func TestIsWrite(t *testing.T) {
	writes := []string{
		"SET", "SETNX", "APPEND", "INCR", "GETDEL", "GETEX", "MSET",
		"SETBIT", "BITFIELD", "BITOP",
		"DEL", "EXPIRE", "PERSIST", "RENAME", "COPY", "RESTORE", "SORT", "FLUSHALL",
		"HSET", "HDEL", "HEXPIRE", "HGETEX",
		"LPUSH", "LPOP", "LMOVE", "BLPOP",
		"SADD", "SPOP", "SINTERSTORE",
		"ZADD", "ZPOPMIN", "ZRANGESTORE", "BZPOPMIN",
		"XADD", "XDEL", "XGROUP", "XREADGROUP", "XACK",
		"GEOADD", "GEOSEARCHSTORE", "GEORADIUS",
		"PFADD", "PFMERGE", "PFDEBUG",
	}
	for _, name := range writes {
		if !IsWrite([]byte(name)) {
			t.Errorf("IsWrite(%q) = false, want true", name)
		}
		// The lookup uppercases the verb the way Dispatch does, so the lower-case
		// form a client actually sends must classify the same.
		if !IsWrite([]byte(lower(name))) {
			t.Errorf("IsWrite(%q lowercased) = false, want true", name)
		}
	}

	reads := []string{
		"GET", "STRLEN", "GETRANGE", "GETBIT", "BITCOUNT", "BITFIELD_RO",
		"TTL", "PTTL", "EXPIRETIME", "TYPE", "EXISTS", "DUMP", "KEYS", "SCAN",
		"HGET", "HGETALL", "HTTL", "HEXPIRETIME",
		"LLEN", "LRANGE", "LINDEX",
		"SCARD", "SMEMBERS", "SISMEMBER", "SINTERCARD",
		"ZSCORE", "ZRANGE", "ZCARD", "ZINTERCARD",
		"XLEN", "XRANGE", "XREAD",
		"GEOPOS", "GEODIST", "GEORADIUS_RO",
		"PFCOUNT",
		// Not a data verb at all.
		"PING", "INFO", "COMMAND", "CONFIG",
	}
	for _, name := range reads {
		if IsWrite([]byte(name)) {
			t.Errorf("IsWrite(%q) = true, want false", name)
		}
	}

	// An unknown or empty verb is not a write, so a WRITE pause never holds a
	// command it cannot classify.
	if IsWrite([]byte("NONESUCH")) {
		t.Errorf("IsWrite(NONESUCH) = true, want false")
	}
	if IsWrite(nil) {
		t.Errorf("IsWrite(nil) = true, want false")
	}
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 32
		}
	}
	return string(b)
}
