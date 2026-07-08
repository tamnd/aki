package f2srv

import "github.com/tamnd/aki/engine/f2raw"

// dispatch routes one parsed command. The verb is matched case-insensitively on
// its first bytes, the same cheap dispatch a benchmark's tiny command set needs.
// Only the string point path and the handful of handshake commands a benchmark
// client sends are handled; anything else answers with an error so an accidental
// unsupported command in a run is loud rather than silently wrong.
func (c *connState) dispatch(argv [][]byte) {
	cmd := argv[0]
	switch {
	case eqFold(cmd, "GET"):
		c.cmdGet(argv)
	case eqFold(cmd, "SET"):
		c.cmdSet(argv)
	case eqFold(cmd, "INCR"):
		c.cmdIncrBy(argv[1:], 1, 1)
	case eqFold(cmd, "DECR"):
		c.cmdIncrBy(argv[1:], -1, 1)
	case eqFold(cmd, "INCRBY"):
		c.cmdIncrBy(argv[1:], 1, 2)
	case eqFold(cmd, "DECRBY"):
		c.cmdIncrBy(argv[1:], -1, 2)
	case eqFold(cmd, "DEL"), eqFold(cmd, "UNLINK"):
		c.cmdDel(argv)
	case eqFold(cmd, "PING"):
		if len(argv) >= 2 {
			c.writeBulk(argv[1])
		} else {
			c.writeSimple("PONG")
		}
	case eqFold(cmd, "ECHO"):
		if len(argv) >= 2 {
			c.writeBulk(argv[1])
		} else {
			c.writeErr("ERR wrong number of arguments for 'echo' command")
		}
	case eqFold(cmd, "DBSIZE"):
		c.writeInt(int64(c.srv.store.Len()))
	case eqFold(cmd, "COMMAND"):
		// redis-benchmark issues COMMAND DOCS at startup and tolerates any reply;
		// an empty array keeps it happy without a command table.
		c.writeArrayHeader(0)
	case eqFold(cmd, "CONFIG"):
		// redis-benchmark may probe CONFIG GET save/maxmemory; an empty map reply
		// is a valid answer it accepts.
		c.writeArrayHeader(0)
	case eqFold(cmd, "HELLO"):
		// A minimal RESP2 HELLO reply so a client that opens with HELLO does not
		// abort. It is a 7-field map; RESP2 encodes a map as a flat array.
		c.cmdHello()
	case eqFold(cmd, "SELECT"):
		c.writeSimple("OK")
	case eqFold(cmd, "QUIT"):
		c.writeSimple("OK")
		c.wantClose = true
	default:
		c.writeErr("ERR unknown command '" + string(cmd) + "'")
	}
}

// cmdGet copies the value into the reused vbuf and writes it, or a null bulk when
// the key is absent.
func (c *connState) cmdGet(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'get' command")
		return
	}
	v, ok := c.srv.store.Get(argv[1], c.vbuf)
	c.vbuf = v[:0]
	if !ok {
		c.writeNil()
		return
	}
	c.writeBulk(v)
}

// cmdSet stores the value and replies +OK. Only the bare three-argument form is
// handled; options (EX/NX/...) are outside the base measurement path.
func (c *connState) cmdSet(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'set' command")
		return
	}
	if err := c.srv.store.Set(argv[1], argv[2]); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeSimple("OK")
}

// cmdIncrBy handles INCR/DECR/INCRBY/DECRBY. sign is +1 or -1; nargs is the
// expected argument count after the verb (1 for INCR/DECR, 2 for INCRBY/DECRBY).
func (c *connState) cmdIncrBy(args [][]byte, sign int64, nargs int) {
	if len(args) != nargs {
		c.writeErr("ERR wrong number of arguments")
		return
	}
	delta := sign
	if nargs == 2 {
		n, ok := parseI64(args[1])
		if !ok {
			c.writeErr("ERR value is not an integer or out of range")
			return
		}
		delta = sign * n
	}
	res, err := c.srv.store.Incr(args[0], delta)
	if err != nil {
		if err == f2raw.ErrNotInt {
			c.writeErr("ERR value is not an integer or out of range")
		} else {
			c.writeErr("ERR " + err.Error())
		}
		return
	}
	c.writeInt(res)
}

// cmdDel deletes one or more keys and replies with the count removed.
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

// cmdHello writes a minimal RESP2 HELLO reply (a flat array of key/value pairs).
func (c *connState) cmdHello() {
	c.writeArrayHeader(14)
	c.writeBulk([]byte("server"))
	c.writeBulk([]byte("f2srv"))
	c.writeBulk([]byte("version"))
	c.writeBulk([]byte("0.0.1"))
	c.writeBulk([]byte("proto"))
	c.writeInt(2)
	c.writeBulk([]byte("id"))
	c.writeInt(c.id)
	c.writeBulk([]byte("mode"))
	c.writeBulk([]byte("standalone"))
	c.writeBulk([]byte("role"))
	c.writeBulk([]byte("master"))
	c.writeBulk([]byte("modules"))
	c.writeArrayHeader(0)
}

// eqFold reports whether b equals s ignoring ASCII case. s is an uppercase literal,
// so the compare folds b's lowercase bytes up.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		x := b[i]
		if x >= 'a' && x <= 'z' {
			x -= 32
		}
		if x != s[i] {
			return false
		}
	}
	return true
}

// parseI64 parses a base-10 signed integer without allocating.
func parseI64(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	i := 0
	neg := false
	if b[0] == '-' || b[0] == '+' {
		neg = b[0] == '-'
		i++
		if i == len(b) {
			return 0, false
		}
	}
	var n int64
	for ; i < len(b); i++ {
		d := b[i]
		if d < '0' || d > '9' {
			return 0, false
		}
		n = n*10 + int64(d-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}
