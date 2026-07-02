package f1srv

import (
	"strconv"
	"time"
)

// The connection-introspection commands TIME, ROLE, and AUTH round out the handshake surface a
// client library probes on connect. None of them touch the keyspace: TIME reads the wall clock,
// ROLE reports this server's replication role, and AUTH answers the way a server with no password
// configured must. They are grouped here because they share nothing with a data type and only
// exist to make a standard client's connect sequence succeed byte-for-byte against Redis.

// cmdTime implements TIME, replying with a two-element array of the current unix time split into
// whole seconds and the leftover microseconds, each as a bulk string. It reads time.Now directly
// rather than the batch-cached nowMs so the microsecond field carries real sub-millisecond
// resolution, the same as Redis calling gettimeofday. TIME takes no arguments.
func (c *connState) cmdTime(argv [][]byte) {
	if len(argv) != 1 {
		c.writeErr("ERR wrong number of arguments for 'time' command")
		return
	}
	now := time.Now()
	c.writeArrayHeader(2)
	c.writeBulk([]byte(strconv.FormatInt(now.Unix(), 10)))
	c.writeBulk([]byte(strconv.FormatInt(int64(now.Nanosecond()/1000), 10)))
}

// cmdRole implements ROLE for a standalone master, replying with the three-element array
// ["master", <replication offset>, <replicas>]. This server takes no replicas and keeps no
// replication backlog, so the offset is 0 and the replica list is empty, matching what a fresh
// Redis master reports before any write. ROLE takes no arguments.
func (c *connState) cmdRole(argv [][]byte) {
	if len(argv) != 1 {
		c.writeErr("ERR wrong number of arguments for 'role' command")
		return
	}
	c.writeArrayHeader(3)
	c.writeBulk([]byte("master"))
	c.writeInt(0)
	c.writeArrayHeader(0)
}

// cmdAuth implements AUTH the way a server with no password configured must. With no requirepass
// and no ACL users, every credential fails, so the reply depends only on the argument count:
// AUTH with a single password reports that no password is configured, AUTH with a username and
// password reports the pair as invalid, and any other count is the usual arity or syntax error.
// The two error strings are byte-for-byte what Redis returns so a client that probes AUTH on
// connect sees exactly the responses it expects.
func (c *connState) cmdAuth(argv [][]byte) {
	switch len(argv) {
	case 2:
		c.writeErr("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	case 3:
		c.writeErr("WRONGPASS invalid username-password pair or user is disabled.")
	case 1:
		c.writeErr("ERR wrong number of arguments for 'auth' command")
	default:
		c.writeErr("ERR syntax error")
	}
}
