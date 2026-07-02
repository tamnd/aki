package f1srv

import "testing"

// COMMAND COUNT returns the number of commands in the table as an integer, not the empty array the
// old blanket produced. A client that parses an integer here would desync on an array.
func TestCommandCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COMMAND", "COUNT")
	expect(t, rw, ":"+itoa(len(cmdTable)))
}

// COMMAND INFO of a known command returns its 10-element spec: name, arity, flags, the key triple,
// ACL categories, then empty tips, key-specs, and subcommands. The name is lowercase, matching Redis.
func TestCommandInfoKnown(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COMMAND", "INFO", "GET")
	expect(t, rw, "*1")  // one requested name
	expect(t, rw, "*10") // the spec array
	expect(t, rw, "$get")
	expect(t, rw, ":2")
	expect(t, rw, "*2") // flags
	expect(t, rw, "+readonly")
	expect(t, rw, "+fast")
	expect(t, rw, ":1") // firstkey
	expect(t, rw, ":1") // lastkey
	expect(t, rw, ":1") // keystep
	expect(t, rw, "*3") // acl categories
	expect(t, rw, "+@read")
	expect(t, rw, "+@string")
	expect(t, rw, "+@fast")
	expect(t, rw, "*0") // tips
	expect(t, rw, "*0") // key-specs
	expect(t, rw, "*0") // subcommands
}

// COMMAND INFO of an unknown command returns a nil element in the array, the way Redis reports a name
// it does not know.
func TestCommandInfoUnknown(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COMMAND", "INFO", "NOSUCHCOMMAND")
	expect(t, rw, "*1")
	expect(t, rw, "*-1")
}

// COMMAND GETKEYS extracts the keys of a full command line for each getkeys shape: a plain range, a
// multi-key range, a keynum-counted list, a keynum behind a destination, a STORE keyword, and the
// STREAMS split. These are the cases Redis and Valkey agree on.
func TestCommandGetKeys(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cases := []struct {
		args []string
		keys []string
	}{
		{[]string{"SET", "foo", "bar"}, []string{"foo"}},
		{[]string{"GET", "k"}, []string{"k"}},
		{[]string{"MSET", "a", "1", "b", "2"}, []string{"a", "b"}},
		{[]string{"MGET", "a", "b", "c"}, []string{"a", "b", "c"}},
		{[]string{"DEL", "x", "y"}, []string{"x", "y"}},
		{[]string{"COPY", "src", "dst"}, []string{"src", "dst"}},
		{[]string{"ZADD", "z", "1", "m"}, []string{"z"}},
		{[]string{"LMPOP", "2", "k1", "k2", "LEFT"}, []string{"k1", "k2"}},
		{[]string{"ZUNIONSTORE", "dst", "2", "a", "b"}, []string{"dst", "a", "b"}},
		{[]string{"SORT", "mylist", "STORE", "out"}, []string{"mylist", "out"}},
		{[]string{"GEORADIUS", "src", "0", "0", "1", "m", "STORE", "d"}, []string{"src", "d"}},
		{[]string{"XREAD", "COUNT", "2", "STREAMS", "s1", "s2", "0", "0"}, []string{"s1", "s2"}},
	}
	for _, tc := range cases {
		args := append([]string{"COMMAND", "GETKEYS"}, tc.args...)
		cmd(t, rw, args...)
		expect(t, rw, "*"+itoa(len(tc.keys)))
		for _, k := range tc.keys {
			expect(t, rw, "$"+k)
		}
	}
}

// COMMAND GETKEYS reports the Redis errors for an unknown command, a command with no keys, and a bad
// arity, so a client sees the same failure it would from Redis.
func TestCommandGetKeysErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COMMAND", "GETKEYS", "NOSUCHCOMMAND", "a")
	expect(t, rw, "-ERR Invalid command specified")
	cmd(t, rw, "COMMAND", "GETKEYS", "PING")
	expect(t, rw, "-ERR The command has no key arguments")
	cmd(t, rw, "COMMAND", "GETKEYS", "GET")
	expect(t, rw, "-ERR Invalid number of arguments specified for command")
	cmd(t, rw, "COMMAND", "GETKEYS")
	expect(t, rw, "-ERR wrong number of arguments for 'command|getkeys' command")
}

// COMMAND GETKEYSANDFLAGS pairs each key with a coarse access flag from the command's write/readonly
// flag: RW for a write, RO for a read.
func TestCommandGetKeysAndFlags(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COMMAND", "GETKEYSANDFLAGS", "SET", "foo", "bar")
	expect(t, rw, "*1")
	expect(t, rw, "*2")
	expect(t, rw, "$foo")
	expect(t, rw, "*1")
	expect(t, rw, "+RW")

	cmd(t, rw, "COMMAND", "GETKEYSANDFLAGS", "GET", "k")
	expect(t, rw, "*1")
	expect(t, rw, "*2")
	expect(t, rw, "$k")
	expect(t, rw, "*1")
	expect(t, rw, "+RO")
}

// COMMAND LIST returns the flat array of every command name, and COMMAND DOCS returns an empty map,
// and an unknown subcommand is the matching error.
func TestCommandListDocsAndErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COMMAND", "LIST")
	expect(t, rw, "*"+itoa(len(cmdTable)))
	for range cmdTable {
		readReply(t, rw)
	}

	cmd(t, rw, "COMMAND", "DOCS", "GET")
	expect(t, rw, "*0")

	cmd(t, rw, "COMMAND", "BADSUB")
	expect(t, rw, "-ERR unknown subcommand 'BADSUB'. Try COMMAND HELP.")

	cmd(t, rw, "COMMAND", "HELP")
	expect(t, rw, "*"+itoa(len(commandHelp)))
	for range commandHelp {
		readReply(t, rw)
	}
}

// A bare COMMAND returns one info spec per command in the table. readReply normalizes an array to
// its header, so drainReply recurses to consume the nested flags and ACL sub-arrays of each spec.
func TestCommandBare(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COMMAND")
	expect(t, rw, "*"+itoa(len(cmdTable)))
	for range cmdTable {
		drainReply(t, rw)
	}
}
