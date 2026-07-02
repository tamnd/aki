package f1srv

import "testing"

// CONFIG GET of a single known parameter returns a two-element array of the name and its value, the
// shape a client parses. The reply is normalized to "*2" by readReply, so the two bulk elements are
// read explicitly.
func TestConfigGetSingle(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CONFIG", "GET", "maxmemory")
	expect(t, rw, "*2")
	expect(t, rw, "$maxmemory")
	expect(t, rw, "$0")

	cmd(t, rw, "CONFIG", "GET", "maxmemory-policy")
	expect(t, rw, "*2")
	expect(t, rw, "$maxmemory-policy")
	expect(t, rw, "$noeviction")

	// The match is case-insensitive, the way Redis treats config names. An exact-literal request
	// echoes the requested spelling back as the reply name, matching Redis, so APPENDONLY answers
	// with APPENDONLY rather than the canonical lowercase.
	cmd(t, rw, "CONFIG", "GET", "APPENDONLY")
	expect(t, rw, "*2")
	expect(t, rw, "$APPENDONLY")
	expect(t, rw, "$no")

	// A parameter with an empty default still returns the pair, with an empty-bulk value.
	cmd(t, rw, "CONFIG", "GET", "save")
	expect(t, rw, "*2")
	expect(t, rw, "$save")
	expect(t, rw, "$")
}

// A glob pattern matches every parameter whose name fits it, and a parameter absent from the table
// contributes nothing, so an unknown name is the empty array.
func TestConfigGetGlobAndUnknown(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// maxmemory* matches maxmemory, maxmemory-policy, maxmemory-samples, maxmemory-clients: four
	// pairs, eight elements.
	cmd(t, rw, "CONFIG", "GET", "maxmemory*")
	expect(t, rw, "*8")
	for i := 0; i < 8; i++ {
		readReply(t, rw)
	}

	cmd(t, rw, "CONFIG", "GET", "no-such-parameter")
	expect(t, rw, "*0")

	cmd(t, rw, "CONFIG", "GET")
	expect(t, rw, "-ERR wrong number of arguments for 'config|get' command")
}

// CONFIG SET takes one or more directive/value pairs and reports OK without storing anything. Fewer
// than a pair is the wrong-args error, and an odd trailing token is the syntax error.
func TestConfigSet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CONFIG", "SET", "maxmemory", "0")
	expect(t, rw, "+OK")
	cmd(t, rw, "CONFIG", "SET", "maxmemory", "0", "timeout", "0")
	expect(t, rw, "+OK")
	cmd(t, rw, "CONFIG", "SET", "maxmemory", "0", "timeout")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "CONFIG", "SET", "foo")
	expect(t, rw, "-ERR wrong number of arguments for 'config|set' command")
	cmd(t, rw, "CONFIG", "SET")
	expect(t, rw, "-ERR wrong number of arguments for 'config|set' command")
}

// RESETSTAT is OK, REWRITE reports there is no config file, HELP returns the subcommand array, and
// an unknown subcommand or a bare CONFIG is the matching error.
func TestConfigMiscAndErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CONFIG", "RESETSTAT")
	expect(t, rw, "+OK")
	cmd(t, rw, "CONFIG", "REWRITE")
	expect(t, rw, "-ERR The server is running without a config file")

	cmd(t, rw, "CONFIG", "HELP")
	expect(t, rw, "*"+itoa(len(configHelp)))
	for range configHelp {
		readReply(t, rw)
	}

	cmd(t, rw, "CONFIG", "BADSUB")
	expect(t, rw, "-ERR unknown subcommand 'BADSUB'. Try CONFIG HELP.")
	cmd(t, rw, "CONFIG")
	expect(t, rw, "-ERR wrong number of arguments for 'config' command")
}
