package drivers

import "testing"

// TestScriptingDeferred checks the F18 scripting boundary (spec 2064/f3/17
// section 17): every scripting verb answers the one plain unsupported-command
// error, named for the verb, and never a NOSCRIPT or an unknown-command reply.
// The error class is what stops a client library from entering a load-and-retry
// livelock, so the wording is a compat contract the conformance harness greps.
func TestScriptingDeferred(t *testing.T) {
	_, nc, br := startServer(t)

	cases := []struct {
		args []string
		verb string
	}{
		{[]string{"EVAL", "return 1", "0"}, "EVAL"},
		{[]string{"EVALSHA", "abc", "0"}, "EVALSHA"},
		{[]string{"EVAL_RO", "return 1", "0"}, "EVAL_RO"},
		{[]string{"EVALSHA_RO", "abc", "0"}, "EVALSHA_RO"},
		{[]string{"FCALL", "f", "0"}, "FCALL"},
		{[]string{"FCALL_RO", "f", "0"}, "FCALL_RO"},
		{[]string{"SCRIPT", "LOAD", "return 1"}, "SCRIPT"},
		{[]string{"SCRIPT", "EXISTS", "abc"}, "SCRIPT"},
		{[]string{"SCRIPT", "FLUSH"}, "SCRIPT"},
		{[]string{"FUNCTION", "LIST"}, "FUNCTION"},
		{[]string{"FUNCTION", "DUMP"}, "FUNCTION"},
	}
	for _, c := range cases {
		send(t, nc, c.args...)
		want := "-ERR unsupported command '" + c.verb + "' (scripting is not available in this build)\r\n"
		expect(t, br, want)
	}
}

// TestScriptingDeferredBareAndLower checks two edges: a bare verb with no
// arguments still gets the deferral error rather than an arity error, and a
// lowercase invocation resolves to the same canonical-name error, since the verb
// name in the message is the command's own, not the bytes the client sent.
func TestScriptingDeferredBareAndLower(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "EVAL")
	expect(t, br, "-ERR unsupported command 'EVAL' (scripting is not available in this build)\r\n")

	send(t, nc, "eval", "return 1", "0")
	expect(t, br, "-ERR unsupported command 'EVAL' (scripting is not available in this build)\r\n")

	send(t, nc, "script", "load", "return 1")
	expect(t, br, "-ERR unsupported command 'SCRIPT' (scripting is not available in this build)\r\n")
}
