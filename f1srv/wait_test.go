package f1srv

import "testing"

// TestWait reports zero acknowledgements on a single instance and validates its arguments.
func TestWait(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "WAIT", "0", "100")
	expect(t, rw, ":0")
	// A request for replicas that can never arrive still resolves to zero, not a hang.
	cmd(t, rw, "WAIT", "1", "50")
	expect(t, rw, ":0")
	// A negative replica count is accepted the way Redis accepts it.
	cmd(t, rw, "WAIT", "-1", "0")
	expect(t, rw, ":0")

	cmd(t, rw, "WAIT", "0")
	expect(t, rw, "-ERR wrong number of arguments for 'wait' command")
	cmd(t, rw, "WAIT", "foo", "0")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "WAIT", "0", "-1")
	expect(t, rw, "-ERR timeout is negative")
}

// TestWaitAOF reports zero local fsyncs and zero replica acknowledgements as a two-integer array,
// errors when a local fsync is requested with the append-only file disabled, and validates ranges.
func TestWaitAOF(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "WAITAOF", "0", "0", "0")
	if h := readReply(t, rw); h != "*2" {
		t.Fatalf("WAITAOF header = %q, want *2", h)
	}
	if v := readReply(t, rw); v != ":0" {
		t.Fatalf("WAITAOF[0] = %q, want :0", v)
	}
	if v := readReply(t, rw); v != ":0" {
		t.Fatalf("WAITAOF[1] = %q, want :0", v)
	}

	// A local fsync request with the append-only file off is an error.
	cmd(t, rw, "WAITAOF", "1", "0", "0")
	expect(t, rw, "-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
	// numlocal is limited to 0 or 1.
	cmd(t, rw, "WAITAOF", "2", "0", "0")
	expect(t, rw, "-ERR value is out of range, value must between 0 and 1")
	cmd(t, rw, "WAITAOF", "-1", "0", "0")
	expect(t, rw, "-ERR value is out of range, value must between 0 and 1")
	// numreplicas cannot be negative.
	cmd(t, rw, "WAITAOF", "0", "-1", "0")
	expect(t, rw, "-ERR value is out of range, must be positive")
	// a negative timeout is an error.
	cmd(t, rw, "WAITAOF", "0", "0", "-1")
	expect(t, rw, "-ERR timeout is negative")
	// arity and integer checks.
	cmd(t, rw, "WAITAOF", "0", "0")
	expect(t, rw, "-ERR wrong number of arguments for 'waitaof' command")
	cmd(t, rw, "WAITAOF", "x", "0", "0")
	expect(t, rw, "-ERR value is not an integer or out of range")
}
