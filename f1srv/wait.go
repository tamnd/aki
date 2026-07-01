package f1srv

// WAIT and WAITAOF block a client until a write has been acknowledged by enough replicas (WAIT) or
// flushed to enough append-only files (WAITAOF). f1srv is a single unreplicated instance with no
// append-only file, so no replica can ever acknowledge and no local AOF fsync can ever be counted:
// the answer is always zero of each. Redis returns the same zero once its own wait resolves (a
// timeout with no replicas, or the immediate no-replica case), so replying zero right away is
// byte-identical to what Redis and Valkey return; only the latency Redis would spend blocking is
// absent, and with nothing to wait for there is nothing to block on.
//
// The argument validation still matches exactly, because a bench harness that sends a malformed
// WAIT or WAITAOF must see the same error: the arities, the not-an-integer and out-of-range and
// negative-timeout errors, and WAITAOF's rule that asking for a local fsync while the append-only
// file is disabled is an error rather than an immediate zero.

// cmdWait implements WAIT numreplicas timeout. With no replicas attached it reports zero
// acknowledgements immediately, which is what Redis reports once the wait resolves.
func (c *connState) cmdWait(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'wait' command")
		return
	}
	if _, ok := parseInt64Strict(argv[1]); !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	timeout, ok := parseInt64Strict(argv[2])
	if !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	if timeout < 0 {
		c.writeErr("ERR timeout is negative")
		return
	}
	// No replicas can ever acknowledge on a single instance, so zero is the final answer.
	c.writeInt(0)
}

// cmdWaitAOF implements WAITAOF numlocal numreplicas timeout. The single-instance answer is zero
// local fsyncs and zero replica acknowledgements, reported as a two-element array. Asking for a
// local fsync (numlocal of 1) while the append-only file is disabled is an error, matching Redis.
func (c *connState) cmdWaitAOF(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'waitaof' command")
		return
	}
	numlocal, ok := parseInt64Strict(argv[1])
	if !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	numreplicas, ok := parseInt64Strict(argv[2])
	if !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	timeout, ok := parseInt64Strict(argv[3])
	if !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	// numlocal is the number of local AOFs to wait for, which can only be this instance's own,
	// so it is 0 or 1; numreplicas cannot be negative; a negative timeout is an error.
	if numlocal < 0 || numlocal > 1 {
		c.writeErr("ERR value is out of range, value must between 0 and 1")
		return
	}
	if numreplicas < 0 {
		c.writeErr("ERR value is out of range, must be positive")
		return
	}
	if timeout < 0 {
		c.writeErr("ERR timeout is negative")
		return
	}
	// With the append-only file disabled there is no local fsync to wait for, so asking for one
	// is an error rather than a wait that can never complete.
	if numlocal == 1 {
		c.writeErr("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
		return
	}
	// Zero local fsyncs (AOF off), zero replica acknowledgements (no replicas).
	c.writeArrayHeader(2)
	c.writeInt(0)
	c.writeInt(0)
}
