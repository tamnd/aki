package command

// reactor_block.go satisfies networking.BlockProber so the epoll reactor can keep
// a possibly-blocking command off an event loop (Spec/2064/reactor, task #101). A
// loop services many connections inline and must never park on one, so before a
// command runs the loop asks whether it might block; if so the connection is
// handed to a dedicated goroutine that runs the existing blocking machinery.

// MayBlock reports whether the command in argv could park the connection. It is
// deliberately conservative: it approves the whole blocking family by command
// flag, plus WAIT and WAITAOF, which park without carrying the blocking flag. A
// false positive only costs a goroutine on a rare command; the hot GET/SET path
// is rejected on the first byte before any table lookup, so it pays one compare.
func (d *Dispatcher) MayBlock(argv [][]byte) bool {
	if len(argv) == 0 || len(argv[0]) == 0 {
		return false
	}
	// Every blocking command name starts with one of these letters (b: BLPOP and
	// the rest, x: XREAD/XREADGROUP, w: WAIT/WAITAOF). Anything else, including the
	// hot GET/SET/INCR path, rejects here without hashing or a table lookup.
	switch argv[0][0] | 0x20 {
	case 'b', 'x', 'w':
	default:
		return false
	}
	cmd := d.table.byName[cmdKey(argv[0])]
	if cmd == nil {
		return false
	}
	if cmd.Flags.Has(FlagBlocking) {
		return true
	}
	return cmd.Name == "wait" || cmd.Name == "waitaof"
}
