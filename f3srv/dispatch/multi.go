package dispatch

import (
	"hash/fnv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// MULTI/EXEC/DISCARD/WATCH/UNWATCH/RESET (spec 2064/f3/17 section on
// transactions), the last M11 command surface. A transaction is a per-connection
// queue: MULTI opens it, every following command is validated and parked with a
// +QUEUED acknowledgement instead of running, and EXEC runs the whole queue in
// one atomic step under an F17 intent transaction (intent.go) holding every key
// the queue touches, so no other client interleaves. WATCH arms optimistic
// concurrency: the watched keys' baseline is fingerprinted at WATCH, re-checked
// under the barrier at EXEC, and a changed key aborts the transaction with the
// null array. DISCARD throws the queue away, UNWATCH forgets the watches, and
// RESET clears both.
//
// The transaction state is connection-local and lives entirely on the reader
// goroutine: it is stashed in the shard.Conn as an opaque handle (Conn.TxState)
// the shard layer never inspects, and every decision here runs before the command
// reaches the shard hop. That is why txnIntercept sits at the very top of
// Dispatch, ahead of the fan/cross/point routing: MULTI has to swallow a command
// the moment it arrives, on every driver, without an owner ever seeing it. Simple
// acknowledgements (+OK, +QUEUED, +RESET) ride InlineReply so they keep their
// pipeline slot; the one EXEC reply, the array gathering every queued command's
// answer, rides DoTxn's loopback exactly like a cross-shard command's reply.

// txnState is a connection's transaction. inMulti is set between MULTI and
// EXEC/DISCARD; queued holds the parked commands; dirty records a queuing error
// (an unknown verb or a bad arity) that makes EXEC abort; watch holds the
// optimistic-lock baselines armed by WATCH, which outlive a DISCARD-less MULTI and
// are only cleared by EXEC, DISCARD, UNWATCH, or RESET. Reader goroutine only.
type txnState struct {
	inMulti bool
	dirty   bool
	queued  []queuedCmd
	watch   []watchEntry
}

// queuedCmd is one parked command: its table row and a private copy of its whole
// argument line (verb included). The copy is mandatory because EXEC runs the queue
// on a spawned coordinator goroutine, long after the reader's parse buffer has been
// reused, the same stable-copy rule dispatchCross follows.
type queuedCmd struct {
	e    *entry
	args [][]byte
}

// watchEntry is one WATCHed key's baseline: the key bytes and a fingerprint of its
// serialized value at WATCH time. exists records whether the key was present, so a
// key that is created or deleted between WATCH and EXEC counts as a change even
// when a fingerprint collision would not. The fingerprint is FNV-1a over the DUMP
// payload, which is deterministic in aki (fixed htable and skiplist iteration
// order), so an unchanged value fingerprints identically at WATCH and at EXEC.
type watchEntry struct {
	key    []byte
	fp     uint64
	exists bool
}

// txnIntercept handles the transaction verbs and, while a MULTI is open, queues
// every other command. It returns handled=true when it owned the command (the
// caller returns err straight away) and false when the command should fall through
// to normal dispatch. It runs at the top of Dispatch on the reader goroutine, so
// the whole transaction machine is driver-agnostic.
func txnIntercept(c *shard.Conn, args [][]byte) (bool, error) {
	verb := args[0]
	switch {
	case tokenIs(verb, "MULTI"):
		return true, doMulti(c, args)
	case tokenIs(verb, "EXEC"):
		return true, doExec(c, args)
	case tokenIs(verb, "DISCARD"):
		return true, doDiscard(c, args)
	case tokenIs(verb, "WATCH"):
		return true, doWatch(c, args)
	case tokenIs(verb, "UNWATCH"):
		return true, doUnwatch(c, args)
	case tokenIs(verb, "RESET"):
		// RESET clears the transaction on the drivers that reach Dispatch with it
		// (the reactor and io_uring paths); the goroutine driver intercepts RESET in
		// its network layer before Dispatch and clears the state through ResetTxn
		// there. With no transaction state to clear, RESET falls through so the plain
		// reset handler answers +RESET.
		if c.TxState() != nil {
			ResetTxn(c)
			return true, c.InlineReply(resp.AppendStatus(nil, "RESET"))
		}
		return false, nil
	}
	// Not a transaction verb: queue it when a MULTI is open, otherwise let normal
	// dispatch run it.
	ts, _ := c.TxState().(*txnState)
	if ts == nil || !ts.inMulti {
		return false, nil
	}
	return true, queueCmd(c, ts, args)
}

// doMulti opens a transaction. A MULTI while one is already open is the "calls can
// not be nested" error and leaves the open transaction untouched, matching Redis;
// otherwise it marks the connection in-MULTI, preserving any WATCH baselines armed
// before it, and answers +OK.
func doMulti(c *shard.Conn, args [][]byte) error {
	if len(args) != 1 {
		return oops(c, "ERR wrong number of arguments for 'multi' command")
	}
	ts, _ := c.TxState().(*txnState)
	if ts != nil && ts.inMulti {
		return oops(c, "ERR MULTI calls can not be nested")
	}
	if ts == nil {
		ts = &txnState{}
		c.SetTxState(ts)
	}
	ts.inMulti = true
	ts.dirty = false
	ts.queued = ts.queued[:0]
	return c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// queueCmd validates a command during a MULTI and parks it. An unknown verb or a
// bad arity is reported at once and flags the transaction dirty, so the later EXEC
// aborts with EXECABORT rather than running a partial queue, the exact Redis
// contract. A valid command is copied into the queue and acknowledged with
// +QUEUED.
func queueCmd(c *shard.Conn, ts *txnState, args [][]byte) error {
	e := lookupEntry(args[0])
	if e == nil {
		ts.dirty = true
		return oops(c, "ERR unknown command '"+string(args[0])+"'")
	}
	n := len(args) - 1
	if n < e.minArgs || (e.maxArgs >= 0 && n > e.maxArgs) {
		ts.dirty = true
		return oops(c, "ERR wrong number of arguments for '"+e.name+"' command")
	}
	ts.queued = append(ts.queued, queuedCmd{e: e, args: copyArgs(args)})
	return c.InlineReply(resp.AppendStatus(nil, "QUEUED"))
}

// doDiscard throws away an open transaction: the queue, the dirty flag, and the
// WATCH baselines all go, and the connection leaves MULTI. A DISCARD with no open
// transaction is an error.
func doDiscard(c *shard.Conn, args [][]byte) error {
	if len(args) != 1 {
		return oops(c, "ERR wrong number of arguments for 'discard' command")
	}
	ts, _ := c.TxState().(*txnState)
	if ts == nil || !ts.inMulti {
		return oops(c, "ERR DISCARD without MULTI")
	}
	clearTxn(c, ts)
	return c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// doWatch arms optimistic locks on its key arguments. WATCH inside a MULTI is an
// error (the watches would have no window to guard). Each key's baseline
// fingerprint is captured now, on its owner, so a change between here and EXEC is
// detectable; re-WATCHing a key refreshes its baseline. The reply is +OK.
func doWatch(c *shard.Conn, args [][]byte) error {
	if len(args) < 2 {
		return oops(c, "ERR wrong number of arguments for 'watch' command")
	}
	ts, _ := c.TxState().(*txnState)
	if ts != nil && ts.inMulti {
		return oops(c, "ERR WATCH inside MULTI is not allowed")
	}
	if ts == nil {
		ts = &txnState{}
		c.SetTxState(ts)
	}
	for _, key := range args[1:] {
		fp, exists := watchFingerprint(c, key)
		ts.watch = appendWatch(ts.watch, key, fp, exists)
	}
	return c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// doUnwatch forgets every WATCH baseline, the manual counterpart of the automatic
// clear EXEC and DISCARD do. It always answers +OK, even with nothing watched. A
// connection left with no watches and no open MULTI drops its transaction state
// so an idle connection carries none.
func doUnwatch(c *shard.Conn, args [][]byte) error {
	if len(args) != 1 {
		return oops(c, "ERR wrong number of arguments for 'unwatch' command")
	}
	ts, _ := c.TxState().(*txnState)
	if ts != nil {
		ts.watch = ts.watch[:0]
		if !ts.inMulti {
			c.SetTxState(nil)
		}
	}
	return c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// doExec runs an open transaction. Four gates come first, in Redis's order: EXEC
// with no MULTI is an error; a queue that took a queuing error aborts with
// EXECABORT; and an empty-or-full queue whose WATCHed keys changed answers the null
// array without running anything. Otherwise the union of every queued command's
// keys and every watched key is armed as one F17 transaction, and under that
// barrier the watches are re-checked and, if still clean, every queued command runs
// captured into one array reply delivered at EXEC's pipeline slot.
func doExec(c *shard.Conn, args [][]byte) error {
	if len(args) != 1 {
		return oops(c, "ERR wrong number of arguments for 'exec' command")
	}
	ts, _ := c.TxState().(*txnState)
	if ts == nil || !ts.inMulti {
		return oops(c, "ERR EXEC without MULTI")
	}
	if ts.dirty {
		clearTxn(c, ts)
		return oops(c, "EXECABORT Transaction discarded because of previous errors.")
	}
	queued := ts.queued
	watch := ts.watch
	clearTxn(c, ts)

	// The transaction's key union: every key every queued command touches, plus
	// every watched key, so both the command effects and the watch re-check run
	// under one held barrier. newTxn collapses the duplicates.
	var union [][]byte
	for i := range queued {
		union = append(union, commandKeys(queued[i].e, queued[i].args[1:])...)
	}
	for i := range watch {
		union = append(union, watch[i].key)
	}

	return c.DoTxn(union, func(t *shard.Txn) []byte {
		if watchDirty(c, t, watch) {
			return resp.AppendNullArray(nil)
		}
		out := resp.AppendArrayHeader(nil, len(queued))
		for i := range queued {
			out = append(out, execOne(c, t, queued[i].e, queued[i].args)...)
		}
		return out
	})
}

// execOne runs one queued command under the transaction and returns its RESP
// reply. It mirrors Dispatch's routing tiers, but every terminal runs captured on
// the owner holding the key rather than enqueuing a hop: a fan decomposes into per-
// key point ops (or a keyless scatter), a genuinely cross-shard command runs its
// tier-two body under the barrier, and everything else runs its point handler on
// the routing key's owner. Blocking verbs never park here: the capture sets the
// no-block flag, so a BLPOP with an empty list answers its would-block reply
// instead of registering a waiter (exec.go).
func execOne(c *shard.Conn, t *shard.Txn, e *entry, args [][]byte) []byte {
	tail := args[1:]
	n := len(tail)
	if e.fan != 0 && (e.fanOnly || n > 1) {
		if e.keyed {
			return execKeyedFan(c, t, e, tail)
		}
		return c.RunFanAllCaptured(e.fanOp, e.fan, tail)
	}
	if e.cross != nil {
		if keys := e.crossKeys(tail); len(keys) > 1 && !colocated(c, keys) {
			return c.RunCrossCaptured(t, func(tx *shard.Txn) []byte { return e.cross(tx, tail) })
		}
	}
	if e.streamKeyAt != nil {
		idx := e.streamKeyAt(tail)
		if idx < 0 {
			return c.RunKeylessCaptured(e.op, tail)
		}
		return c.RunPointCaptured(t, tail[idx], e.op, tail)
	}
	if e.subFan != nil {
		if kind, ok := e.subFan(args); ok {
			return c.RunFanAllCaptured(e.fanOp, kind, tail)
		}
	}
	if e.keyAt > 0 && n > e.keyAt {
		return c.RunPointCaptured(t, tail[e.keyAt], e.op, tail)
	}
	if e.keyed && n >= 1 {
		return c.RunPointCaptured(t, tail[0], e.op, tail)
	}
	return c.RunKeylessCaptured(e.op, tail)
}

// execKeyedFan runs a keyed multi-key command as a sequence of point ops under the
// barrier, the capture counterpart of the live fan-out. MSET writes each pair
// through the SET op and answers +OK (or the first error); MGET reads each key
// through the GET op and wraps the bulks in one array; the counting forms (DEL,
// UNLINK, EXISTS, TOUCH) run their own point handler per key and sum the integer
// replies. Every key is one the union holds, so each point op runs on its owner
// under the transaction.
func execKeyedFan(c *shard.Conn, t *shard.Txn, e *entry, tail [][]byte) []byte {
	switch {
	case e.paired: // MSET
		if len(tail)%2 != 0 {
			return resp.AppendError(nil, "ERR wrong number of arguments for '"+e.name+"' command")
		}
		setOp := table["SET"].op
		for i := 0; i+1 < len(tail); i += 2 {
			part := c.RunPointCaptured(t, tail[i], setOp, tail[i:i+2])
			if len(part) > 0 && part[0] == '-' {
				return part
			}
		}
		return resp.AppendStatus(nil, "OK")
	case e.fan == shard.FanMGet: // MGET
		getOp := table["GET"].op
		out := resp.AppendArrayHeader(nil, len(tail))
		for _, key := range tail {
			out = append(out, c.RunPointCaptured(t, key, getOp, [][]byte{key})...)
		}
		return out
	default: // FanCount: DEL, UNLINK, EXISTS, TOUCH
		var sum int64
		for _, key := range tail {
			sum += respInt(c.RunPointCaptured(t, key, e.op, [][]byte{key}))
		}
		return resp.AppendInt(nil, sum)
	}
}

// commandKeys returns every key a command touches, so EXEC can arm one intent per
// key over the whole queue. It mirrors execOne's routing: a cross-shard command's
// own extractor, a keyed fan's key positions (every even position for a paired
// MSET, every argument otherwise), the post-count key for a keyAt verb, or the
// first argument for a plain keyed command. A keyless command contributes nothing.
func commandKeys(e *entry, tail [][]byte) [][]byte {
	if e.crossKeys != nil {
		if k := e.crossKeys(tail); k != nil {
			return k
		}
	}
	n := len(tail)
	if e.fan != 0 && e.keyed && (e.fanOnly || n > 1) {
		if e.paired {
			m := n / 2
			keys := make([][]byte, m)
			for i := 0; i < m; i++ {
				keys[i] = tail[2*i]
			}
			return keys
		}
		return tail
	}
	if e.keyAt > 0 {
		if n > e.keyAt {
			return tail[e.keyAt : e.keyAt+1]
		}
		return nil
	}
	if e.keyed && n >= 1 {
		return tail[:1]
	}
	return nil
}

// watchFingerprint captures a WATCHed key's baseline on its owner: the FNV-1a
// fingerprint of its DUMP payload and whether it exists. It runs synchronously
// through CallOwner, which posts a keyless owner op and waits, so it is off the
// intent barrier and never blocks another key's traffic.
func watchFingerprint(c *shard.Conn, key []byte) (uint64, bool) {
	var fp uint64
	var exists bool
	c.CallOwner(c.ShardOf(key), func(cx *shard.Ctx) {
		fp, exists = fingerprintOf(cx, key)
	})
	return fp, exists
}

// watchDirty reports whether any WATCHed key changed since its baseline. It runs
// under the EXEC barrier, re-fingerprinting each key on its owner through the
// transaction (every watched key is in the union, so t.Do reaches its owner); a
// key that changed value, was created, or was deleted trips it.
func watchDirty(c *shard.Conn, t *shard.Txn, watch []watchEntry) bool {
	for i := range watch {
		fp, exists := currentFingerprint(c, t, watch[i].key)
		if exists != watch[i].exists || fp != watch[i].fp {
			return true
		}
	}
	return false
}

// currentFingerprint re-reads a watched key's fingerprint under the barrier, on the
// owner that holds its intent. The fallback to a direct owner call covers a key
// that somehow escaped the union, so a re-check never silently skips a key.
func currentFingerprint(c *shard.Conn, t *shard.Txn, key []byte) (uint64, bool) {
	var fp uint64
	var exists, ran bool
	t.Do(key, func(cx *shard.Ctx) {
		fp, exists = fingerprintOf(cx, key)
		ran = true
	})
	if !ran {
		c.CallOwner(c.ShardOf(key), func(cx *shard.Ctx) {
			fp, exists = fingerprintOf(cx, key)
		})
	}
	return fp, exists
}

// fingerprintOf serializes key through the DUMP payload and hashes it, the stable
// value identity WATCH compares. A missing key fingerprints as zero-and-absent, so
// creation and deletion both register as a change. Owner goroutine only.
func fingerprintOf(cx *shard.Ctx, key []byte) (uint64, bool) {
	payload, ok := dumpPayload(cx, key)
	if !ok {
		return 0, false
	}
	h := fnv.New64a()
	h.Write(payload)
	return h.Sum64(), true
}

// ResetTxn clears a connection's transaction state, the hook the goroutine driver's
// RESET path calls (client.go) so RESET unwinds a half-built MULTI along with the
// subscriptions and the client name. It is safe on a connection that never touched
// a transaction.
func ResetTxn(c *shard.Conn) {
	if ts, _ := c.TxState().(*txnState); ts != nil {
		c.SetTxState(nil)
	}
}

// clearTxn returns a connection to the no-transaction state, dropping the queue,
// the dirty flag, and the watches. It nils the handle so an idle connection carries
// nothing.
func clearTxn(c *shard.Conn, ts *txnState) {
	ts.inMulti = false
	ts.dirty = false
	ts.queued = nil
	ts.watch = nil
	c.SetTxState(nil)
}

// appendWatch adds or refreshes a key's baseline. Re-WATCHing a key overwrites its
// prior entry rather than stacking a second, so the watch list holds one entry per
// distinct key, matching Redis.
func appendWatch(watch []watchEntry, key []byte, fp uint64, exists bool) []watchEntry {
	kc := append([]byte(nil), key...)
	for i := range watch {
		if string(watch[i].key) == string(kc) {
			watch[i].fp = fp
			watch[i].exists = exists
			return watch
		}
	}
	return append(watch, watchEntry{key: kc, fp: fp, exists: exists})
}

// copyArgs deep-copies a command's whole argument line into fresh storage, the
// stable copy EXEC's coordinator goroutine reads after the reader's parse buffer is
// reused, the same rule copyTail follows for a cross-shard command.
func copyArgs(args [][]byte) [][]byte {
	a := make([][]byte, len(args))
	for i := range args {
		a[i] = append([]byte(nil), args[i]...)
	}
	return a
}

// respInt reads the integer a RESP `:N\r\n` reply carries, the value execKeyedFan
// sums for the counting fans. A non-integer reply (an error element) folds as zero,
// which cannot happen for the point handlers this sums (DEL, EXISTS, TOUCH always
// answer an integer).
func respInt(rep []byte) int64 {
	if len(rep) < 3 || rep[0] != ':' {
		return 0
	}
	i := 1
	neg := false
	if rep[i] == '-' {
		neg = true
		i++
	}
	var n int64
	for ; i < len(rep) && rep[i] >= '0' && rep[i] <= '9'; i++ {
		n = n*10 + int64(rep[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}

// lookupEntry finds a verb's table row, uppercasing into a stack scratch the same
// way Dispatch does, so queuing validates a command exactly as the live path would.
func lookupEntry(verb []byte) *entry {
	if len(verb) > maxVerb {
		return nil
	}
	var vb [maxVerb]byte
	for i := 0; i < len(verb); i++ {
		ch := verb[i]
		if ch >= 'a' && ch <= 'z' {
			ch -= 32
		}
		vb[i] = ch
	}
	return table[string(vb[:len(verb)])]
}
