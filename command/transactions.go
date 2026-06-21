package command

import (
	"bytes"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// queuedCmd is one command waiting in a MULTI queue: the descriptor already
// resolved at queue time and the argument vector to replay at EXEC.
type queuedCmd struct {
	cmd  *CmdDesc
	argv [][]byte
}

// watchEntry records a key watched for optimistic locking and its write version
// at WATCH time. A version of 0 means the key did not exist then.
type watchEntry struct {
	db      int
	key     []byte
	version uint64
}

// transactionCommands returns the MULTI/EXEC/DISCARD/WATCH/UNWATCH group.
func transactionCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "multi", Group: GroupTransactions, Since: "1.2.0",
			Arity: 1, Flags: FlagFast, Handler: handleMulti},
		{Name: "exec", Group: GroupTransactions, Since: "1.2.0",
			Arity: 1, Flags: 0, Handler: handleExec},
		{Name: "discard", Group: GroupTransactions, Since: "2.0.0",
			Arity: 1, Flags: FlagFast, Handler: handleDiscard},
		{Name: "watch", Group: GroupTransactions, Since: "2.2.0",
			Arity: -2, Flags: FlagFast, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handleWatch},
		{Name: "unwatch", Group: GroupTransactions, Since: "2.2.0",
			Arity: 1, Flags: FlagFast, Handler: handleUnwatch},
	}
}

// isMultiControl reports whether a command is handled directly even inside a
// transaction instead of being queued. UNWATCH is not here on purpose: Redis
// queues it like any other command.
func isMultiControl(name string) bool {
	switch name {
	case "multi", "exec", "discard", "watch", "reset", "quit":
		return true
	default:
		return false
	}
}

// queueCommand appends a command to the open transaction. It resolves the
// descriptor and checks arity now, the same as normal dispatch, so a bad command
// is reported at queue time and marks the transaction so EXEC aborts.
func (d *Dispatcher) queueCommand(c *networking.Conn, sess *session, name string, argv [][]byte) {
	cmd, err := d.table.lookup(name, argv)
	if err != nil {
		sess.dirtyExec = true
		c.Enc().WriteError(err.Error())
		return
	}
	if !checkArity(cmd, len(argv)) {
		sess.dirtyExec = true
		c.Enc().WriteError(arityError(cmd))
		return
	}
	sess.queue = append(sess.queue, queuedCmd{cmd: cmd, argv: argv})
	c.WriteRaw(resp.ReplyQueued)
}

// handleMulti opens a transaction. Nesting is an error.
func handleMulti(ctx *Ctx) {
	if ctx.sess.inMulti {
		ctx.enc().WriteError("ERR MULTI calls can not be nested")
		return
	}
	ctx.sess.inMulti = true
	ctx.sess.queue = nil
	ctx.sess.dirtyExec = false
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleDiscard throws away the queue and leaves transaction mode. It also drops
// the WATCH registrations, matching Redis.
func handleDiscard(ctx *Ctx) {
	if !ctx.sess.inMulti {
		ctx.enc().WriteError("ERR DISCARD without MULTI")
		return
	}
	ctx.sess.clearMulti()
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleExec runs the queued commands. A queue-time error aborts the whole
// transaction; a watched key that changed since WATCH returns a null array; a
// run-time error in one command becomes an error element and the rest still run.
func handleExec(ctx *Ctx) {
	sess := ctx.sess
	if !sess.inMulti {
		ctx.enc().WriteError("ERR EXEC without MULTI")
		return
	}
	queue := sess.queue
	dirtyExec := sess.dirtyExec
	watched := sess.watched
	sess.clearMulti()

	if dirtyExec {
		ctx.enc().WriteError("EXECABORT Transaction discarded because of previous errors.")
		return
	}
	if ctx.d.watchedChanged(watched) {
		ctx.enc().WriteNullArray()
		return
	}

	enc := ctx.enc()
	enc.WriteArrayLen(len(queue))
	for _, q := range queue {
		q.cmd.Handler(&Ctx{Conn: ctx.Conn, Argv: q.argv, d: ctx.d, sess: sess})
	}
}

// handleWatch registers keys for optimistic locking, recording each key's current
// version. WATCH inside a transaction is an error.
func handleWatch(ctx *Ctx) {
	if ctx.sess.inMulti {
		ctx.enc().WriteError("ERR WATCH inside MULTI is not allowed")
		return
	}
	db := ctx.Conn.DB()
	for _, key := range ctx.Argv[1:] {
		ver, err := ctx.d.keyVersion(db, key)
		if err != nil {
			ctx.enc().WriteError("ERR " + err.Error())
			return
		}
		ctx.sess.watch(db, key, ver)
	}
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleUnwatch forgets all watched keys. It is always a success, even with
// nothing watched.
func handleUnwatch(ctx *Ctx) {
	ctx.sess.watched = nil
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// clearMulti returns the session to normal mode and drops the queue and the
// watch set.
func (s *session) clearMulti() {
	s.inMulti = false
	s.queue = nil
	s.dirtyExec = false
	s.watched = nil
}

// watch adds or refreshes a watched key. Watching the same key twice keeps the
// most recent version, matching Redis.
func (s *session) watch(db int, key []byte, version uint64) {
	for i := range s.watched {
		if s.watched[i].db == db && bytes.Equal(s.watched[i].key, key) {
			s.watched[i].version = version
			return
		}
	}
	s.watched = append(s.watched, watchEntry{db: db, key: bytes.Clone(key), version: version})
}

// keyVersion reads a key's current write version, returning 0 when the key is
// absent. WATCH and the EXEC dirty check both go through here.
func (d *Dispatcher) keyVersion(db int, key []byte) (uint64, error) {
	if d.engine == nil {
		return 0, nil
	}
	ver, found, err := d.engine.version(db, key)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	return ver, nil
}

// watchedChanged reports whether any watched key now differs from its recorded
// version, which is what makes EXEC return a null array. An error reading a key
// is treated as a change, so the transaction does not run on stale state.
func (d *Dispatcher) watchedChanged(watched []watchEntry) bool {
	for _, w := range watched {
		ver, err := d.keyVersion(w.db, w.key)
		if err != nil || ver != w.version {
			return true
		}
	}
	return false
}
