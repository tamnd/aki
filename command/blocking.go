package command

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/keyspace"
)

// This file implements the blocking list commands (doc 09 §7): BLPOP, BRPOP,
// BRPOPLPUSH, BLMOVE and BLMPOP, plus the registry that wakes a parked client
// when a key gains elements.
//
// aki runs one goroutine per connection, so a blocking command parks that
// goroutine inside its handler until a key is ready, the timeout elapses, the
// client is unblocked, or the connection closes. A write on another connection
// records the keys it made ready (Ctx.signalReady) and runCommand calls
// serveReady once the write has been applied and propagated, so a woken client
// sees the element and its own propagation follows in order.

// blockKey identifies a waited-on key in a specific database.
type blockKey struct {
	db  int
	key string
}

// blockWaiter is one client parked on a blocking command. ready is signaled when
// a key it waits on gains elements; unblock is signaled by CLIENT UNBLOCK, with
// true asking for an error reply and false for the timeout reply.
type blockWaiter struct {
	id      uint64
	seq     uint64
	ready   chan struct{}
	unblock chan bool
}

// blockState is the registry of parked clients keyed by the keys they wait on.
// waiters keeps each key's list in arrival order so wakeups are FIFO, byID maps
// a client id to its waiter for CLIENT UNBLOCK, and active lets the write path
// skip all bookkeeping with a single load when nobody is blocked.
type blockState struct {
	mu      sync.Mutex
	seq     uint64
	waiters map[blockKey][]*blockWaiter
	byID    map[uint64]*blockWaiter
	active  atomic.Int64
}

// blockingInit prepares the registry maps. New calls it once at startup.
func (d *Dispatcher) blockingInit() {
	d.blocking.waiters = map[blockKey][]*blockWaiter{}
	d.blocking.byID = map[uint64]*blockWaiter{}
}

// blockRegister enrolls a client as a waiter on each of keys and returns its
// waiter handle. The caller pairs every register with a blockUnregister.
func (d *Dispatcher) blockRegister(db int, keys [][]byte, id uint64) *blockWaiter {
	b := &d.blocking
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	w := &blockWaiter{
		id:      id,
		seq:     b.seq,
		ready:   make(chan struct{}, 1),
		unblock: make(chan bool, 1),
	}
	for _, key := range keys {
		bk := blockKey{db: db, key: string(key)}
		b.waiters[bk] = append(b.waiters[bk], w)
	}
	b.byID[id] = w
	b.active.Add(1)
	return w
}

// blockUnregister removes a waiter from every key it was parked on and from the
// id index. It is safe to call more than once for the same waiter.
func (d *Dispatcher) blockUnregister(w *blockWaiter, db int, keys [][]byte) {
	b := &d.blocking
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, key := range keys {
		bk := blockKey{db: db, key: string(key)}
		list := b.waiters[bk]
		out := list[:0]
		for _, x := range list {
			if x != w {
				out = append(out, x)
			}
		}
		if len(out) == 0 {
			delete(b.waiters, bk)
		} else {
			b.waiters[bk] = out
		}
	}
	if b.byID[w.id] == w {
		delete(b.byID, w.id)
		b.active.Add(-1)
	}
}

// serveReady wakes the oldest waiter parked on key, skipping the client with
// skipID. The write path passes the writer's own id so a client that pushes to
// itself is never woken by its own write, and a serving waiter passes its own id
// so the cascade hands the turn to the next waiter rather than back to itself.
func (d *Dispatcher) serveReady(db int, key []byte, skipID uint64) {
	b := &d.blocking
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, w := range b.waiters[blockKey{db: db, key: string(key)}] {
		if w.id == skipID {
			continue
		}
		select {
		case w.ready <- struct{}{}:
		default:
		}
		return
	}
}

// serveReadyAll wakes every waiter parked on key, skipping the client with
// skipID. A stream XADD makes one entry visible to every blocked XREAD on the
// key at once (a fan-out read, not a hand-off), so the whole waiter list is
// signaled rather than just the oldest one.
func (d *Dispatcher) serveReadyAll(db int, key []byte, skipID uint64) {
	b := &d.blocking
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, w := range b.waiters[blockKey{db: db, key: string(key)}] {
		if w.id == skipID {
			continue
		}
		select {
		case w.ready <- struct{}{}:
		default:
		}
	}
}

// unblockClient signals a parked client to stop waiting. errReply asks for the
// CLIENT UNBLOCK ERROR reply; otherwise the command returns as if it timed out.
// It reports whether a client was actually blocked.
func (d *Dispatcher) unblockClient(id uint64, errReply bool) bool {
	b := &d.blocking
	b.mu.Lock()
	w, ok := b.byID[id]
	b.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case w.unblock <- errReply:
	default:
	}
	return true
}

// propagateBlocking writes the resolved non-blocking command to the AOF and the
// replication stream. A blocking command parks without holding the replication
// lock, so the woken handler takes it here instead, after the pusher's own write
// has already been propagated.
func (d *Dispatcher) propagateBlocking(db int, argv [][]byte) {
	if d.aofEnabled() {
		d.appendAOF(db, argv)
	}
	if d.replActive() {
		d.repl.mu.Lock()
		d.propagateRepl(db, argv)
		d.repl.mu.Unlock()
	}
}

// parseTimeout reads a blocking command's timeout argument: a non-negative float
// number of seconds, where 0 means wait forever. The error strings match Redis.
func parseTimeout(arg []byte) (float64, string) {
	v, err := strconv.ParseFloat(string(arg), 64)
	if err != nil {
		return 0, "ERR timeout is not a float or out of range"
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, "ERR timeout is out of range"
	}
	if v < 0 {
		return 0, "ERR timeout is negative"
	}
	return v, ""
}

// blockingListCommands returns the blocking list command table (doc 09 §7).
func blockingListCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "blpop", Group: GroupList, Since: "2.0.0",
			Arity: -3, Flags: FlagWrite | FlagBlocking, FirstKey: 1, LastKey: -2, Step: 1,
			Handler: func(ctx *Ctx) { blockPop(ctx, true) }},
		{Name: "brpop", Group: GroupList, Since: "2.0.0",
			Arity: -3, Flags: FlagWrite | FlagBlocking, FirstKey: 1, LastKey: -2, Step: 1,
			Handler: func(ctx *Ctx) { blockPop(ctx, false) }},
		{Name: "brpoplpush", Group: GroupList, Since: "2.2.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM | FlagBlocking, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleBRPopLPush},
		{Name: "blmove", Group: GroupList, Since: "6.2.0",
			Arity: 6, Flags: FlagWrite | FlagDenyOOM | FlagBlocking, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleBLMove},
		{Name: "blmpop", Group: GroupList, Since: "7.0.0",
			Arity: -5, Flags: FlagWrite | FlagBlocking, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: handleBLMPop},
	}
}

// blockDrive runs the wait protocol shared by every blocking list command. It
// calls attempt right away (the fast path), then on every wake, until attempt
// serves a reply. When the command must not park (an offline connection or
// inside EXEC) it writes the empty reply instead of waiting. onTimeout writes the
// command's own empty reply for a real timeout or a CLIENT UNBLOCK with no error.
func (d *Dispatcher) blockDrive(ctx *Ctx, keys [][]byte, timeout float64, attempt func() bool, onTimeout func()) {
	if attempt() {
		return
	}
	if ctx.noBlock() {
		onTimeout()
		return
	}
	db := ctx.Conn.DB()
	w := d.blockRegister(db, keys, ctx.Conn.ID())
	defer d.blockUnregister(w, db, keys)

	var timerC <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(time.Duration(timeout * float64(time.Second)))
		defer t.Stop()
		timerC = t.C
	}
	for {
		// Re-check after registering so a push that landed between the fast-path
		// attempt and the register is not lost.
		if attempt() {
			return
		}
		select {
		case <-w.ready:
			// A key may be ready; loop and try again.
		case errReply := <-w.unblock:
			if errReply {
				ctx.enc().WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			} else {
				onTimeout()
			}
			return
		case <-timerC:
			onTimeout()
			return
		case <-ctx.Conn.Closed():
			return
		}
	}
}

// blockPop implements BLPOP (head) and BRPOP (tail). It pops one element from the
// first key that has one, replying with the key and the element.
func blockPop(ctx *Ctx, head bool) {
	keys := ctx.Argv[1 : len(ctx.Argv)-1]
	timeout, errMsg := parseTimeout(ctx.Argv[len(ctx.Argv)-1])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	db := ctx.Conn.DB()
	attempt := func() bool {
		var (
			poppedKey []byte
			popped    []byte
			emptied   bool
			wrongTyp  bool
		)
		lim := ctx.encLimits()
		done := ctx.update(func(d *keyspace.DB) error {
			for _, key := range keys {
				hdr, found, err := listHeader(d, key)
				if err != nil {
					return err
				}
				if !found {
					continue
				}
				if hdr.Type != keyspace.TypeList {
					wrongTyp = true
					return nil
				}
				// Pop one boundary element in place rather than cloning the list on
				// each blocking attempt.
				elem, e, err := listPopOne(d, key, hdr, head, lim)
				if err != nil {
					return err
				}
				if elem == nil {
					continue
				}
				popped = elem
				emptied = e
				poppedKey = key
				return nil
			}
			return nil
		})
		if !done {
			return true
		}
		if wrongTyp {
			ctx.enc().WriteError(wrongTypeError)
			return true
		}
		if poppedKey == nil {
			return false
		}
		event := "rpop"
		resolved := "RPOP"
		if head {
			event = "lpop"
			resolved = "LPOP"
		}
		ctx.notify(notifyList, event, poppedKey)
		if emptied {
			ctx.notify(notifyGeneric, "del", poppedKey)
		}
		ctx.d.trackingInvalidateKey(poppedKey, ctx.Conn.ID())
		ctx.d.propagateBlocking(db, [][]byte{[]byte(resolved), poppedKey})
		if !emptied {
			ctx.d.serveReady(db, poppedKey, ctx.Conn.ID())
		}
		enc := ctx.enc()
		enc.WriteArrayLen(2)
		enc.WriteBulkString(poppedKey)
		enc.WriteBulkString(popped)
		return true
	}
	ctx.d.blockDrive(ctx, keys, timeout, attempt, func() { ctx.enc().WriteNullArray() })
}

// handleBRPopLPush implements BRPOPLPUSH src dst timeout, the blocking form of
// RPOPLPUSH.
func handleBRPopLPush(ctx *Ctx) {
	timeout, errMsg := parseTimeout(ctx.Argv[3])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	blockMove(ctx, ctx.Argv[1], ctx.Argv[2], false, true, timeout,
		[][]byte{[]byte("RPOPLPUSH"), ctx.Argv[1], ctx.Argv[2]})
}

// handleBLMove implements BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout, the
// blocking form of LMOVE.
func handleBLMove(ctx *Ctx) {
	fromLeft, ok1 := parseLeftRight(ctx.Argv[3])
	toLeft, ok2 := parseLeftRight(ctx.Argv[4])
	if !ok1 || !ok2 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	timeout, errMsg := parseTimeout(ctx.Argv[5])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	blockMove(ctx, ctx.Argv[1], ctx.Argv[2], fromLeft, toLeft, timeout,
		[][]byte{[]byte("LMOVE"), ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], ctx.Argv[4]})
}

// blockMove is the shared body of BRPOPLPUSH and BLMOVE: it waits for src to have
// an element, moves one to dst, and replies with the moved element. resolved is
// the non-blocking command propagated on success.
func blockMove(ctx *Ctx, src, dst []byte, fromLeft, toLeft bool, timeout float64, resolved [][]byte) {
	db := ctx.Conn.DB()
	attempt := func() bool {
		var (
			moved      []byte
			srcEmptied bool
			wrongTyp   bool
			ok         bool
		)
		lim := ctx.encLimits()
		done := ctx.update(func(d *keyspace.DB) error {
			srcHdr, srcFound, err := listHeader(d, src)
			if err != nil {
				return err
			}
			if srcFound && srcHdr.Type != keyspace.TypeList {
				wrongTyp = true
				return nil
			}
			sameKey := bytes.Equal(src, dst)
			// Validate the destination type before touching src, and keep waiting
			// (ok stays false) when src is absent or empty.
			if !sameKey {
				dstHdr, dstFound, err := listHeader(d, dst)
				if err != nil {
					return err
				}
				if dstFound && dstHdr.Type != keyspace.TypeList {
					wrongTyp = true
					return nil
				}
			}
			if !srcFound {
				return nil
			}
			// Pop one element from src in place and push it onto dst through the
			// shared bounded push core, never cloning either list.
			elem, emptied, err := listPopOne(d, src, srcHdr, fromLeft, lim)
			if err != nil {
				return err
			}
			if elem == nil {
				return nil
			}
			moved = elem
			ok = true
			pushKey := dst
			if sameKey {
				pushKey = src
			} else {
				srcEmptied = emptied
			}
			r, err := applyListPush(d, pushKey, [][]byte{elem}, toLeft, false, lim)
			if err != nil {
				return err
			}
			if r.res == pushResWrongType {
				wrongTyp = true
			}
			return nil
		})
		if !done {
			return true
		}
		if wrongTyp {
			ctx.enc().WriteError(wrongTypeError)
			return true
		}
		if !ok {
			return false
		}
		fromEvent := "rpop"
		if fromLeft {
			fromEvent = "lpop"
		}
		toEvent := "rpush"
		if toLeft {
			toEvent = "lpush"
		}
		ctx.notify(notifyList, fromEvent, src)
		ctx.notify(notifyList, toEvent, dst)
		if srcEmptied {
			ctx.notify(notifyGeneric, "del", src)
		}
		ctx.d.trackingInvalidateKey(src, ctx.Conn.ID())
		ctx.d.trackingInvalidateKey(dst, ctx.Conn.ID())
		ctx.d.propagateBlocking(db, resolved)
		// The destination gained an element, and the source may still hold more, so
		// hand the turn to the next waiter on either key.
		ctx.d.serveReady(db, dst, ctx.Conn.ID())
		if !srcEmptied {
			ctx.d.serveReady(db, src, ctx.Conn.ID())
		}
		ctx.enc().WriteBulkString(moved)
		return true
	}
	ctx.d.blockDrive(ctx, [][]byte{src}, timeout, attempt, func() { ctx.enc().WriteNull() })
}

// handleBLMPop implements BLMPOP timeout numkeys key [key ...] LEFT|RIGHT
// [COUNT count], the blocking form of LMPOP.
func handleBLMPop(ctx *Ctx) {
	timeout, errMsg := parseTimeout(ctx.Argv[1])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	numkeys, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR numkeys should be greater than 0")
		return
	}
	if numkeys < 0 {
		ctx.enc().WriteError("ERR numkeys can't be negative")
		return
	}
	if numkeys == 0 {
		ctx.enc().WriteError("ERR numkeys can't be zero")
		return
	}
	keyStart := 3
	dirIdx := keyStart + int(numkeys)
	if dirIdx >= len(ctx.Argv) {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	keys := ctx.Argv[keyStart:dirIdx]
	fromLeft, okDir := parseLeftRight(ctx.Argv[dirIdx])
	if !okDir {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	count := int64(1)
	rest := ctx.Argv[dirIdx+1:]
	if len(rest) > 0 {
		if len(rest) != 2 || !strings.EqualFold(string(rest[0]), "COUNT") {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		c, okc := parseInteger(rest[1])
		if !okc {
			ctx.enc().WriteError("ERR count should be greater than 0")
			return
		}
		if c < 0 {
			ctx.enc().WriteError("ERR COUNT can't be negative")
			return
		}
		if c == 0 {
			ctx.enc().WriteError("ERR COUNT can't be zero")
			return
		}
		count = c
	}

	db := ctx.Conn.DB()
	attempt := func() bool {
		var (
			poppedKey []byte
			popped    [][]byte
			emptied   bool
			wrongTyp  bool
		)
		lim := ctx.encLimits()
		done := ctx.update(func(d *keyspace.DB) error {
			for _, key := range keys {
				hdr, found, err := listHeader(d, key)
				if err != nil {
					return err
				}
				if !found {
					continue
				}
				if hdr.Type != keyspace.TypeList {
					wrongTyp = true
					return nil
				}
				// Pop up to count elements from this key's end in place, never
				// cloning the whole list on a blocking attempt.
				p, e, err := listPopN(d, key, hdr, count, fromLeft, lim)
				if err != nil {
					return err
				}
				if len(p) == 0 {
					continue
				}
				popped = p
				emptied = e
				poppedKey = key
				return nil
			}
			return nil
		})
		if !done {
			return true
		}
		if wrongTyp {
			ctx.enc().WriteError(wrongTypeError)
			return true
		}
		if poppedKey == nil {
			return false
		}
		event := "rpop"
		resolved := "RPOP"
		if fromLeft {
			event = "lpop"
			resolved = "LPOP"
		}
		ctx.notify(notifyList, event, poppedKey)
		if emptied {
			ctx.notify(notifyGeneric, "del", poppedKey)
		}
		ctx.d.trackingInvalidateKey(poppedKey, ctx.Conn.ID())
		ctx.d.propagateBlocking(db, [][]byte{
			[]byte(resolved), poppedKey, []byte(strconv.Itoa(len(popped)))})
		if !emptied {
			ctx.d.serveReady(db, poppedKey, ctx.Conn.ID())
		}
		enc := ctx.enc()
		enc.WriteArrayLen(2)
		enc.WriteBulkString(poppedKey)
		enc.WriteArrayLen(len(popped))
		for _, e := range popped {
			enc.WriteBulkString(e)
		}
		return true
	}
	ctx.d.blockDrive(ctx, keys, timeout, attempt, func() { ctx.enc().WriteNullArray() })
}
