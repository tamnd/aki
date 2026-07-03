package f1srv

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// The blocking list commands (BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP) wait for an
// element instead of replying immediately when their keys are empty. The list model
// (spec 2064/f1_rewrite_ltm/08 section 9) is explicit that blocking is a connection-level
// and scheduling concern, not a change to the abstract list: a blocked client holds no
// element, reads no row, and materializes nothing. It is one entry in a per-key wait
// queue plus a timer, and the wakeup is an ordinary pop through the same single-writer
// serialization a foreground pop takes, so an element is delivered exactly once to
// exactly one client, in the order the clients blocked.
//
// The park happens on the connection's own goroutine (the default driver), which is free
// to sleep because the store is lock-free and a parked connection holds no lock. Under the
// shared-goroutine epoll reactor a park would stall every other connection on the loop, so
// there a blocking command serves non-blocking (immediate element or nil); connState.blockable
// carries which driver is running. Proper async parking under the reactor is a later slice.

// listWaiter is one blocked client's wakeup handle. ch is a one-slot buffered channel a
// pusher signals; the buffer means a signal that arrives while the waiter is mid-recheck
// is not lost and is coalesced with any other, since the waiter always rescans the keys
// after a wake rather than trusting the signal to carry an element.
type listWaiter struct {
	ch chan struct{}
}

// blockReg is the per-key blocked-client registry. waiters maps a list key to the FIFO of
// clients blocked on it, oldest first, which is the fairness order the wakeup serves in. n
// is the total number of (waiter, key) registrations across the whole map, read locklessly
// on the hot push path so a push with no client blocked anywhere pays a single atomic load
// and never touches the mutex or allocates.
type blockReg struct {
	n       atomic.Int64
	mu      sync.Mutex
	waiters map[string][]*listWaiter
}

// addWaiter appends w to the tail of every key's queue, so it blocks on all of them at once
// (a multi-key BLPOP) and a wakeup on any one can serve it. It runs once when a client
// parks.
func (s *Server) addWaiter(w *listWaiter, keys [][]byte) {
	s.block.mu.Lock()
	for _, k := range keys {
		s.block.waiters[string(k)] = append(s.block.waiters[string(k)], w)
	}
	s.block.n.Add(int64(len(keys)))
	s.block.mu.Unlock()
}

// removeWaiter drops w from every key's queue, called when the client is served, times out,
// or disconnects, so a dead or satisfied waiter never holds a wakeup slot. It tolerates w not
// being present in a queue (a key it was already removed from).
func (s *Server) removeWaiter(w *listWaiter, keys [][]byte) {
	s.block.mu.Lock()
	for _, k := range keys {
		q := s.block.waiters[string(k)]
		for i, x := range q {
			if x == w {
				q = append(q[:i], q[i+1:]...)
				s.block.n.Add(-1)
				break
			}
		}
		if len(q) == 0 {
			delete(s.block.waiters, string(k))
		} else {
			s.block.waiters[string(k)] = q
		}
	}
	s.block.mu.Unlock()
}

// signalListKey wakes the longest-waiting client blocked on key, the FIFO head, so a push
// that made key non-empty serves the fairest waiter first. It is a non-blocking send: if the
// head's one-slot channel is already armed the signal is dropped, because that waiter will
// rescan on its pending wake anyway. The atomic fast-out means a push to a key with no
// blocked client anywhere costs one load and returns, keeping the common push path free of
// the registry lock. A woken waiter that pops an element re-signals the same key, so a
// multi-element push chains down the queue one served waiter at a time.
func (s *Server) signalListKey(key []byte) {
	if s.block.n.Load() == 0 {
		return
	}
	s.block.mu.Lock()
	if q := s.block.waiters[string(key)]; len(q) > 0 {
		select {
		case q[0].ch <- struct{}{}:
		default:
		}
	}
	s.block.mu.Unlock()
}

// signalStreamKey wakes every client blocked on key, not just the FIFO head, because a stream read
// is non-consuming: a new entry is visible to every blocked XREAD on the stream at once, and every
// blocked XREADGROUP consumer must re-run its delivery to learn whether the entry fell to it. This
// differs from signalListKey, where an element goes to exactly one popper so only the head is woken.
// A woken reader that finds nothing new (its group cursor was already advanced by a peer under the
// stripe lock) simply re-parks, the same way Redis re-blocks a consumer it served nothing. The atomic
// fast-out keeps an XADD with no blocked client anywhere off the registry lock.
func (s *Server) signalStreamKey(key []byte) {
	if s.block.n.Load() == 0 {
		return
	}
	s.block.mu.Lock()
	for _, w := range s.block.waiters[string(key)] {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
	s.block.mu.Unlock()
}

// flushBeforeBlock writes any replies buffered so far and empties the buffer, so a reply that
// precedes a blocking command in the same pipeline reaches the client before this connection
// parks rather than being held until the block resolves. It is a no-op under the reactor,
// where c.conn is nil and the command never parks.
func (c *connState) flushBeforeBlock() {
	if c.conn != nil && len(c.out) > 0 {
		_, _ = c.conn.Write(c.out)
		c.out = c.out[:0]
	}
}

// parkOnReactor hands a blocking command to the reactor's park goroutine when the connection cannot
// sleep on the loop, which is the case under the epoll reactor where connState.blockable is false. It
// reruns rerun on the park goroutine, where the command re-scans its keys and, still finding them
// empty, parks properly on the wait channel and timer, writing the eventual reply through the owning
// loop. It reports true when the handoff was installed, so the caller returns without replying; false
// means there is no reactor park facility (the goroutine driver, which never reaches this since
// blockable is true there) and the caller writes the non-blocking reply itself. The argv rerun closes
// over must be a copy, since the loop reuses the connection's read buffer once it resumes.
func (c *connState) parkOnReactor(rerun func()) bool {
	if c.park == nil {
		return false
	}
	c.park.begin(rerun)
	return true
}

// dupArgv deep-copies a command's arguments so a park goroutine can reuse them after the loop has
// moved on and overwritten the read buffer the originals pointed into.
func dupArgv(argv [][]byte) [][]byte {
	out := make([][]byte, len(argv))
	for i, a := range argv {
		out[i] = append([]byte(nil), a...)
	}
	return out
}

// parseTimeout reads a blocking command's timeout, a double count of seconds since Redis 6.0
// where 0 means block forever. It returns the duration, whether the block is unbounded, and a
// Redis-shaped error string when the value is not a finite non-negative float. A fractional
// timeout is honored to the sub-second the duration can carry.
func parseTimeout(b []byte) (d time.Duration, forever bool, errMsg string) {
	secs, err := strconv.ParseFloat(string(b), 64)
	if err != nil || secs != secs || secs > 9.999999999e17 {
		return 0, false, "ERR timeout is not a float or out of range"
	}
	if secs < 0 {
		return 0, false, "ERR timeout is negative"
	}
	if secs == 0 {
		return 0, true, ""
	}
	return time.Duration(secs * float64(time.Second)), false, ""
}

func (c *connState) cmdBLPop(argv [][]byte) { c.blockingPop(argv, true) }
func (c *connState) cmdBRPop(argv [][]byte) { c.blockingPop(argv, false) }

// blockingPop is the shared body for BLPOP (atHead) and BRPOP. Both take one or more keys and
// a trailing timeout: they pop one element from the first key in the list that is a non-empty
// list, scanning the keys left to right, and reply with a two-element array of that key's name
// and the element. When every key is empty they block on all of them until a push wakes them or
// the timeout fires, at which point the reply is a null array. A key holding a plain string is
// WRONGTYPE. The pop goes through the same stripe lock and listPopEnd a foreground LPOP/RPOP
// takes, so a wakeup and a racing foreground pop never both get the element.
func (c *connState) blockingPop(argv [][]byte, atHead bool) {
	name := "brpop"
	if atHead {
		name = "blpop"
	}
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	d, forever, errMsg := parseTimeout(argv[len(argv)-1])
	if errMsg != "" {
		c.writeErr(errMsg)
		return
	}
	keys := argv[1 : len(argv)-1]

	var w *listWaiter
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if w != nil {
			c.srv.removeWaiter(w, keys)
		}
	}()
	for {
		// Serve pass: the first non-empty key yields one element. Running this every loop turn
		// is also the recheck that closes the register-then-park race, so a push that lands
		// between an empty scan and the registration is never missed.
		for _, key := range keys {
			mu := &c.srv.incrMu[c.srv.stripe(key)]
			mu.Lock()
			if c.listWrongType(key) {
				mu.Unlock()
				c.writeErr(wrongType)
				return
			}
			v, ok := c.listPopEnd(key, atHead)
			if ok {
				mu.Unlock()
				c.writeArrayHeader(2)
				c.writeBulk(key)
				c.writeBulk(v)
				c.srv.signalListKey(key) // chain the wakeup if the key still has elements and waiters
				return
			}
			mu.Unlock()
		}
		// Every key empty. On the goroutine driver this connection parks below; under the reactor
		// it cannot sleep on the loop, so it hands the command to a park goroutine that reruns it
		// with parking enabled, and only if there is no park facility does it reply non-blocking.
		if !c.blockable {
			ac := dupArgv(argv)
			if c.parkOnReactor(func() { c.blockingPop(ac, atHead) }) {
				return
			}
			c.writeNilArray()
			return
		}
		if w == nil {
			w = &listWaiter{ch: make(chan struct{}, 1)}
			c.srv.addWaiter(w, keys)
			continue // recheck under the registration before parking
		}
		c.flushBeforeBlock()
		if timer == nil && !forever {
			timer = time.NewTimer(d)
		}
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-w.ch:
			// Woken by a push: loop back and rescan the keys.
		case <-timeout:
			c.writeNilArray()
			return
		case <-c.parkCancel:
			// Reactor only: the peer disconnected while this park goroutine was blocked, so unwind
			// without replying. The channel is nil on the goroutine driver, where this never fires.
			return
		}
	}
}

func (c *connState) cmdBLMove(argv [][]byte) {
	// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
	if len(argv) != 6 {
		c.writeErr("ERR wrong number of arguments for 'blmove' command")
		return
	}
	c.blockingMove(argv[1], argv[2], argv[3], argv[4], argv[5], "blmove")
}

func (c *connState) cmdBRPopLPush(argv [][]byte) {
	// BRPOPLPUSH source destination timeout, exactly BLMOVE source destination RIGHT LEFT timeout.
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'brpoplpush' command")
		return
	}
	c.blockingMove(argv[1], argv[2], []byte("RIGHT"), []byte("LEFT"), argv[3], "brpoplpush")
}

// blockingMove is the shared body for BLMOVE and BRPOPLPUSH. It moves one element from the
// chosen end of source to the chosen end of destination, blocking on source until it has an
// element or the timeout fires. The move is the same pop-then-push under both keys' stripe
// locks the non-blocking LMOVE takes, so a same-key rotate works and a woken mover delivers
// exactly once. On timeout the reply is a null bulk (not a null array, since the non-blocking
// reply is a bulk element). The push may itself wake a client blocked on destination, so it
// signals destination after the move.
func (c *connState) blockingMove(source, destination, fromTok, toTok, timeoutTok []byte, name string) {
	fromHead, ok1 := parseLR(fromTok)
	toHead, ok2 := parseLR(toTok)
	if !ok1 || !ok2 {
		c.writeErr("ERR syntax error")
		return
	}
	d, forever, errMsg := parseTimeout(timeoutTok)
	if errMsg != "" {
		c.writeErr(errMsg)
		return
	}
	keys := [][]byte{source}

	var w *listWaiter
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if w != nil {
			c.srv.removeWaiter(w, keys)
		}
	}()
	for {
		unlock := c.lockTwoStripes(source, destination)
		if c.listWrongType(source) || c.listWrongType(destination) {
			unlock()
			c.writeErr(wrongType)
			return
		}
		v, ok := c.listPopEnd(source, fromHead)
		if ok {
			err := c.listPushEnd(destination, v, toHead)
			unlock()
			if err != nil {
				c.writeErr("ERR " + err.Error())
				return
			}
			c.writeBulk(v)
			c.srv.signalListKey(destination) // the pushed element may serve a client blocked on dst
			c.srv.signalListKey(source)      // source may still hold elements for another mover
			return
		}
		unlock()
		if !c.blockable {
			src := append([]byte(nil), source...)
			dst := append([]byte(nil), destination...)
			ft := append([]byte(nil), fromTok...)
			tt := append([]byte(nil), toTok...)
			tot := append([]byte(nil), timeoutTok...)
			if c.parkOnReactor(func() { c.blockingMove(src, dst, ft, tt, tot, name) }) {
				return
			}
			c.writeNil()
			return
		}
		if w == nil {
			w = &listWaiter{ch: make(chan struct{}, 1)}
			c.srv.addWaiter(w, keys)
			continue
		}
		c.flushBeforeBlock()
		if timer == nil && !forever {
			timer = time.NewTimer(d)
		}
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-w.ch:
		case <-timeout:
			c.writeNil()
			return
		case <-c.parkCancel:
			return
		}
	}
}

// cmdBLMPop implements BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count], the
// blocking LMPOP: it pops up to count elements from the chosen end of the first non-empty key,
// scanning left to right, and blocks on all the keys until one receives elements or the timeout
// fires. The served reply is LMPOP's two-element [key, elements] array; a timeout is a null
// array. A listed key holding a plain string is WRONGTYPE.
func (c *connState) cmdBLMPop(argv [][]byte) {
	// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'blmpop' command")
		return
	}
	d, forever, errMsg := parseTimeout(argv[1])
	if errMsg != "" {
		c.writeErr(errMsg)
		return
	}
	numkeys, err := atoi64(argv[2])
	if err != nil || numkeys <= 0 {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	// After timeout and numkeys: numkeys keys, then the direction, then an optional COUNT count.
	if int64(len(argv)) < 3+numkeys+1 {
		c.writeErr("ERR syntax error")
		return
	}
	keys := argv[3 : 3+numkeys]
	rest := argv[3+numkeys:]
	atHead, ok := parseLR(rest[0])
	if !ok {
		c.writeErr("ERR syntax error")
		return
	}
	count := int64(1)
	switch len(rest) {
	case 1:
		// direction only, count defaults to 1
	case 3:
		if !eqFold(rest[1], "COUNT") {
			c.writeErr("ERR syntax error")
			return
		}
		n, err := atoi64(rest[2])
		if err != nil || n <= 0 {
			c.writeErr("ERR count should be greater than 0")
			return
		}
		count = n
	default:
		c.writeErr("ERR syntax error")
		return
	}

	var w *listWaiter
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if w != nil {
			c.srv.removeWaiter(w, keys)
		}
	}()
	for {
		for _, key := range keys {
			mu := &c.srv.incrMu[c.srv.stripe(key)]
			mu.Lock()
			if c.listWrongType(key) {
				mu.Unlock()
				c.writeErr(wrongType)
				return
			}
			c.listWinDrainEvict(key)
			head, tail, lpBytes, everLarge, hoff, have := c.listHeaderAt(key)
			if !have {
				mu.Unlock()
				continue
			}
			n := tail - head
			want := count
			if want > n {
				want = n
			}
			c.writeArrayHeader(2)
			c.writeBulk(key)
			c.writeArrayHeader(int(want))
			for i := int64(0); i < want; i++ {
				var pos int64
				if atHead {
					pos = head
					head++
				} else {
					pos = tail - 1
					tail--
				}
				v, _ := c.srv.store.TakeKind(c.listElemKey(key, pos), c.vbuf[:0], kindListElem)
				c.vbuf = v
				lpBytes -= uint64(listEntrySize(v))
				c.writeBulk(v)
			}
			c.listWriteBackHeader(key, head, tail, lpBytes, everLarge, hoff)
			mu.Unlock()
			c.srv.signalListKey(key) // the key may still hold elements for another waiter
			return
		}
		if !c.blockable {
			ac := dupArgv(argv)
			if c.parkOnReactor(func() { c.cmdBLMPop(ac) }) {
				return
			}
			c.writeNilArray()
			return
		}
		if w == nil {
			w = &listWaiter{ch: make(chan struct{}, 1)}
			c.srv.addWaiter(w, keys)
			continue
		}
		c.flushBeforeBlock()
		if timer == nil && !forever {
			timer = time.NewTimer(d)
		}
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-w.ch:
		case <-timeout:
			c.writeNilArray()
			return
		case <-c.parkCancel:
			return
		}
	}
}
