package f1srv

import (
	"net"
	"strconv"
	"sync"
	"time"
)

// connState is one connection's parse-dispatch-reply state. rbuf holds bytes read
// from the socket that have not yet been consumed into a complete command; argv is
// reused across commands and points directly into rbuf, so a command costs no
// per-argument allocation. out is the batched reply buffer: every reply writer appends
// to it and the driver flushes it once per drained batch, so a pipeline of N commands
// is one read and one write. Keeping the replies in a plain byte slice rather than a
// bufio.Writer bound to the net.Conn is what lets the goroutine driver and the epoll
// reactor share the exact same parse-dispatch-reply code and differ only in who reads
// the socket and who flushes out.
type connState struct {
	srv  *Server
	conn net.Conn
	// id is this connection's unique identifier, assigned once at accept from the server's
	// monotonic counter, the value CLIENT ID reports. name is the label CLIENT SETNAME assigns
	// and CLIENT GETNAME reads back; it is nil until a name is set, which is the nil-reply case.
	id        int64
	name      []byte
	rbuf      []byte
	out       []byte // batched reply bytes, flushed once per drained batch
	wantClose bool   // QUIT sets this; the driver flushes out, then closes the socket
	blockable bool   // true on the goroutine driver, where a blocking command may park this
	//                  connection's own goroutine; false under the shared-goroutine reactor,
	//                  where a park would stall every other connection, so the blocking
	//                  commands there serve non-blocking (immediate element or nil).
	// nowMs is the wall-clock ms cached once per drained batch, the "now" every command in
	// the batch reads for expiry, like Redis server.mstime.
	nowMs    int64
	argv     [][]byte
	vbuf     []byte    // reused destination for GET/MGET value copies
	kbuf     []byte    // reused scratch for building composite collection element keys
	pbuf     []byte    // reused scratch for a collection enumeration prefix, held across a scan
	sbuf     []byte    // reused scratch for formatting a float score reply (ZSCORE/ZINCRBY)
	zscores  []float64 // reused scratch for a ZADD's parsed scores, one per score-member pair
	zkeys    [][]byte  // reused scratch for a ZRANGE window's score-family key subslices
	kscan    [][]byte  // reused scratch for a KEYS/SCAN/RANDOMKEY bucket-walk key batch
	wkeys    [][]byte  // reused scratch for a write command's touched-key list (WATCH signalling)
	hscanK   [][]byte  // reused scratch for a whole-hash read's element-key batch (HGETALL/HKEYS/HVALS)
	hscanO   []uint64  // reused scratch for a whole-hash value-carrying read's record-offset batch
	pushColl [][]byte  // reused scratch for a coalesced push run's elements, in arrival order
	pushBnd  []int     // reused scratch for the coalesced push run's per-command element boundaries
	popBufs  [][]byte  // reused scratch for a window pop run's claimed element slices, framed after the commit mutex releases

	// Transaction state (MULTI/EXEC/DISCARD/WATCH/UNWATCH). inMulti is set between MULTI
	// and EXEC/DISCARD; while it is set every non-transaction command is copied into
	// multiQueue and answered +QUEUED instead of running. multiAbort records that a queued
	// command could not be queued (an unknown command), which turns the EXEC into an
	// EXECABORT. watched is this connection's optimistic-lock set: each entry is a watched
	// key and the version it held when WATCH ran, and EXEC aborts if any of them has since
	// moved. dirtyCAS is unused as a field today; the version compare is done at EXEC time.
	inMulti    bool
	multiQueue [][][]byte
	multiAbort bool
	watched    []watchedKey

	// Pub/sub state. psChannels/psPatterns/psShard are this connection's own subscription
	// sets, allocated lazily so a connection that never subscribes carries no map. psMode is
	// the hot-path gate: it is true exactly when any of the three sets is non-empty, so the
	// flush path and the subscribe-context command restriction test one bool instead of three
	// map lengths. deliver is installed by the driver: on the goroutine driver it writes a
	// message frame straight to the socket under writeMu (a publisher on another goroutine and
	// this connection's own goroutine can both write), on the reactor driver it posts the frame
	// to this connection's owning loop, which serializes all writes and needs no lock. writeMu
	// guards the socket write on the goroutine driver and is taken at flush time only while
	// psMode is set.
	psChannels map[string]struct{}
	psPatterns map[string]struct{}
	psShard    map[string]struct{}
	psMode     bool
	deliver    func(frame []byte)
	writeMu    sync.Mutex
}

// loop reads from the socket, drains every complete command in the buffer, and
// flushes the batched replies, until the peer closes, a protocol error ends it, or a
// QUIT asks to close. This is the goroutine driver: it may park on Read and Write, both
// of which are fine on a dedicated per-connection goroutine.
func (c *connState) loop() {
	for {
		if !c.fill() {
			return
		}
		if !c.drain() {
			return
		}
		if len(c.out) > 0 {
			// While this connection is subscribed, a publisher running on another goroutine
			// may write a message frame straight to the same socket through deliver, so the
			// per-batch flush must serialize against it. A connection that never subscribes
			// keeps psMode false and takes no lock, so the GET/SET path is untouched.
			if c.psMode {
				c.writeMu.Lock()
				_, err := c.conn.Write(c.out)
				c.writeMu.Unlock()
				if err != nil {
					return
				}
			} else if _, err := c.conn.Write(c.out); err != nil {
				return
			}
			c.out = c.out[:0]
		}
		if c.wantClose {
			return
		}
	}
}

// fill reads one chunk from the socket into rbuf, growing the buffer when it is full
// so a value larger than the initial buffer still parses. It returns false on EOF or
// error.
func (c *connState) fill() bool {
	if len(c.rbuf) == cap(c.rbuf) {
		grown := make([]byte, len(c.rbuf), cap(c.rbuf)*2)
		copy(grown, c.rbuf)
		c.rbuf = grown
	}
	n, err := c.conn.Read(c.rbuf[len(c.rbuf):cap(c.rbuf)])
	if n > 0 {
		c.rbuf = c.rbuf[:len(c.rbuf)+n]
	}
	return err == nil
}

// drain parses and dispatches every complete command currently in rbuf, then
// outHighWater is the reply-buffer size past which the goroutine driver streams the batch
// out mid-drain instead of letting a pipeline of large replies grow one contiguous buffer
// without bound. A pipeline of whole-collection replies (HGETALL, SMEMBERS, HVALS, LRANGE,
// ZRANGE over a large collection) would otherwise append every reply into c.out and grow it
// through gigabytes of doubling copies before a single socket write, so the encode races the
// allocator instead of the network and P16 collapses well under P1. Flushing at the high-water
// mark bounds the buffer to this size and overlaps the socket write with the encoding of the
// rest of the pipeline. 256 KiB is large enough that an ordinary pipeline of small replies
// never trips it, so the GET/SET path still flushes exactly once per batch, and small enough
// that the buffer never grows into a multi-megabyte realloc storm.
const outHighWater = 256 << 10

// streamOut writes the reply buffer to the socket mid-drain and resets it, the high-water
// counterpart to the driver's once-per-batch flush. It serializes against a pub/sub publisher
// on another goroutine when this connection is subscribed, the same guard loop() takes, so a
// message frame and a batched reply never interleave on the socket. It only runs on the
// goroutine driver, where the caller has already checked c.conn is non-nil; the reactor owns
// its own non-blocking writes and never calls this. The write is best effort: a dead socket
// surfaces at the terminal per-batch flush, which ends the connection, so a mid-drain write
// error need not unwind the drain loop, and it only ever flushes at a command boundary where
// c.out holds whole replies, never a partial one.
func (c *connState) streamOut() {
	if c.psMode {
		c.writeMu.Lock()
		_, _ = c.conn.Write(c.out)
		c.writeMu.Unlock()
	} else {
		_, _ = c.conn.Write(c.out)
	}
	c.out = c.out[:0]
}

// compacts any partial trailing bytes to the front. It returns false on a protocol
// error that should close the connection.
func (c *connState) drain() bool {
	// Cache the wall clock once for the whole batch so every command in this drain sees
	// one consistent "now" for expiry, the way Redis caches server.mstime per event-loop
	// pass. A batch is short, so a single clock read amortizes over the whole pipeline and
	// no command can observe a key as both alive and dead within itself.
	c.nowMs = time.Now().UnixMilli()
	pos := 0
	for {
		argv, consumed, status := c.parse(c.rbuf[pos:])
		switch status {
		case parseOK:
			c.argv = argv
			pos += consumed
			// A run of same-key, same-verb pushes from this one connection collapses into a
			// single locked batch instead of one lock cycle per command. The gate is the plain
			// execution path (no open transaction, not in subscribe context) so MULTI queuing
			// and the subscribe-mode command restriction keep their own dispatch. A push under
			// either of those, or any other command, takes the ordinary one-command dispatch.
			if atHead, requireExisting, ok := pushVerb(argv); ok && !c.inMulti && !c.psMode {
				pos = c.drainPush(argv, atHead, requireExisting, pos)
			} else if atHead, ok := popVerb(argv); ok && !c.inMulti && !c.psMode {
				pos = c.drainPop(argv, atHead, pos)
			} else {
				c.dispatch(argv)
			}
			if c.wantClose {
				// QUIT: reply to it, then stop draining so a pipeline queued behind
				// QUIT is discarded, matching Redis, and let the driver flush and close.
				if pos > 0 {
					c.rbuf = append(c.rbuf[:0], c.rbuf[pos:]...)
				}
				return true
			}
			// Stream the batch out once its replies pass the high-water mark, so a pipeline of
			// large materialize replies never accumulates one unbounded reply buffer. Only the
			// goroutine driver (c.conn set) writes here; the reactor flushes its own way. c.out
			// holds only whole replies at this command boundary, so the flush never splits one.
			if c.conn != nil && len(c.out) >= outHighWater {
				c.streamOut()
			}
		case parseNeedMore:
			if pos > 0 {
				c.rbuf = append(c.rbuf[:0], c.rbuf[pos:]...)
			}
			return true
		case parseErr:
			c.writeErr("ERR Protocol error")
			return false
		}
	}
}

type parseStatus int

const (
	parseOK parseStatus = iota
	parseNeedMore
	parseErr
)

// parse reads one command from the front of b. A RESP multibulk (*N then N bulk
// strings) is the client path; a bare line is the inline path for a hand-typed
// client. argv slices point into b. consumed is the byte count of the parsed command.
func (c *connState) parse(b []byte) (argv [][]byte, consumed int, status parseStatus) {
	if len(b) == 0 {
		return nil, 0, parseNeedMore
	}
	if b[0] != '*' {
		return c.parseInline(b)
	}
	count, i, ok := readIntLine(b, 1)
	if !ok {
		return nil, 0, parseNeedMore
	}
	if count <= 0 {
		return c.argv[:0], i, parseOK // empty or null array: a no-op command
	}
	argv = c.argv[:0]
	for k := 0; k < count; k++ {
		if i >= len(b) {
			return nil, 0, parseNeedMore
		}
		if b[i] != '$' {
			return nil, 0, parseErr
		}
		blen, ni, ok := readIntLine(b, i+1)
		if !ok {
			return nil, 0, parseNeedMore
		}
		i = ni
		if blen < 0 {
			argv = append(argv, nil)
			continue
		}
		if i+blen+2 > len(b) {
			return nil, 0, parseNeedMore
		}
		argv = append(argv, b[i:i+blen])
		i += blen + 2 // bulk bytes plus trailing CRLF
	}
	return argv, i, parseOK
}

// parseInline handles a single space-separated line, enough for redis-cli's inline
// PING and manual probing. It is not the benchmark path.
func (c *connState) parseInline(b []byte) (argv [][]byte, consumed int, status parseStatus) {
	nl := indexByte(b, '\n')
	if nl < 0 {
		return nil, 0, parseNeedMore
	}
	line := b[:nl]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	argv = c.argv[:0]
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}
		start := i
		for i < len(line) && line[i] != ' ' {
			i++
		}
		argv = append(argv, line[start:i])
	}
	return argv, nl + 1, parseOK
}

// readIntLine parses an optionally-negative base-10 integer terminated by CRLF,
// starting at b[i]. It returns the value and the index just past the CRLF. ok is
// false when the terminator is not yet in the buffer, which the caller treats as
// "need more data".
func readIntLine(b []byte, i int) (val int, next int, ok bool) {
	neg := false
	if i < len(b) && b[i] == '-' {
		neg = true
		i++
	}
	v := 0
	digits := 0
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		v = v*10 + int(b[i]-'0')
		i++
		digits++
	}
	if i+1 >= len(b) {
		return 0, 0, false
	}
	if b[i] != '\r' || b[i+1] != '\n' {
		return 0, 0, false
	}
	if digits == 0 && !neg {
		return 0, 0, false
	}
	if neg {
		v = -v
	}
	return v, i + 2, true
}

func indexByte(b []byte, c byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// --- reply writers, all appending to the batched reply buffer c.out ---

// The writers append RESP bytes straight to c.out. There is no intermediate writer and
// no error to record: a byte slice append never fails, and the one flush per drain is
// where a socket error surfaces. The driver owns c.out's lifetime and resets it after
// each flush.
func (c *connState) writeSimple(s string) {
	c.out = append(c.out, '+')
	c.out = append(c.out, s...)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeErr(s string) {
	c.out = append(c.out, '-')
	c.out = append(c.out, s...)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeInt(n int64) {
	c.out = append(c.out, ':')
	c.out = strconv.AppendInt(c.out, n, 10)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeBulk(b []byte) {
	c.out = append(c.out, '$')
	c.out = strconv.AppendInt(c.out, int64(len(b)), 10)
	c.out = append(c.out, '\r', '\n')
	c.out = append(c.out, b...)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeNil() {
	c.out = append(c.out, "$-1\r\n"...)
}

// writeNilArray writes the RESP2 null array (*-1), the reply ZRANK WITHSCORE and the
// other array-returning commands use for an absent element, distinct from the null bulk
// string a scalar reply uses.
func (c *connState) writeNilArray() {
	c.out = append(c.out, "*-1\r\n"...)
}

func (c *connState) writeArrayHeader(n int) {
	c.out = append(c.out, '*')
	c.out = strconv.AppendInt(c.out, int64(n), 10)
	c.out = append(c.out, '\r', '\n')
}
