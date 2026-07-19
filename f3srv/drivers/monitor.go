// MONITOR streams every command the server processes to the issuing connection,
// the redis debugging feed (doc 17 section 13's introspection lane). Like pub/sub
// it is network-layer state: the monitor set lives on the server, not in the shard
// workers, so a busy monitor feed never slows a GET, and the feed is built from the
// command args the reader already parsed, before the shard hop. A monitored
// command is delivered as a RESP simple string, one per processed command, through
// the same DeliverOOB push path pub/sub uses, so it needs a writer that is not the
// reader (the pair shape or the reactor); the single shape's one goroutine sits in
// Read with nobody to drive the push, the same requirement pub/sub carries.
//
// Like the CLIENT/HELLO/pub/sub intercepts, MONITOR is wired only into the
// goroutine driver's read loop (the default production driver): the command is
// answered in connIntercept and the feed is tapped in readLoop. The reactor is the
// opt-in perf driver with no network-layer intercept, so there MONITOR falls
// through to the unknown-command answer and no feed runs, the same pre-existing gap
// pub/sub has. The default driver every real client meets answers it.
package drivers

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// doMonitor answers MONITOR: put the connection into monitor mode and confirm
// with +OK. From here the reader loop feeds it every command other connections
// process (monitor.go's feed tap), delivered through the OOB push path, so the
// connection needs a writer that is not its reader, the pair shape or the reactor.
// MONITOR takes no arguments; a tail is the arity error redis gives. The +OK is a
// solicited reply, so it rides InlineReply in pipeline order, and marking the flag
// after the reply keeps the connection's own MONITOR out of its first feed line.
func (s *Server) doMonitor(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) != 1 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'monitor' command"))
		return
	}
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
	cs.monitoring = true
	s.monitors.add(cs)
}

// stopMonitor takes a connection out of monitor mode, on RESET or teardown. It is
// idempotent: a connection that never monitored clears nothing.
func (s *Server) stopMonitor(cs *connState) {
	if cs.monitoring {
		cs.monitoring = false
	}
	s.monitors.remove(cs)
}

// monitorRegistry is the network-layer set of connections in MONITOR mode. One
// mutex guards the set because a command processed on one connection's reader
// goroutine fans out to monitors registered from other connections' readers,
// exactly the pub/sub registry's cross-goroutine shape. The atomic count mirrors
// len(conns) so the hot command path gates on a single relaxed load, never the
// mutex: a server with no monitor pays one atomic compare per command and no lock.
type monitorRegistry struct {
	mu    sync.Mutex
	conns map[*connState]struct{}
	count atomic.Int64
}

func newMonitorRegistry() *monitorRegistry {
	return &monitorRegistry{conns: make(map[*connState]struct{})}
}

// active reports whether any connection is monitoring, the command path's gate. A
// relaxed atomic load, so a server with no monitor never touches the mutex.
func (m *monitorRegistry) active() bool { return m.count.Load() > 0 }

// add puts a connection into the monitor set and restamps the count. Called from
// the connection's reader goroutine when it runs MONITOR.
func (m *monitorRegistry) add(cs *connState) {
	m.mu.Lock()
	m.conns[cs] = struct{}{}
	m.count.Store(int64(len(m.conns)))
	m.mu.Unlock()
}

// remove drops a connection from the monitor set, on RESET or teardown. A remove
// of a connection that was never monitoring is a no-op.
func (m *monitorRegistry) remove(cs *connState) {
	m.mu.Lock()
	if _, ok := m.conns[cs]; ok {
		delete(m.conns, cs)
		m.count.Store(int64(len(m.conns)))
	}
	m.mu.Unlock()
}

// feed formats one processed command and delivers it to every monitor. The target
// set is snapshotted under the lock and the deliveries run outside it, the pub/sub
// discipline: a slow wake never stalls a concurrent MONITOR, and the registry
// mutex never nests under a connection waker. The line is built once and every
// monitor copies the same bytes into its own node.
func (m *monitorRegistry) feed(now time.Time, db int, addr string, args [][]byte) {
	m.mu.Lock()
	if len(m.conns) == 0 {
		m.mu.Unlock()
		return
	}
	targets := make([]*shard.Conn, 0, len(m.conns))
	for cs := range m.conns {
		targets = append(targets, cs.sc)
	}
	m.mu.Unlock()
	line := appendMonitorLine(nil, now, db, addr, args)
	for _, sc := range targets {
		sc.DeliverOOB(line)
	}
}

// appendMonitorLine renders one monitor feed entry, the redis format: a RESP
// simple string carrying the command's unix timestamp with microseconds, the
// database and client address in brackets, and every argument quoted. The quoting
// escapes the bytes that would break a simple string (a bare CR or LF) or the
// quoting itself, so the whole line stays a single +...\r\n frame with no embedded
// terminator, which is what lets a monitored SET of a value holding a newline ride
// the simple-string reply safely.
func appendMonitorLine(dst []byte, now time.Time, db int, addr string, args [][]byte) []byte {
	dst = append(dst, '+')
	dst = strconv.AppendInt(dst, now.Unix(), 10)
	dst = append(dst, '.')
	// Microseconds, always six digits so 1.000042 does not render as 1.42.
	usec := now.Nanosecond() / 1000
	dst = appendPad6(dst, usec)
	dst = append(dst, " ["...)
	dst = strconv.AppendInt(dst, int64(db), 10)
	dst = append(dst, ' ')
	dst = append(dst, addr...)
	dst = append(dst, "]"...)
	for _, a := range args {
		dst = append(dst, ' ')
		dst = appendMonitorArg(dst, a)
	}
	dst = append(dst, '\r', '\n')
	return dst
}

// appendPad6 renders a microsecond value zero-padded to six digits.
func appendPad6(dst []byte, v int) []byte {
	var buf [6]byte
	for i := 5; i >= 0; i-- {
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return append(dst, buf[:]...)
}

// appendMonitorArg quotes one argument the way redis's sdscatrepr does: wrap in
// double quotes, backslash-escape the quote and the backslash, map the common
// control bytes to their C escapes, and render any other non-printable byte as
// \xHH. This keeps the feed line free of raw CR and LF so it stays one simple
// string, and it matches the shape a redis-cli MONITOR session prints.
func appendMonitorArg(dst []byte, a []byte) []byte {
	dst = append(dst, '"')
	for _, b := range a {
		switch b {
		case '\\', '"':
			dst = append(dst, '\\', b)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case 7:
			dst = append(dst, '\\', 'a')
		case 8:
			dst = append(dst, '\\', 'b')
		default:
			if b >= 0x20 && b < 0x7f {
				dst = append(dst, b)
			} else {
				const hex = "0123456789abcdef"
				dst = append(dst, '\\', 'x', hex[b>>4], hex[b&0xf])
			}
		}
	}
	dst = append(dst, '"')
	return dst
}
