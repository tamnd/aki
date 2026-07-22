// CLIENT PAUSE suspends command processing for a bounded window, the redis
// primitive an operator leans on before a failover so in-flight writes settle
// (doc 17 section 13's connection-control lane). It is network-layer state like
// pub/sub and MONITOR: the deadline lives on the server, and the gate sits in the
// read loop just ahead of the shard hop, so a paused command is held on its own
// reader goroutine without touching a shard worker. A server that was never
// paused pays one relaxed atomic load per command and nothing more.
//
// Two modes: ALL holds every command that reaches a shard, WRITE holds only the
// writes (dispatch.IsWrite) and lets reads flow. CLIENT UNPAUSE lifts the pause
// early. Because the gate sits behind the network-layer intercepts, the
// connection-control verbs (CLIENT, HELLO, QUIT, RESET) and pub/sub are never
// paused, which is what keeps CLIENT UNPAUSE reachable from any connection while
// an ALL pause is in force. Like the other intercept-driven features this is
// wired into the goroutine driver only; the reactor answers CLIENT PAUSE with the
// unknown-subcommand reply, the same pre-existing gap pub/sub and MONITOR carry.
package drivers

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/dispatch"
	"github.com/tamnd/aki/f3srv/resp"
)

// pauseState is the server's CLIENT PAUSE deadline and mode. until is the unix
// milli the pause lifts at, zero when not paused; writeOnly picks WRITE mode
// over ALL. Both are plain atomics: the read loop reads them on the hot path, a
// CLIENT PAUSE/UNPAUSE handler writes them from another connection's reader
// goroutine.
type pauseState struct {
	until     atomic.Int64
	writeOnly atomic.Bool
}

// active reports whether a pause is currently armed, the one relaxed load the
// read loop does per command. A stale true (the deadline just passed) costs only
// the wait call below, which lifts it; a stale false never happens, since the
// arming store publishes the deadline before the handler returns its +OK.
func (p *pauseState) active() bool { return p.until.Load() != 0 }

// arm sets a pause for ms milliseconds in the given mode. It writes the mode
// before the deadline so a read loop that observes the deadline observes the
// matching mode; the reverse order could briefly hold a read under a WRITE
// pause, which arm avoids.
func (p *pauseState) arm(ms int64, writeOnly bool) {
	p.writeOnly.Store(writeOnly)
	p.until.Store(time.Now().UnixMilli() + ms)
}

// lift clears the pause (CLIENT UNPAUSE).
func (p *pauseState) lift() { p.until.Store(0) }

// wait holds the calling reader goroutine until the pause lifts, either because
// its deadline passed or because CLIENT UNPAUSE cleared it. verb is the command
// about to run: under WRITE mode a non-write returns at once so reads keep
// flowing. It sleeps in short slices and re-reads the deadline each time, so an
// UNPAUSE from another connection is honoured within a slice rather than only at
// the deadline. A pause that has already expired is cleared here on the first
// look, so the next command sees active() false again.
func (p *pauseState) wait(verb []byte) {
	if p.writeOnly.Load() && !dispatch.IsWrite(verb) {
		return
	}
	for {
		until := p.until.Load()
		if until == 0 {
			return
		}
		now := time.Now().UnixMilli()
		if now >= until {
			p.until.CompareAndSwap(until, 0)
			return
		}
		d := until - now
		if d > pauseSliceMs {
			d = pauseSliceMs
		}
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
}

// pauseSliceMs bounds how long the gate sleeps between deadline checks, so an
// UNPAUSE is picked up promptly without busy-waiting.
const pauseSliceMs = 20

// doClientPause answers CLIENT PAUSE <timeout> [WRITE|ALL]: arm a pause for
// timeout milliseconds, defaulting to ALL when no mode token follows. A missing
// or non-integer timeout, a negative timeout, or an unknown mode token is the
// error redis gives. The +OK is a solicited reply, so it rides InlineReply in
// pipeline order; the command itself is answered in the intercept ahead of the
// gate, so it never pauses itself.
func (s *Server) doClientPause(c *shard.Conn, args [][]byte) {
	if len(args) != 3 && len(args) != 4 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|pause' command"))
		return
	}
	ms, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || ms < 0 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR timeout is not an integer or out of range"))
		return
	}
	writeOnly := false
	if len(args) == 4 {
		switch {
		case eqFold(args[3], "WRITE"):
			writeOnly = true
		case eqFold(args[3], "ALL"):
			writeOnly = false
		default:
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
	}
	s.pauses.arm(ms, writeOnly)
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// doClientUnpause answers CLIENT UNPAUSE: lift any pause and confirm +OK. It
// takes no arguments; a tail is the arity error. Answered in the intercept ahead
// of the gate so it flows even while an ALL pause is in force.
func (s *Server) doClientUnpause(c *shard.Conn, args [][]byte) {
	if len(args) != 2 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|unpause' command"))
		return
	}
	s.pauses.lift()
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
}
