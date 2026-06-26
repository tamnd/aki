package command

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// MONITOR streams every command the server processes to the issuing connection
// as a running debug feed. A client that sends MONITOR gets +OK and then, for
// each command any client runs, a status line of the form
//
//	1700000000.123456 [0 127.0.0.1:54321] "SET" "foo" "bar"
//
// the timestamp with microseconds, the database and client address in brackets,
// then the command verb and every argument quoted. This is the same surface
// redis-cli MONITOR shows. The feed is best effort: it is meant for debugging a
// live server, not for durability, so a delivery to a closed monitor is dropped.
//
// Admin commands and the few commands that would leak a secret or duplicate the
// feed (AUTH, EXEC, RESET, MONITOR itself) are not shown, matching Redis, which
// flags them skip-monitor. The hot path pays nothing when no monitor is attached:
// a single atomic load gates the whole feed.

// monitorRegistry holds the set of connections in monitor mode. The count mirror
// is an atomic so runCommand can skip the feed with one load when the set is
// empty, the common case.
type monitorRegistry struct {
	mu    sync.RWMutex
	conns map[uint64]*networking.Conn
	count atomic.Int32
}

func newMonitorRegistry() *monitorRegistry {
	return &monitorRegistry{conns: map[uint64]*networking.Conn{}}
}

// add puts a connection into monitor mode.
func (m *monitorRegistry) add(c *networking.Conn) {
	m.mu.Lock()
	m.conns[c.ID()] = c
	m.count.Store(int32(len(m.conns)))
	m.mu.Unlock()
}

// remove takes a connection out of monitor mode, called when it disconnects.
func (m *monitorRegistry) remove(id uint64) {
	m.mu.Lock()
	if _, ok := m.conns[id]; ok {
		delete(m.conns, id)
		m.count.Store(int32(len(m.conns)))
	}
	m.mu.Unlock()
}

// active reports whether any connection is monitoring, the gate runCommand reads
// before doing any feed work.
func (m *monitorRegistry) active() bool { return m.count.Load() > 0 }

func monitorCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "monitor", Group: GroupServer, Since: "1.0.0",
			Arity: 1, Flags: FlagAdmin | FlagLoading | FlagStale | FlagNoScript,
			Handler: func(ctx *Ctx) { ctx.d.handleMonitor(ctx) }},
	}
}

// handleMonitor puts the calling connection into monitor mode and replies OK. The
// connection still owns its own goroutine; the feed reaches it through Deliver
// from the goroutines running other clients' commands.
func (d *Dispatcher) handleMonitor(ctx *Ctx) {
	sess, ok := ctx.Conn.Session().(*session)
	if ok {
		sess.isMonitor = true
	}
	d.monitors.add(ctx.Conn)
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// feedMonitors writes one command to every attached monitor. It runs at the start
// of runCommand for an ordinary command and is gated by monitors.active(), so a
// server with no monitor attached never builds a line. The monitor's own
// connection does not run commands while monitoring, so it never feeds itself.
func (d *Dispatcher) feedMonitors(ctx *Ctx, cmd *CmdDesc) {
	if !d.monitors.active() || d.loading.Load() {
		return
	}
	if cmd.Flags.Has(FlagAdmin) || skipMonitor[cmd.Name] {
		return
	}
	line := formatMonitorLine(time.Now(), ctx.Conn.DB(), ctx.Conn.RemoteAddr(), ctx.Argv)

	d.monitors.mu.RLock()
	var dead []uint64
	for id, conn := range d.monitors.conns {
		if err := conn.Deliver(line); err != nil {
			dead = append(dead, id)
		}
	}
	d.monitors.mu.RUnlock()
	for _, id := range dead {
		d.monitors.remove(id)
	}
}

// skipMonitor names the commands Redis hides from the monitor feed because they
// would leak a credential (AUTH, HELLO with AUTH) or duplicate the feed (EXEC
// replays its queued commands, each fed on its own; RESET and MONITOR are control
// commands).
var skipMonitor = map[string]bool{
	"auth":    true,
	"exec":    true,
	"reset":   true,
	"monitor": true,
}

// formatMonitorLine builds the RESP simple-string a monitor receives for one
// command: "<sec>.<usec> [<db> <addr>] "<arg0>" "<arg1>" ...\r\n". Each argument
// is quoted and non-printable bytes are escaped the way Redis escapes them, so
// binary-safe arguments stay on one line.
func formatMonitorLine(now time.Time, db int, addr string, argv [][]byte) []byte {
	var b []byte
	b = append(b, '+')
	b = strconv.AppendInt(b, now.Unix(), 10)
	b = append(b, '.')
	// Microseconds, always six digits, zero padded.
	usec := now.Nanosecond() / 1000
	var pad [6]byte
	for i := 5; i >= 0; i-- {
		pad[i] = byte('0' + usec%10)
		usec /= 10
	}
	b = append(b, pad[:]...)
	b = append(b, " ["...)
	b = strconv.AppendInt(b, int64(db), 10)
	b = append(b, ' ')
	b = append(b, addr...)
	b = append(b, ']')
	for _, arg := range argv {
		b = append(b, ' ')
		b = appendQuotedArg(b, arg)
	}
	b = append(b, '\r', '\n')
	return b
}

// appendQuotedArg appends one argument wrapped in double quotes with Redis-style
// escaping (sdscatrepr): the control characters get their backslash form, a quote
// and a backslash are escaped, and any other non-printable byte becomes \xHH.
func appendQuotedArg(b, arg []byte) []byte {
	const hex = "0123456789abcdef"
	b = append(b, '"')
	for _, c := range arg {
		switch c {
		case '\\', '"':
			b = append(b, '\\', c)
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		case 7:
			b = append(b, '\\', 'a')
		case 8:
			b = append(b, '\\', 'b')
		default:
			if c >= 0x20 && c < 0x7f {
				b = append(b, c)
			} else {
				b = append(b, '\\', 'x', hex[c>>4], hex[c&0x0f])
			}
		}
	}
	return append(b, '"')
}
