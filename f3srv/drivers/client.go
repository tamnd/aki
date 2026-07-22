package drivers

import (
	"bytes"
	"net"
	"strconv"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/dispatch"
	"github.com/tamnd/aki/f3srv/resp"
)

// The connection-lifecycle surface (spec 2064/f3/11, the M11 command-closure
// milestone): CLIENT, HELLO, QUIT, and RESET, the verbs a client runs to open,
// label, reset, and close a connection. Each reads or clears per-connection
// state (the id stamped in register, the SETNAME label, the subscription set),
// and that state lives in the network layer, not the shard workers, the same
// split pub/sub uses. So they are intercepted here before the shard hop, and
// their solicited replies go out through InlineReply in pipeline order.
//
// This intercept is wired only into the goroutine driver's read loop, the
// default production driver. The reactor driver has no network-layer intercept
// (it dispatches straight to the shard hop), so on the reactor these fall
// through to the shard hop: CLIENT and HELLO reach the unknown-command answer
// (the same pre-existing gap pub/sub has there), while RESET still reaches its
// dispatch handler and QUIT the unknown-command answer. That is acceptable
// because the reactor is the opt-in perf driver a benchmark harness selects, and
// it never enters subscribe mode (SUBSCRIBE is not intercepted there either), so
// RESET has no connection state to unwind; the default driver every real client
// meets answers all four.
//
// HELLO declines protocol version 3. f3 speaks RESP2 only (RESP3 is deferred),
// so HELLO 3 answers NOPROTO rather than switching the connection into a
// reply-encoding it cannot produce. A bare HELLO or HELLO 2 confirms RESP2 with
// the standard handshake map.

// helloServerName and helloVersion are the server identity HELLO advertises. f3
// reports its own name and version rather than impersonating redis: the major
// client libraries key the handshake map by field and do not gate on the server
// being literally "redis", so honest identity costs no compatibility. INFO makes
// the same choice, carrying an f3 section instead of a forged redis_version.
const (
	helloServerName = "aki"
	helloVersion    = "0.1.0"
)

// connIntercept answers CLIENT, HELLO, QUIT, and RESET in the network layer,
// before dispatch.Dispatch, so none enter the shard hop. It returns true when it
// owned the command and false to let the normal dispatch run. It sits ahead of
// pubsubIntercept in the read loop, so a client may run these even in subscribe
// mode, which redis also allows for the handshake and lifecycle verbs.
func (s *Server) connIntercept(c *shard.Conn, cs *connState, args [][]byte) bool {
	switch {
	case eqFold(args[0], "CLIENT"):
		s.doClient(c, cs, args)
		return true
	case eqFold(args[0], "HELLO"):
		s.doHello(c, cs, args)
		return true
	case eqFold(args[0], "QUIT"):
		s.doQuit(c, cs, args)
		return true
	case eqFold(args[0], "RESET"):
		s.doReset(c, cs, args)
		return true
	case eqFold(args[0], "MONITOR"):
		s.doMonitor(c, cs, args)
		return true
	case eqFold(args[0], "DEBUG") && len(args) >= 2 && eqFold(args[1], "SLEEP"):
		s.doDebugSleep(c, args)
		return true
	}
	return false
}

// doDebugSleep answers DEBUG SLEEP seconds by blocking this connection for the
// requested time and then acknowledging with +OK. Test harnesses use it to make
// a command hang on purpose, so they can exercise client-side timeouts. It sleeps
// the connection's reader goroutine, so only this one connection stalls, not a
// shard worker or any other client; that is a narrower block than redis's whole-
// server sleep and serves the harness use exactly, since the harness watches this
// connection. Any other DEBUG subcommand falls through to the dispatch handler
// (debug.go). On the reactor driver, which has no intercept, DEBUG SLEEP reaches
// that dispatch handler instead and sleeps its shard worker.
func (s *Server) doDebugSleep(c *shard.Conn, args [][]byte) {
	if len(args) != 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'debug|sleep' command"))
		return
	}
	secs, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil || secs < 0 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR value is not a valid float"))
		return
	}
	time.Sleep(time.Duration(secs * float64(time.Second)))
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// doQuit answers QUIT: acknowledge with +OK, then mark the connection for close.
// The read loop returns after the next boundary flush, so the +OK reaches the
// client before the socket shuts, the redis contract. QUIT takes no arguments;
// redis ignores a tail, so any extra args are accepted.
func (s *Server) doQuit(c *shard.Conn, cs *connState, args [][]byte) {
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
	cs.quit = true
}

// doReset answers RESET: return the connection to a clean state and reply +RESET.
// f3 offers one database (SELECT accepts only 0) and no auth to unwind, so the
// state RESET clears here is any open MULTI transaction (dispatch.ResetTxn), the
// pub/sub subscription set, and the CLIENT SETNAME label, the per-connection state
// a session accumulates. Dropping the subscriptions also takes the connection out
// of subscribe mode.
// RESET takes no arguments, so a tail is the arity error redis gives, the same
// answer the shard-hop handler gave before this intercept caught the verb.
func (s *Server) doReset(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) != 1 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'reset' command"))
		return
	}
	s.pubsub.removeConn(cs)
	s.stopMonitor(cs)
	cs.setName(nil)
	dispatch.ResetTxn(c)
	_ = c.InlineReply(resp.AppendStatus(nil, "RESET"))
}

// validClientName reports whether every byte of a proposed CLIENT SETNAME name
// is a printable ASCII character other than space, matching redis: a name may
// hold only bytes in 0x21..0x7e, so spaces, newlines, control bytes and high
// bytes are all rejected. An empty name is allowed and clears the label.
func validClientName(name []byte) bool {
	for _, b := range name {
		if b < '!' || b > '~' {
			return false
		}
	}
	return true
}

// doClient handles the connection-identity subcommands of CLIENT: ID to learn
// the connection number, SETNAME/GETNAME to label it, SETINFO to advertise the
// client library, INFO/LIST to describe connections, and the NO-EVICT/NO-TOUCH
// flag toggles. Subcommands f3 does not model (KILL, PAUSE, TRACKING, ...) fall
// through to the unknown-subcommand error, the same reply redis gives, rather
// than a misleading OK. The reply is solicited, so it goes out through
// InlineReply in order.
func (s *Server) doClient(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) < 2 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client' command"))
		return
	}
	sub := args[1]
	switch {
	case eqFold(sub, "ID"):
		if len(args) != 2 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|id' command"))
			return
		}
		_ = c.InlineReply(resp.AppendInt(nil, int64(cs.id)))
	case eqFold(sub, "GETNAME"):
		if len(args) != 2 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|getname' command"))
			return
		}
		// An unnamed connection replies with the empty bulk, the shape redis 8.8
		// and valkey 9.1 both return; a named one replies with the name.
		_ = c.InlineReply(resp.AppendBulk(nil, cs.loadName()))
	case eqFold(sub, "SETNAME"):
		if len(args) != 3 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|setname' command"))
			return
		}
		if !validClientName(args[2]) {
			_ = c.InlineReply(resp.AppendError(nil, "ERR Client names cannot contain spaces, newlines or special characters."))
			return
		}
		// setName copies the name out of the parse buffer (reused for the next
		// command) and publishes it atomically; an empty name clears the label.
		cs.setName(args[2])
		_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
	case eqFold(sub, "SETINFO"):
		if len(args) != 4 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|setinfo' command"))
			return
		}
		// The only recognized attributes are the library name and version; f3
		// records neither, but it validates the option name so a client that
		// probes SETINFO sees the same accept/reject redis gives.
		if !eqFold(args[2], "LIB-NAME") && !eqFold(args[2], "LIB-VER") {
			_ = c.InlineReply(resp.AppendError(nil, "ERR Unrecognized option '"+string(args[2])+"'"))
			return
		}
		_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
	case eqFold(sub, "NO-EVICT"):
		s.clientOnOff(c, args, "no-evict")
	case eqFold(sub, "NO-TOUCH"):
		s.clientOnOff(c, args, "no-touch")
	case eqFold(sub, "GETREDIR"):
		if len(args) != 2 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|getredir' command"))
			return
		}
		// -1 when tracking is off; 0 when it is on with no redirection target; else
		// the target client id set through CLIENT TRACKING ON REDIRECT.
		id, _ := s.tracking.redirectState(cs)
		_ = c.InlineReply(resp.AppendInt(nil, id))
	case eqFold(sub, "TRACKING"):
		s.doClientTracking(c, cs, args)
	case eqFold(sub, "TRACKINGINFO"):
		s.doClientTrackingInfo(c, cs, args)
	case eqFold(sub, "CACHING"):
		s.doClientCaching(c, cs, args)
	case eqFold(sub, "INFO"):
		if len(args) != 2 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|info' command"))
			return
		}
		// CLIENT INFO describes THIS connection: every field it reports is the
		// connection's own network-layer identity, read on its own reader
		// goroutine, so it needs no lock and races with nobody. The reply is one
		// bulk string, the same shape redis 8.8 and valkey 9.1 return (CLIENT LIST
		// is the multi-line form, a later slice once cross-connection reads are
		// made safe).
		line := appendClientLine(nil, cs, "client|info")
		_ = c.InlineReply(resp.AppendBulk(nil, line))
	case eqFold(sub, "LIST"):
		s.doClientList(c, cs, args)
	case eqFold(sub, "KILL"):
		s.doClientKill(c, cs, args)
	case eqFold(sub, "REPLY"):
		s.doClientReply(c, cs, args)
	case eqFold(sub, "PAUSE"):
		s.doClientPause(c, args)
	case eqFold(sub, "UNPAUSE"):
		s.doClientUnpause(c, args)
	default:
		_ = c.InlineReply(resp.AppendError(nil, "ERR unknown subcommand '"+string(sub)+"'. Try CLIENT HELP."))
	}
}

// doClientList answers CLIENT LIST: one appendClientLine per live connection,
// newline-terminated, in a single bulk string, the multi-line form of CLIENT
// INFO. It supports the two redis filters, TYPE and ID: TYPE normal keeps the
// connections not in subscribe mode and TYPE pubsub keeps those that are, while
// TYPE master/replica match nothing because f3 runs no replication; ID keeps the
// listed connection ids. The live registry is snapshotted under netMu and the
// lines are rendered outside it, so a long list never blocks admission, and every
// field a line reads off another connection is either immutable or atomic, so the
// walk is race-free. The cmd field is honest: this connection's own line shows
// client|list, and the others show NULL because f3 keeps no per-connection
// last-command record.
func (s *Server) doClientList(c *shard.Conn, cs *connState, args [][]byte) {
	var wantType []byte
	var idFilter map[uint64]struct{}
	for i := 2; i < len(args); {
		switch {
		case eqFold(args[i], "TYPE"):
			if i+1 >= len(args) {
				_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
				return
			}
			wantType = args[i+1]
			if !eqFold(wantType, "NORMAL") && !eqFold(wantType, "MASTER") &&
				!eqFold(wantType, "REPLICA") && !eqFold(wantType, "SLAVE") &&
				!eqFold(wantType, "PUBSUB") {
				_ = c.InlineReply(resp.AppendError(nil, "ERR Unknown client type '"+string(wantType)+"'"))
				return
			}
			i += 2
		case eqFold(args[i], "ID"):
			idFilter = make(map[uint64]struct{})
			for i++; i < len(args); i++ {
				n, err := strconv.ParseUint(string(args[i]), 10, 64)
				if err != nil {
					_ = c.InlineReply(resp.AppendError(nil, "ERR Invalid client ID"))
					return
				}
				idFilter[n] = struct{}{}
			}
		default:
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
	}

	s.netMu.Lock()
	snap := make([]*connState, 0, len(s.netLive))
	for other := range s.netLive {
		snap = append(snap, other)
	}
	s.netMu.Unlock()

	var out []byte
	for _, other := range snap {
		if idFilter != nil {
			if _, ok := idFilter[other.id]; !ok {
				continue
			}
		}
		if wantType != nil {
			// f3 has no replication, so the master and replica types match nothing.
			if eqFold(wantType, "MASTER") || eqFold(wantType, "REPLICA") || eqFold(wantType, "SLAVE") {
				continue
			}
			if (other.subCount.Load()+other.psubCount.Load()+other.ssubCount.Load() > 0) != eqFold(wantType, "PUBSUB") {
				continue
			}
		}
		cmd := "NULL"
		if other == cs {
			cmd = "client|list"
		}
		out = appendClientLine(out, other, cmd)
		out = append(out, '\n')
	}
	_ = c.InlineReply(resp.AppendBulk(nil, out))
}

// doClientKill answers CLIENT KILL, which drops connections. It has two forms,
// distinguished the way redis does, by argument count.
//
// Old form, CLIENT KILL <addr:port>: kill the one connection at that remote
// endpoint. It replies +OK if a connection matched or "ERR No such client" if
// none did.
//
// New form, CLIENT KILL <filter> <value> [<filter> <value> ...]: kill every
// connection matching all the given filters and reply with the count. The
// filters f3 models honestly are ID (client id), ADDR (remote endpoint), LADDR
// (local endpoint), MAXAGE (age in seconds at least this large), TYPE, USER, and
// SKIPME. TYPE normal keeps the connections not in subscribe mode and TYPE
// pubsub keeps those in it; master, replica, and slave match nothing because f3
// runs no replication. USER matches only the default user, since f3 is
// passwordless, so USER default keeps every connection and any other name keeps
// none. SKIPME yes (the default) spares the calling connection; SKIPME no lets
// it be killed too.
//
// Killing another connection closes its socket through the killConn hook stamped
// at admission: the target's blocking Read then returns an error and its own
// read loop tears the connection down. Killing the caller instead sets its quit
// flag, so the reply flushes before the socket closes (redis's
// CLOSE_AFTER_REPLY). The live registry is snapshotted under netMu and the
// closes happen outside it, so a kill never holds the admission lock across a
// socket close. This runs on the goroutine drivers only, like the other
// intercepted verbs; the event-loop drivers have no intercept, so CLIENT KILL
// there falls to the unknown-subcommand error and never reaches a nil killConn.
func (s *Server) doClientKill(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) < 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|kill' command"))
		return
	}

	// Old form: a lone token is the address to kill.
	if len(args) == 3 {
		var target *connState
		want := string(args[2])
		s.netMu.Lock()
		for other := range s.netLive {
			if other.addr == want {
				target = other
				break
			}
		}
		s.netMu.Unlock()
		if target == nil {
			_ = c.InlineReply(resp.AppendError(nil, "ERR No such client"))
			return
		}
		s.killConn(cs, target)
		_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
		return
	}

	// New form: filter/value pairs.
	var (
		haveID    bool
		wantID    uint64
		wantAddr  string
		wantLaddr string
		wantType  []byte
		userOnly  bool // a USER filter naming a non-default user matches nothing
		skipme    = true
		haveMaxA  bool
		maxAge    int64
	)
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
		opt, val := args[i], args[i+1]
		switch {
		case eqFold(opt, "ID"):
			n, err := strconv.ParseUint(string(val), 10, 64)
			if err != nil {
				_ = c.InlineReply(resp.AppendError(nil, "ERR client-id should be greater than 0"))
				return
			}
			haveID, wantID = true, n
		case eqFold(opt, "ADDR"):
			wantAddr = string(val)
		case eqFold(opt, "LADDR"):
			wantLaddr = string(val)
		case eqFold(opt, "TYPE"):
			if !eqFold(val, "NORMAL") && !eqFold(val, "MASTER") &&
				!eqFold(val, "REPLICA") && !eqFold(val, "SLAVE") &&
				!eqFold(val, "PUBSUB") {
				_ = c.InlineReply(resp.AppendError(nil, "ERR Unknown client type '"+string(val)+"'"))
				return
			}
			wantType = val
		case eqFold(opt, "USER"):
			// f3 is passwordless, so every connection is the default user. Any
			// other name matches nothing. eqFold folds the value to upper case, so
			// the word it compares against must be upper case too.
			userOnly = !eqFold(val, "DEFAULT")
		case eqFold(opt, "SKIPME"):
			switch {
			case eqFold(val, "YES"):
				skipme = true
			case eqFold(val, "NO"):
				skipme = false
			default:
				_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
				return
			}
		case eqFold(opt, "MAXAGE"):
			n, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
				return
			}
			haveMaxA, maxAge = true, n
		default:
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
	}

	now := time.Now().Unix()
	s.netMu.Lock()
	var targets []*connState
	for other := range s.netLive {
		if haveID && other.id != wantID {
			continue
		}
		if wantAddr != "" && other.addr != wantAddr {
			continue
		}
		if wantLaddr != "" && other.laddr != wantLaddr {
			continue
		}
		if wantType != nil {
			if eqFold(wantType, "MASTER") || eqFold(wantType, "REPLICA") || eqFold(wantType, "SLAVE") {
				continue
			}
			isPubsub := other.subCount.Load()+other.psubCount.Load()+other.ssubCount.Load() > 0
			if isPubsub != eqFold(wantType, "PUBSUB") {
				continue
			}
		}
		if userOnly {
			continue
		}
		if haveMaxA {
			age := now - other.connUnix
			if age < 0 {
				age = 0
			}
			if age < maxAge {
				continue
			}
		}
		if skipme && other == cs {
			continue
		}
		targets = append(targets, other)
	}
	s.netMu.Unlock()

	for _, t := range targets {
		s.killConn(cs, t)
	}
	_ = c.InlineReply(resp.AppendInt(nil, int64(len(targets))))
}

// killConn drops target on behalf of caller. If the target is the calling
// connection, it defers the close to the quit flag so the KILL reply flushes
// first (CLOSE_AFTER_REPLY); otherwise it closes the target's socket, which ends
// the target's blocking Read and lets its own read loop tear the connection
// down. The killConn hook is nil on the event-loop drivers, but those have no
// intercept so CLIENT KILL never runs there; the guard keeps a stray nil from
// panicking regardless.
func (s *Server) killConn(caller, target *connState) {
	if target == caller {
		caller.quit = true
		return
	}
	if target.killConn != nil {
		_ = target.killConn()
	}
}

// doClientReply answers CLIENT REPLY ON|OFF|SKIP, the per-connection switch that
// mutes command replies (spec 2064/f3/11). ON is the default: every command
// replies, and running ON while muted re-enables replies and acknowledges +OK.
// OFF mutes every reply until the next ON, and produces no reply for itself.
// SKIP mutes exactly the reply of the command that follows it, and produces no
// reply for itself either.
//
// The suppression is stamped per command in the read loop (readLoop computes it
// from these flags and calls shard.Conn.SetReplySilent before each dispatch), so
// the shard still runs the command, its writes still take effect, and the reply
// reorder cursor still advances; only the bytes are dropped at emit time. This
// handler just moves the flags: OFF latches replyOff, SKIP arms replySkip for the
// one following command, ON clears both. ON's own +OK must reach the wire even
// when the connection was muted, so it un-silences this reply explicitly before
// answering (the read loop set silentNext from the still-set replyOff).
func (s *Server) doClientReply(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) != 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|reply' command"))
		return
	}
	switch {
	case eqFold(args[2], "ON"):
		cs.replyOff = false
		cs.replySkip = false
		// The read loop may have stamped this command silent off the still-set
		// replyOff; ON is the one command that always answers, so clear it here.
		c.SetReplySilent(false)
		_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
	case eqFold(args[2], "OFF"):
		cs.replyOff = true
		// No reply: OFF acknowledges nothing, it just mutes.
	case eqFold(args[2], "SKIP"):
		// Arm the next command's suppression. If the connection is already OFF,
		// SKIP is a no-op the same way redis treats it: everything is muted
		// anyway. No reply for SKIP itself.
		if !cs.replyOff {
			cs.replySkip = true
		}
	default:
		_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
	}
}

// clientOnOff handles CLIENT NO-EVICT and NO-TOUCH, which take one ON or OFF
// argument. Both are accepted and answered OK without changing behaviour: f3
// runs no eviction, so there is nothing to protect a connection from, and it
// keeps no LRU/LFU stats to skip touching.
func (s *Server) clientOnOff(c *shard.Conn, args [][]byte, name string) {
	if len(args) != 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|"+name+"' command"))
		return
	}
	if !eqFold(args[2], "ON") && !eqFold(args[2], "OFF") {
		_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
		return
	}
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// doClientTracking answers CLIENT TRACKING ON|OFF, the enable switch for client-
// side caching (spec 2064/f3/17). ON arms this connection in one of two shapes.
// Default mode (with the OPTIN/OPTOUT recording gates) records every key the
// connection reads through a cacheable command and pushes one RESP3 invalidate on
// the first write to such a key. BCAST mode registers a prefix set instead and
// pushes an invalidate for every write to a key matching a prefix, statelessly. Both
// honour NOLOOP (skip the invalidation for a key this connection wrote). OFF disarms
// the connection and drops its registration. Tracking requires RESP3 unless it names
// a REDIRECT target, the second connection its invalidations are delivered to (as
// pub/sub messages on __redis__:invalidate), which lets a RESP2 client cache. A
// positive REDIRECT id must name a live connection; 0 is the explicit no-redirect.
// Re-running ON on an already-tracking connection, or OFF on one that never tracked,
// is answered OK, matching redis's idempotent switch.
func (s *Server) doClientTracking(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) < 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|tracking' command"))
		return
	}
	onoff := args[2]
	if !eqFold(onoff, "ON") && !eqFold(onoff, "OFF") {
		_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
		return
	}
	// Parse the option tail. This slice supports the two recording modes OPTIN and
	// OPTOUT, NOLOOP, and BCAST with its PREFIX list; REDIRECT is a later slice,
	// refused with an honest not-yet error rather than a silent accept so a client
	// cannot believe it configured a mode f3 does not run.
	var optin, optout, noloop, bcast, hasRedirect bool
	var redirectID uint64
	var prefixes [][]byte
	for i := 3; i < len(args); i++ {
		switch {
		case eqFold(args[i], "OPTIN"):
			optin = true
		case eqFold(args[i], "OPTOUT"):
			optout = true
		case eqFold(args[i], "NOLOOP"):
			noloop = true
		case eqFold(args[i], "BCAST"):
			bcast = true
		case eqFold(args[i], "PREFIX"):
			// PREFIX takes one argument, the prefix string, and may repeat.
			if i+1 >= len(args) {
				_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
				return
			}
			i++
			prefixes = append(prefixes, args[i])
		case eqFold(args[i], "REDIRECT"):
			// REDIRECT takes one argument, the target client id. 0 means "no redirect"
			// (deliver to this connection directly); a positive id names the connection
			// the invalidations are pushed to instead, the mechanism a RESP2 client uses
			// to receive them on a second, subscribed connection.
			if i+1 >= len(args) {
				_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
				return
			}
			i++
			n, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil || n < 0 {
				_ = c.InlineReply(resp.AppendError(nil, "ERR Invalid client ID"))
				return
			}
			hasRedirect = true
			redirectID = uint64(n)
		default:
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
	}
	if optin && optout {
		_ = c.InlineReply(resp.AppendError(nil, "ERR You can't specify both OPTIN mode and OPTOUT mode"))
		return
	}
	if bcast && (optin || optout) {
		_ = c.InlineReply(resp.AppendError(nil, "ERR OPTIN and OPTOUT are not compatible with BCAST"))
		return
	}
	if len(prefixes) > 0 && !bcast {
		_ = c.InlineReply(resp.AppendError(nil, "ERR PREFIX option requires BCAST mode to be enabled"))
		return
	}
	// Prefixes for a single client must not overlap: if one is a prefix of another a
	// key could match both and the client would get two invalidations for one write,
	// so redis rejects the pair at registration.
	if msg := checkPrefixOverlap(prefixes); msg != "" {
		_ = c.InlineReply(resp.AppendError(nil, msg))
		return
	}
	if eqFold(onoff, "OFF") {
		if optin || optout || noloop || bcast || hasRedirect || len(prefixes) > 0 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
			return
		}
		if cs.tracking != nil {
			s.tracking.removeConn(cs)
			cs.tracking = nil
		}
		_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
		return
	}
	// ON: tracking rides RESP3 pushes, so a RESP2 connection cannot enable it unless it
	// names a redirection target, in which case the invalidations go to that second
	// connection (which carries them as pub/sub messages) and this one stays RESP2.
	// REDIRECT 0 is the explicit "no redirect", so it does not lift the RESP3 rule.
	if !c.Resp3() && redirectID == 0 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR Client tracking can be enabled only in RESP3 mode or when a redirection client is specified via the 'REDIRECT' option"))
		return
	}
	// Resolve the redirection target: a positive id must name a live connection, else
	// redis rejects the enable. id 0 (or no REDIRECT) leaves redir nil, direct delivery.
	var redir *connState
	if redirectID != 0 {
		redir = s.connByID(redirectID)
		if redir == nil {
			_ = c.InlineReply(resp.AppendError(nil, "ERR The client ID you want redirect to does not exist"))
			return
		}
	}
	if bcast {
		// BCAST is a distinct registration (a prefix set, no recorded-key table), so a
		// connection crossing into or re-declaring it re-arms clean rather than layering
		// onto stale default-mode state.
		if cs.tracking != nil {
			s.tracking.removeConn(cs)
			cs.tracking = nil
		}
		s.tracking.armBcast(cs, prefixes, redir)
		cs.tracking.noloop.Store(noloop)
		_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
		return
	}
	// Default / OPTIN / OPTOUT. A connection leaving BCAST re-arms clean the other way.
	if cs.tracking != nil && cs.tracking.bcast {
		s.tracking.removeConn(cs)
		cs.tracking = nil
	}
	if cs.tracking == nil {
		s.tracking.arm(cs, redir)
	} else {
		// Already tracking default mode: a re-run of ON updates the redirect target in
		// place (changing it, or clearing it with REDIRECT 0) without disarming.
		s.tracking.setRedirect(cs, redir)
	}
	cs.tracking.optin = optin
	cs.tracking.optout = optout
	cs.tracking.noloop.Store(noloop)
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// connByID resolves a CLIENT ID to its live connection state, or nil when no live
// connection carries that id (it disconnected, or the id was never issued). It scans
// the live registry under netMu, the same snapshot CLIENT LIST walks; the scan is
// linear but runs only on the cold CLIENT TRACKING REDIRECT path, never per command.
// It takes netMu alone and calls nothing under it, so it never nests with the
// tracking registry mutex the caller takes next.
func (s *Server) connByID(id uint64) *connState {
	s.netMu.Lock()
	defer s.netMu.Unlock()
	for other := range s.netLive {
		if other.id == id {
			return other
		}
	}
	return nil
}

// checkPrefixOverlap reports redis's prefix-overlap error when one registered
// prefix is a prefix of another (the empty prefix overlaps every prefix), else the
// empty string. Prefixes for a single client must not overlap so a key never
// matches two of them and draws a doubled invalidation.
func checkPrefixOverlap(prefixes [][]byte) string {
	for i := 0; i < len(prefixes); i++ {
		for j := i + 1; j < len(prefixes); j++ {
			a, b := prefixes[i], prefixes[j]
			if bytes.HasPrefix(a, b) || bytes.HasPrefix(b, a) {
				return "ERR Prefix '" + string(a) + "' overlaps with an existing prefix '" + string(b) + "'. Prefixes for a single client must not overlap"
			}
		}
	}
	return ""
}

// doClientTrackingInfo answers CLIENT TRACKINGINFO, the introspection command that
// reports this connection's client-side-caching state. It returns the three fields
// redis models: flags (the mode tokens on/bcast/optin/optout/noloop/broken_redirect,
// or off), redirect (the target client id, 0 when tracking on with no redirect, -1
// when off), and prefixes (the BCAST prefix list). Under RESP3 it is a real map
// frame; under RESP2 the flat six-element array redis falls back to. The redirect
// fields are read under the tracking mutex (a foreign teardown can set broken); the
// rest is this connection's own reader-owned state.
func (s *Server) doClientTrackingInfo(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) != 2 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|trackinginfo' command"))
		return
	}
	on := cs.tracking != nil
	// redirect is the target client id (0 when tracking on with no redirect, -1 when
	// off); broken is set once a redirect target has disconnected. Both are read under
	// the registry mutex since a foreign teardown writes the broken flag.
	redirID, broken := s.tracking.redirectState(cs)
	// The flags array renders redis's mode tokens: off when disabled, else on plus
	// bcast, the optin/optout mode token when one is set, noloop when it is armed, and
	// broken_redirect when the redirect target has gone away.
	var flags []string
	if on {
		flags = append(flags, "on")
		if cs.tracking.bcast {
			flags = append(flags, "bcast")
		}
		if cs.tracking.optin {
			flags = append(flags, "optin")
		}
		if cs.tracking.optout {
			flags = append(flags, "optout")
		}
		if cs.tracking.noloop.Load() {
			flags = append(flags, "noloop")
		}
		if broken {
			flags = append(flags, "broken_redirect")
		}
	} else {
		flags = append(flags, "off")
	}
	var out []byte
	if c.Resp3() {
		out = resp.AppendMapHeader(nil, 3)
	} else {
		out = resp.AppendArrayHeader(nil, 6)
	}
	out = resp.AppendBulk(out, []byte("flags"))
	out = resp.AppendArrayHeader(out, len(flags))
	for _, f := range flags {
		out = resp.AppendBulk(out, []byte(f))
	}
	out = resp.AppendBulk(out, []byte("redirect"))
	out = resp.AppendInt(out, redirID)
	out = resp.AppendBulk(out, []byte("prefixes"))
	if on {
		out = resp.AppendArrayHeader(out, len(cs.tracking.prefixes))
		for _, p := range cs.tracking.prefixes {
			out = resp.AppendBulk(out, p)
		}
	} else {
		out = resp.AppendArrayHeader(out, 0)
	}
	_ = c.InlineReply(out)
}

// doClientCaching answers CLIENT CACHING YES|NO, the per-command opt-in/opt-out
// selector. It is meaningful only when tracking runs in OPTIN or OPTOUT mode: YES
// belongs to OPTIN (cache the next command's reads), NO to OPTOUT (skip the next
// command's reads). It refuses a connection that is not tracking or is in default
// mode, and a YES/NO that does not match the connection's mode, the same errors
// redis gives. On success it stamps the transient selector the next command's
// recordReadKeys consumes.
func (s *Server) doClientCaching(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) != 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|caching' command"))
		return
	}
	yes := eqFold(args[2], "YES")
	no := eqFold(args[2], "NO")
	if !yes && !no {
		_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error"))
		return
	}
	if cs.tracking == nil || (!cs.tracking.optin && !cs.tracking.optout) {
		_ = c.InlineReply(resp.AppendError(nil, "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled"))
		return
	}
	if yes && !cs.tracking.optin {
		_ = c.InlineReply(resp.AppendError(nil, "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode"))
		return
	}
	if no && !cs.tracking.optout {
		_ = c.InlineReply(resp.AppendError(nil, "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode"))
		return
	}
	if yes {
		cs.tracking.caching = cachingYes
	} else {
		cs.tracking.caching = cachingNo
	}
	_ = c.InlineReply(resp.AppendStatus(nil, "OK"))
}

// doHello answers the HELLO handshake and negotiates the protocol version. Bare
// HELLO reports the version in use; HELLO 2 switches the connection to RESP2 and
// HELLO 3 to RESP3, both recorded on the connection so the reply writer picks the
// frame types (SetResp3). Any other protover is the NOPROTO redis gives. The AUTH
// and SETNAME options after the protover are parsed: SETNAME labels the
// connection, AUTH is declined because f3 sets no password (the honest redis
// answer for a passwordless server). The reply map is rendered in whatever
// version the connection now holds, so a HELLO 3 handshake reply is itself a
// RESP3 map.
func (s *Server) doHello(c *shard.Conn, cs *connState, args [][]byte) {
	i := 1
	if len(args) > 1 && !isHelloOption(args[1]) {
		// A protocol version is present. redis accepts 2 or 3, and so does f3.
		switch {
		case len(args[1]) == 1 && args[1][0] == '2':
			c.SetResp3(false)
		case len(args[1]) == 1 && args[1][0] == '3':
			c.SetResp3(true)
		default:
			_ = c.InlineReply(resp.AppendError(nil, "NOPROTO unsupported protocol version"))
			return
		}
		i = 2
	}
	// The options tail: AUTH username password and SETNAME name, in any order.
	var setName []byte
	haveName := false
	for i < len(args) {
		switch {
		case eqFold(args[i], "AUTH"):
			if i+2 >= len(args) {
				_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error in HELLO"))
				return
			}
			// f3 configures no password, so AUTH cannot succeed. This is the same
			// answer redis gives a client that authenticates against a
			// passwordless server, and it is honest: f3 enforces no access
			// control, so it will not pretend a credential was checked.
			_ = c.InlineReply(resp.AppendError(nil, "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?"))
			return
		case eqFold(args[i], "SETNAME"):
			if i+1 >= len(args) {
				_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error in HELLO"))
				return
			}
			if !validClientName(args[i+1]) {
				_ = c.InlineReply(resp.AppendError(nil, "ERR Client names cannot contain spaces, newlines or special characters."))
				return
			}
			setName = args[i+1]
			haveName = true
			i += 2
		default:
			_ = c.InlineReply(resp.AppendError(nil, "ERR syntax error in HELLO"))
			return
		}
	}
	// The handshake parsed clean; apply the SETNAME option before answering, the
	// same order redis uses, so a client that reads its name back sees it set.
	if haveName {
		cs.setName(setName)
	}
	_ = c.InlineReply(appendHelloMap(nil, cs.id, c.Resp3()))
}

// connAddr renders a net.Addr as the "ip:port" string CLIENT INFO reports, the
// empty string when the address is nil (a listener that does not expose one).
// A net.TCPAddr already stringifies to that shape, so this is String with a nil
// guard; the event-loop drivers, which close the net.Conn on adoption, build the
// same string from the accepted fd instead (fdAddr in the *_linux driver).
func connAddr(a net.Addr) string {
	if a == nil {
		return ""
	}
	return a.String()
}

// appendClientLine renders one connection's CLIENT INFO / LIST line: the
// space-separated key=value fields redis 8.8 and valkey 9.1 emit. f3 fills the
// fields it models honestly (id, the endpoints, name, age, subscription count,
// resp, and the command that asked) and reports the fields it keeps no state for
// as their neutral value rather than forging a number: no per-connection fd is
// tracked on the goroutine driver (fd=-1), f3 runs one database and no MULTI,
// tracking, or ACL users (db=0, multi=-1, watch=0, redir=-1, user=default), and
// it keeps no query/output buffer byte gauges (the qbuf/obl/mem family is 0). The
// resp field is live: it reads the connection's negotiated protocol version, 2 or
// 3, the same value HELLO set. cmd is the "verb|sub" that produced the line.
// Every mutable field it reads (name, sub count, tot-cmds) goes through an atomic, and the
// endpoints and id are immutable after admission, so the same renderer serves
// CLIENT INFO on the connection's own goroutine and CLIENT LIST reading every
// connection from another goroutine, both race-free.
func appendClientLine(dst []byte, cs *connState, cmd string) []byte {
	kv := func(k, v string) {
		dst = append(dst, k...)
		dst = append(dst, '=')
		dst = append(dst, v...)
		dst = append(dst, ' ')
	}
	kvn := func(k string, n int64) {
		dst = append(dst, k...)
		dst = append(dst, '=')
		dst = strconv.AppendInt(dst, n, 10)
		dst = append(dst, ' ')
	}
	age := time.Now().Unix() - cs.connUnix
	if age < 0 {
		age = 0
	}
	kvn("id", int64(cs.id))
	kv("addr", cs.addr)
	kv("laddr", cs.laddr)
	kvn("fd", -1)
	kv("name", string(cs.loadName()))
	kvn("age", age)
	kvn("idle", 0)
	kv("flags", "N")
	kvn("db", 0)
	kvn("sub", cs.subCount.Load())
	kvn("psub", cs.psubCount.Load())
	kvn("ssub", cs.ssubCount.Load())
	kvn("multi", -1)
	kvn("watch", 0)
	kvn("qbuf", 0)
	kvn("qbuf-free", 0)
	kvn("argv-mem", 0)
	kvn("multi-mem", 0)
	kvn("tot-net-in", 0)
	kvn("tot-net-out", 0)
	kvn("rbs", 0)
	kvn("rbp", 0)
	kvn("obl", 0)
	kvn("oll", 0)
	kvn("omem", 0)
	kvn("tot-mem", 0)
	kv("events", "r")
	kv("cmd", cmd)
	kv("user", "default")
	kvn("redir", -1)
	kvn("resp", clientResp(cs))
	kv("lib-name", "")
	kv("lib-ver", "")
	kvn("tot-cmds", int64(cs.commands.load()))
	// Every field appended a trailing space; drop the last so the line matches
	// the redis shape (fields separated, none trailing).
	if len(dst) > 0 && dst[len(dst)-1] == ' ' {
		dst = dst[:len(dst)-1]
	}
	return dst
}

// clientResp reports the connection's negotiated protocol version as the integer
// the CLIENT INFO resp= field carries: 3 when the connection ran HELLO 3, else 2.
func clientResp(cs *connState) int64 {
	if cs.sc != nil && cs.sc.Resp3() {
		return 3
	}
	return 2
}

// isHelloOption reports whether a HELLO argument is one of the option keywords
// rather than the protocol version, so a HELLO with no version (HELLO AUTH ...)
// is parsed without mistaking AUTH for a protover.
func isHelloOption(arg []byte) bool {
	return eqFold(arg, "AUTH") || eqFold(arg, "SETNAME")
}

// appendHelloMap renders the HELLO handshake reply, the seven-pair map redis
// answers: server, version, proto, id, mode standalone (no cluster), role master
// (no replication), modules the empty array (none loaded). Under RESP2 the map is
// a flat array of fourteen elements and proto reads 2; under RESP3 it is a real
// map frame (%7) and proto reads 3. id is the connection's own CLIENT ID.
func appendHelloMap(dst []byte, id uint64, resp3 bool) []byte {
	if resp3 {
		dst = resp.AppendMapHeader(dst, 7)
	} else {
		dst = resp.AppendArrayHeader(dst, 14)
	}
	proto := int64(2)
	if resp3 {
		proto = 3
	}
	dst = resp.AppendBulk(dst, []byte("server"))
	dst = resp.AppendBulk(dst, []byte(helloServerName))
	dst = resp.AppendBulk(dst, []byte("version"))
	dst = resp.AppendBulk(dst, []byte(helloVersion))
	dst = resp.AppendBulk(dst, []byte("proto"))
	dst = resp.AppendInt(dst, proto)
	dst = resp.AppendBulk(dst, []byte("id"))
	dst = resp.AppendInt(dst, int64(id))
	dst = resp.AppendBulk(dst, []byte("mode"))
	dst = resp.AppendBulk(dst, []byte("standalone"))
	dst = resp.AppendBulk(dst, []byte("role"))
	dst = resp.AppendBulk(dst, []byte("master"))
	dst = resp.AppendBulk(dst, []byte("modules"))
	dst = resp.AppendArrayHeader(dst, 0)
	return dst
}
