package drivers

import (
	"strconv"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
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
// f3 offers one database (SELECT accepts only 0) and no MULTI or auth to unwind,
// so the state RESET clears here is the pub/sub subscription set and the CLIENT
// SETNAME label, the two pieces of per-connection state a session accumulates.
// Dropping the subscriptions also takes the connection out of subscribe mode.
// RESET takes no arguments, so a tail is the arity error redis gives, the same
// answer the shard-hop handler gave before this intercept caught the verb.
func (s *Server) doReset(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) != 1 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'reset' command"))
		return
	}
	s.pubsub.removeConn(cs)
	cs.name = nil
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
// client library, and the NO-EVICT/NO-TOUCH flag toggles. Subcommands f3 does
// not model (KILL, LIST, PAUSE, TRACKING, ...) fall through to the
// unknown-subcommand error, the same reply redis gives, rather than a misleading
// OK. The reply is solicited, so it goes out through InlineReply in order.
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
		_ = c.InlineReply(resp.AppendBulk(nil, cs.name))
	case eqFold(sub, "SETNAME"):
		if len(args) != 3 {
			_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'client|setname' command"))
			return
		}
		if !validClientName(args[2]) {
			_ = c.InlineReply(resp.AppendError(nil, "ERR Client names cannot contain spaces, newlines or special characters."))
			return
		}
		// Copy the name out of the parse buffer, which is reused for the next
		// command; an empty name clears the label back to unnamed.
		if len(args[2]) == 0 {
			cs.name = nil
		} else {
			cs.name = append(cs.name[:0], args[2]...)
		}
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
		// No client tracking is configured, so there is no redirection target.
		_ = c.InlineReply(resp.AppendInt(nil, -1))
	default:
		_ = c.InlineReply(resp.AppendError(nil, "ERR unknown subcommand '"+string(sub)+"'. Try CLIENT HELP."))
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

// doHello answers the HELLO handshake. Bare, or with protover 2, it confirms
// RESP2 and returns the seven-pair handshake map (RESP2 renders a map as a flat
// array). Protover 3 is declined with NOPROTO because f3 speaks RESP2 only, and
// any other protover is the same NOPROTO redis gives. The AUTH and SETNAME
// options after the protover are parsed: SETNAME labels the connection, AUTH is
// declined because f3 sets no password (the honest redis answer for a
// passwordless server).
func (s *Server) doHello(c *shard.Conn, cs *connState, args [][]byte) {
	i := 1
	if len(args) > 1 && !isHelloOption(args[1]) {
		// A protocol version is present. redis accepts 2 or 3; f3 accepts only 2.
		switch {
		case len(args[1]) == 1 && args[1][0] == '2':
			// RESP2, the version f3 speaks.
		case len(args[1]) == 1 && args[1][0] == '3':
			_ = c.InlineReply(resp.AppendError(nil, "NOPROTO unsupported protocol version"))
			return
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
		if len(setName) == 0 {
			cs.name = nil
		} else {
			cs.name = append(cs.name[:0], setName...)
		}
	}
	_ = c.InlineReply(appendHelloMap(nil, cs.id))
}

// isHelloOption reports whether a HELLO argument is one of the option keywords
// rather than the protocol version, so a HELLO with no version (HELLO AUTH ...)
// is parsed without mistaking AUTH for a protover.
func isHelloOption(arg []byte) bool {
	return eqFold(arg, "AUTH") || eqFold(arg, "SETNAME")
}

// appendHelloMap renders the RESP2 HELLO reply: the seven-pair handshake map as
// a flat array of fourteen elements. proto is 2 (f3 is RESP2-only), mode is
// standalone (no cluster), role is master (no replication), and modules is the
// empty array (none loaded). id is the connection's own CLIENT ID.
func appendHelloMap(dst []byte, id uint64) []byte {
	dst = resp.AppendArrayHeader(dst, 14)
	dst = resp.AppendBulk(dst, []byte("server"))
	dst = resp.AppendBulk(dst, []byte(helloServerName))
	dst = resp.AppendBulk(dst, []byte("version"))
	dst = resp.AppendBulk(dst, []byte(helloVersion))
	dst = resp.AppendBulk(dst, []byte("proto"))
	dst = resp.AppendInt(dst, 2)
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
