package drivers

import (
	"net"
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
	cs.setName(nil)
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
		// No client tracking is configured, so there is no redirection target.
		_ = c.InlineReply(resp.AppendInt(nil, -1))
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
			if (other.subCount.Load() > 0) != eqFold(wantType, "PUBSUB") {
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
		cs.setName(setName)
	}
	_ = c.InlineReply(appendHelloMap(nil, cs.id))
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
// tracking, or ACL users (db=0, multi=-1, watch=0, redir=-1, resp=2,
// user=default), and it keeps no query/output buffer byte gauges (the qbuf/obl/
// mem family is 0). cmd is the "verb|sub" that produced the line. Every mutable
// field it reads (name, sub count, tot-cmds) goes through an atomic, and the
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
	kvn("psub", 0)
	kvn("ssub", 0)
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
	kvn("resp", 2)
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
