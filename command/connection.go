package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/resp"
)

// connectionCommands returns the connection-group command table for this
// milestone. Later slices append their own groups to the dispatcher's table.
func connectionCommands() []*CmdDesc {
	command := &CmdDesc{
		Name: "command", Group: GroupServer, Since: "2.8.13",
		Arity: -1, Flags: FlagLoading | FlagStale,
		Handler: handleCommand,
		SubCmds: []*CmdDesc{
			{Name: "count", SubName: "command|count", Group: GroupServer, Since: "2.8.13",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleCommandCount},
			{Name: "info", SubName: "command|info", Group: GroupServer, Since: "2.8.13",
				Arity: -2, Flags: FlagLoading | FlagStale, Handler: handleCommandInfo},
			{Name: "list", SubName: "command|list", Group: GroupServer, Since: "7.0.0",
				Arity: -2, Flags: FlagLoading | FlagStale, Handler: handleCommandList},
			{Name: "getkeys", SubName: "command|getkeys", Group: GroupServer, Since: "2.8.13",
				Arity: -3, Flags: FlagLoading | FlagStale, Handler: handleCommandGetKeys},
			{Name: "getkeysandflags", SubName: "command|getkeysandflags", Group: GroupServer, Since: "7.0.0",
				Arity: -3, Flags: FlagLoading | FlagStale, Handler: handleCommandGetKeysAndFlags},
		},
	}
	return []*CmdDesc{
		{Name: "ping", Group: GroupConnection, Since: "1.0.0",
			Arity: -1, Flags: FlagFast, Handler: handlePing},
		{Name: "echo", Group: GroupConnection, Since: "1.0.0",
			Arity: 2, Flags: FlagFast, Handler: handleEcho},
		{Name: "select", Group: GroupConnection, Since: "1.0.0",
			Arity: 2, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleSelect},
		{Name: "quit", Group: GroupConnection, Since: "1.0.0",
			Arity: -1, Flags: FlagLoading | FlagStale | FlagFast | FlagNoAuth, Handler: handleQuit},
		{Name: "reset", Group: GroupConnection, Since: "6.2.0",
			Arity: 1, Flags: FlagLoading | FlagStale | FlagFast | FlagNoAuth, Handler: handleReset},
		{Name: "hello", Group: GroupConnection, Since: "6.0.0",
			Arity: -1, Flags: FlagLoading | FlagStale | FlagFast | FlagNoAuth, Handler: handleHello},
		{Name: "auth", Group: GroupConnection, Since: "1.0.0",
			Arity: -2, Flags: FlagLoading | FlagStale | FlagFast | FlagNoAuth, Handler: handleAuth},
		{Name: "time", Group: GroupServer, Since: "2.6.0",
			Arity: 1, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleTime},
		command,
	}
}

// handlePing answers PONG, or echoes its single argument. PING carries arity -1
// so the table accepts it, but more than one argument is an error. A RESP2 client
// in subscriber mode gets the two-element ["pong", message] push array instead,
// matching Redis.
func handlePing(ctx *Ctx) {
	if len(ctx.Argv) > 2 {
		ctx.enc().WriteError(arityError(&CmdDesc{Name: "ping"}))
		return
	}
	if ctx.Conn.Proto() == 2 && ctx.sess.subCount() > 0 {
		enc := ctx.enc()
		enc.WriteArrayLen(2)
		enc.WriteBulkStringStr("pong")
		if len(ctx.Argv) == 2 {
			enc.WriteBulkString(ctx.Argv[1])
		} else {
			enc.WriteBulkStringStr("")
		}
		return
	}
	if len(ctx.Argv) == 2 {
		ctx.enc().WriteBulkString(ctx.Argv[1])
		return
	}
	ctx.Conn.WriteRaw(resp.ReplyPong)
}

// handleEcho returns its argument unchanged.
func handleEcho(ctx *Ctx) {
	ctx.enc().WriteBulkString(ctx.Argv[1])
}

// handleSelect switches the connection's logical database.
func handleSelect(ctx *Ctx) {
	n, err := strconv.Atoi(string(ctx.Argv[1]))
	if err != nil {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	if n < 0 || n >= ctx.d.cfg.Databases {
		ctx.enc().WriteError("ERR DB index is out of range")
		return
	}
	ctx.Conn.SetDB(n)
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleQuit writes OK and asks the loop to close after the reply is flushed.
func handleQuit(ctx *Ctx) {
	ctx.Conn.WriteRaw(resp.ReplyOK)
	ctx.Conn.Quit()
}

// handleReset returns the connection to its default state: RESP2, database 0,
// no name, and re-authenticated only if no password is required.
func handleReset(ctx *Ctx) {
	ctx.Conn.SetProto(2)
	ctx.Conn.SetDB(0)
	ctx.Conn.SetName("")
	def := ctx.d.acl.get("default")
	ctx.sess.authenticated = def != nil && def.nopass
	ctx.sess.user = def
	ctx.sess.username = "default"
	ctx.sess.clearMulti()
	if ctx.sess.trackingOn {
		ctx.d.trackingDisable(ctx.Conn.ID(), ctx.sess)
	}
	ctx.sess.cachingYes = false
	ctx.sess.cachingNo = false
	ctx.Conn.WriteRaw(resp.ReplyReset)
}

// handleAuth verifies the password (and optional username) against the
// configured default user.
func handleAuth(ctx *Ctx) {
	// AUTH <password> or AUTH <username> <password>.
	var user, pass string
	switch len(ctx.Argv) {
	case 2:
		user, pass = "default", string(ctx.Argv[1])
	case 3:
		user, pass = string(ctx.Argv[1]), string(ctx.Argv[2])
	default:
		ctx.enc().WriteError(arityError(&CmdDesc{Name: "auth"}))
		return
	}
	if !ctx.authWith(user, pass) {
		return
	}
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleHello negotiates the protocol version and returns the server handshake
// map. It optionally authenticates (AUTH user pass) and sets the name (SETNAME).
func handleHello(ctx *Ctx) {
	argv := ctx.Argv
	proto := ctx.Conn.Proto()

	i := 1
	if i < len(argv) {
		v, err := strconv.Atoi(string(argv[i]))
		if err != nil || (v != 2 && v != 3) {
			ctx.enc().WriteError("NOPROTO unsupported protocol version")
			return
		}
		proto = v
		i++
	}

	name := ctx.Conn.Name()
	for i < len(argv) {
		opt := strings.ToUpper(string(argv[i]))
		switch opt {
		case "AUTH":
			if i+2 >= len(argv) {
				ctx.enc().WriteError(arityError(&CmdDesc{Name: "hello"}))
				return
			}
			user, pass := string(argv[i+1]), string(argv[i+2])
			if !ctx.authWith(user, pass) {
				return
			}
			i += 3
		case "SETNAME":
			if i+1 >= len(argv) {
				ctx.enc().WriteError(arityError(&CmdDesc{Name: "hello"}))
				return
			}
			name = string(argv[i+1])
			i += 2
		default:
			ctx.enc().WriteError("ERR Syntax error in HELLO")
			return
		}
	}

	if ctx.d.cfg.RequirePass != "" && !ctx.sess.authenticated {
		ctx.enc().WriteError("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
		return
	}

	ctx.Conn.SetProto(proto)
	ctx.Conn.SetName(name)
	ctx.writeHelloMap(proto)
}

// authWith performs the AUTH check used by both AUTH and HELLO ... AUTH. It
// writes the error reply itself and reports whether authentication succeeded.
func (ctx *Ctx) authWith(user, pass string) bool {
	// The one-argument AUTH form maps to the default user. If that user needs no
	// password, the legacy form has nothing to check, so report it the way Redis
	// does rather than silently succeeding.
	if user == "default" {
		if def := ctx.d.acl.get("default"); def != nil && def.nopass {
			ctx.enc().WriteError("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?")
			return false
		}
	}
	u, ok := ctx.d.acl.authenticate(user, pass)
	if !ok {
		ctx.enc().WriteError("WRONGPASS invalid username-password pair or user is disabled.")
		return false
	}
	ctx.sess.authenticated = true
	ctx.sess.user = u
	ctx.sess.username = user
	return true
}

// writeHelloMap writes the seven-field handshake map. RESP2 sees it as a flat
// fourteen-element array, which is how redis-cli reads HELLO on RESP2.
func (ctx *Ctx) writeHelloMap(proto int) {
	e := ctx.enc()
	e.WriteMapLen(7)
	e.WriteBulkStringStr("server")
	e.WriteBulkStringStr("redis")
	e.WriteBulkStringStr("version")
	e.WriteBulkStringStr(ctx.d.cfg.Version)
	e.WriteBulkStringStr("proto")
	e.WriteInteger(int64(proto))
	e.WriteBulkStringStr("id")
	e.WriteInteger(int64(ctx.Conn.ID()))
	e.WriteBulkStringStr("mode")
	e.WriteBulkStringStr(ctx.d.cfg.Mode)
	e.WriteBulkStringStr("role")
	e.WriteBulkStringStr("master")
	e.WriteBulkStringStr("modules")
	e.WriteArrayLen(0)
}

// handleTime returns the server time as a two-element array of strings: Unix
// seconds and the microseconds within the current second.
func handleTime(ctx *Ctx) {
	now := time.Now()
	e := ctx.enc()
	e.WriteArrayLen(2)
	e.WriteBulkStringStr(strconv.FormatInt(now.Unix(), 10))
	e.WriteBulkStringStr(strconv.FormatInt(int64(now.Nanosecond()/1000), 10))
}

// handleCommand with no subcommand returns the info array for every command, the
// same shape as COMMAND INFO with all names. The implementation lives in
// command.go alongside the other introspection subcommands.
func handleCommand(ctx *Ctx) {
	cmds := ctx.d.table.commands()
	ctx.enc().WriteArrayLen(len(cmds))
	for _, c := range cmds {
		writeCommandInfo(ctx.enc(), c)
	}
}

// handleCommandCount returns the number of commands registered in the table.
func handleCommandCount(ctx *Ctx) {
	ctx.enc().WriteInteger(int64(ctx.d.table.Count()))
}
