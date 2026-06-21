package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/networking"
)

// This file implements the CLIENT command (doc 19): connection introspection and
// per-connection toggles. The connection registry lives in the network server,
// reached through the dispatcher's server handle, so LIST and KILL can see every
// live client.

// clientCommands returns the CLIENT container command.
func clientCommands() []*CmdDesc {
	client := &CmdDesc{
		Name: "client", Group: GroupConnection, Since: "2.4.0",
		Arity: -2, Flags: FlagLoading | FlagStale,
		Handler: handleClientHelp,
		SubCmds: []*CmdDesc{
			{Name: "id", SubName: "client|id", Group: GroupConnection, Since: "5.0.0",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleClientID},
			{Name: "getname", SubName: "client|getname", Group: GroupConnection, Since: "2.6.9",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleClientGetName},
			{Name: "setname", SubName: "client|setname", Group: GroupConnection, Since: "2.6.9",
				Arity: 3, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleClientSetName},
			{Name: "setinfo", SubName: "client|setinfo", Group: GroupConnection, Since: "7.2.0",
				Arity: 4, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleClientSetInfo},
			{Name: "info", SubName: "client|info", Group: GroupConnection, Since: "6.2.0",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleClientInfo},
			{Name: "list", SubName: "client|list", Group: GroupConnection, Since: "2.4.0",
				Arity: -2, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleClientList},
			{Name: "no-evict", SubName: "client|no-evict", Group: GroupConnection, Since: "7.0.0",
				Arity: 3, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleClientNoEvict},
			{Name: "no-touch", SubName: "client|no-touch", Group: GroupConnection, Since: "7.2.0",
				Arity: 3, Flags: FlagLoading | FlagStale, Handler: handleClientNoTouch},
			{Name: "getredir", SubName: "client|getredir", Group: GroupConnection, Since: "6.0.0",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleClientGetRedir},
			{Name: "kill", SubName: "client|kill", Group: GroupConnection, Since: "2.4.0",
				Arity: -3, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleClientKill},
			{Name: "unpause", SubName: "client|unpause", Group: GroupConnection, Since: "6.2.0",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleClientUnpause},
			{Name: "pause", SubName: "client|pause", Group: GroupConnection, Since: "3.0.0",
				Arity: -3, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleClientPause},
			{Name: "help", SubName: "client|help", Group: GroupConnection, Since: "5.0.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleClientHelp},
		},
	}
	return []*CmdDesc{client}
}

func handleClientID(ctx *Ctx) {
	ctx.enc().WriteInteger(int64(ctx.Conn.ID()))
}

func handleClientGetName(ctx *Ctx) {
	ctx.enc().WriteBulkStringStr(ctx.Conn.Name())
}

// validClientName rejects names with spaces or newlines, the same constraint
// Redis enforces so a name never breaks the CLIENT LIST line format.
func validClientName(name string) bool {
	for i := 0; i < len(name); i++ {
		if name[i] < 0x21 || name[i] > 0x7e {
			return false
		}
	}
	return true
}

func handleClientSetName(ctx *Ctx) {
	name := string(ctx.Argv[2])
	if !validClientName(name) {
		ctx.enc().WriteError("ERR Client names cannot contain spaces, newlines or special characters.")
		return
	}
	ctx.Conn.SetName(name)
	ctx.enc().WriteStatus("OK")
}

func handleClientSetInfo(ctx *Ctx) {
	attr := strings.ToLower(string(ctx.Argv[2]))
	val := string(ctx.Argv[3])
	if !validClientName(val) {
		ctx.enc().WriteError("ERR " + string(ctx.Argv[3]) + ": newlines and spaces are not allowed in the attribute value")
		return
	}
	switch attr {
	case "lib-name":
		ctx.sess.libName = val
	case "lib-ver":
		ctx.sess.libVer = val
	default:
		ctx.enc().WriteError("ERR Unrecognized option '" + string(ctx.Argv[2]) + "'")
		return
	}
	ctx.enc().WriteStatus("OK")
}

func handleClientInfo(ctx *Ctx) {
	ctx.enc().WriteBulkStringStr(buildClientLine(ctx.Conn, time.Now()))
}

// handleClientList writes one line per connection. The optional ID filter limits
// the output to the listed connection ids.
func handleClientList(ctx *Ctx) {
	if ctx.d.srv == nil {
		ctx.enc().WriteBulkStringStr("")
		return
	}
	var idFilter map[uint64]bool
	args := ctx.Argv[2:]
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "ID":
			idFilter = make(map[uint64]bool)
			for i++; i < len(args); i++ {
				n, err := strconv.ParseUint(string(args[i]), 10, 64)
				if err != nil {
					ctx.enc().WriteError("ERR Invalid client ID")
					return
				}
				idFilter[n] = true
			}
		case "TYPE":
			// TYPE filters by client kind. aki has only normal clients, so a
			// non-normal type matches nothing and normal matches all.
			if i+1 >= len(args) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			if strings.ToLower(string(args[i+1])) != "normal" {
				ctx.enc().WriteBulkStringStr("")
				return
			}
			i++
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	now := time.Now()
	var b strings.Builder
	for _, c := range ctx.d.srv.Snapshot() {
		if idFilter != nil && !idFilter[c.ID()] {
			continue
		}
		b.WriteString(buildClientLine(c, now))
		b.WriteByte('\n')
	}
	ctx.enc().WriteBulkStringStr(b.String())
}

func handleClientNoEvict(ctx *Ctx) {
	on, ok := parseOnOff(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	ctx.sess.noEvict = on
	ctx.enc().WriteStatus("OK")
}

func handleClientNoTouch(ctx *Ctx) {
	on, ok := parseOnOff(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	ctx.sess.noTouch = on
	ctx.enc().WriteStatus("OK")
}

// handleClientGetRedir reports the client tracking redirection target. aki does
// not implement client-side caching, so there is never a redirection.
func handleClientGetRedir(ctx *Ctx) {
	ctx.enc().WriteInteger(-1)
}

func handleClientUnpause(ctx *Ctx) {
	ctx.enc().WriteStatus("OK")
}

// handleClientPause accepts the pause request and acknowledges it. aki does not
// pause command processing yet, so the timeout is validated and ignored.
func handleClientPause(ctx *Ctx) {
	if _, err := strconv.ParseInt(string(ctx.Argv[2]), 10, 64); err != nil {
		ctx.enc().WriteError("ERR timeout is not an integer or out of range")
		return
	}
	if len(ctx.Argv) > 3 {
		switch strings.ToUpper(string(ctx.Argv[3])) {
		case "WRITE", "ALL":
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	ctx.enc().WriteStatus("OK")
}

// handleClientKill closes one or more connections. The single-argument form
// takes an addr:port and returns OK; the filter form returns the number of
// clients closed.
func handleClientKill(ctx *Ctx) {
	if ctx.d.srv == nil {
		ctx.enc().WriteError("ERR No such client")
		return
	}
	args := ctx.Argv[2:]

	// Old form: CLIENT KILL addr:port.
	if len(args) == 1 {
		addr := string(args[0])
		for _, c := range ctx.d.srv.Snapshot() {
			if c.RemoteAddr() == addr {
				killConn(ctx, c)
				ctx.enc().WriteStatus("OK")
				return
			}
		}
		ctx.enc().WriteError("ERR No such client")
		return
	}

	// Filter form.
	var (
		byID    uint64
		haveID  bool
		byAddr  string
		byLAddr string
		skipme  = true
	)
	for i := 0; i+1 < len(args); i += 2 {
		val := string(args[i+1])
		switch strings.ToUpper(string(args[i])) {
		case "ID":
			n, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				ctx.enc().WriteError("ERR client-id should be greater than 0")
				return
			}
			byID, haveID = n, true
		case "ADDR":
			byAddr = val
		case "LADDR":
			byLAddr = val
		case "SKIPME":
			on, ok := parseYesNo(val)
			if !ok {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			skipme = on
		case "TYPE", "USER", "MAXAGE":
			// Accepted and ignored: aki has one client type, one user, and does
			// not track age for kill selection.
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	var killed int64
	for _, c := range ctx.d.srv.Snapshot() {
		if haveID && c.ID() != byID {
			continue
		}
		if byAddr != "" && c.RemoteAddr() != byAddr {
			continue
		}
		if byLAddr != "" && c.LocalAddr() != byLAddr {
			continue
		}
		if skipme && c.ID() == ctx.Conn.ID() {
			continue
		}
		killConn(ctx, c)
		killed++
	}
	ctx.enc().WriteInteger(killed)
}

// killConn closes a target connection. Killing the current connection uses the
// flush-then-close path so the reply still reaches the client.
func killConn(ctx *Ctx, c *networking.Conn) {
	if c.ID() == ctx.Conn.ID() {
		ctx.Conn.Quit()
		return
	}
	c.CloseASAP()
}

func handleClientHelp(ctx *Ctx) {
	lines := []string{
		"CLIENT <subcommand> [<arg> ...]. Subcommands are:",
		"ID",
		"    Return the client ID for this connection.",
		"GETNAME",
		"    Return the name of the current connection.",
		"SETNAME <name>",
		"    Assign the name <name> to the current connection.",
		"SETINFO <lib-name|lib-ver> <value>",
		"    Set client library name or version.",
		"INFO",
		"    Return information about the current client connection.",
		"LIST [ID <id> ...] [TYPE normal]",
		"    Return information about client connections.",
		"KILL <ip:port> | <filter> ...",
		"    Kill connections by address or by filter.",
		"NO-EVICT (ON|OFF)",
		"    Set client eviction mode for the current connection.",
		"NO-TOUCH (ON|OFF)",
		"    Stop the current command touching the LRU/LFU of keys it reads.",
		"HELP",
		"    Print this help.",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}

// buildClientLine renders the CLIENT LIST / CLIENT INFO field line for a
// connection. Fields aki does not track are reported as zero or a fixed value so
// the line still parses with the field names redis clients expect.
func buildClientLine(c *networking.Conn, now time.Time) string {
	sub, psub, ssub, multi, watch := 0, 0, 0, -1, 0
	libName, libVer := "", ""
	if s, ok := c.Session().(*session); ok {
		sub = len(s.subChannels)
		psub = len(s.subPatterns)
		ssub = len(s.subShards)
		watch = len(s.watched)
		if s.inMulti {
			multi = len(s.queue)
		}
		libName, libVer = s.libName, s.libVer
	}
	cmd := "NULL"
	if s, ok := c.Session().(*session); ok && s.lastCmd != "" {
		cmd = s.lastCmd
	}
	age := int64(now.Sub(c.Created()).Seconds())
	idle := max(int64(now.Sub(c.LastInteraction()).Seconds()), 0)

	var b strings.Builder
	b.WriteString("id=" + strconv.FormatUint(c.ID(), 10))
	b.WriteString(" addr=" + c.RemoteAddr())
	b.WriteString(" laddr=" + c.LocalAddr())
	b.WriteString(" fd=8")
	b.WriteString(" name=" + c.Name())
	b.WriteString(" age=" + strconv.FormatInt(age, 10))
	b.WriteString(" idle=" + strconv.FormatInt(idle, 10))
	b.WriteString(" flags=N")
	b.WriteString(" db=" + strconv.Itoa(c.DB()))
	b.WriteString(" sub=" + strconv.Itoa(sub))
	b.WriteString(" psub=" + strconv.Itoa(psub))
	b.WriteString(" ssub=" + strconv.Itoa(ssub))
	b.WriteString(" multi=" + strconv.Itoa(multi))
	b.WriteString(" watch=" + strconv.Itoa(watch))
	b.WriteString(" qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0")
	b.WriteString(" tot-net-in=" + strconv.FormatUint(c.TotNetIn(), 10))
	b.WriteString(" tot-net-out=" + strconv.FormatUint(c.TotNetOut(), 10))
	b.WriteString(" rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=0")
	b.WriteString(" events=r")
	b.WriteString(" cmd=" + cmd)
	b.WriteString(" user=default")
	b.WriteString(" redir=-1")
	b.WriteString(" resp=" + strconv.Itoa(c.Proto()))
	b.WriteString(" lib-name=" + libName)
	b.WriteString(" lib-ver=" + libVer)
	b.WriteString(" tot-cmds=" + strconv.FormatUint(c.TotCmds(), 10))
	return b.String()
}

// parseOnOff reads an ON or OFF token.
func parseOnOff(b []byte) (bool, bool) {
	switch strings.ToUpper(string(b)) {
	case "ON":
		return true, true
	case "OFF":
		return false, true
	}
	return false, false
}

// parseYesNo reads a yes or no token.
func parseYesNo(s string) (bool, bool) {
	switch strings.ToLower(s) {
	case "yes":
		return true, true
	case "no":
		return false, true
	}
	return false, false
}
