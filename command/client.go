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
			{Name: "tracking", SubName: "client|tracking", Group: GroupConnection, Since: "6.0.0",
				Arity: -3, Flags: FlagLoading | FlagStale, Handler: handleClientTracking},
			{Name: "trackinginfo", SubName: "client|trackinginfo", Group: GroupConnection, Since: "6.2.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleClientTrackingInfo},
			{Name: "caching", SubName: "client|caching", Group: GroupConnection, Since: "6.0.0",
				Arity: 3, Flags: FlagLoading | FlagStale, Handler: handleClientCaching},
			{Name: "kill", SubName: "client|kill", Group: GroupConnection, Since: "2.4.0",
				Arity: -3, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleClientKill},
			{Name: "unblock", SubName: "client|unblock", Group: GroupConnection, Since: "5.0.0",
				Arity: -3, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleClientUnblock},
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

// handleClientGetRedir reports the client tracking redirection target: the client
// id RESP2 invalidations are forwarded to, or -1 when there is no redirect.
func handleClientGetRedir(ctx *Ctx) {
	if ctx.sess.trackingRedir != 0 {
		ctx.enc().WriteInteger(int64(ctx.sess.trackingRedir))
		return
	}
	ctx.enc().WriteInteger(-1)
}

// handleClientTracking turns client-side caching on or off for this connection
// and validates the option combination before applying it.
func handleClientTracking(ctx *Ctx) {
	mode := strings.ToUpper(string(ctx.Argv[2]))
	if mode != "ON" && mode != "OFF" {
		ctx.enc().WriteError("ERR syntax error")
		return
	}

	var (
		redir          uint64
		bcast          bool
		optIn          bool
		optOut         bool
		noLoop         bool
		prefixes       []string
		havePrefix     bool
		haveRedirToken bool
	)
	args := ctx.Argv[3:]
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "REDIRECT":
			if i+1 >= len(args) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			n, err := strconv.ParseUint(string(args[i+1]), 10, 64)
			if err != nil {
				ctx.enc().WriteError("ERR Invalid client ID")
				return
			}
			redir = n
			haveRedirToken = true
			i++
		case "BCAST":
			bcast = true
		case "PREFIX":
			if i+1 >= len(args) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			prefixes = append(prefixes, string(args[i+1]))
			havePrefix = true
			i++
		case "OPTIN":
			optIn = true
		case "OPTOUT":
			optOut = true
		case "NOLOOP":
			noLoop = true
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	if mode == "OFF" {
		if ctx.sess.trackingOn {
			ctx.d.trackingDisable(ctx.Conn.ID(), ctx.sess)
		}
		ctx.enc().WriteStatus("OK")
		return
	}

	if optIn && optOut {
		ctx.enc().WriteError("ERR You can't specify both OPTIN and OPTOUT")
		return
	}
	if havePrefix && !bcast {
		ctx.enc().WriteError("ERR PREFIX option requires BCAST mode to be enabled")
		return
	}
	if (optIn || optOut) && bcast {
		ctx.enc().WriteError("ERR OPTIN and OPTOUT are not compatible with BCAST")
		return
	}
	// A non-redirect tracking client must speak RESP3 to take the inline push. A
	// redirect forwards to another client, so the tracking client's own protocol
	// does not matter in that case.
	if redir == 0 && ctx.Conn.Proto() != 3 {
		ctx.enc().WriteError("ERR Client tracking can be enabled only in RESP3 mode or when a redirection client is specified via the 'REDIRECT' option")
		return
	}
	if haveRedirToken && redir != 0 {
		if ctx.d.srv == nil || ctx.d.srv.ConnByID(redir) == nil {
			ctx.enc().WriteError("ERR The client ID you want redirect to does not exist")
			return
		}
	}

	ctx.sess.trackingBcast = bcast
	ctx.sess.trackingOptIn = optIn
	ctx.sess.trackingOptOut = optOut
	ctx.sess.trackingNoLoop = noLoop
	ctx.sess.trackingPrefixes = prefixes
	ctx.sess.trackingRedir = redir
	ctx.d.trackingEnable(ctx.Conn.ID(), ctx.sess)
	ctx.enc().WriteStatus("OK")
}

// handleClientTrackingInfo reports the current tracking configuration as a map:
// the active flags, the redirect target, and the BCAST prefixes.
func handleClientTrackingInfo(ctx *Ctx) {
	enc := ctx.enc()
	s := ctx.sess

	var flags []string
	if s.trackingOn {
		flags = append(flags, "on")
	} else {
		flags = append(flags, "off")
	}
	if s.trackingBcast {
		flags = append(flags, "bcast")
	}
	if s.trackingOptIn {
		flags = append(flags, "optin")
	}
	if s.trackingOptOut {
		flags = append(flags, "optout")
	}
	if s.cachingYes {
		flags = append(flags, "caching-yes")
	}
	if s.cachingNo {
		flags = append(flags, "caching-no")
	}
	if s.trackingNoLoop {
		flags = append(flags, "noloop")
	}
	if s.trackingOn && s.trackingRedir != 0 &&
		(ctx.d.srv == nil || ctx.d.srv.ConnByID(s.trackingRedir) == nil) {
		flags = append(flags, "broken_redirect")
	}

	redir := int64(-1)
	if s.trackingRedir != 0 {
		redir = int64(s.trackingRedir)
	}

	enc.WriteMapLen(3)
	enc.WriteBulkStringStr("flags")
	enc.WriteArrayLen(len(flags))
	for _, f := range flags {
		enc.WriteBulkStringStr(f)
	}
	enc.WriteBulkStringStr("redirect")
	enc.WriteInteger(redir)
	enc.WriteBulkStringStr("prefixes")
	enc.WriteArrayLen(len(s.trackingPrefixes))
	for _, p := range s.trackingPrefixes {
		enc.WriteBulkStringStr(p)
	}
}

// handleClientCaching records a one-shot tracking decision for the next command.
// It is only meaningful in OPTIN or OPTOUT mode; YES is for OPTIN, NO for OPTOUT.
func handleClientCaching(ctx *Ctx) {
	yes, ok := parseYesNo(strings.ToLower(string(ctx.Argv[2])))
	if !ok {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	s := ctx.sess
	if !s.trackingOn || (!s.trackingOptIn && !s.trackingOptOut) {
		ctx.enc().WriteError("ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled")
		return
	}
	if yes && s.trackingOptOut {
		ctx.enc().WriteError("ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.")
		return
	}
	if !yes && s.trackingOptIn {
		ctx.enc().WriteError("ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.")
		return
	}
	s.cachingYes = yes
	s.cachingNo = !yes
	ctx.enc().WriteStatus("OK")
}

// handleClientUnblock wakes a client blocked on a blocking command. The default
// (or the TIMEOUT modifier) makes that command return as if it timed out; ERROR
// makes it return an UNBLOCKED error. It replies 1 when a client was unblocked
// and 0 when the target was not blocking.
func handleClientUnblock(ctx *Ctx) {
	id, err := strconv.ParseUint(string(ctx.Argv[2]), 10, 64)
	if err != nil {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	errReply := false
	if len(ctx.Argv) > 3 {
		switch strings.ToUpper(string(ctx.Argv[3])) {
		case "TIMEOUT":
			errReply = false
		case "ERROR":
			errReply = true
		default:
			ctx.enc().WriteError("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR")
			return
		}
	}
	if ctx.d.unblockClient(id, errReply) {
		ctx.enc().WriteInteger(1)
		return
	}
	ctx.enc().WriteInteger(0)
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
		"UNBLOCK <id> [TIMEOUT|ERROR]",
		"    Unblock the connection blocked on a blocking command.",
		"NO-EVICT (ON|OFF)",
		"    Set client eviction mode for the current connection.",
		"NO-TOUCH (ON|OFF)",
		"    Stop the current command touching the LRU/LFU of keys it reads.",
		"GETREDIR",
		"    Return the client ID tracking invalidations are redirected to.",
		"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> ...] [OPTIN] [OPTOUT] [NOLOOP]",
		"    Enable or disable server-assisted client-side caching.",
		"TRACKINGINFO",
		"    Return the tracking status for the current connection.",
		"CACHING (YES|NO)",
		"    Enable or disable tracking of the next command in OPTIN/OPTOUT mode.",
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
	flags := "N"
	redir := int64(-1)
	if s, ok := c.Session().(*session); ok {
		sub = len(s.subChannels)
		psub = len(s.subPatterns)
		ssub = len(s.subShards)
		watch = len(s.watched)
		if s.inMulti {
			multi = len(s.queue)
		}
		libName, libVer = s.libName, s.libVer
		if s.trackingOn {
			flags = "t"
		}
		if s.trackingRedir != 0 {
			redir = int64(s.trackingRedir)
		}
	}
	cmd := "NULL"
	user := "default"
	if s, ok := c.Session().(*session); ok {
		if s.lastCmd != "" {
			cmd = s.lastCmd
		}
		if s.username != "" {
			user = s.username
		}
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
	b.WriteString(" flags=" + flags)
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
	b.WriteString(" user=" + user)
	b.WriteString(" redir=" + strconv.FormatInt(redir, 10))
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
