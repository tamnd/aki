package f1srv

// CLIENT groups the per-connection introspection and configuration subcommands a client library
// runs right after connecting: ID to learn its own connection number, SETNAME/GETNAME to label
// the connection so it shows up named in monitoring, SETINFO to advertise the library name and
// version, and NO-EVICT/NO-TOUCH/GETREDIR to set or read connection flags. f1srv holds real
// per-connection id and name state, so these answer from that state; the flag setters accept the
// options and reply OK without changing eviction behaviour (there is no eviction to protect a
// connection from) and GETREDIR reports no redirection (no client tracking). Redis 8.8 and Valkey
// 9.1 differ only in the HELP text (Valkey lists extra subcommands), so HELP follows Redis 8.8,
// the same compat choice aki makes everywhere else.

// clientHelp is the CLIENT HELP reply, the array of RESP simple strings copied verbatim from
// Redis 8.8. Valkey 9.1 emits a longer list (CAPA, IMPORT-SOURCE, extra KILL/LIST options), so
// matching Redis exactly is the deliberate choice.
var clientHelp = []string{
	"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CACHING (YES|NO)",
	"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
	"GETREDIR",
	"    Return the client ID we are redirecting to when tracking is enabled.",
	"GETNAME",
	"    Return the name of the current connection.",
	"ID",
	"    Return the ID of the current connection.",
	"INFO",
	"    Return information about the current client connection.",
	"KILL <ip:port>",
	"    Kill connection made from <ip:port>.",
	"KILL <option> <value> [<option> <value> [...]]",
	"    Kill connections. Options are:",
	"    * ADDR (<ip:port>|<unixsocket>:0)",
	"      Kill connections made from the specified address",
	"    * LADDR (<ip:port>|<unixsocket>:0)",
	"      Kill connections made to specified local address",
	"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
	"      Kill connections by type.",
	"    * USER <username>",
	"      Kill connections authenticated by <username>.",
	"    * SKIPME (YES|NO)",
	"      Skip killing current connection (default: yes).",
	"    * ID <client-id>",
	"      Kill connections by client id.",
	"    * MAXAGE <maxage>",
	"      Kill connections older than the specified age.",
	"LIST [options ...]",
	"    Return information about client connections. Options:",
	"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
	"      Return clients of specified type.",
	"UNPAUSE",
	"    Stop the current client pause, resuming traffic.",
	"PAUSE <timeout> [WRITE|ALL]",
	"    Suspend all, or just write, clients for <timeout> milliseconds.",
	"REPLY (ON|OFF|SKIP)",
	"    Control the replies sent to the current connection.",
	"SETNAME <name>",
	"    Assign the name <name> to the current connection.",
	"SETINFO <option> <value>",
	"    Set client meta attr. Options are:",
	"    * LIB-NAME: the client lib name.",
	"    * LIB-VER: the client lib version.",
	"UNBLOCK <clientid> [TIMEOUT|ERROR]",
	"    Unblock the specified blocked client.",
	"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
	"         [OPTIN] [OPTOUT] [NOLOOP]",
	"    Control server assisted client side caching.",
	"TRACKINGINFO",
	"    Report tracking status for the current connection.",
	"NO-EVICT (ON|OFF)",
	"    Protect current client connection from eviction.",
	"NO-TOUCH (ON|OFF)",
	"    Will not touch LRU/LFU stats when this mode is on.",
	"HELP",
	"    Print this help.",
}

// validClientName reports whether every byte of a proposed CLIENT SETNAME name is a printable
// ASCII character other than space, matching Redis: a name may hold only bytes in 0x21..0x7e, so
// spaces, newlines, control bytes and high bytes are all rejected. An empty name is allowed and
// clears the connection's name.
func validClientName(name []byte) bool {
	for _, b := range name {
		if b < '!' || b > '~' {
			return false
		}
	}
	return true
}

// cmdClient implements the connection-identity subcommands of CLIENT. Subcommands aki does not
// model (KILL, LIST, INFO, PAUSE, TRACKING, ...) fall through to the unknown-subcommand error so a
// client sees the same reply Redis gives for a subcommand it does not recognize, rather than a
// misleading OK.
func (c *connState) cmdClient(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'client' command")
		return
	}
	sub := argv[1]
	switch {
	case eqFold(sub, "ID"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'client|id' command")
			return
		}
		c.writeInt(c.id)
	case eqFold(sub, "GETNAME"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'client|getname' command")
			return
		}
		// An unnamed connection replies with the nil bulk, the shape Redis 8.8 and Valkey 9.1 both
		// return; a named one replies with the name.
		if c.name == nil {
			c.writeNil()
			return
		}
		c.writeBulk(c.name)
	case eqFold(sub, "SETNAME"):
		if len(argv) != 3 {
			c.writeErr("ERR wrong number of arguments for 'client|setname' command")
			return
		}
		if !validClientName(argv[2]) {
			c.writeErr("ERR Client names cannot contain spaces, newlines or special characters.")
			return
		}
		// Copy the name out of the parse buffer, which is reused for the next command; an empty name
		// clears the label back to unnamed.
		if len(argv[2]) == 0 {
			c.name = nil
		} else {
			c.name = append(c.name[:0], argv[2]...)
		}
		c.writeSimple("OK")
	case eqFold(sub, "SETINFO"):
		if len(argv) != 4 {
			c.writeErr("ERR wrong number of arguments for 'client|setinfo' command")
			return
		}
		// The only recognized attributes are the library name and version; aki records neither, but
		// it validates the option name so a client that probes SETINFO sees the same accept/reject.
		if !eqFold(argv[2], "LIB-NAME") && !eqFold(argv[2], "LIB-VER") {
			c.writeErr("ERR Unrecognized option '" + string(argv[2]) + "'")
			return
		}
		c.writeSimple("OK")
	case eqFold(sub, "NO-EVICT"):
		c.clientOnOff(argv, "no-evict")
	case eqFold(sub, "NO-TOUCH"):
		c.clientOnOff(argv, "no-touch")
	case eqFold(sub, "GETREDIR"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'client|getredir' command")
			return
		}
		// No client tracking is configured, so there is no redirection target.
		c.writeInt(-1)
	case eqFold(sub, "HELP"):
		c.writeSimpleArray(clientHelp)
	default:
		c.writeErr("ERR unknown subcommand '" + string(sub) + "'. Try CLIENT HELP.")
	}
}

// clientOnOff handles the CLIENT NO-EVICT and NO-TOUCH toggles, which take exactly one ON or OFF
// argument. Both are accepted and answered OK without changing behaviour: aki runs no eviction, so
// there is nothing to protect a connection from, and it keeps no LRU/LFU stats to skip touching.
func (c *connState) clientOnOff(argv [][]byte, name string) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'client|" + name + "' command")
		return
	}
	if !eqFold(argv[2], "ON") && !eqFold(argv[2], "OFF") {
		c.writeErr("ERR syntax error")
		return
	}
	c.writeSimple("OK")
}
