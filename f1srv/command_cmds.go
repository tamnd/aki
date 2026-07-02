package f1srv

// COMMAND is the introspection surface a client library probes on connect to learn the command
// table and how to pull key names out of a command for routing. The old blanket answered every
// COMMAND form with an empty array, which desynchronizes a client that expects an integer from
// COMMAND COUNT or a key list from COMMAND GETKEYS. This file gives COMMAND a real dispatcher driven
// by the generated cmdTable in command_table.go.
//
// The subcommands and their reply shapes follow Redis 8.8: bare COMMAND and COMMAND INFO return the
// 10-element info spec per command, COMMAND COUNT an integer, COMMAND LIST the flat name array,
// COMMAND GETKEYS the extracted keys or a defined error, and COMMAND DOCS a map. aki reports an empty
// map from COMMAND DOCS because it does not carry the human documentation strings Redis ships; the
// machine-readable spec every client needs for routing is in COMMAND INFO, which is complete.

// cmdCommand dispatches the COMMAND subcommands. A bare COMMAND is the whole info array; the named
// subcommands each carry their own reply; an unrecognized one is the Redis error naming the
// requested spelling.
func (c *connState) cmdCommand(argv [][]byte) {
	if len(argv) == 1 {
		c.writeArrayHeader(len(cmdTable))
		for i := range cmdTable {
			c.writeCommandInfo(&cmdTable[i])
		}
		return
	}
	switch lowerName(argv[1]) {
	case "count":
		c.writeInt(int64(len(cmdTable)))
	case "list":
		// FILTERBY narrows the list by module, ACL category, or pattern in Redis. aki carries no
		// modules and answers the common bare LIST; a FILTERBY request still gets the full name list,
		// which a client reading names tolerates.
		c.writeArrayHeader(len(cmdTable))
		for i := range cmdTable {
			c.writeBulk([]byte(cmdTable[i].name))
		}
	case "info":
		c.commandInfo(argv)
	case "docs":
		c.commandDocs(argv)
	case "getkeys":
		c.commandGetKeys(argv, false)
	case "getkeysandflags":
		c.commandGetKeys(argv, true)
	case "help":
		c.writeArrayHeader(len(commandHelp))
		for _, line := range commandHelp {
			c.writeSimple(line)
		}
	default:
		c.writeErr("ERR unknown subcommand '" + string(argv[1]) + "'. Try COMMAND HELP.")
	}
}

// commandInfo answers COMMAND INFO [name ...]. With no names it returns the whole table; with names
// it returns one element per requested name, in request order, with a nil element for a name aki
// does not know, matching Redis.
func (c *connState) commandInfo(argv [][]byte) {
	if len(argv) == 2 {
		c.writeArrayHeader(len(cmdTable))
		for i := range cmdTable {
			c.writeCommandInfo(&cmdTable[i])
		}
		return
	}
	names := argv[2:]
	c.writeArrayHeader(len(names))
	for _, n := range names {
		if sp, ok := cmdByName[lowerName(n)]; ok {
			c.writeCommandInfo(sp)
		} else {
			c.writeNilArray()
		}
	}
}

// writeCommandInfo emits the 10-element info array Redis returns for one command: name, arity, the
// command flags, the first-key/last-key/key-step triple, the ACL categories, then command tips,
// key-specs, and subcommands. aki reports the last three empty: a client that cannot read a key-spec
// falls back to the first-key/last-key/step triple this row carries, tips are cluster routing hints
// a single-node server does not need, and container-command subcommands are not enumerated.
func (c *connState) writeCommandInfo(sp *cmdSpec) {
	c.writeArrayHeader(10)
	c.writeBulk([]byte(sp.name))
	c.writeInt(int64(sp.arity))
	c.writeSimpleArray(sp.flags)
	c.writeInt(int64(sp.firstKey))
	c.writeInt(int64(sp.lastKey))
	c.writeInt(int64(sp.keyStep))
	c.writeSimpleArray(sp.aclCats)
	c.writeArrayHeader(0) // tips
	c.writeArrayHeader(0) // key-specs
	c.writeArrayHeader(0) // subcommands
}

// commandDocs answers COMMAND DOCS with an empty map. Redis returns a map of human documentation
// strings (summary, since, group, arguments) that aki does not carry; an empty map is the same reply
// Redis gives for a DOCS request that matches nothing, so a client that parses the map sees a valid,
// if empty, result rather than a desync, and the machine-readable spec it needs is in COMMAND INFO.
func (c *connState) commandDocs(argv [][]byte) {
	c.writeArrayHeader(0)
}

// commandGetKeys answers COMMAND GETKEYS and COMMAND GETKEYSANDFLAGS: it takes a full command line as
// its arguments, looks the command up, checks the arity, and returns the key names Redis would route
// on. GETKEYSANDFLAGS pairs each key with a coarse access flag derived from the command's write or
// readonly flag; the fine-grained per-key flags Redis derives from a key-spec are not reproduced.
func (c *connState) commandGetKeys(argv [][]byte, withFlags bool) {
	sub := "command|getkeys"
	if withFlags {
		sub = "command|getkeysandflags"
	}
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for '" + sub + "' command")
		return
	}
	line := argv[2:]
	sp, ok := cmdByName[lowerName(line[0])]
	if !ok {
		c.writeErr("ERR Invalid command specified")
		return
	}
	if !arityOK(sp.arity, len(line)) {
		c.writeErr("ERR Invalid number of arguments specified for command")
		return
	}
	keys := extractKeys(sp, line)
	if len(keys) == 0 {
		c.writeErr("ERR The command has no key arguments")
		return
	}
	c.writeArrayHeader(len(keys))
	if !withFlags {
		for _, k := range keys {
			c.writeBulk(k)
		}
		return
	}
	flag := "RO"
	for _, f := range sp.flags {
		if f == "write" {
			flag = "RW"
			break
		}
	}
	for _, k := range keys {
		c.writeArrayHeader(2)
		c.writeBulk(k)
		c.writeArrayHeader(1)
		c.writeSimple(flag)
	}
}

// arityOK reports whether an argument count matches a command's arity the way Redis checks it: a
// positive arity is an exact count, a negative arity is a minimum of its absolute value. The count
// includes the command name, matching how the arity is stated in the table.
func arityOK(arity, n int) bool {
	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
}

// extractKeys returns the key arguments of a command line according to the command's getkeys kind.
// line[0] is the command name, so key positions are one-based indexes into line. This reproduces the
// Redis key finder for each shape: a plain range, a numkeys-counted list, a numkeys list behind a
// destination, an optional STORE target, or the STREAMS split.
func extractKeys(sp *cmdSpec, line [][]byte) [][]byte {
	var keys [][]byte
	switch sp.gk {
	case gkRange:
		last := sp.lastKey
		if last < 0 {
			last = len(line) + last
		}
		for i := sp.firstKey; i <= last && i < len(line); i += sp.keyStep {
			keys = append(keys, line[i])
		}
	case gkKeynum:
		if sp.gkIdx >= len(line) {
			break
		}
		n, err := atoi64(line[sp.gkIdx])
		if err != nil || n <= 0 {
			break
		}
		for j := 0; j < int(n) && sp.gkIdx+1+j < len(line); j++ {
			keys = append(keys, line[sp.gkIdx+1+j])
		}
	case gkKeynumDest:
		if len(line) < 3 {
			break
		}
		keys = append(keys, line[1])
		n, err := atoi64(line[2])
		if err != nil || n <= 0 {
			break
		}
		for j := 0; j < int(n) && 3+j < len(line); j++ {
			keys = append(keys, line[3+j])
		}
	case gkSortStore:
		if len(line) < 2 {
			break
		}
		keys = append(keys, line[1])
		for i := 2; i+1 < len(line); i++ {
			if eqFold(line[i], "STORE") {
				keys = append(keys, line[i+1])
				i++
			}
		}
	case gkGeoStore:
		if len(line) < 2 {
			break
		}
		keys = append(keys, line[1])
		for i := 2; i+1 < len(line); i++ {
			if eqFold(line[i], "STORE") || eqFold(line[i], "STOREDIST") {
				keys = append(keys, line[i+1])
				i++
			}
		}
	case gkStreams:
		for i := 1; i < len(line); i++ {
			if eqFold(line[i], "STREAMS") {
				rest := line[i+1:]
				nk := len(rest) / 2
				keys = append(keys, rest[:nk]...)
				break
			}
		}
	}
	return keys
}

// commandHelp is the COMMAND HELP reply, the array of simple strings Redis 8.8 returns.
var commandHelp = []string{
	"COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"(no subcommand)",
	"    Return details about all Redis commands.",
	"COUNT",
	"    Return the total number of commands in this Redis server.",
	"LIST",
	"    Return a list of all commands in this Redis server.",
	"INFO [<command-name> ...]",
	"    Return details about multiple Redis commands.",
	"GETKEYS <full-command>",
	"    Return the keys from a full Redis command.",
	"GETKEYSANDFLAGS <full-command>",
	"    Return the keys and the access flags from a full Redis command.",
	"DOCS [<command-name> ...]",
	"    Return documentation details about multiple Redis commands.",
	"HELP",
	"    Print this help.",
}
