package f1srv

// CONFIG is what a client library and the redis-cli call to read and tune server parameters. The
// reply shapes matter for wire compatibility: CONFIG GET returns a flat array of name/value pairs,
// so answering it (like the rest of CONFIG once did) with a bare +OK desynchronizes any client that
// tries to parse the array. This file gives CONFIG a real subcommand dispatcher.
//
// f1srv has no config file and does not expose the hundreds of tunables Redis carries, so CONFIG
// SET accepts and ignores, and CONFIG GET answers from a curated table of the parameters clients
// actually probe on connect. Every value in that table is the Redis 8.8 default and was checked to
// match live Redis 8.8 and Valkey 9.1, so a client that reads maxmemory, the eviction policy, or an
// encoding threshold sees the answer it expects. Parameters outside the table are reported as absent
// (an empty pair list), which is exactly how Redis answers a pattern that matches nothing. The full
// glob-over-every-parameter form is not reproduced: Redis and Valkey iterate their config dictionary
// in different orders and expose different parameter sets, so that output is not portable across
// servers, only the exact-name lookups clients rely on are.

// configParam is one entry in the curated CONFIG GET table: the parameter name and the value aki
// reports for it. The value is the Redis 8.8 default; aki's storage does not use the encoding
// thresholds (it stores every collection element per row rather than in a listpack), but reporting
// the standard default is the compatible answer a client expects when it reads them.
type configParam struct {
	name string
	val  string
}

// configTable is the set of parameters CONFIG GET answers, in a fixed order so a multi-pattern GET
// is deterministic. The values were verified against live Redis 8.8 and Valkey 9.1, which agree on
// every entry here. Instance-specific parameters (port, bind) are deliberately left out: their
// values depend on how a given server was launched and would not match across instances anyway.
var configTable = []configParam{
	{"maxmemory", "0"},
	{"maxmemory-policy", "noeviction"},
	{"maxmemory-samples", "5"},
	{"maxmemory-clients", "0"},
	{"appendonly", "no"},
	{"save", ""},
	{"databases", "16"},
	{"timeout", "0"},
	{"tcp-keepalive", "300"},
	{"maxclients", "10000"},
	{"list-max-listpack-size", "-2"},
	{"hash-max-listpack-entries", "512"},
	{"set-max-intset-entries", "512"},
	{"zset-max-listpack-entries", "128"},
	{"proto-max-bulk-len", "536870912"},
	{"io-threads", "1"},
}

// configHelp is the CONFIG HELP reply, the array of simple strings Redis 8.8 returns.
var configHelp = []string{
	"CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"GET <pattern>",
	"    Return parameters matching the glob-like <pattern> and their values.",
	"SET <directive> <value>",
	"    Set the configuration <directive> to <value>.",
	"RESETSTAT",
	"    Reset statistics reported by the INFO command.",
	"REWRITE",
	"    Rewrite the configuration file.",
	"HELP",
	"    Print this help.",
}

// cmdConfig dispatches the CONFIG subcommands. GET and SET carry the real behaviour; RESETSTAT
// reports success with no counters to clear; REWRITE reports there is no config file to rewrite,
// the same error Redis gives a server started without one; HELP prints the subcommand list; and an
// unrecognized subcommand or a bare CONFIG is the matching error.
func (c *connState) cmdConfig(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'config' command")
		return
	}
	sub := argv[1]
	switch {
	case eqFold(sub, "GET"):
		c.configGet(argv)
	case eqFold(sub, "SET"):
		c.configSet(argv)
	case eqFold(sub, "RESETSTAT"):
		c.writeSimple("OK")
	case eqFold(sub, "REWRITE"):
		c.writeErr("ERR The server is running without a config file")
	case eqFold(sub, "HELP"):
		c.writeArrayHeader(len(configHelp))
		for _, line := range configHelp {
			c.writeSimple(line)
		}
	default:
		c.writeErr("ERR unknown subcommand '" + string(sub) + "'. Try CONFIG HELP.")
	}
}

// configGet answers CONFIG GET by matching each pattern argument against the curated table and
// emitting a flat array of name/value bulk pairs, deduplicated by parameter so a name matched twice
// appears once, with a pattern that matches nothing contributing nothing (the empty array Redis
// returns for an unknown parameter). Names are matched case-insensitively, the way Redis treats
// config names. The reply name follows Redis: for an exact-literal pattern the requested spelling is
// echoed back verbatim, so CONFIG GET APPENDONLY answers with APPENDONLY; only a glob pattern reports
// the canonical lowercase name.
func (c *connState) configGet(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'config|get' command")
		return
	}
	type pair struct{ name, val string }
	var pairs []pair
	// seen keys on the canonical parameter name so a parameter matched by more than one pattern is
	// emitted once, even when the reply name echoes a different requested spelling.
	var seenNames []string
	seen := func(name string) bool {
		for _, s := range seenNames {
			if s == name {
				return true
			}
		}
		return false
	}
	for _, pat := range argv[2:] {
		low := string(lowerASCII(pat))
		if isLiteralPattern(pat) {
			// An exact name: look it up case-insensitively and echo the requested spelling as the
			// reply name, matching Redis. Dedup keys on the canonical table name.
			for _, p := range configTable {
				if p.name == low {
					if !seen(p.name) {
						pairs = append(pairs, pair{string(pat), p.val})
						seenNames = append(seenNames, p.name)
					}
					break
				}
			}
			continue
		}
		// A glob: report every unseen matching parameter under its canonical name, in table order.
		for _, p := range configTable {
			if globMatch([]byte(low), []byte(p.name)) && !seen(p.name) {
				pairs = append(pairs, pair(p))
				seenNames = append(seenNames, p.name)
			}
		}
	}
	c.writeArrayHeader(len(pairs) * 2)
	for _, p := range pairs {
		c.writeBulk([]byte(p.name))
		c.writeBulk([]byte(p.val))
	}
}

// isLiteralPattern reports whether p carries no glob metacharacters, so it is an exact parameter
// name rather than a pattern. Redis echoes a literal request name back verbatim in CONFIG GET, so
// the two cases produce different reply names for the same parameter.
func isLiteralPattern(p []byte) bool {
	for _, b := range p {
		if b == '*' || b == '?' || b == '[' || b == '\\' {
			return false
		}
	}
	return true
}

// configSet answers CONFIG SET, which takes one or more directive/value pairs. aki has no tunables
// that change wire behaviour, so it validates the shape and reports success without storing
// anything. Fewer than a single pair is the wrong-args error; an odd trailing token (a directive
// without a value) is the syntax error Redis gives, matching Redis's own accept/reject.
func (c *connState) configSet(argv [][]byte) {
	rest := argv[2:]
	if len(rest) < 2 {
		c.writeErr("ERR wrong number of arguments for 'config|set' command")
		return
	}
	if len(rest)%2 != 0 {
		c.writeErr("ERR syntax error")
		return
	}
	c.writeSimple("OK")
}

// lowerASCII returns a lowercase copy of b for case-insensitive glob matching of config names. The
// names are ASCII, so a byte-wise fold is enough. It always copies so the caller's parse buffer is
// left untouched.
func lowerASCII(b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		ch := b[i]
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		out[i] = ch
	}
	return out
}
