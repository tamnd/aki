package f1srv

import "strconv"

// SLOWLOG and LATENCY are the two introspection commands a client library or a monitoring tool
// probes to read a server's slow-query log and its latency-event history. f1srv keeps neither
// log, so every read is empty and every reset is a no-op, but the replies still have to be the
// exact shapes Redis returns so a monitoring client parses them without complaint. Redis 8.8 and
// Valkey 9.1 agree byte-for-byte on all of these except LATENCY DOCTOR (whose text names the
// server) and LATENCY HISTOGRAM (live per-command state), so DOCTOR follows the Redis wording
// aki advertises everywhere else and HISTOGRAM reports the empty set aki actually tracks.

// slowlogHelp and latencyHelp are the HELP subcommand replies, arrays of RESP simple strings
// copied verbatim from Redis 8.8 (Valkey 9.1 emits the same bytes). They are the one place the
// subcommand text has to be reproduced exactly, so they live as literals next to the dispatch.
var slowlogHelp = []string{
	"SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"GET [<count>]",
	"    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
	"    Entries are made of:",
	"    id, timestamp, time in microseconds, arguments array, client IP and port,",
	"    client name",
	"LEN",
	"    Return the length of the slowlog.",
	"RESET",
	"    Reset the slowlog.",
	"HELP",
	"    Print this help.",
}

var latencyHelp = []string{
	"LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"DOCTOR",
	"    Return a human readable latency analysis report.",
	"GRAPH <event>",
	"    Return an ASCII latency graph for the <event> class.",
	"HISTORY <event>",
	"    Return time-latency samples for the <event> class.",
	"LATEST",
	"    Return the latest latency samples for all events.",
	"RESET [<event> ...]",
	"    Reset latency data of one or more <event> classes.",
	"    (default: reset all data for all event classes)",
	"HISTOGRAM [COMMAND ...]",
	"    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
	"    If no commands are specified then all histograms are replied.",
	"HELP",
	"    Print this help.",
}

// latencyDoctor is the report Redis returns when latency monitoring is off, which it always is
// here. Valkey substitutes its own name in this string, so matching Redis exactly is the
// deliberate compat choice, the same call aki makes for the INFO version and the ROLE tag.
const latencyDoctor = "I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this Redis instance. " +
	"You may use \"CONFIG SET latency-monitor-threshold <milliseconds>.\" in order to enable it. " +
	"If we weren't in a deep space mission I'd suggest to take a look at " +
	"https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency-monitor.\n"

// writeSimpleArray writes an array whose every element is a RESP simple string, the reply shape
// a HELP subcommand uses.
func (c *connState) writeSimpleArray(lines []string) {
	c.writeArrayHeader(len(lines))
	for _, l := range lines {
		c.writeSimple(l)
	}
}

// cmdSlowlog implements SLOWLOG GET/LEN/RESET/HELP. The server keeps no slow log, so GET is
// always the empty array and LEN is always zero, but the argument checking and the subcommand
// error strings mirror Redis so a client that probes SLOWLOG on connect sees the exact replies.
func (c *connState) cmdSlowlog(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'slowlog' command")
		return
	}
	sub := argv[1]
	switch {
	case eqFold(sub, "GET"):
		// GET takes an optional count; more than one trailing argument is the wrong-arg error, and a
		// count that is not an integer >= -1 is rejected before the (always empty) log is returned.
		if len(argv) > 3 {
			c.writeErr("ERR unknown subcommand or wrong number of arguments for '" + string(sub) + "'. Try SLOWLOG HELP.")
			return
		}
		if len(argv) == 3 {
			n, err := strconv.Atoi(string(argv[2]))
			if err != nil || n < -1 {
				c.writeErr("ERR count should be greater than or equal to -1")
				return
			}
		}
		c.writeArrayHeader(0)
	case eqFold(sub, "LEN"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'slowlog|len' command")
			return
		}
		c.writeInt(0)
	case eqFold(sub, "RESET"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'slowlog|reset' command")
			return
		}
		c.writeSimple("OK")
	case eqFold(sub, "HELP"):
		c.writeSimpleArray(slowlogHelp)
	default:
		c.writeErr("ERR unknown subcommand '" + string(sub) + "'. Try SLOWLOG HELP.")
	}
}

// cmdLatency implements LATENCY RESET/HISTORY/LATEST/GRAPH/HELP plus DOCTOR and HISTOGRAM. The
// server records no latency events, so RESET reports zero classes cleared, HISTORY and LATEST are
// empty, GRAPH has no samples for any event, and HISTOGRAM is the empty set. DOCTOR returns the
// latency-monitoring-disabled report.
func (c *connState) cmdLatency(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'latency' command")
		return
	}
	sub := argv[1]
	switch {
	case eqFold(sub, "RESET"):
		// RESET takes any number of event names and answers with the count of classes it cleared,
		// which is always zero because nothing is recorded.
		c.writeInt(0)
	case eqFold(sub, "HISTORY"):
		if len(argv) != 3 {
			c.writeErr("ERR wrong number of arguments for 'latency|history' command")
			return
		}
		c.writeArrayHeader(0)
	case eqFold(sub, "LATEST"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'latency|latest' command")
			return
		}
		c.writeArrayHeader(0)
	case eqFold(sub, "GRAPH"):
		if len(argv) != 3 {
			c.writeErr("ERR wrong number of arguments for 'latency|graph' command")
			return
		}
		c.writeErr("ERR No samples available for event '" + string(argv[2]) + "'")
	case eqFold(sub, "DOCTOR"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'latency|doctor' command")
			return
		}
		c.writeBulk([]byte(latencyDoctor))
	case eqFold(sub, "HISTOGRAM"):
		// f1srv tracks no per-command latency, so the histogram is empty regardless of the command
		// filters the client passes.
		c.writeArrayHeader(0)
	case eqFold(sub, "HELP"):
		c.writeSimpleArray(latencyHelp)
	default:
		c.writeErr("ERR unknown subcommand '" + string(sub) + "'. Try LATENCY HELP.")
	}
}
