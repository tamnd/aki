package dispatch

import (
	"sync"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// CONFIG serves the server-parameter surface (spec 2064/f3/11, the M11
// command-closure milestone). Clients read it on connect: redis-cli and many
// drivers issue CONFIG GET maxmemory or CONFIG GET save before their first real
// command, and test harnesses set a handful of parameters to shape a run. f3
// holds a small table of the parameters a client actually queries, each seeded
// with the value that describes how f3 already behaves.
//
// The honest edge: these values are cosmetic today. GET reflects the seed or
// whatever a later SET stored, but SET does not change engine behavior yet.
// maxmemory does not drive eviction and save does not drive snapshotting,
// because the eviction budgets (M9) and the .aki snapshot timer (M8) are their
// own arcs. CONFIG lets a client negotiate and read back its settings without
// erroring; the day those arcs land they read their live value from this same
// store. Parameters f3 has no analog for, chiefly the encoding thresholds, are
// deliberately absent rather than exposed as knobs that do nothing: f3's
// encodings are adaptive and not client-tunable, so a GET for them matches
// Redis's answer for an unknown parameter, the empty result.

var (
	configMu sync.RWMutex
	// configOrder fixes the CONFIG GET * reply order so it is stable across
	// calls; configVals holds the live value of each parameter, seeded to
	// describe f3's actual behavior and mutated by CONFIG SET.
	configOrder = []string{
		"maxmemory",
		"maxmemory-policy",
		"maxmemory-samples",
		"save",
		"appendonly",
		"appendfsync",
		"databases",
		"timeout",
		"tcp-keepalive",
		"maxclients",
		"proto-max-bulk-len",
	}
	configVals = map[string]string{
		"maxmemory":          "0",
		"maxmemory-policy":   "noeviction",
		"maxmemory-samples":  "5",
		"save":               "",
		"appendonly":         "no",
		"appendfsync":        "everysec",
		"databases":          "1",
		"timeout":            "0",
		"tcp-keepalive":      "300",
		"maxclients":         "10000",
		"proto-max-bulk-len": "536870912",
	}
)

// configCmd answers CONFIG GET/SET/RESETSTAT/REWRITE. The subcommand sits at
// args[0]; register bounds the arity at one argument so args[0] is always
// present here.
func configCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch upperVerb(args[0]) {
	case "GET":
		configGet(cx, args[1:], r)
	case "SET":
		configSet(args[1:], r)
	case "RESETSTAT":
		// f3's INFO counters live across the shards; resetting them is a
		// separate arc, so this acks without clearing them. Clients call it
		// between benchmark runs and expect the OK.
		r.Status("OK")
	case "REWRITE":
		// f3 runs from flags, not a config file, so there is nothing to rewrite,
		// which is exactly the case Redis reports this way.
		r.Err("ERR The server is running without a config file")
	default:
		r.Err("ERR Unknown CONFIG subcommand or wrong number of arguments")
	}
}

// configGet answers CONFIG GET pattern [pattern ...]: a flat array of the name
// and value of every known parameter matching any pattern. A parameter matched
// by more than one pattern appears once, in the fixed configOrder, matching
// Redis. No match is the empty array, not an error.
func configGet(cx *shard.Ctx, patterns [][]byte, r shard.Reply) {
	if len(patterns) == 0 {
		r.Err("ERR wrong number of arguments for 'config|get' command")
		return
	}
	configMu.RLock()
	defer configMu.RUnlock()
	// Walk configOrder once, emitting a parameter the first time any pattern
	// matches it, so the reply order is stable and each name appears once.
	matched := make([]string, 0, len(configOrder))
	for _, name := range configOrder {
		for _, pat := range patterns {
			if globMatch(pat, []byte(name)) {
				matched = append(matched, name)
				break
			}
		}
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], len(matched)*2)
	for _, name := range matched {
		out = resp.AppendBulk(out, []byte(name))
		out = resp.AppendBulk(out, []byte(configVals[name]))
	}
	cx.Aux = out
	r.Raw(out)
}

// configSet answers CONFIG SET param value [param value ...]. It validates every
// pair before applying any, so an unknown parameter in the tail leaves the store
// untouched, the atomic contract Redis holds. The value is stored verbatim; f3
// does not normalize memory suffixes because the value is cosmetic until the
// eviction and persistence arcs read it.
func configSet(pairs [][]byte, r shard.Reply) {
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		r.Err("ERR wrong number of arguments for 'config|set' command")
		return
	}
	configMu.Lock()
	defer configMu.Unlock()
	for i := 0; i < len(pairs); i += 2 {
		name := lowerASCII(pairs[i])
		if _, ok := configVals[name]; !ok {
			r.Err("ERR Unknown option or number of arguments for CONFIG SET - '" + name + "'")
			return
		}
	}
	for i := 0; i < len(pairs); i += 2 {
		configVals[lowerASCII(pairs[i])] = string(pairs[i+1])
	}
	r.Status("OK")
}

// lowerASCII lowercases an ASCII parameter name for the case-insensitive lookup
// Redis does on CONFIG parameter names.
func lowerASCII(b []byte) string {
	buf := make([]byte, len(b))
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 0x20
		}
		buf[i] = c
	}
	return string(buf)
}
