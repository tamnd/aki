package command

import (
	"slices"
	"strconv"
	"strings"
	"sync"
)

// This file implements the runtime configuration store and the CONFIG command
// (doc 24 part A, doc 20). Directives are held as strings in their canonical
// form, the same form CONFIG GET prints and CONFIG REWRITE would write. Each
// directive carries a validator that parses and re-canonicalizes a value, so a
// CONFIG SET round trips through CONFIG GET unchanged.

// directiveKind drives validation and canonical formatting.
type directiveKind uint8

const (
	dirString directiveKind = iota
	dirBool
	dirInt
	dirMemory
	dirEnum
	dirSave
)

// directive describes one configuration setting.
type directive struct {
	name    string
	kind    directiveKind
	def     string   // default value, already canonical
	mutable bool     // true if CONFIG SET may change it at runtime
	enum    []string // allowed values for dirEnum, lowercase
}

// configStore holds the live value of every known directive. It is shared across
// connection goroutines, so every access takes the lock.
type configStore struct {
	mu    sync.RWMutex
	defs  map[string]*directive
	order []string // directive names in registration order
	vals  map[string]string
}

// newConfigStore builds the store seeded with defaults. The dispatcher overrides
// a few entries from its Config afterwards so CONFIG GET reflects the running
// server.
func newConfigStore() *configStore {
	cs := &configStore{
		defs: make(map[string]*directive),
		vals: make(map[string]string),
	}
	for _, d := range configDirectives() {
		cs.defs[d.name] = d
		cs.order = append(cs.order, d.name)
		cs.vals[d.name] = d.def
	}
	return cs
}

// set writes a value already known to be valid and canonical.
func (cs *configStore) set(name, val string) {
	cs.mu.Lock()
	cs.vals[name] = val
	cs.mu.Unlock()
}

// get returns the current value of a directive. The second result is false when
// the directive is unknown. INFO reads a few directives through it.
func (cs *configStore) get(name string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	v, ok := cs.vals[name]
	return v, ok
}

// configDirectives is the directive table. It is a representative cut of the
// redis.conf surface: the network, memory, persistence, data-type-limit,
// notification and housekeeping directives clients actually read and write.
func configDirectives() []*directive {
	memPolicies := []string{
		"noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random",
		"volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl",
	}
	return []*directive{
		// Network.
		{name: "bind", kind: dirString, def: "127.0.0.1 -::1"},
		{name: "port", kind: dirInt, def: "6379"},
		{name: "tcp-backlog", kind: dirInt, def: "511"},
		{name: "timeout", kind: dirInt, def: "0", mutable: true},
		{name: "tcp-keepalive", kind: dirInt, def: "300", mutable: true},
		{name: "protected-mode", kind: dirBool, def: "yes", mutable: true},
		{name: "maxclients", kind: dirInt, def: "10000", mutable: true},
		{name: "unixsocket", kind: dirString, def: ""},

		// General.
		{name: "databases", kind: dirInt, def: "16"},
		{name: "loglevel", kind: dirEnum, def: "notice", mutable: true,
			enum: []string{"nothing", "warning", "notice", "verbose", "debug"}},
		{name: "logfile", kind: dirString, def: ""},
		{name: "requirepass", kind: dirString, def: "", mutable: true},

		// Memory and eviction.
		{name: "maxmemory", kind: dirMemory, def: "0", mutable: true},
		{name: "maxmemory-policy", kind: dirEnum, def: "noeviction", mutable: true, enum: memPolicies},
		{name: "maxmemory-samples", kind: dirInt, def: "5", mutable: true},
		{name: "maxmemory-eviction-tenacity", kind: dirInt, def: "10", mutable: true},
		{name: "maxmemory-clients", kind: dirString, def: "0", mutable: true},
		{name: "lfu-log-factor", kind: dirInt, def: "10", mutable: true},
		{name: "lfu-decay-time", kind: dirInt, def: "1", mutable: true},
		{name: "active-expire-enabled", kind: dirBool, def: "yes", mutable: true},
		{name: "active-expire-effort", kind: dirInt, def: "1", mutable: true},

		// Persistence.
		{name: "save", kind: dirSave, def: "3600 1 300 100 60 10000", mutable: true},
		{name: "stop-writes-on-bgsave-error", kind: dirBool, def: "yes", mutable: true},
		{name: "rdbcompression", kind: dirBool, def: "yes", mutable: true},
		{name: "rdbchecksum", kind: dirBool, def: "yes", mutable: true},
		{name: "dbfilename", kind: dirString, def: "dump.rdb", mutable: true},
		{name: "dir", kind: dirString, def: "."},
		{name: "appendonly", kind: dirBool, def: "no", mutable: true},
		{name: "appendfilename", kind: dirString, def: "appendonly.aof"},
		{name: "appendfsync", kind: dirEnum, def: "everysec", mutable: true,
			enum: []string{"always", "everysec", "no"}},

		// Replication.
		{name: "replica-read-only", kind: dirBool, def: "yes", mutable: true},
		{name: "masterauth", kind: dirString, def: "", mutable: true},
		{name: "repl-backlog-size", kind: dirMemory, def: "1048576", mutable: true},

		// Data-type limits.
		{name: "list-max-listpack-size", kind: dirInt, def: "128", mutable: true},
		{name: "list-max-ziplist-size", kind: dirInt, def: "128", mutable: true},
		{name: "hash-max-listpack-entries", kind: dirInt, def: "128", mutable: true},
		{name: "hash-max-listpack-value", kind: dirInt, def: "64", mutable: true},
		{name: "set-max-intset-entries", kind: dirInt, def: "512", mutable: true},
		{name: "set-max-listpack-entries", kind: dirInt, def: "128", mutable: true},
		{name: "set-max-listpack-value", kind: dirInt, def: "64", mutable: true},
		{name: "zset-max-listpack-entries", kind: dirInt, def: "128", mutable: true},
		{name: "zset-max-listpack-value", kind: dirInt, def: "64", mutable: true},
		{name: "proto-max-bulk-len", kind: dirMemory, def: "536870912", mutable: true},

		// Notifications, slowlog, housekeeping.
		{name: "notify-keyspace-events", kind: dirString, def: "", mutable: true},
		{name: "slowlog-log-slower-than", kind: dirInt, def: "10000", mutable: true},
		{name: "slowlog-max-len", kind: dirInt, def: "128", mutable: true},
		{name: "latency-monitor-threshold", kind: dirInt, def: "0", mutable: true},
		{name: "hz", kind: dirInt, def: "10", mutable: true},
		{name: "activerehashing", kind: dirBool, def: "yes", mutable: true},
		{name: "lazyfree-lazy-eviction", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-expire", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-server-del", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-user-del", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-user-flush", kind: dirBool, def: "no", mutable: true},
	}
}

// validateValue parses raw against a directive and returns the canonical form.
func validateValue(d *directive, raw string) (string, bool) {
	switch d.kind {
	case dirBool:
		return parseConfigBool(raw)
	case dirInt:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(n, 10), true
	case dirMemory:
		n, ok := parseMemory(raw)
		if !ok {
			return "", false
		}
		return strconv.FormatInt(n, 10), true
	case dirEnum:
		low := strings.ToLower(raw)
		if slices.Contains(d.enum, low) {
			return low, true
		}
		return "", false
	case dirSave:
		return canonicalizeSave(raw)
	default:
		return raw, true
	}
}

// parseConfigBool accepts the boolean spellings redis.conf allows and returns the
// canonical yes or no.
func parseConfigBool(raw string) (string, bool) {
	switch strings.ToLower(raw) {
	case "yes", "true", "1":
		return "yes", true
	case "no", "false", "0":
		return "no", true
	}
	return "", false
}

// parseMemory parses a byte count with an optional kb/mb/gb/tb suffix. Fractions
// are rejected, matching Redis.
func parseMemory(raw string) (int64, bool) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return 0, false
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "kb"):
		mult, s = 1024, s[:len(s)-2]
	case strings.HasSuffix(s, "mb"):
		mult, s = 1024*1024, s[:len(s)-2]
	case strings.HasSuffix(s, "gb"):
		mult, s = 1024*1024*1024, s[:len(s)-2]
	case strings.HasSuffix(s, "tb"):
		mult, s = 1024*1024*1024*1024, s[:len(s)-2]
	case strings.HasSuffix(s, "b"):
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n * mult, true
}

// canonicalizeSave validates a save string of whitespace-separated
// seconds/changes pairs and returns it with single-space separators. An empty
// string disables auto-save and is valid.
func canonicalizeSave(raw string) (string, bool) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return "", true
	}
	if len(fields)%2 != 0 {
		return "", false
	}
	for _, f := range fields {
		if _, err := strconv.Atoi(f); err != nil {
			return "", false
		}
	}
	return strings.Join(fields, " "), true
}

// configCommands returns the CONFIG container command.
func configCommands() []*CmdDesc {
	config := &CmdDesc{
		Name: "config", Group: GroupServer, Since: "2.0.0",
		Arity: -2, Flags: FlagLoading | FlagStale,
		Handler: handleConfigHelp,
		SubCmds: []*CmdDesc{
			{Name: "get", SubName: "config|get", Group: GroupServer, Since: "2.0.0",
				Arity: -3, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleConfigGet},
			{Name: "set", SubName: "config|set", Group: GroupServer, Since: "2.0.0",
				Arity: -4, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleConfigSet},
			{Name: "resetstat", SubName: "config|resetstat", Group: GroupServer, Since: "2.0.0",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleConfigResetStat},
			{Name: "rewrite", SubName: "config|rewrite", Group: GroupServer, Since: "2.8.0",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagAdmin, Handler: handleConfigRewrite},
			{Name: "help", SubName: "config|help", Group: GroupServer, Since: "5.0.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleConfigHelp},
		},
	}
	return []*CmdDesc{config}
}

// handleConfigGet implements CONFIG GET pattern [pattern ...]. It returns every
// directive whose name matches any glob pattern, as a map of name to value.
func handleConfigGet(ctx *Ctx) {
	cs := ctx.d.conf
	patterns := ctx.Argv[2:]

	cs.mu.RLock()
	type pair struct{ name, val string }
	var out []pair
	seen := make(map[string]bool)
	for _, name := range cs.order {
		for _, p := range patterns {
			if stringMatch(p, []byte(name), true) {
				if !seen[name] {
					seen[name] = true
					out = append(out, pair{name, cs.vals[name]})
				}
				break
			}
		}
	}
	cs.mu.RUnlock()

	enc := ctx.enc()
	enc.WriteMapLen(len(out))
	for _, p := range out {
		enc.WriteBulkStringStr(p.name)
		enc.WriteBulkStringStr(p.val)
	}
}

// handleConfigSet implements CONFIG SET directive value [directive value ...].
// Every pair is validated before any is applied, so a bad pair leaves the whole
// command without effect.
func handleConfigSet(ctx *Ctx) {
	args := ctx.Argv[2:]
	if len(args)%2 != 0 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'config|set' command")
		return
	}
	cs := ctx.d.conf

	type change struct{ name, val string }
	changes := make([]change, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		name := strings.ToLower(string(args[i]))
		d, ok := cs.defs[name]
		if !ok {
			ctx.enc().WriteError("ERR Unknown option or number of arguments for CONFIG SET - '" + name + "'")
			return
		}
		if !d.mutable {
			ctx.enc().WriteError("ERR CONFIG SET failed (possibly related to argument '" + name + "') - can't set immutable config")
			return
		}
		canon, ok := validateValue(d, string(args[i+1]))
		if !ok {
			ctx.enc().WriteError("ERR CONFIG SET failed (possibly related to argument '" + name + "') - argument couldn't be parsed into an integer")
			return
		}
		changes = append(changes, change{name, canon})
	}

	cs.mu.Lock()
	for _, c := range changes {
		cs.vals[c.name] = c.val
	}
	cs.mu.Unlock()
	ctx.enc().WriteStatus("OK")
}

// handleConfigResetStat clears server statistics. aki keeps no command stats yet,
// so this is an acknowledged no-op.
func handleConfigResetStat(ctx *Ctx) {
	ctx.enc().WriteStatus("OK")
}

// handleConfigRewrite rewrites the startup config file. aki does not load a
// config file yet, so it always reports the no-file condition Redis returns in
// that case.
func handleConfigRewrite(ctx *Ctx) {
	ctx.enc().WriteError("ERR The server is running without a config file")
}

// handleConfigHelp returns the subcommand summary.
func handleConfigHelp(ctx *Ctx) {
	lines := []string{
		"CONFIG <subcommand> [<arg> ...]. Subcommands are:",
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
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}
