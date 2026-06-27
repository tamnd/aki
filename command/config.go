package command

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	dirNotify
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
//
// A handful of directives are read on the per-command hot path (appendonly,
// appendfsync, maxmemory, cluster-enabled, the slowlog and latency thresholds,
// and the per-write replica and bgsave guards). Taking the RWMutex for those
// reads showed up as the top contended atomic in the post-E6 profile, so the
// store mirrors them into atomics that set() keeps current. The hot readers load
// the atomic and never touch the map lock; the map stays the source of truth for
// CONFIG GET and the cold directives.
type configStore struct {
	mu    sync.RWMutex
	defs  map[string]*directive
	order []string // directive names in registration order
	vals  map[string]string
	alias map[string]string // each alias name to its twin, both directions

	aofOn    atomic.Bool  // mirrors appendonly == "yes"
	aofFsync atomic.Int32 // mirrors appendfsync, one of the fsync* codes
	maxmem   atomic.Int64 // mirrors maxmemory in bytes, read on every denyoom write

	// The directives below are each read once per command (the cluster, slowlog,
	// and latency checks fire on reads too; the rest fire per write). They mirror
	// the same way so the per-command path never takes the config lock.
	clusterOn        atomic.Bool  // mirrors cluster-enabled == "yes"
	slowlogThresh    atomic.Int64 // mirrors slowlog-log-slower-than
	latencyThresh    atomic.Int64 // mirrors latency-monitor-threshold
	serveStale       atomic.Bool  // mirrors replica-serve-stale-data == "yes"
	stopWritesBgsave atomic.Bool  // mirrors stop-writes-on-bgsave-error == "yes"
	minReplWrite     atomic.Int64 // mirrors min-replicas-to-write
	minReplMaxLag    atomic.Int64 // mirrors min-replicas-max-lag
	aofTimestamp     atomic.Bool  // mirrors aof-timestamp-enabled == "yes"

	// The OBJECT ENCODING thresholds are read together once per write, eight
	// directives at a time, through encLimits. Taking the config RWMutex eight
	// times per command made confInt the top reader-counter contention under a
	// write storm (every connection on one shard incrementing the same RWMutex
	// readerCount), so they mirror into atomics the same way the flags above do.
	encListSize    atomic.Int64 // mirrors list-max-listpack-size
	encHashEntries atomic.Int64 // mirrors hash-max-listpack-entries
	encHashValue   atomic.Int64 // mirrors hash-max-listpack-value
	encSetIntset   atomic.Int64 // mirrors set-max-intset-entries
	encSetEntries  atomic.Int64 // mirrors set-max-listpack-entries
	encSetValue    atomic.Int64 // mirrors set-max-listpack-value
	encZsetEntries atomic.Int64 // mirrors zset-max-listpack-entries
	encZsetValue   atomic.Int64 // mirrors zset-max-listpack-value
}

// fsync policy codes mirror the appendfsync directive. everysec is zero so the
// store's zero value matches the directive default.
const (
	fsyncEverysec int32 = iota
	fsyncAlways
	fsyncNo
)

// fsyncCode maps a canonical appendfsync value to its code. An unrecognized value
// falls back to everysec, the directive default.
func fsyncCode(v string) int32 {
	switch v {
	case "always":
		return fsyncAlways
	case "no":
		return fsyncNo
	default:
		return fsyncEverysec
	}
}

// newConfigStore builds the store seeded with defaults. The dispatcher overrides
// a few entries from its Config afterwards so CONFIG GET reflects the running
// server.
func newConfigStore() *configStore {
	cs := &configStore{
		defs:  make(map[string]*directive),
		vals:  make(map[string]string),
		alias: make(map[string]string),
	}
	for _, d := range configDirectives() {
		cs.defs[d.name] = d
		cs.order = append(cs.order, d.name)
		cs.vals[d.name] = d.def
	}
	for _, p := range configAliasPairs() {
		cs.alias[p[0]] = p[1]
		cs.alias[p[1]] = p[0]
	}
	for _, name := range mirroredDirectives {
		cs.mirror(name, cs.vals[name])
	}
	return cs
}

// configAliasPairs lists the directive names Redis keeps as aliases of each
// other. The slave-era names alias the replica-era names, and the ziplist-era
// names alias the listpack-era names. CONFIG GET returns both, and a CONFIG SET
// to either name updates both so a client reading the other name sees the change.
func configAliasPairs() [][2]string {
	return [][2]string{
		{"list-max-listpack-size", "list-max-ziplist-size"},
		{"hash-max-listpack-entries", "hash-max-ziplist-entries"},
		{"hash-max-listpack-value", "hash-max-ziplist-value"},
		{"zset-max-listpack-entries", "zset-max-ziplist-entries"},
		{"zset-max-listpack-value", "zset-max-ziplist-value"},
		{"lua-time-limit", "busy-reply-threshold"},
		{"min-replicas-to-write", "min-slaves-to-write"},
		{"min-replicas-max-lag", "min-slaves-max-lag"},
		{"cluster-replica-no-failover", "cluster-slave-no-failover"},
	}
}

// set writes a value already known to be valid and canonical. The two hot-path
// directives also refresh their atomic mirror so the per-command readers stay
// lock-free.
func (cs *configStore) set(name, val string) {
	cs.mu.Lock()
	cs.vals[name] = val
	cs.mu.Unlock()
	cs.mirror(name, val)
}

// mirroredDirectives lists every directive whose value is also kept in an atomic
// for the per-command path. newConfigStore seeds the atomics by replaying these
// through mirror, and the alias twins appear so a CONFIG SET to the slave-era
// name updates the mirror too.
var mirroredDirectives = []string{
	"appendonly",
	"appendfsync",
	"maxmemory",
	"cluster-enabled",
	"slowlog-log-slower-than",
	"latency-monitor-threshold",
	"replica-serve-stale-data",
	"stop-writes-on-bgsave-error",
	"min-replicas-to-write",
	"min-slaves-to-write",
	"min-replicas-max-lag",
	"min-slaves-max-lag",
	"aof-timestamp-enabled",
	"list-max-listpack-size",
	"list-max-ziplist-size",
	"hash-max-listpack-entries",
	"hash-max-ziplist-entries",
	"hash-max-listpack-value",
	"hash-max-ziplist-value",
	"set-max-intset-entries",
	"set-max-listpack-entries",
	"set-max-listpack-value",
	"zset-max-listpack-entries",
	"zset-max-ziplist-entries",
	"zset-max-listpack-value",
	"zset-max-ziplist-value",
}

// mirror refreshes the atomic copy of a hot-path directive after its map value
// changes. It is a no-op for every directive that is not mirrored. CONFIG SET
// writes the map directly, so it calls this too. The min-replicas pair lists both
// the replica-era and slave-era spellings because either name can carry the set.
func (cs *configStore) mirror(name, val string) {
	switch name {
	case "appendonly":
		cs.aofOn.Store(val == "yes")
	case "appendfsync":
		cs.aofFsync.Store(fsyncCode(val))
	case "maxmemory":
		cs.storeInt(&cs.maxmem, val)
	case "cluster-enabled":
		cs.clusterOn.Store(val == "yes")
	case "slowlog-log-slower-than":
		cs.storeInt(&cs.slowlogThresh, val)
	case "latency-monitor-threshold":
		cs.storeInt(&cs.latencyThresh, val)
	case "replica-serve-stale-data":
		cs.serveStale.Store(val == "yes")
	case "stop-writes-on-bgsave-error":
		cs.stopWritesBgsave.Store(val == "yes")
	case "min-replicas-to-write", "min-slaves-to-write":
		cs.storeInt(&cs.minReplWrite, val)
	case "min-replicas-max-lag", "min-slaves-max-lag":
		cs.storeInt(&cs.minReplMaxLag, val)
	case "aof-timestamp-enabled":
		cs.aofTimestamp.Store(val == "yes")
	case "list-max-listpack-size", "list-max-ziplist-size":
		cs.storeInt(&cs.encListSize, val)
	case "hash-max-listpack-entries", "hash-max-ziplist-entries":
		cs.storeInt(&cs.encHashEntries, val)
	case "hash-max-listpack-value", "hash-max-ziplist-value":
		cs.storeInt(&cs.encHashValue, val)
	case "set-max-intset-entries":
		cs.storeInt(&cs.encSetIntset, val)
	case "set-max-listpack-entries":
		cs.storeInt(&cs.encSetEntries, val)
	case "set-max-listpack-value":
		cs.storeInt(&cs.encSetValue, val)
	case "zset-max-listpack-entries", "zset-max-ziplist-entries":
		cs.storeInt(&cs.encZsetEntries, val)
	case "zset-max-listpack-value", "zset-max-ziplist-value":
		cs.storeInt(&cs.encZsetValue, val)
	}
}

// encodingLimits snapshots the eight OBJECT ENCODING thresholds from their atomic
// mirrors so the per-write encLimits read never takes the config lock. The values
// are seeded from the directive defaults in newConfigStore and refreshed by every
// CONFIG SET through mirror, so a load here matches what the map would return.
func (cs *configStore) encodingLimits() encLimits {
	return encLimits{
		listSize:    cs.encListSize.Load(),
		hashEntries: cs.encHashEntries.Load(),
		hashValue:   cs.encHashValue.Load(),
		setIntset:   cs.encSetIntset.Load(),
		setEntries:  cs.encSetEntries.Load(),
		setValue:    cs.encSetValue.Load(),
		zsetEntries: cs.encZsetEntries.Load(),
		zsetValue:   cs.encZsetValue.Load(),
	}
}

// storeInt parses a canonical integer directive value and stores it in dst,
// leaving dst unchanged when the value does not parse.
func (cs *configStore) storeInt(dst *atomic.Int64, val string) {
	if n, err := strconv.ParseInt(val, 10, 64); err == nil {
		dst.Store(n)
	}
}

// The accessors below read the atomic mirror so the per-command path stays
// lock-free. Each mirrors one directive and matches the default its directive
// declares.
func (cs *configStore) maxMemory() int64              { return cs.maxmem.Load() }
func (cs *configStore) clusterEnabled() bool          { return cs.clusterOn.Load() }
func (cs *configStore) slowlogThreshold() int64       { return cs.slowlogThresh.Load() }
func (cs *configStore) latencyThreshold() int64       { return cs.latencyThresh.Load() }
func (cs *configStore) serveStaleData() bool          { return cs.serveStale.Load() }
func (cs *configStore) stopWritesOnBgsaveError() bool { return cs.stopWritesBgsave.Load() }
func (cs *configStore) minReplicasToWrite() int64     { return cs.minReplWrite.Load() }
func (cs *configStore) minReplicasMaxLag() int64      { return cs.minReplMaxLag.Load() }
func (cs *configStore) aofTimestampEnabled() bool     { return cs.aofTimestamp.Load() }

// appendOnly reports whether appendonly is on, reading the atomic mirror so the
// per-command write path never takes the config lock.
func (cs *configStore) appendOnly() bool { return cs.aofOn.Load() }

// fsyncPolicy returns the canonical appendfsync value from the atomic mirror.
func (cs *configStore) fsyncPolicy() string {
	switch cs.aofFsync.Load() {
	case fsyncAlways:
		return "always"
	case fsyncNo:
		return "no"
	default:
		return "everysec"
	}
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
		{name: "tracking-table-max-keys", kind: dirInt, def: "1000000", mutable: true},

		// General.
		{name: "databases", kind: dirInt, def: "16"},
		{name: "loglevel", kind: dirEnum, def: "notice", mutable: true,
			enum: []string{"nothing", "warning", "notice", "verbose", "debug"}},
		{name: "logfile", kind: dirString, def: "", mutable: true},
		{name: "log-format", kind: dirEnum, def: "redis", mutable: true,
			enum: []string{"redis", "json"}},
		{name: "syslog-enabled", kind: dirBool, def: "no"},
		{name: "syslog-ident", kind: dirString, def: "aki"},
		{name: "syslog-facility", kind: dirString, def: "local0"},
		{name: "crash-log-enabled", kind: dirBool, def: "yes", mutable: true},
		{name: "crash-memlog-enabled", kind: dirBool, def: "yes", mutable: true},
		{name: "shutdown-timeout", kind: dirInt, def: "10", mutable: true},
		{name: "requirepass", kind: dirString, def: "", mutable: true},

		// Metrics export.
		{name: "metrics-port", kind: dirInt, def: "0"},
		{name: "metrics-bind", kind: dirString, def: "127.0.0.1"},
		{name: "metrics-tls", kind: dirBool, def: "no"},

		// Admin endpoint (doc 21 section 10.1). Serves Go's net/http/pprof
		// handlers, plus /metrics and the health probes, on its own port.
		// Bound to loopback by default. Set admin-port to 0 to turn it off.
		{name: "admin-port", kind: dirInt, def: "6399"},
		{name: "admin-bind", kind: dirString, def: "127.0.0.1"},

		// Go GC knobs (doc 21 section 10.3). go-gogc drives debug.SetGCPercent
		// (100 is the runtime default, 0 turns the GC off) and go-memlimit drives
		// debug.SetMemoryLimit (0 means no soft limit). Both apply at startup and
		// take effect live through CONFIG SET.
		{name: "go-gogc", kind: dirInt, def: "100", mutable: true},
		{name: "go-memlimit", kind: dirMemory, def: "0", mutable: true},
		// go-maxprocs pins GOMAXPROCS. 0 leaves the runtime default of one P per
		// CPU; a positive value caps the scheduler's parallelism to cut the futex
		// wakeup churn the per-request goroutine hops cause on a many-core box.
		{name: "go-maxprocs", kind: dirInt, def: "0", mutable: true},

		// Continuous profiling (doc 21 section 10.2). When on, a background
		// goroutine writes cpu, heap, and mutex pprof snapshots to profiling-dir
		// every profiling-interval seconds and keeps the newest profiling-keep.
		{name: "continuous-profiling", kind: dirBool, def: "no"},
		{name: "profiling-dir", kind: dirString, def: "./profiles"},
		{name: "profiling-interval", kind: dirInt, def: "60"},
		{name: "profiling-keep", kind: dirInt, def: "10"},

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
		{name: "dir", kind: dirString, def: ".", mutable: true},
		{name: "appendonly", kind: dirBool, def: "no", mutable: true},
		{name: "appendfilename", kind: dirString, def: "appendonly.aof"},
		{name: "appenddirname", kind: dirString, def: "appendonlydir"},
		{name: "appendfsync", kind: dirEnum, def: "everysec", mutable: true,
			enum: []string{"always", "everysec", "no"}},
		{name: "no-appendfsync-on-rewrite", kind: dirBool, def: "no", mutable: true},
		{name: "auto-aof-rewrite-percentage", kind: dirInt, def: "100", mutable: true},
		{name: "auto-aof-rewrite-min-size", kind: dirMemory, def: "67108864", mutable: true},

		// Checkpoint triggers (doc 20 section 9.8). aki has no separate WAL
		// checkpointer yet, so these are accepted and reported for compatibility
		// and take effect when that work lands.
		{name: "aki-checkpoint-interval", kind: dirInt, def: "300", mutable: true},
		{name: "aki-checkpoint-wal-frames", kind: dirInt, def: "1000", mutable: true},
		{name: "aki-checkpoint-dirty-pages", kind: dirInt, def: "500", mutable: true},

		// aki-hash-overlay turns on the in-memory hash write fast path: a large
		// btree-backed hash absorbs HSET/HDEL element writes into a resident map and
		// folds them back into its sub-tree in batches, trading a per-op sub-tree
		// descent for a map write. Off by default; it is forced off under
		// appendfsync=always without the AOF, where there is no durable record of an
		// unfolded write before its reply. See keyspace/overlay.go.
		{name: "aki-hash-overlay", kind: dirBool, def: "no", mutable: true},

		// Replication.
		{name: "replica-read-only", kind: dirBool, def: "yes", mutable: true},
		{name: "masterauth", kind: dirString, def: "", mutable: true},
		{name: "masteruser", kind: dirString, def: "", mutable: true},
		{name: "repl-backlog-size", kind: dirMemory, def: "1048576", mutable: true},
		{name: "repl-ping-replica-period", kind: dirInt, def: "10", mutable: true},
		{name: "repl-timeout", kind: dirInt, def: "60", mutable: true},
		{name: "replica-priority", kind: dirInt, def: "100", mutable: true},

		// Sentinel compatibility. aki is not a Sentinel, but it answers the SENTINEL
		// command family so discovery clients can resolve the master address.
		{name: "sentinel-compat-mode", kind: dirBool, def: "yes", mutable: true},
		{name: "sentinel-monitor-name", kind: dirString, def: "mymaster", mutable: true},
		{name: "sentinel-down-after-milliseconds", kind: dirInt, def: "30000", mutable: true},
		{name: "sentinel-failover-timeout", kind: dirInt, def: "180000", mutable: true},

		// Cluster. cluster-enabled is immutable in real Redis (it needs a restart);
		// aki lets it be toggled at runtime so a single-node cluster can be brought
		// up without relaunching, which a wire-compatible client never depends on.
		{name: "cluster-enabled", kind: dirBool, def: "no", mutable: true},
		{name: "cluster-config-file", kind: dirString, def: "nodes.conf"},
		{name: "cluster-node-timeout", kind: dirInt, def: "15000", mutable: true},
		{name: "cluster-announce-ip", kind: dirString, def: "", mutable: true},
		{name: "cluster-announce-port", kind: dirInt, def: "0", mutable: true},
		{name: "cluster-announce-tls-port", kind: dirInt, def: "0", mutable: true},
		{name: "cluster-announce-bus-port", kind: dirInt, def: "0", mutable: true},
		{name: "cluster-announce-hostname", kind: dirString, def: "", mutable: true},
		{name: "cluster-migration-barrier", kind: dirInt, def: "1", mutable: true},
		{name: "cluster-require-full-coverage", kind: dirBool, def: "yes", mutable: true},
		{name: "cluster-replica-no-failover", kind: dirBool, def: "no", mutable: true},
		{name: "cluster-allow-reads-when-down", kind: dirBool, def: "no", mutable: true},
		{name: "cluster-allow-pubsubshard-when-down", kind: dirBool, def: "yes", mutable: true},
		{name: "cluster-link-sendbuf-limit", kind: dirMemory, def: "0", mutable: true},
		{name: "cluster-replica-validity-factor", kind: dirInt, def: "10", mutable: true},

		// Data-type limits.
		{name: "list-max-listpack-size", kind: dirInt, def: "-2", mutable: true},
		{name: "list-max-ziplist-size", kind: dirInt, def: "-2", mutable: true},
		{name: "hash-max-listpack-entries", kind: dirInt, def: "128", mutable: true},
		{name: "hash-max-listpack-value", kind: dirInt, def: "64", mutable: true},
		{name: "set-max-intset-entries", kind: dirInt, def: "512", mutable: true},
		{name: "set-max-listpack-entries", kind: dirInt, def: "128", mutable: true},
		{name: "set-max-listpack-value", kind: dirInt, def: "64", mutable: true},
		{name: "zset-max-listpack-entries", kind: dirInt, def: "128", mutable: true},
		{name: "zset-max-listpack-value", kind: dirInt, def: "64", mutable: true},
		{name: "proto-max-bulk-len", kind: dirMemory, def: "536870912", mutable: true},

		// Notifications, slowlog, housekeeping.
		{name: "notify-keyspace-events", kind: dirNotify, def: "", mutable: true},
		{name: "slowlog-log-slower-than", kind: dirInt, def: "10000", mutable: true},
		{name: "slowlog-max-len", kind: dirInt, def: "128", mutable: true},
		{name: "latency-monitor-threshold", kind: dirInt, def: "0", mutable: true},
		{name: "latency-tracking", kind: dirBool, def: "yes", mutable: true},
		{name: "latency-tracking-info-percentiles", kind: dirString, def: "50 99 99.9", mutable: true},
		{name: "hz", kind: dirInt, def: "10", mutable: true},
		{name: "activerehashing", kind: dirBool, def: "yes", mutable: true},
		{name: "lazyfree-lazy-eviction", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-expire", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-server-del", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-user-del", kind: dirBool, def: "no", mutable: true},
		{name: "lazyfree-lazy-user-flush", kind: dirBool, def: "no", mutable: true},

		// The rest of the canonical CONFIG name surface from doc 24 A.24. aki
		// reports and round-trips these so a redis.conf and a CONFIG GET * look
		// complete to a client and to migration tooling. Many are accepted-and-
		// reported only today, the same way real Redis reports configs that are
		// no-ops on a given platform. The data-type-limit names here are not yet
		// wired to the encoding codecs (those read compile-time thresholds), so
		// setting them changes only what CONFIG GET reports, not the encoding.

		// More network directives (A.5).
		{name: "bind-source-addr", kind: dirString, def: "", mutable: true},
		{name: "socket-mark-id", kind: dirInt, def: "0"},
		{name: "unixsocketperm", kind: dirInt, def: "0"},
		{name: "enable-protected-configs", kind: dirEnum, def: "no",
			enum: []string{"no", "yes", "local"}},

		// More process directives (A.6).
		{name: "daemonize", kind: dirBool, def: "no"},
		{name: "pidfile", kind: dirString, def: ""},
		{name: "supervised", kind: dirEnum, def: "no",
			enum: []string{"no", "upstart", "systemd", "auto"}},
		{name: "proc-title-template", kind: dirString, def: "{title} {listen-addr} {server-mode}", mutable: true},
		{name: "set-proc-title", kind: dirBool, def: "yes", mutable: true},
		{name: "locale-collate", kind: dirString, def: "", mutable: true},

		// More persistence directives (A.7).
		{name: "rdb-del-sync-files", kind: dirBool, def: "no", mutable: true},
		{name: "sanitize-dump-payload", kind: dirEnum, def: "no", mutable: true,
			enum: []string{"no", "yes", "clients"}},

		// More AOF directives (A.12).
		{name: "aof-load-truncated", kind: dirBool, def: "yes", mutable: true},
		{name: "aof-rewrite-incremental-fsync", kind: dirBool, def: "yes", mutable: true},
		{name: "aof-timestamp-enabled", kind: dirBool, def: "no", mutable: true},
		{name: "aof-use-rdb-preamble", kind: dirBool, def: "yes", mutable: true},
		{name: "rdb-save-incremental-fsync", kind: dirBool, def: "yes", mutable: true},

		// More replication directives (A.8).
		{name: "repl-backlog-ttl", kind: dirInt, def: "3600", mutable: true},
		{name: "repl-disable-tcp-nodelay", kind: dirBool, def: "no", mutable: true},
		{name: "repl-diskless-sync", kind: dirBool, def: "yes", mutable: true},
		{name: "repl-diskless-sync-delay", kind: dirInt, def: "5", mutable: true},
		{name: "repl-diskless-sync-max-replicas", kind: dirInt, def: "0", mutable: true},
		{name: "repl-diskless-load", kind: dirEnum, def: "disabled", mutable: true,
			enum: []string{"disabled", "on-empty-db", "swapdb"}},
		{name: "replica-serve-stale-data", kind: dirBool, def: "yes", mutable: true},
		{name: "replica-announce-ip", kind: dirString, def: "", mutable: true},
		{name: "replica-announce-port", kind: dirInt, def: "0", mutable: true},
		{name: "replica-announced", kind: dirBool, def: "yes", mutable: true},
		{name: "min-replicas-to-write", kind: dirInt, def: "0", mutable: true},
		{name: "min-slaves-to-write", kind: dirInt, def: "0", mutable: true},
		{name: "min-replicas-max-lag", kind: dirInt, def: "10", mutable: true},
		{name: "min-slaves-max-lag", kind: dirInt, def: "10", mutable: true},
		{name: "propagation-error-behavior", kind: dirEnum, def: "ignore",
			enum: []string{"ignore", "panic", "panic-on-replicas"}},

		// More memory and lazy-free directives (A.9, A.10).
		{name: "replica-ignore-maxmemory", kind: dirBool, def: "yes", mutable: true},
		{name: "replica-lazy-flush", kind: dirBool, def: "no", mutable: true},

		// Threading directives (A.11). aki runs one serial writer, so the io-thread
		// knobs are reported for compatibility and do not change execution.
		{name: "io-threads", kind: dirInt, def: "1"},
		{name: "io-threads-do-reads", kind: dirBool, def: "no"},
		{name: "dynamic-hz", kind: dirBool, def: "yes", mutable: true},

		// More data-type-limit directives (A.13).
		{name: "hash-max-ziplist-entries", kind: dirInt, def: "128", mutable: true},
		{name: "hash-max-ziplist-value", kind: dirInt, def: "64", mutable: true},
		{name: "zset-max-ziplist-entries", kind: dirInt, def: "128", mutable: true},
		{name: "zset-max-ziplist-value", kind: dirInt, def: "64", mutable: true},
		{name: "list-compress-depth", kind: dirInt, def: "0", mutable: true},
		{name: "stream-node-max-bytes", kind: dirMemory, def: "4096", mutable: true},
		{name: "stream-node-max-entries", kind: dirInt, def: "100", mutable: true},
		{name: "hll-sparse-max-bytes", kind: dirMemory, def: "3000", mutable: true},

		// Scripting directives (A.14). busy-reply-threshold is the modern alias of
		// lua-time-limit.
		{name: "lua-time-limit", kind: dirInt, def: "5000", mutable: true},
		{name: "busy-reply-threshold", kind: dirInt, def: "5000", mutable: true},

		// Client and output-buffer directives (A.17).
		{name: "client-output-buffer-limit", kind: dirString,
			def: "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60", mutable: true},
		{name: "client-query-buffer-limit", kind: dirMemory, def: "1073741824", mutable: true},
		{name: "close-on-oom-score-adj-error", kind: dirBool, def: "no", mutable: true},

		// More ACL directives (A.20).
		{name: "acl-pubsub-default", kind: dirEnum, def: "resetchannels", mutable: true,
			enum: []string{"resetchannels", "allchannels"}},
		{name: "aclfile", kind: dirString, def: ""},
		{name: "acllog-max-len", kind: dirInt, def: "128", mutable: true},
		{name: "acllog-max-entry-bytes", kind: dirInt, def: "0", mutable: true},

		// Active-rehashing and active-defrag directives (A.21).
		{name: "activedefrag", kind: dirBool, def: "no", mutable: true},
		{name: "active-defrag-cycle-max", kind: dirInt, def: "25", mutable: true},
		{name: "active-defrag-cycle-min", kind: dirInt, def: "1", mutable: true},
		{name: "active-defrag-ignore-bytes", kind: dirMemory, def: "104857600", mutable: true},
		{name: "active-defrag-max-scan-fields", kind: dirInt, def: "1000", mutable: true},
		{name: "active-defrag-threshold-lower", kind: dirInt, def: "10", mutable: true},
		{name: "active-defrag-threshold-upper", kind: dirInt, def: "100", mutable: true},

		// More cluster directives (A.18).
		{name: "cluster-slave-no-failover", kind: dirBool, def: "no", mutable: true},
		{name: "cluster-allow-nodelay", kind: dirBool, def: "no", mutable: true},
		{name: "cluster-announce-human-nodename", kind: dirString, def: "", mutable: true},
		{name: "cluster-preferred-endpoint-type", kind: dirEnum, def: "ip", mutable: true,
			enum: []string{"ip", "hostname", "unknown-endpoint"}},

		// TLS directives (A.19). aki uses crypto/tls; these are accepted and
		// reported so a TLS-aware deployment config loads cleanly.
		{name: "tls-port", kind: dirInt, def: "0"},
		{name: "tls-cert-file", kind: dirString, def: "", mutable: true},
		{name: "tls-key-file", kind: dirString, def: "", mutable: true},
		{name: "tls-key-file-pass", kind: dirString, def: "", mutable: true},
		{name: "tls-client-cert-file", kind: dirString, def: "", mutable: true},
		{name: "tls-client-key-file", kind: dirString, def: "", mutable: true},
		{name: "tls-dh-params-file", kind: dirString, def: "", mutable: true},
		{name: "tls-ca-cert-file", kind: dirString, def: "", mutable: true},
		{name: "tls-ca-cert-dir", kind: dirString, def: "", mutable: true},
		{name: "tls-auth-clients", kind: dirEnum, def: "yes", mutable: true,
			enum: []string{"no", "yes", "optional"}},
		{name: "tls-protocols", kind: dirString, def: "", mutable: true},
		{name: "tls-ciphers", kind: dirString, def: "", mutable: true},
		{name: "tls-ciphersuites", kind: dirString, def: "", mutable: true},
		{name: "tls-prefer-server-ciphers", kind: dirBool, def: "no", mutable: true},
		{name: "tls-session-caching", kind: dirBool, def: "yes", mutable: true},
		{name: "tls-session-cache-size", kind: dirInt, def: "20480", mutable: true},
		{name: "tls-session-cache-timeout", kind: dirInt, def: "300", mutable: true},
		{name: "tls-cluster", kind: dirBool, def: "no", mutable: true},
		{name: "tls-replication", kind: dirBool, def: "no", mutable: true},

		// aki-specific extensions (A.22). The storage knobs are fixed at file
		// creation or wait on the WAL and compression milestones, so most are
		// reported for completeness and a few are mutable.
		{name: "in-memory", kind: dirBool, def: "no"},
		{name: "page-size", kind: dirInt, def: "16384"},
		{name: "shards", kind: dirInt, def: "1"},
		{name: "aki-filename", kind: dirString, def: "aki.aki"},
		{name: "buffer-pool-size", kind: dirMemory, def: "134217728", mutable: true},
		{name: "buffer-pool-max", kind: dirMemory, def: "0", mutable: true},
		{name: "wal-autocheckpoint", kind: dirInt, def: "1000", mutable: true},
		{name: "wal-size-limit", kind: dirMemory, def: "0", mutable: true},
		{name: "synchronous", kind: dirEnum, def: "normal", mutable: true,
			enum: []string{"off", "normal", "full", "extra"}},
		{name: "compression", kind: dirEnum, def: "none", mutable: true,
			enum: []string{"none", "lz4", "zstd"}},
		{name: "compression-level", kind: dirInt, def: "0", mutable: true},
		{name: "encryption", kind: dirBool, def: "no"},
		{name: "encryption-key", kind: dirString, def: ""},
		{name: "encryption-key-file", kind: dirString, def: ""},
		{name: "io-uring", kind: dirBool, def: "no"},
		{name: "o-direct", kind: dirBool, def: "no"},
		{name: "rdb-compat", kind: dirBool, def: "yes", mutable: true},
		{name: "max-io-latency-warn", kind: dirInt, def: "5", mutable: true},
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
	case dirNotify:
		flags, ok := parseNotifyFlags(raw)
		if !ok {
			return "", false
		}
		return canonicalNotifyFlags(flags), true
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
		if twin, ok := cs.alias[c.name]; ok {
			cs.vals[twin] = c.val
		}
	}
	cs.mu.Unlock()
	for _, c := range changes {
		cs.mirror(c.name, c.val)
	}
	// The notification write path reads the flags atomically, so mirror any change
	// to notify-keyspace-events into the dispatcher's atomic copy.
	for _, c := range changes {
		switch c.name {
		case "notify-keyspace-events":
			if flags, ok := parseNotifyFlags(c.val); ok {
				atomic.StoreUint32(&ctx.d.notifyFlags, flags)
			}
		case "loglevel", "log-format":
			ctx.d.logApplyConfig()
		case "logfile":
			// Reopen so the change takes effect at once, the same as Redis.
			if err := ctx.d.logReopen(); err != nil {
				ctx.enc().WriteError("ERR Changing directory: " + err.Error())
				return
			}
		case "acllog-max-len":
			// Resize the ACL denial log right away, trimming if it shrank.
			if n, ok := parseInteger([]byte(c.val)); ok && ctx.d.acl != nil {
				ctx.d.acl.setLogMax(int(n))
			}
		case "go-gogc", "go-memlimit", "go-maxprocs":
			// Re-tune the Go runtime so the new GC percentage, memory limit, or
			// GOMAXPROCS cap takes hold without a restart.
			ctx.d.ApplyGCTuning()
		case "lfu-log-factor", "lfu-decay-time":
			// Push the new LFU tuning to the keyspace so the eviction counter uses it.
			ctx.d.applyLFUConfig()
		case "appendonly", "appendfsync":
			// Retune the pager checkpoint cadence so the durability contract tracks
			// the new policy. Tightening to always flushes any pending writes now.
			// Toggling appendonly flips whether the AOF carries the always guarantee,
			// which changes the policy too, so both directives recompute it.
			// applyCommitPolicy also recomputes the hash overlay gate.
			ctx.d.applyCommitPolicy()
		case "aki-hash-overlay":
			// Turn the in-memory hash write overlay on or off to match the directive.
			ctx.d.applyHashOverlay()
		case "timeout":
			// Push the new idle timeout to the server so it applies on the next read.
			ctx.d.applyIdleTimeout()
		case "tcp-keepalive":
			// Push the new keepalive period to the server for connections accepted
			// after this point.
			ctx.d.applyTCPKeepAlive()
		case "proto-max-bulk-len":
			// Push the new bulk cap to the server so the parser uses it on the next
			// request.
			ctx.d.applyMaxBulkLen()
		case "client-query-buffer-limit":
			// Push the new query buffer cap to the server so the read loop uses it on
			// the next read.
			ctx.d.applyQueryBufLimit()
		}
	}
	ctx.enc().WriteStatus("OK")
}

// handleConfigResetStat clears the per-command call, latency, and error counters
// behind the INFO commandstats, latencystats, and errorstats sections.
func handleConfigResetStat(ctx *Ctx) {
	ctx.d.statResetAll()
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
