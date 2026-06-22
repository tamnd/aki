package command

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// configGet returns the value CONFIG GET reports for a single directive, or the
// empty string when the directive does not match.
func configGet(t *testing.T, r *bufio.Reader, c net.Conn, name string) string {
	t.Helper()
	got := readArray(t, r, c, "CONFIG GET "+name)
	if len(got) == 0 {
		return ""
	}
	if len(got) != 2 {
		t.Fatalf("CONFIG GET %s = %v want one pair", name, got)
	}
	return got[1]
}

func TestConfigGetDefault(t *testing.T) {
	r, c := startData(t)
	if got := configGet(t, r, c, "maxmemory"); got != "0" {
		t.Fatalf("maxmemory default = %q want 0", got)
	}
	if got := configGet(t, r, c, "maxmemory-policy"); got != "noeviction" {
		t.Fatalf("policy default = %q", got)
	}
}

func TestConfigSetMemory(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory 100mb"); got != "+OK" {
		t.Fatalf("SET maxmemory = %q", got)
	}
	if got := configGet(t, r, c, "maxmemory"); got != "104857600" {
		t.Fatalf("maxmemory = %q want 104857600", got)
	}
}

func TestConfigSetEnum(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory-policy ALLKEYS-LRU"); got != "+OK" {
		t.Fatalf("SET policy = %q", got)
	}
	if got := configGet(t, r, c, "maxmemory-policy"); got != "allkeys-lru" {
		t.Fatalf("policy = %q", got)
	}
	if got := sendLine(t, r, c, "CONFIG SET maxmemory-policy bogus"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("SET bad policy = %q", got)
	}
}

func TestConfigSetBool(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "CONFIG SET appendonly 1")
	if got := configGet(t, r, c, "appendonly"); got != "yes" {
		t.Fatalf("appendonly = %q want yes", got)
	}
	_ = sendLine(t, r, c, "CONFIG SET appendonly false")
	if got := configGet(t, r, c, "appendonly"); got != "no" {
		t.Fatalf("appendonly = %q want no", got)
	}
}

func TestConfigSetUnknown(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "CONFIG SET no-such-directive 1")
	if !strings.HasPrefix(got, "-ERR Unknown option") {
		t.Fatalf("SET unknown = %q", got)
	}
}

func TestConfigSetImmutable(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "CONFIG SET port 7000")
	if !strings.Contains(got, "immutable") {
		t.Fatalf("SET port = %q want immutable error", got)
	}
}

func TestConfigSetAtomic(t *testing.T) {
	r, c := startData(t)
	// The second pair is invalid, so neither change applies.
	got := sendLine(t, r, c, "CONFIG SET maxmemory 50mb maxmemory-policy bogus")
	if !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("partial SET = %q want error", got)
	}
	if v := configGet(t, r, c, "maxmemory"); v != "0" {
		t.Fatalf("maxmemory changed despite failed SET: %q", v)
	}
}

func TestConfigSetMultiple(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory 1mb maxmemory-samples 10"); got != "+OK" {
		t.Fatalf("multi SET = %q", got)
	}
	if v := configGet(t, r, c, "maxmemory"); v != "1048576" {
		t.Fatalf("maxmemory = %q", v)
	}
	if v := configGet(t, r, c, "maxmemory-samples"); v != "10" {
		t.Fatalf("samples = %q", v)
	}
}

func TestConfigGetGlob(t *testing.T) {
	r, c := startData(t)
	got := readArray(t, r, c, "CONFIG GET maxmemory*")
	// At least maxmemory, maxmemory-policy, maxmemory-samples and more, each a
	// name/value pair, so the flat array length is even and well above two.
	if len(got) < 6 || len(got)%2 != 0 {
		t.Fatalf("glob len = %d", len(got))
	}
}

func TestConfigGetMultiPattern(t *testing.T) {
	r, c := startData(t)
	got := readArray(t, r, c, "CONFIG GET maxclients databases")
	if len(got) != 4 {
		t.Fatalf("multi-pattern len = %d want 4", len(got))
	}
}

func TestConfigGetUnknownEmpty(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG GET no-such-directive"); got != "*0" {
		t.Fatalf("GET unknown = %q want *0", got)
	}
}

func TestConfigResetStat(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG RESETSTAT"); got != "+OK" {
		t.Fatalf("RESETSTAT = %q", got)
	}
}

func TestConfigRewriteNoFile(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "CONFIG REWRITE")
	if !strings.HasPrefix(got, "-ERR The server is running without a config file") {
		t.Fatalf("REWRITE = %q", got)
	}
}

func TestConfigSaveCanonical(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET save \"900 1 300 10\""); got != "+OK" {
		t.Fatalf("SET save = %q", got)
	}
	if v := configGet(t, r, c, "save"); v != "900 1 300 10" {
		t.Fatalf("save = %q", v)
	}
	// An odd number of fields is rejected.
	if got := sendLine(t, r, c, "CONFIG SET save \"900\""); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("SET bad save = %q", got)
	}
}

// a24CanonicalNames is the canonical CONFIG name list from doc 24 A.24. Every
// name here must be registered so a CONFIG GET * and a redis.conf look complete
// to a client and to migration tooling.
var a24CanonicalNames = []string{
	"acl-pubsub-default", "aclfile", "acllog-max-entry-bytes", "acllog-max-len",
	"active-defrag-cycle-max", "active-defrag-cycle-min", "active-defrag-ignore-bytes",
	"active-defrag-max-scan-fields", "active-defrag-threshold-lower", "active-defrag-threshold-upper",
	"active-expire-effort", "active-expire-enabled", "activedefrag", "activerehashing",
	"aof-load-truncated", "aof-rewrite-incremental-fsync", "aof-timestamp-enabled",
	"aof-use-rdb-preamble", "appenddirname", "appendfilename", "appendfsync", "appendonly",
	"auto-aof-rewrite-min-size", "auto-aof-rewrite-percentage", "bind", "bind-source-addr",
	"buffer-pool-max", "buffer-pool-size", "busy-reply-threshold", "client-output-buffer-limit",
	"client-query-buffer-limit", "cluster-allow-nodelay", "cluster-allow-pubsubshard-when-down",
	"cluster-allow-reads-when-down", "cluster-announce-bus-port", "cluster-announce-hostname",
	"cluster-announce-human-nodename", "cluster-announce-ip", "cluster-announce-port",
	"cluster-announce-tls-port", "cluster-config-file", "cluster-enabled", "cluster-link-sendbuf-limit",
	"cluster-migration-barrier", "cluster-node-timeout", "cluster-preferred-endpoint-type",
	"cluster-require-full-coverage", "cluster-slave-no-failover", "close-on-oom-score-adj-error",
	"compression", "compression-level", "crash-log-enabled", "crash-memlog-enabled", "databases",
	"daemonize", "dbfilename", "dir", "dynamic-hz", "enable-protected-configs", "encryption",
	"encryption-key", "encryption-key-file", "hash-max-listpack-entries", "hash-max-listpack-value",
	"hash-max-ziplist-entries", "hash-max-ziplist-value", "hll-sparse-max-bytes", "hz", "in-memory",
	"io-threads", "io-threads-do-reads", "io-uring", "aki-filename", "latency-monitor-threshold",
	"latency-tracking", "latency-tracking-info-percentiles", "lazyfree-lazy-eviction",
	"lazyfree-lazy-expire", "lazyfree-lazy-server-del", "lazyfree-lazy-user-del",
	"lazyfree-lazy-user-flush", "lfu-decay-time", "lfu-log-factor", "list-compress-depth",
	"list-max-listpack-size", "locale-collate", "logfile", "loglevel", "lua-time-limit",
	"masterauth", "masteruser", "max-io-latency-warn", "maxclients", "maxmemory", "maxmemory-clients",
	"maxmemory-eviction-tenacity", "maxmemory-policy", "maxmemory-samples", "min-replicas-max-lag",
	"min-replicas-to-write", "min-slaves-max-lag", "min-slaves-to-write", "no-appendfsync-on-rewrite",
	"notify-keyspace-events", "o-direct", "page-size", "pidfile", "port", "proc-title-template",
	"propagation-error-behavior", "proto-max-bulk-len", "protected-mode", "rdb-compat",
	"rdb-del-sync-files", "rdb-save-incremental-fsync", "rdbchecksum", "rdbcompression",
	"replica-announce-ip", "replica-announce-port", "replica-announced", "replica-ignore-maxmemory",
	"replica-lazy-flush", "replica-priority", "replica-read-only", "replica-serve-stale-data",
	"repl-backlog-size", "repl-backlog-ttl", "repl-disable-tcp-nodelay", "repl-diskless-load",
	"repl-diskless-sync", "repl-diskless-sync-delay", "repl-diskless-sync-max-replicas",
	"repl-ping-replica-period", "repl-timeout", "requirepass", "sanitize-dump-payload", "save",
	"set-max-intset-entries", "set-max-listpack-entries", "set-max-listpack-value", "set-proc-title",
	"shards", "slowlog-log-slower-than", "slowlog-max-len", "socket-mark-id",
	"stop-writes-on-bgsave-error", "stream-node-max-bytes", "stream-node-max-entries", "supervised",
	"synchronous", "syslog-enabled", "syslog-facility", "syslog-ident", "tcp-backlog", "tcp-keepalive",
	"timeout", "tls-auth-clients", "tls-ca-cert-dir", "tls-ca-cert-file", "tls-cert-file",
	"tls-ciphersuites", "tls-ciphers", "tls-client-cert-file", "tls-client-key-file", "tls-cluster",
	"tls-dh-params-file", "tls-key-file", "tls-key-file-pass", "tls-port", "tls-prefer-server-ciphers",
	"tls-protocols", "tls-replication", "tls-session-cache-size", "tls-session-cache-timeout",
	"tls-session-caching", "tracking-table-max-keys", "unixsocket", "unixsocketperm",
	"wal-autocheckpoint", "wal-size-limit", "zset-max-listpack-entries", "zset-max-listpack-value",
	"zset-max-ziplist-entries", "zset-max-ziplist-value",
}

// TestConfigA24Coverage asserts every canonical name in doc 24 A.24 is a known
// directive. The three names left out (replicaof, slaveof, user) are command or
// startup driven and real Redis does not expose them through CONFIG GET, so aki
// follows real Redis and does not register them as CONFIG parameters.
func TestConfigA24Coverage(t *testing.T) {
	cs := newConfigStore()
	var missing []string
	for _, name := range a24CanonicalNames {
		if _, ok := cs.defs[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("A.24 names not registered: %s", strings.Join(missing, ", "))
	}
}

// TestConfigAliasMirror checks that the alias pairs Redis keeps queryable update
// each other on a CONFIG SET.
func TestConfigAliasMirror(t *testing.T) {
	r, c := startData(t)
	cases := []struct{ set, read string }{
		{"hash-max-listpack-entries", "hash-max-ziplist-entries"},
		{"hash-max-ziplist-value", "hash-max-listpack-value"},
		{"zset-max-listpack-entries", "zset-max-ziplist-entries"},
		{"lua-time-limit", "busy-reply-threshold"},
		{"min-slaves-to-write", "min-replicas-to-write"},
	}
	for _, tc := range cases {
		val := "7"
		if got := sendLine(t, r, c, "CONFIG SET "+tc.set+" "+val); got != "+OK" {
			t.Fatalf("SET %s = %q", tc.set, got)
		}
		if got := configGet(t, r, c, tc.read); got != val {
			t.Fatalf("set %s, read %s = %q want %q", tc.set, tc.read, got, val)
		}
	}
	// cluster-replica-no-failover takes a boolean, so set it back with a boolean
	// to confirm a non-int alias mirrors too.
	if got := sendLine(t, r, c, "CONFIG SET cluster-slave-no-failover yes"); got != "+OK" {
		t.Fatalf("SET cluster-slave-no-failover = %q", got)
	}
	if got := configGet(t, r, c, "cluster-replica-no-failover"); got != "yes" {
		t.Fatalf("cluster-replica-no-failover = %q want yes", got)
	}
}
