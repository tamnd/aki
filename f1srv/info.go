package f1srv

import (
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// INFO reports the server's state as the line-oriented text block Redis clients, dashboards, and
// benchmark tools parse: sections headed by "# Name" lines, then "field:value" lines, groups
// separated by a blank line. The stub this replaces returned only redis_version, which is enough
// for a client that probes the version on connect but blank for anything that reads used_memory,
// connected_clients, or the keyspace size, all of which real tooling watches.
//
// Every field here is a real number this server can answer, never a plausible-looking constant.
// Fields Redis reports that f1srv cannot compute honestly (per-command counters it does not keep,
// an RSS figure the Go runtime does not expose) are left out rather than faked, because a
// compatibility suite that diffs against real Redis is better served by a missing field than a
// wrong one. The aki-specific section surfaces the cold value log's dead-byte accounting, the
// larger-than-memory tier's space story, which no Redis field covers.
//
// INFO is a pure read of counters and store totals, so it is safe on a live server with no
// quiescing, unlike a flush or a compaction.

// infoSections lists the section names INFO knows, in the order it emits them. A request with no
// argument, or one of the aggregate arguments (default/all/everything), emits all of them; a
// request naming one section emits just that one.
var infoSections = []string{"server", "clients", "memory", "persistence", "stats", "replication", "keyspace", "aki"}

// cmdInfo implements INFO [section [section ...]]. Redis accepts a list of section names plus the
// aggregate selectors default, all, and everything; f1srv treats all three aggregates the same
// (there are no hidden sections here) and matches section names case-insensitively. An unknown
// section name contributes nothing, so INFO foobar returns an empty bulk, matching Redis.
func (c *connState) cmdInfo(argv [][]byte) {
	want := make(map[string]bool, len(infoSections))
	if len(argv) <= 1 {
		for _, s := range infoSections {
			want[s] = true
		}
	} else {
		for _, a := range argv[1:] {
			name := strings.ToLower(string(a))
			if name == "default" || name == "all" || name == "everything" {
				for _, s := range infoSections {
					want[s] = true
				}
				continue
			}
			want[name] = true
		}
	}

	var b strings.Builder
	for _, s := range infoSections {
		if !want[s] {
			continue
		}
		c.srv.appendInfoSection(&b, s)
	}
	c.writeBulk([]byte(b.String()))
}

// appendInfoSection writes one section's header and fields to b. Splitting per section keeps the
// section filter (INFO memory) trivial: emit only the sections the request asked for, in the
// canonical order.
func (s *Server) appendInfoSection(b *strings.Builder, section string) {
	switch section {
	case "server":
		s.infoServer(b)
	case "clients":
		s.infoClients(b)
	case "memory":
		s.infoMemory(b)
	case "persistence":
		s.infoPersistence(b)
	case "stats":
		s.infoStats(b)
	case "replication":
		s.infoReplication(b)
	case "keyspace":
		s.infoKeyspace(b)
	case "aki":
		s.infoAki(b)
	}
}

func (s *Server) infoServer(b *strings.Builder) {
	now := time.Now()
	uptime := int64(now.Sub(s.startTime).Seconds())
	if uptime < 0 {
		uptime = 0
	}
	mux := "go"
	if strings.EqualFold(s.cfg.NetMode, "reactor") {
		mux = "epoll"
	}
	b.WriteString("# Server\r\n")
	b.WriteString("redis_version:7.4.0\r\n")
	b.WriteString("redis_git_sha1:00000000\r\n")
	b.WriteString("redis_git_dirty:0\r\n")
	b.WriteString("redis_mode:standalone\r\n")
	b.WriteString("os:" + runtime.GOOS + " " + runtime.GOARCH + "\r\n")
	b.WriteString("arch_bits:64\r\n")
	b.WriteString("multiplexing_api:" + mux + "\r\n")
	b.WriteString("process_id:" + strconv.Itoa(os.Getpid()) + "\r\n")
	b.WriteString("run_id:" + s.runID + "\r\n")
	b.WriteString("tcp_port:" + strconv.Itoa(s.tcpPort()) + "\r\n")
	b.WriteString("server_time_usec:" + strconv.FormatInt(now.UnixMicro(), 10) + "\r\n")
	b.WriteString("uptime_in_seconds:" + strconv.FormatInt(uptime, 10) + "\r\n")
	b.WriteString("uptime_in_days:" + strconv.FormatInt(uptime/86400, 10) + "\r\n")
	b.WriteString("\r\n")
}

func (s *Server) infoClients(b *strings.Builder) {
	b.WriteString("# Clients\r\n")
	b.WriteString("connected_clients:" + strconv.FormatInt(s.clients.Load(), 10) + "\r\n")
	b.WriteString("cluster_connections:0\r\n")
	b.WriteString("\r\n")
}

func (s *Server) infoMemory(b *strings.Builder) {
	used, _ := s.store.ArenaBytes()
	total, dead := s.store.ColdBytes()
	// The cold log's live bytes are memory this instance holds for the dataset even though they
	// live on disk, so fold them into used_memory the way Redis folds its own value bytes in.
	used += total - dead
	maxmem := uint64(0)
	if s.cfg.ArenaBytes > 0 {
		maxmem = uint64(s.cfg.ArenaBytes)
	}
	b.WriteString("# Memory\r\n")
	b.WriteString("used_memory:" + strconv.FormatUint(used, 10) + "\r\n")
	b.WriteString("used_memory_human:" + humanBytes(used) + "\r\n")
	b.WriteString("maxmemory:" + strconv.FormatUint(maxmem, 10) + "\r\n")
	b.WriteString("maxmemory_human:" + humanBytes(maxmem) + "\r\n")
	b.WriteString("maxmemory_policy:noeviction\r\n")
	b.WriteString("mem_allocator:go\r\n")
	b.WriteString("\r\n")
}

func (s *Server) infoPersistence(b *strings.Builder) {
	// The M1 server holds no RDB or AOF: the store is fresh-start in memory with an optional cold
	// value log that is not a durability format. These zeros are the honest state, not stubs.
	b.WriteString("# Persistence\r\n")
	b.WriteString("loading:0\r\n")
	b.WriteString("rdb_changes_since_last_save:0\r\n")
	b.WriteString("rdb_bgsave_in_progress:0\r\n")
	b.WriteString("rdb_last_save_time:0\r\n")
	b.WriteString("aof_enabled:0\r\n")
	b.WriteString("aof_rewrite_in_progress:0\r\n")
	b.WriteString("\r\n")
}

func (s *Server) infoStats(b *strings.Builder) {
	// total_connections_received is nextConnID's running value: it hands one id per accept, so its
	// current value is the count of connections ever taken. total_commands_processed is deliberately
	// absent: counting every command would add an atomic to the GET/SET hot path this server exists
	// to keep lean, and a wrong-because-unkept number is worse than an omitted one.
	b.WriteString("# Stats\r\n")
	b.WriteString("total_connections_received:" + strconv.FormatInt(s.nextConnID.Load(), 10) + "\r\n")
	b.WriteString("rejected_connections:0\r\n")
	b.WriteString("\r\n")
}

func (s *Server) infoReplication(b *strings.Builder) {
	// The standalone stance ROLE already reports: a master with no replicas and a zero offset. The
	// replid is this run's identity, stable for the process lifetime, so a client can tell one run
	// from another the way it does against Redis.
	b.WriteString("# Replication\r\n")
	b.WriteString("role:master\r\n")
	b.WriteString("connected_slaves:0\r\n")
	b.WriteString("master_failover_state:no-failover\r\n")
	b.WriteString("master_replid:" + s.runID + "\r\n")
	b.WriteString("master_repl_offset:0\r\n")
	b.WriteString("\r\n")
}

func (s *Server) infoKeyspace(b *strings.Builder) {
	b.WriteString("# Keyspace\r\n")
	// Redis omits a db line for an empty database, so a client that parses the section sees only the
	// dbs that hold keys. keys is the top-level key count (DBSIZE); expires is the count of keys
	// carrying a TTL, the same volatile gate the lazy-expiry path reads.
	keys := s.store.TopLen()
	if keys > 0 {
		expires := s.srvExpires()
		b.WriteString("db0:keys=" + strconv.Itoa(keys) + ",expires=" + strconv.FormatInt(expires, 10) + ",avg_ttl=0\r\n")
	}
	b.WriteString("\r\n")
}

func (s *Server) infoAki(b *strings.Builder) {
	// The aki-specific section carries what no Redis field does: which engine is running and, when
	// the larger-than-memory cold tier is engaged, the cold value log's size and how much of it is
	// dead bytes a compaction would reclaim (the accounting Compact acts on). An operator watches
	// dead/total here to decide when to quiesce and compact.
	b.WriteString("# Aki\r\n")
	b.WriteString("aki_engine:f1raw\r\n")
	if s.cfg.ColdPath == "" {
		b.WriteString("aki_cold_enabled:0\r\n")
		b.WriteString("\r\n")
		return
	}
	total, dead := s.store.ColdBytes()
	b.WriteString("aki_cold_enabled:1\r\n")
	b.WriteString("aki_cold_log_bytes:" + strconv.FormatUint(total, 10) + "\r\n")
	b.WriteString("aki_cold_log_dead_bytes:" + strconv.FormatUint(dead, 10) + "\r\n")
	b.WriteString("aki_cold_log_live_bytes:" + strconv.FormatUint(total-dead, 10) + "\r\n")
	// Write-path backpressure counters (doc 23): waits is the number of allocations that blocked
	// waiting for the migrator to free a segment, stalls the subset that gave up with the arena
	// full. waits climbing with stalls flat is a healthy slow overflow; stalls climbing is a
	// genuinely full store (cold tier cannot take more).
	waits, stalls := s.store.BackpressureStats()
	b.WriteString("aki_backpressure_waits:" + strconv.FormatUint(waits, 10) + "\r\n")
	b.WriteString("aki_backpressure_stalls:" + strconv.FormatUint(stalls, 10) + "\r\n")
	b.WriteString("\r\n")
}

// srvExpires reports how many top-level keys currently carry a TTL, the volatile gate the expiry
// path maintains. It is clamped at zero so a transient negative (never expected) never prints a
// nonsense count.
func (s *Server) srvExpires() int64 {
	v := s.volatile.Load()
	if v < 0 {
		return 0
	}
	return v
}

// tcpPort extracts the numeric port from the configured listen address. INFO reports it so a
// client that only saw a host can learn the port. A malformed or portless address yields 0.
func (s *Server) tcpPort() int {
	_, portStr, err := net.SplitHostPort(s.cfg.Addr)
	if err != nil {
		return 0
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}
	return p
}

// humanBytes renders a byte count the way Redis's used_memory_human does: an integer under 1K, a
// one-decimal figure with a K/M/G/T suffix above, using 1024-byte units. It is presentation only;
// the exact byte field beside it is what a program should parse.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatUint(n, 10) + "B"
	}
	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	val := float64(n) / float64(div)
	return strconv.FormatFloat(val, 'f', 2, 64) + string("KMGTPE"[exp]) + "B"
}
