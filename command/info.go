package command

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// This file implements the server introspection commands from doc 20: INFO with
// its sections and LOLWUT. INFO reports a faithful subset of the Redis field set.
// Fields aki does not yet track are reported with stable placeholder values so a
// client library that parses INFO finds the names it expects.

// redisVersionCompat is the version INFO reports as redis_version. It is fixed at
// a recent Redis so client libraries that gate features on the server version
// enable the commands aki implements.
const redisVersionCompat = "7.4.0"

// newRunID returns a 40-hex random identifier, the format Redis uses for run_id
// and the replication id.
func newRunID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read never fails on the platforms aki targets, but if it ever did
		// a zero id is still a valid 40-hex string.
		return strings.Repeat("0", 40)
	}
	return hex.EncodeToString(b[:])
}

func infoCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "info", Group: GroupServer, Since: "1.0.0",
			Arity: -1, Flags: FlagLoading | FlagStale, Handler: handleInfo},
		{Name: "lolwut", Group: GroupServer, Since: "5.0.0",
			Arity: -1, Flags: FlagFast, Handler: handleLolwut},
	}
}

// infoSection names a section and whether it appears in the default INFO output.
type infoSection struct {
	name      string
	inDefault bool
	write     func(ctx *Ctx, b *strings.Builder)
}

// infoSections is the ordered section list. The order matches the order Redis
// emits sections so output diffs line up.
func infoSections() []infoSection {
	return []infoSection{
		{"Server", true, infoServer},
		{"Clients", true, infoClients},
		{"Memory", true, infoMemory},
		{"Persistence", true, infoPersistence},
		{"Stats", true, infoStats},
		{"Replication", true, infoReplication},
		{"CPU", true, infoCPU},
		{"Commandstats", false, infoCommandstats},
		{"Latencystats", false, infoLatencystats},
		{"Errorstats", true, infoErrorstats},
		{"Cluster", true, infoCluster},
		{"Keyspace", true, infoKeyspace},
	}
}

// handleInfo selects the requested sections and writes the report as a bulk
// string. With no argument it emits the default set; "all" or "everything" emit
// every section; named sections emit just those.
func handleInfo(ctx *Ctx) {
	want := map[string]bool{}
	all := false
	for _, a := range ctx.Argv[1:] {
		s := strings.ToLower(string(a))
		switch s {
		case "all", "everything":
			all = true
		case "default":
			// handled by the inDefault flag below
		default:
			want[s] = true
		}
	}
	explicit := len(want) > 0

	var b strings.Builder
	for _, sec := range infoSections() {
		emit := false
		switch {
		case all:
			emit = true
		case explicit:
			emit = want[strings.ToLower(sec.name)]
		default:
			emit = sec.inDefault
		}
		if !emit {
			continue
		}
		b.WriteString("# " + sec.name + "\r\n")
		sec.write(ctx, &b)
		b.WriteString("\r\n")
	}
	ctx.enc().WriteBulkStringStr(b.String())
}

// line writes one "key:value\r\n" field.
func line(b *strings.Builder, key, val string) {
	b.WriteString(key)
	b.WriteByte(':')
	b.WriteString(val)
	b.WriteString("\r\n")
}

func lineInt(b *strings.Builder, key string, v int64) {
	line(b, key, strconv.FormatInt(v, 10))
}

func infoServer(ctx *Ctx, b *strings.Builder) {
	now := time.Now()
	uptime := max(int64(now.Sub(ctx.d.startTime).Seconds()), 0)
	line(b, "redis_version", redisVersionCompat)
	line(b, "redis_git_sha1", "00000000")
	line(b, "redis_git_dirty", "0")
	line(b, "redis_build_id", "0000000000000000")
	line(b, "redis_mode", ctx.d.cfg.Mode)
	line(b, "os", runtime.GOOS+" "+runtime.GOARCH)
	lineInt(b, "arch_bits", int64(strconv.IntSize))
	line(b, "monotonic_clock", "go-runtime-nanotime")
	line(b, "multiplexing_api", multiplexingAPI())
	line(b, "atomicvar_api", "atomic-builtin")
	line(b, "gcc_version", "0.0.0")
	lineInt(b, "process_id", int64(syscall.Getpid()))
	line(b, "run_id", ctx.d.runID)
	lineInt(b, "tcp_port", int64(ctx.tcpPort()))
	lineInt(b, "server_time_usec", now.UnixMicro())
	lineInt(b, "uptime_in_seconds", uptime)
	lineInt(b, "uptime_in_days", uptime/86400)
	line(b, "hz", "10")
	line(b, "configured_hz", "10")
	line(b, "aof_rewrites", "0")
	line(b, "executable", executablePath())
	line(b, "config_file", "")
	line(b, "io_threads_active", "1")
	line(b, "aki_version", ctx.d.cfg.Version)
	line(b, "aki_build_go_version", runtime.Version())
}

func infoClients(ctx *Ctx, b *strings.Builder) {
	connected := 1
	if ctx.d.srv != nil {
		connected = ctx.d.srv.CountClients()
	}
	maxClients := ctx.confInt("maxclients", 10000)
	lineInt(b, "connected_clients", int64(connected))
	line(b, "cluster_connections", "0")
	lineInt(b, "maxclients", maxClients)
	line(b, "client_recent_max_input_buffer", "0")
	line(b, "client_recent_max_output_buffer", "0")
	line(b, "blocked_clients", "0")
	line(b, "tracking_clients", "0")
	line(b, "clients_in_timeout_table", "0")
	line(b, "total_watched_keys", "0")
	line(b, "total_blocking_keys", "0")
	line(b, "total_blocking_keys_on_nokey", "0")
	lineInt(b, "aki_io_goroutines", int64(connected))
}

func infoMemory(ctx *Ctx, b *strings.Builder) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	used := int64(ms.HeapAlloc)
	if ctx.d.engine != nil {
		// The live-data estimate is the figure maxmemory eviction compares against,
		// so used_memory reports the same number rather than the Go heap size.
		used = ctx.d.engine.usedMemory()
	}
	rss := int64(ms.Sys)
	peak := int64(ms.HeapSys)
	lineInt(b, "used_memory", used)
	line(b, "used_memory_human", humanBytes(used))
	lineInt(b, "used_memory_rss", rss)
	line(b, "used_memory_rss_human", humanBytes(rss))
	lineInt(b, "used_memory_peak", peak)
	line(b, "used_memory_peak_human", humanBytes(peak))
	lineInt(b, "used_memory_lua", 0)
	lineInt(b, "number_of_cached_scripts", 0)
	lineInt(b, "maxmemory", ctx.confMemory("maxmemory", 0))
	line(b, "maxmemory_human", humanBytes(ctx.confMemory("maxmemory", 0)))
	line(b, "maxmemory_policy", ctx.confStr("maxmemory-policy", "noeviction"))
	line(b, "mem_allocator", "go-runtime")
	line(b, "mem_fragmentation_ratio", fmtRatio(rss, used))
	lineInt(b, "active_defrag_running", 0)
	lineInt(b, "lazyfree_pending_objects", 0)
	lineInt(b, "lazyfreed_objects", 0)
}

func infoPersistence(ctx *Ctx, b *strings.Builder) {
	p := &ctx.d.persist
	p.mu.Lock()
	dirty := p.dirty
	inProgress := p.inProgress
	lastSave := p.lastSaveUnix
	status := p.lastStatus
	lastTimeSec := p.lastTimeSec
	curStart := p.curStartUnix
	saves := p.saves
	p.mu.Unlock()

	if lastSave == 0 {
		lastSave = ctx.d.startTime.Unix()
	}
	if status == "" {
		status = "ok"
	}
	lastTime := "-1"
	if saves > 0 {
		lastTime = strconv.FormatFloat(lastTimeSec, 'f', -1, 64)
	}
	curTime := int64(-1)
	if curStart != 0 {
		curTime = time.Now().Unix() - curStart
	}

	line(b, "loading", "0")
	line(b, "async_loading", "0")
	line(b, "current_cow_size", "0")
	line(b, "current_fork_perc", "0.00")
	line(b, "current_save_keys_processed", "0")
	line(b, "current_save_keys_total", "0")
	lineInt(b, "rdb_changes_since_last_save", dirty)
	line(b, "rdb_bgsave_in_progress", boolField(inProgress))
	lineInt(b, "rdb_last_save_time", lastSave)
	line(b, "rdb_last_bgsave_status", status)
	line(b, "rdb_last_bgsave_time_sec", lastTime)
	lineInt(b, "rdb_current_bgsave_time_sec", curTime)
	lineInt(b, "rdb_saves", saves)
	line(b, "rdb_last_cow_size", "0")

	a := &ctx.d.aof
	a.mu.Lock()
	aofEnabled := ctx.d.aofEnabled()
	aofRewriting := a.rewriteInProgress
	aofScheduled := a.scheduled
	aofStatus := a.lastStatus
	aofWriteStatus := a.lastWriteStatus
	aofTimeSec := a.lastTimeSec
	aofCurStart := a.curStartUnix
	aofRewrites := a.seq
	aofBaseSize := a.baseSize
	aofIncrSize := a.incrSize
	a.mu.Unlock()
	if aofStatus == "" {
		aofStatus = "ok"
	}
	if aofWriteStatus == "" {
		aofWriteStatus = "ok"
	}
	aofRewriteTime := "-1"
	if aofRewrites > 0 {
		aofRewriteTime = strconv.FormatFloat(aofTimeSec, 'f', -1, 64)
	}
	aofCurTime := int64(-1)
	if aofCurStart != 0 {
		aofCurTime = time.Now().Unix() - aofCurStart
	}

	line(b, "aof_enabled", boolField(aofEnabled))
	line(b, "aof_rewrite_in_progress", boolField(aofRewriting))
	line(b, "aof_rewrite_scheduled", boolField(aofScheduled))
	line(b, "aof_last_rewrite_time_sec", aofRewriteTime)
	lineInt(b, "aof_current_rewrite_time_sec", aofCurTime)
	line(b, "aof_last_bgrewrite_status", aofStatus)
	line(b, "aof_last_write_status", aofWriteStatus)
	line(b, "aof_pending_rewrite", boolField(aofScheduled))
	if aofEnabled {
		lineInt(b, "aof_current_size", aofBaseSize+aofIncrSize)
		lineInt(b, "aof_base_size", aofBaseSize)
		lineInt(b, "aof_pending_bio_fsync", 0)
		lineInt(b, "aof_delayed_fsync", 0)
	}
}

// boolField renders a flag as the "1" or "0" INFO uses.
func boolField(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func infoStats(ctx *Ctx, b *strings.Builder) {
	var connsRecv, cmds, netIn, netOut int64
	if ctx.d.srv != nil {
		for _, c := range ctx.d.srv.Snapshot() {
			cmds += int64(c.TotCmds())
			netIn += int64(c.TotNetIn())
			netOut += int64(c.TotNetOut())
		}
		connsRecv = int64(ctx.d.srv.CountClients())
	}
	ch, pat, shard := 0, 0, 0
	if ctx.d.ps != nil {
		ch, pat, shard = ctx.d.ps.counts()
	}
	lineInt(b, "total_connections_received", connsRecv)
	lineInt(b, "total_commands_processed", cmds)
	line(b, "instantaneous_ops_per_sec", "0")
	lineInt(b, "total_net_input_bytes", netIn)
	lineInt(b, "total_net_output_bytes", netOut)
	line(b, "rejected_connections", "0")
	line(b, "sync_full", "0")
	line(b, "sync_partial_ok", "0")
	line(b, "sync_partial_err", "0")
	line(b, "expired_keys", "0")
	line(b, "evicted_keys", "0")
	line(b, "keyspace_hits", "0")
	line(b, "keyspace_misses", "0")
	lineInt(b, "pubsub_channels", int64(ch))
	lineInt(b, "pubsub_patterns", int64(pat))
	lineInt(b, "pubsub_shardchannels", int64(shard))
	line(b, "latest_fork_usec", "0")
	line(b, "total_forks", "0")
}

func infoReplication(ctx *Ctx, b *strings.Builder) {
	line(b, "role", "master")
	line(b, "connected_slaves", "0")
	line(b, "master_failover_state", "no-failover")
	line(b, "master_replid", ctx.d.runID)
	line(b, "master_replid2", strings.Repeat("0", 40))
	line(b, "master_repl_offset", "0")
	line(b, "second_repl_offset", "-1")
	line(b, "repl_backlog_active", "0")
	lineInt(b, "repl_backlog_size", ctx.confMemory("repl-backlog-size", 1048576))
	line(b, "repl_backlog_first_byte_offset", "0")
	line(b, "repl_backlog_histlen", "0")
}

func infoCPU(_ *Ctx, b *strings.Builder) {
	sys, user := cpuSeconds()
	line(b, "used_cpu_sys", strconv.FormatFloat(sys, 'f', 6, 64))
	line(b, "used_cpu_user", strconv.FormatFloat(user, 'f', 6, 64))
	line(b, "used_cpu_sys_children", "0.000000")
	line(b, "used_cpu_user_children", "0.000000")
}

// infoCommandstats and infoLatencystats are header-only for now: aki does not
// track per-command call counts or latency histograms yet.
func infoCommandstats(_ *Ctx, _ *strings.Builder) {}

func infoLatencystats(_ *Ctx, _ *strings.Builder) {}

// infoErrorstats is header-only: aki does not track per-error counts yet.
func infoErrorstats(_ *Ctx, _ *strings.Builder) {}

func infoCluster(_ *Ctx, b *strings.Builder) {
	line(b, "cluster_enabled", "0")
}

func infoKeyspace(ctx *Ctx, b *strings.Builder) {
	if ctx.d.engine == nil {
		return
	}
	for i, n := range ctx.d.engine.dbSizes() {
		if n == 0 {
			continue
		}
		line(b, "db"+strconv.Itoa(i),
			"keys="+strconv.FormatUint(n, 10)+",expires=0,avg_ttl=0,subexpiry=0")
	}
}

// tcpPort returns the port the server listens on, or 0 when there is no TCP
// listener or no server handle.
func (ctx *Ctx) tcpPort() int {
	if ctx.d.srv == nil {
		return 0
	}
	addr := ctx.d.srv.Addr()
	if tcp, ok := addr.(*net.TCPAddr); ok {
		return tcp.Port
	}
	return 0
}

// confStr reads a config directive as a string, falling back to def.
func (ctx *Ctx) confStr(name, def string) string {
	if ctx.d.conf == nil {
		return def
	}
	if v, ok := ctx.d.conf.get(name); ok {
		return v
	}
	return def
}

// confInt reads a config directive as an integer, falling back to def.
func (ctx *Ctx) confInt(name string, def int64) int64 {
	v := ctx.confStr(name, "")
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// confMemory reads a memory directive, which the config store keeps as a byte
// count string, falling back to def.
func (ctx *Ctx) confMemory(name string, def int64) int64 {
	return ctx.confInt(name, def)
}

// humanBytes renders a byte count the way Redis does, with an IEC suffix and two
// decimals above 1K.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + "B"
	}
	f := float64(n)
	suffixes := []string{"K", "M", "G", "T", "P"}
	i := -1
	for f >= unit && i < len(suffixes)-1 {
		f /= unit
		i++
	}
	return strconv.FormatFloat(f, 'f', 2, 64) + suffixes[i]
}

// fmtRatio formats a/b with two decimals, or 0.00 when b is zero.
func fmtRatio(a, b int64) string {
	if b == 0 {
		return "0.00"
	}
	return strconv.FormatFloat(float64(a)/float64(b), 'f', 2, 64)
}

// cpuSeconds returns the process system and user CPU time in seconds.
func cpuSeconds() (sys, user float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	user = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sys = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return sys, user
}

// executablePath returns the running binary path, or "" when it cannot be found.
func executablePath() string {
	p, err := os.Executable()
	if err != nil {
		return ""
	}
	return p
}

// handleLolwut returns the ASCII art banner with a version footer.
func handleLolwut(ctx *Ctx) {
	art := lolwutArt()
	footer := fmt.Sprintf("Redis ver. %s, aki ver. %s\n", redisVersionCompat, ctx.d.cfg.Version)
	ctx.enc().WriteBulkStringStr(art + footer)
}

// lolwutArt is the drawing LOLWUT prints. aki draws its name rune in a small
// block-art frame rather than the Redis dragon curve.
func lolwutArt() string {
	return strings.Join([]string{
		"       _    _ ",
		"  __ _| | _(_)",
		" / _` | |/ / |",
		"| (_| |   <| |",
		" \\__,_|_|\\_\\_|",
		"",
	}, "\n")
}

// multiplexingAPI names the polling mechanism for the platform, matching the
// label Redis prints.
func multiplexingAPI() string {
	switch runtime.GOOS {
	case "linux":
		return "epoll"
	case "darwin", "freebsd", "netbsd", "openbsd", "dragonfly":
		return "kqueue"
	default:
		return "select"
	}
}
