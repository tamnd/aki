package command

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tamnd/aki/format"
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
		{"go_runtime", false, infoGoRuntime},
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
	// Storage facts about the .aki file (doc 20 section 1.2). The format string is
	// the header's format version rendered the way the magic carries it, fmt001.
	// Page size comes from the open file, and the shard count from config.
	line(b, "aki_storage_format", fmt.Sprintf("fmt%03d", format.FormatVersion))
	if ctx.d.engine != nil {
		lineInt(b, "aki_page_size", int64(ctx.d.engine.fileStats().PageSize))
	} else {
		lineInt(b, "aki_page_size", 0)
	}
	lineInt(b, "aki_shard_count", ctx.confInt("shards", 1))
	line(b, "aki_file", akiFilePath(ctx))
	// The WAL sidecar is not wired into the pager yet, so there is no WAL file to
	// name. This stays empty until that lands, matching aki_wal_bytes.
	line(b, "aki_wal_file", "")
	lineInt(b, "aki_in_memory", boolToInt(ctx.d.confBool("in-memory", false)))
	line(b, "aki_build_go_version", runtime.Version())
}

// akiFilePath returns the absolute path of the .aki file for INFO, or empty when
// the server runs in memory with no file behind it.
func akiFilePath(ctx *Ctx) string {
	if ctx.d.engine == nil {
		return ""
	}
	name := ctx.d.engine.filePath()
	if name == "" {
		return ""
	}
	if abs, err := filepath.Abs(name); err == nil {
		return abs
	}
	return name
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
	pubsubClients := 0
	if ctx.d.ps != nil {
		pubsubClients = ctx.d.ps.clientCount()
	}
	lineInt(b, "pubsub_clients", int64(pubsubClients))
	watchingClients, watchedKeys := watchStats(ctx)
	lineInt(b, "watching_clients", int64(watchingClients))
	line(b, "clients_in_timeout_table", "0")
	lineInt(b, "total_watched_keys", int64(watchedKeys))
	line(b, "total_blocking_keys", "0")
	line(b, "total_blocking_keys_on_nokey", "0")
	lineInt(b, "aki_io_goroutines", int64(connected))
}

// watchStats counts the clients that hold at least one WATCH and the total number
// of watched keys across all connections. INFO's clients section reads both for
// watching_clients and total_watched_keys.
func watchStats(ctx *Ctx) (clients, keys int) {
	if ctx.d.srv == nil {
		return 0, 0
	}
	for _, c := range ctx.d.srv.Snapshot() {
		s, ok := c.Session().(*session)
		if !ok {
			continue
		}
		if n := len(s.watched); n > 0 {
			clients++
			keys += n
		}
	}
	return clients, keys
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
	line(b, "used_memory_peak_perc", fmtPercent(used, peak))
	// aki does not meter bookkeeping apart from the dataset, so overhead and
	// startup are 0 and the whole of used_memory is the dataset. MEMORY STATS
	// reports the same split.
	lineInt(b, "used_memory_overhead", 0)
	lineInt(b, "used_memory_startup", 0)
	lineInt(b, "used_memory_dataset", used)
	line(b, "used_memory_dataset_perc", fmtPercent(used, peak))
	sysmem := systemMemoryBytes()
	lineInt(b, "total_system_memory", sysmem)
	line(b, "total_system_memory_human", humanBytes(sysmem))
	// Go allocator view of the heap. allocated is live bytes, active is the heap
	// in use, resident is what the runtime holds from the OS.
	allocated := int64(ms.HeapAlloc)
	active := int64(ms.HeapInuse)
	resident := int64(ms.HeapSys)
	lineInt(b, "allocator_allocated", allocated)
	lineInt(b, "allocator_active", active)
	lineInt(b, "allocator_resident", resident)
	// Scripting memory. aki does not meter the Lua VM in bytes, so the byte fields
	// stay 0 and only the counts carry real values. The vm and scripts totals keep
	// the sums the spec defines so a client that reads them sees consistent math.
	scripts := ctx.d.scripts.count()
	libs, funcs := ctx.d.functions.counts()
	lineInt(b, "used_memory_lua", 0)
	lineInt(b, "used_memory_vm_eval", 0)
	line(b, "used_memory_lua_human", humanBytes(0))
	lineInt(b, "used_memory_scripts_eval", 0)
	lineInt(b, "number_of_cached_scripts", int64(scripts))
	lineInt(b, "number_of_functions", int64(funcs))
	lineInt(b, "number_of_libraries", int64(libs))
	lineInt(b, "used_memory_vm_functions", 0)
	lineInt(b, "used_memory_vm_total", 0)
	line(b, "used_memory_vm_total_human", humanBytes(0))
	lineInt(b, "used_memory_functions", 0)
	lineInt(b, "used_memory_scripts", 0)
	line(b, "used_memory_scripts_human", humanBytes(0))
	lineInt(b, "maxmemory", ctx.confMemory("maxmemory", 0))
	line(b, "maxmemory_human", humanBytes(ctx.confMemory("maxmemory", 0)))
	line(b, "maxmemory_policy", ctx.confStr("maxmemory-policy", "noeviction"))
	line(b, "allocator_frag_ratio", fmtRatio(active, allocated))
	lineInt(b, "allocator_frag_bytes", active-allocated)
	line(b, "allocator_rss_ratio", fmtRatio(resident, active))
	lineInt(b, "allocator_rss_bytes", resident-active)
	line(b, "rss_overhead_ratio", fmtRatio(rss, resident))
	lineInt(b, "rss_overhead_bytes", rss-resident)
	line(b, "mem_allocator", "go-runtime")
	line(b, "mem_fragmentation_ratio", fmtRatio(rss, used))
	lineInt(b, "mem_fragmentation_bytes", rss-used)
	// aki does not meter client output buffers, the replication backlog, cluster
	// links, or an AOF buffer in bytes, so these report 0. They exist so a client
	// that reads them finds the field rather than a gap.
	lineInt(b, "mem_not_counted_for_evict", 0)
	lineInt(b, "mem_replication_backlog", 0)
	lineInt(b, "mem_total_replication_buffers", 0)
	lineInt(b, "mem_clients_slaves", 0)
	lineInt(b, "mem_clients_normal", 0)
	lineInt(b, "mem_cluster_links", 0)
	lineInt(b, "mem_aof_buffer", 0)
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

	loadingFlag := "0"
	if ctx.d.loading.Load() {
		loadingFlag = "1"
	}
	line(b, "loading", loadingFlag)
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

	// aki file-growth fields (doc 20 section 9.8). These are aki extensions that
	// monitoring scrapes to watch the .aki file and the buffer pool.
	if ctx.d.engine != nil {
		st := ctx.d.engine.fileStats()
		lineInt(b, "aki_dataset_file_bytes", st.FileBytes)
		// The WAL sidecar is not wired into the pager yet, so there are no
		// unmerged frames to report. These stay 0 until that lands.
		lineInt(b, "aki_wal_bytes", 0)
		lineInt(b, "aki_wal_frame_count", 0)
		lineInt(b, "aki_dirty_pages", int64(st.DirtyPages))
		lineInt(b, "aki_buffer_pool_pages", int64(st.ResidentPages))
		line(b, "aki_page_cache_hit_ratio", fmtCacheRatio(st.CacheHits, st.CacheMisses))
		line(b, "aki_on_disk_vs_ram_ratio", fmtDiskRamRatio(st.FileBytes))
	}
}

// fmtCacheRatio renders the buffer-pool hit ratio with four decimals. It reports
// 0 when nothing has been read yet so an idle server does not look like a perfect
// cache.
func fmtCacheRatio(hits, misses uint64) string {
	total := hits + misses
	if total == 0 {
		return "0.0000"
	}
	return strconv.FormatFloat(float64(hits)/float64(total), 'f', 4, 64)
}

// fmtPercent renders part over whole as the "50.00%" string the memory section
// uses. It reports "0.00%" when whole is not positive so an empty server does not
// divide by zero.
func fmtPercent(part, whole int64) string {
	if whole <= 0 {
		return "0.00%"
	}
	return strconv.FormatFloat(float64(part)/float64(whole)*100, 'f', 2, 64) + "%"
}

// fmtDiskRamRatio renders the dataset-file size over total system memory. It
// reports 0 when the total cannot be read.
func fmtDiskRamRatio(fileBytes int64) string {
	ram := systemMemoryBytes()
	if ram <= 0 {
		return "0.0000"
	}
	return strconv.FormatFloat(float64(fileBytes)/float64(ram), 'f', 4, 64)
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
	lineInt(b, "io_slowops_total", int64(ctx.d.ioSlowOps.Load()))
}

func infoReplication(ctx *Ctx, b *strings.Builder) {
	d := ctx.d
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()

	line(b, "role", d.repl.role)
	if d.repl.role == "slave" {
		line(b, "master_host", d.repl.masterHost)
		lineInt(b, "master_port", int64(d.repl.masterPort))
		status := "down"
		if d.repl.link == "connected" {
			status = "up"
		}
		line(b, "master_link_status", status)
		line(b, "master_last_io_seconds_ago", "0")
		sync := "0"
		if d.repl.link == "sync" {
			sync = "1"
		}
		line(b, "master_sync_in_progress", sync)
		lineInt(b, "slave_read_repl_offset", d.repl.slaveOff)
		lineInt(b, "slave_repl_offset", d.repl.slaveOff)
		line(b, "slave_priority", "100")
		ro := "1"
		if strings.EqualFold(d.confValue("replica-read-only", "yes"), "no") {
			ro = "0"
		}
		line(b, "slave_read_only", ro)
		line(b, "replica_announced", "1")
	}
	lineInt(b, "connected_slaves", int64(len(d.repl.replicas)))
	i := 0
	for _, h := range d.repl.replicas {
		line(b, "slave"+strconv.Itoa(i),
			"ip="+h.addr+",port="+strconv.Itoa(h.port)+",state="+h.state+
				",offset="+strconv.FormatInt(h.ackOffset, 10)+",lag=0")
		i++
	}
	line(b, "master_failover_state", "no-failover")
	line(b, "master_replid", d.repl.replid)
	line(b, "master_replid2", d.repl.replid2)
	lineInt(b, "master_repl_offset", d.repl.offset)
	lineInt(b, "second_repl_offset", d.repl.secondOffset)
	active := "0"
	first := int64(0)
	histlen := int64(0)
	if d.repl.backlog != nil {
		active = "1"
		first = d.repl.backlog.off
		histlen = d.repl.backlog.histlen
	}
	line(b, "repl_backlog_active", active)
	lineInt(b, "repl_backlog_size", ctx.confMemory("repl-backlog-size", 1048576))
	lineInt(b, "repl_backlog_first_byte_offset", first)
	lineInt(b, "repl_backlog_histlen", histlen)
}

func infoCPU(_ *Ctx, b *strings.Builder) {
	sys, user := cpuSeconds()
	line(b, "used_cpu_sys", strconv.FormatFloat(sys, 'f', 6, 64))
	line(b, "used_cpu_user", strconv.FormatFloat(user, 'f', 6, 64))
	line(b, "used_cpu_sys_children", "0.000000")
	line(b, "used_cpu_user_children", "0.000000")
}

// infoCommandstats writes one cmdstat line per command that has run, in name
// order so the output is stable. usec_per_call is the mean execution time, zero
// when a command has only ever been rejected.
func infoCommandstats(ctx *Ctx, b *strings.Builder) {
	for _, name := range ctx.d.statNames() {
		cs := ctx.d.cmdStatFor(name)
		calls := cs.calls.Load()
		usec := cs.usec.Load()
		perCall := 0.0
		if calls > 0 {
			perCall = float64(usec) / float64(calls)
		}
		line(b, "cmdstat_"+name, "calls="+strconv.FormatUint(calls, 10)+
			",usec="+strconv.FormatUint(usec, 10)+
			",usec_per_call="+fmtUsec(perCall)+
			",rejected_calls="+strconv.FormatUint(cs.rejected.Load(), 10)+
			",failed_calls="+strconv.FormatUint(cs.failed.Load(), 10))
	}
}

// infoLatencystats writes one latency_percentiles_usec line per command that has
// run, but only when latency-tracking is on. The percentiles reported come from
// latency-tracking-info-percentiles.
func infoLatencystats(ctx *Ctx, b *strings.Builder) {
	if !strings.EqualFold(ctx.confStr("latency-tracking", "yes"), "yes") {
		return
	}
	pcts := parsePercentiles(ctx.confStr("latency-tracking-info-percentiles", "50 99 99.9"))
	for _, name := range ctx.d.statNames() {
		cs := ctx.d.cmdStatFor(name)
		if cs.calls.Load() == 0 {
			continue
		}
		var parts []string
		for _, p := range pcts {
			parts = append(parts, "p"+trimPct(p)+"="+fmtUsec(float64(cs.hist.percentile(p))))
		}
		line(b, "latency_percentiles_usec_"+name, strings.Join(parts, ","))
	}
}

// infoErrorstats writes one errorstat line per error code seen, in code order.
func infoErrorstats(ctx *Ctx, b *strings.Builder) {
	var codes []string
	ctx.d.stats.errs.Range(func(k, _ any) bool {
		codes = append(codes, k.(string))
		return true
	})
	sort.Strings(codes)
	for _, code := range codes {
		v, ok := ctx.d.stats.errs.Load(code)
		if !ok {
			continue
		}
		line(b, "errorstat_"+code, "count="+strconv.FormatUint(v.(*atomic.Uint64).Load(), 10))
	}
}

// statNames returns the recorded command names sorted, so commandstats and
// latencystats emit in a stable order.
func (d *Dispatcher) statNames() []string {
	d.stats.mu.RLock()
	names := make([]string, 0, len(d.stats.cmds))
	for name := range d.stats.cmds {
		names = append(names, name)
	}
	d.stats.mu.RUnlock()
	sort.Strings(names)
	return names
}

// parsePercentiles reads the space-separated percentile list from the
// latency-tracking-info-percentiles config into floats, skipping junk.
func parsePercentiles(s string) []float64 {
	var out []float64
	for _, f := range strings.Fields(s) {
		if v, err := strconv.ParseFloat(f, 64); err == nil {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		out = []float64{50, 99, 99.9}
	}
	return out
}

// trimPct formats a percentile for the field label, dropping a trailing ".0" so
// 50 stays "50" while 99.9 stays "99.9".
func trimPct(p float64) string {
	s := strconv.FormatFloat(p, 'f', -1, 64)
	return s
}

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
