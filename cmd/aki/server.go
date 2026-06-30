package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/tamnd/aki/command"
	"github.com/tamnd/aki/engine/hot"
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/store"
	"github.com/tamnd/aki/vfs"
)

// cmdServer starts the aki server: it opens (or creates) the data file, builds
// the keyspace and command dispatcher, and runs the network listener until
// interrupted.
//
// The flag surface mirrors redis-server so an operator coming from Redis or
// Valkey can reuse the names they already know: --port, --bind, --dir,
// --dbfilename, --appendonly, --appendfsync, --save, --maxmemory and friends all
// behave the way they do over there. An optional leading positional argument is a
// redis.conf-style config file; values from it are overridden by any flag passed
// on the command line, the same precedence redis-server uses.
func cmdServer(args []string) error {
	// A leading non-flag argument is a config file path, the classic
	// `redis-server /etc/redis.conf` form. Parse it first so flags can override.
	var fileConf map[string]string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		var err error
		fileConf, err = parseConfigFile(args[0])
		if err != nil {
			return err
		}
		args = args[1:]
	}
	if fileConf == nil {
		fileConf = map[string]string{}
	}

	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	// Listen address. --addr is the aki-native host:port form; --bind and --port
	// are the redis spellings and compose into the same listen address.
	addr := fs.String("addr", "127.0.0.1:6379", "TCP listen address (host:port)")
	bind := fs.String("bind", "", "interface address to listen on (redis spelling; combined with --port)")
	port := fs.String("port", "", "TCP port to listen on (redis spelling; combined with --bind)")
	unixSocket := fs.String("unixsocket", "", "Unix socket path to listen on as well")
	maxClients := fs.Int("maxclients", 10000, "maximum number of connected clients")
	databases := fs.Int("databases", 16, "number of logical databases")
	requirePass := fs.String("requirepass", "", "password required for the default user")
	aclFile := fs.String("aclfile", "", "path to an external ACL file loaded at startup and written by ACL SAVE")
	logfile := fs.String("logfile", "", "path to the log file (empty logs to stderr)")
	loglevel := fs.String("loglevel", "", "minimum log level: debug, verbose, notice, warning")
	// Data file location. --dbfile is the aki-native path; --dir and --dbfilename
	// are the redis spellings. The data file lands at --dir/--dbfile unless --dbfile
	// is absolute.
	dbfile := fs.String("dbfile", "aki.db", "path to the .aki data file")
	dir := fs.String("dir", "", "working directory for the data file, RDB dumps, and AOF (redis spelling)")
	dbfilename := fs.String("dbfilename", "", "RDB dump filename written by SAVE/BGSAVE (redis spelling)")
	// Durability and limits, redis spellings mapped onto aki's config store.
	appendonly := fs.String("appendonly", "", "enable the append-only file: yes or no")
	appendfsync := fs.String("appendfsync", "", "durability policy: always, everysec, or no")
	hashOverlay := fs.String("aki-hash-overlay", "", "in-memory hash write fast path: yes or no (default no)")
	engine := fs.String("aki-engine", "", "storage engine for the string point path: btree (default, durable, all types), hybrid (experimental in-memory durable-spill store, string-only), or hot (experimental in-memory lock-free hot tier, string-only); spec 2064 rewrite")
	akiNet := fs.String("aki-net", "", "TCP networking model: goroutine (default, one read-loop goroutine per connection), reactor (experimental epoll event loops, Linux+TCP only), or uring (experimental reactor that batches a turn's writes into one io_uring_enter, Linux+TCP only; spec 2064 reactor)")
	save := fs.String("save", "", `RDB save points, e.g. "3600 1 300 100", or "" to disable`)
	maxmemory := fs.String("maxmemory", "", "memory limit before eviction, e.g. 256mb (0 disables)")
	maxmemoryPolicy := fs.String("maxmemory-policy", "", "eviction policy when maxmemory is reached")
	daemonize := fs.String("daemonize", "", "accepted for redis compatibility; only 'no' is supported")
	loadRDB := fs.String("load-rdb", "", "import this dump.rdb on first open (only when the .aki file does not exist)")
	rdbDB := fs.Int("rdb-db", -1, "with --load-rdb, import only this source database")
	bufferPoolSize := fs.String("buffer-pool-size", "auto", "buffer pool capacity (e.g. 128mb, 512mb); controls how much of the .aki file stays in memory. \"auto\" (default) sizes the pool to a quarter of a detected cgroup memory cap so the rest of the cap stays free for the OS page cache, and to 128mb on an uncapped host")
	// Diagnostic admin endpoint (pprof, /metrics, health, ready). It defaults to
	// 127.0.0.1:6399; --admin-port 0 disables it so several instances can share one
	// host without colliding on the port.
	adminPort := fs.String("admin-port", "", "HTTP port for the diagnostic admin endpoint; 0 disables it (default 6399)")
	adminBind := fs.String("admin-bind", "", "interface address for the admin endpoint (default 127.0.0.1)")
	valueCacheFraction := fs.Float64("value-cache-fraction", 0.10, "share of the buffer-pool budget held as a decoded value cache for GET (perf/03); 0 disables it")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Track which flags the operator set explicitly so config-file values fill in
	// only where the command line was silent. Command line beats config file.
	setFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
	resolve := func(name, flagVal string) string {
		if setFlags[name] {
			return flagVal
		}
		if v, ok := fileConf[name]; ok {
			return v
		}
		return flagVal
	}

	if d := resolve("daemonize", *daemonize); d != "" && d != "no" {
		return fmt.Errorf("--daemonize %s is not supported; aki runs in the foreground (use a process manager or a shell &)", d)
	}

	// Compose the listen address from --bind/--port when given, else keep --addr.
	host, p := "127.0.0.1", "6379"
	if b := resolve("bind", *bind); b != "" {
		host = firstField(b)
	}
	if pv := resolve("port", *port); pv != "" {
		p = pv
	}
	listenAddr := resolve("addr", *addr)
	if setFlags["bind"] || setFlags["port"] || fileConf["bind"] != "" || fileConf["port"] != "" {
		listenAddr = net.JoinHostPort(host, p)
	}

	// Resolve the data file path: --dir is the base directory unless --dbfile is
	// already absolute.
	dataFile := resolve("dbfile", *dbfile)
	if d := resolve("dir", *dir); d != "" && !filepath.IsAbs(dataFile) {
		dataFile = filepath.Join(d, dataFile)
	}

	dbCount := *databases
	if v := resolve("databases", ""); v != "" {
		if n, ok := parseIntFlag(v); ok {
			dbCount = n
		}
	}

	fresh := !vfs.NewOS().Exists(dataFile)
	if resolve("load-rdb", *loadRDB) != "" && !fresh {
		return fmt.Errorf("--load-rdb only applies on first open; %s already exists", dataFile)
	}

	poolPages, err := parseBufPoolPages(resolve("buffer-pool-size", *bufferPoolSize))
	if err != nil {
		return fmt.Errorf("--buffer-pool-size: %w", err)
	}

	// The value cache holds value-cache-fraction of the buffer-pool budget as
	// decoded GET results (perf/03 section 13.2). Sizing it from the same budget
	// keeps the three in-memory consumers (pages, log, value cache) bounded
	// together. A zero pool (unbounded) or a zero fraction leaves the cache at its
	// built-in default.
	valueCacheBytes := int64(0)
	if frac := *valueCacheFraction; frac > 0 && poolPages > 0 {
		valueCacheBytes = int64(float64(poolPages) * defaultPageBytes * frac)
	}

	// The resident hybrid-log engines (spec 2064 rewrite) are opt-in and
	// experimental: they serve the string point path from a resident
	// open-addressed index over an in-memory log instead of the durable B-tree.
	// "hybrid" is the original durable-spill store/ engine; "hot" is the clean
	// lock-free, in-place hot/ engine (the F2 hot tier). Both are string-only and
	// non-durable in this slice, so they are gated behind the flag and never the
	// default; the durable B-tree stays the engine for everything else.
	var engineOpt keyspace.Option
	switch eng := resolve("aki-engine", *engine); eng {
	case "", "btree":
		// default durable engine
	case "hybrid":
		engineOpt = keyspace.WithHybridLog(store.Tunables{Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0, Dir: ""})
	case "hot":
		engineOpt = keyspace.WithHotEngine(hot.Tunables{Shards: 256})
	default:
		return fmt.Errorf("--aki-engine %s is not a known engine (use btree, hybrid, or hot)", eng)
	}

	ks, closeKS, err := openKeyspace(dataFile, dbCount, poolPages, valueCacheBytes, engineOpt)
	if err != nil {
		return err
	}
	defer closeKS()

	rdbPath := resolve("load-rdb", *loadRDB)
	var importedFuncs []string
	if rdbPath != "" && fresh {
		n, funcs, err := importRDBInto(ks, rdbPath, *rdbDB)
		if err != nil {
			return err
		}
		importedFuncs = funcs
		fmt.Printf("loaded %d keys from %s\n", n, rdbPath)
	}

	d := command.New(command.Config{
		Databases:   dbCount,
		RequirePass: resolve("requirepass", *requirePass),
		AclFile:     resolve("aclfile", *aclFile),
		Version:     fmt.Sprintf("7.2.0-aki-%s", Version),
		Engine:      command.NewEngine(ks),
	})
	if err := d.LoadFunctionsFromKeyspace(); err != nil {
		return fmt.Errorf("load functions from data file: %w", err)
	}
	if len(importedFuncs) > 0 {
		d.LoadFunctions(importedFuncs)
		d.PersistFunctions()
	}
	if err := d.LoadACLFromKeyspace(); err != nil {
		return fmt.Errorf("load ACL from data file: %w", err)
	}
	if err := d.LoadScriptsFromKeyspace(); err != nil {
		return fmt.Errorf("load scripts from data file: %w", err)
	}

	cfg := networking.Config{
		Addr:       listenAddr,
		UnixSocket: resolve("unixsocket", *unixSocket),
		MaxClients: *maxClients,
		NetMode:    resolve("aki-net", *akiNet),
	}

	// Apply config-mirroring directives through the same path CONFIG SET uses, so
	// they validate identically and run their side effects (durability retune,
	// RDB/AOF location). Empty means "leave the default".
	applyConf := func(name, val string) error {
		if val == "" {
			return nil
		}
		if err := d.SetConfig(name, val); err != nil {
			return fmt.Errorf("--%s: %w", name, err)
		}
		return nil
	}
	for _, kv := range [][2]string{
		{"logfile", resolve("logfile", *logfile)},
		{"loglevel", resolve("loglevel", *loglevel)},
		{"dir", resolve("dir", *dir)},
		{"dbfilename", resolve("dbfilename", *dbfilename)},
		{"appendonly", resolve("appendonly", *appendonly)},
		{"appendfsync", resolve("appendfsync", *appendfsync)},
		{"aki-hash-overlay", resolve("aki-hash-overlay", *hashOverlay)},
		{"save", resolveSave(setFlags, fileConf, *save)},
		{"maxmemory", resolve("maxmemory", *maxmemory)},
		{"maxmemory-policy", resolve("maxmemory-policy", *maxmemoryPolicy)},
		{"admin-port", resolve("admin-port", *adminPort)},
		{"admin-bind", resolve("admin-bind", *adminBind)},
	} {
		if err := applyConf(kv[0], kv[1]); err != nil {
			return err
		}
	}

	if err := d.LogStart(); err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer d.LogClose()

	// Any remaining redis.conf directive that matches a known runtime config key
	// is applied too, so a reused redis.conf carries over things like timeout,
	// tcp-keepalive, and notify-keyspace-events without a flag for each. Runs after
	// LogStart so a skipped directive logs through the configured sink.
	applyExtraConfig(d, fileConf)

	// Apply the Go GC knobs before serving so go-gogc and go-memlimit take hold
	// from the first request.
	d.ApplyGCTuning()

	srv := networking.New(cfg, d)
	d.SetServer(srv)
	// Apply the network idle knobs now that the server is attached so timeout and
	// tcp-keepalive take hold from the first connection.
	d.ApplyNetworkConfig()
	d.StartBackground()
	defer d.StopBackground()

	if err := d.StartMetrics(); err != nil {
		return fmt.Errorf("start metrics endpoint: %w", err)
	}
	defer d.StopMetrics()

	if err := d.StartProfiler(); err != nil {
		return fmt.Errorf("start profiler: %w", err)
	}
	defer d.StopProfiler()

	if err := d.StartAdmin(); err != nil {
		// The admin endpoint (pprof, /metrics, health, ready) is an optional
		// diagnostic surface, not part of serving data. A bind failure there, most
		// often its port already held by another aki on the same host, must not stop
		// the database from coming up, so log it and serve without the endpoint.
		d.LogWarn("admin endpoint unavailable, continuing without it", "error", err.Error())
	}
	defer d.StopAdmin()

	d.LogNotice("Server started", "aki_version", Version, "addr", listenAddr)

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(cfg) }()
	d.SetReady(true)

	// The SHUTDOWN command signals on this channel. The handler has already run its
	// save policy, so the main loop only has to stop the server and let the deferred
	// cleanup close the data file.
	shutdownC := make(chan struct{}, 1)
	d.SetShutdown(func() {
		select {
		case shutdownC <- struct{}{}:
		default:
		}
	})

	fmt.Printf("aki %s listening on %s\n", Version, listenAddr)
	if us := resolve("unixsocket", *unixSocket); us != "" {
		fmt.Printf("aki also listening on unix:%s\n", us)
	}
	if maddr := d.MetricsAddr(); maddr != "" {
		fmt.Printf("aki metrics on http://%s/metrics\n", maddr)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		select {
		case err := <-errc:
			return err
		case <-shutdownC:
			fmt.Println("\naki shutting down")
			return srv.Close()
		case s := <-sig:
			if s == syscall.SIGHUP {
				// logrotate renames the file then sends SIGHUP; reopen so aki
				// continues in the fresh file.
				if err := d.ReopenLog(); err != nil {
					fmt.Fprintf(os.Stderr, "aki: reopen log on SIGHUP: %v\n", err)
				}
				continue
			}
			fmt.Println("\naki shutting down")
			d.LogNotice("Server shutting down")
			return srv.Close()
		}
	}
}

// extraConfigSkip lists the redis.conf directives that the server command already
// consumes as flags. Everything else in a config file that names a known runtime
// config key is forwarded to the dispatcher so a reused redis.conf keeps working.
var extraConfigSkip = map[string]bool{
	"addr": true, "bind": true, "port": true, "unixsocket": true,
	"maxclients": true, "databases": true, "requirepass": true, "aclfile": true,
	"logfile": true, "loglevel": true, "dbfile": true, "dir": true,
	"dbfilename": true, "appendonly": true, "appendfsync": true, "save": true,
	"maxmemory": true, "maxmemory-policy": true, "daemonize": true,
	"load-rdb": true, "rdb-db": true, "buffer-pool-size": true,
	"value-cache-fraction": true,
}

// applyExtraConfig forwards config-file directives that are not server flags to
// the dispatcher's config store, the same path CONFIG SET uses. A directive that
// is not a known config key is logged and skipped rather than failing startup, so
// a redis.conf that carries directives aki does not model still boots.
func applyExtraConfig(d *command.Dispatcher, fileConf map[string]string) {
	for name, val := range fileConf {
		if extraConfigSkip[name] {
			continue
		}
		if err := d.SetConfig(name, val); err != nil {
			d.LogNotice("Ignoring config directive", "directive", name, "reason", err.Error())
		}
	}
}

// resolveSave resolves the save directive with the same precedence as the other
// flags but treats an explicit empty string as a real value, since `save ""`
// disables RDB snapshots and is distinct from "leave the default".
func resolveSave(setFlags map[string]bool, fileConf map[string]string, flagVal string) string {
	if setFlags["save"] {
		if flagVal == "" {
			// `--save ""` disables snapshots. SetConfig rejects "", so encode the
			// off state the way redis stores it.
			return ""
		}
		return flagVal
	}
	if v, ok := fileConf["save"]; ok {
		return v
	}
	return ""
}

// parseConfigFile reads a redis.conf-style file into a directive map. Each
// non-empty, non-comment line is a directive name followed by its value; the
// value keeps its internal spacing (so `save 3600 1` round-trips) but loses a
// single pair of surrounding double quotes. Directive names are lowercased to
// match the flag and config-key spellings.
func parseConfigFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config file: %w", err)
	}
	defer func() { _ = f.Close() }()
	conf := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val, _ := strings.Cut(line, " ")
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		conf[name] = dequote(strings.TrimSpace(val))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	return conf, nil
}

// dequote strips one pair of surrounding double quotes, the form redis.conf uses
// for values that contain spaces or are empty.
func dequote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// firstField returns the first whitespace-separated token of s, used to pick one
// address out of a `bind` directive that lists several.
func firstField(s string) string {
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		return s[:i]
	}
	return s
}

// parseIntFlag parses a plain base-10 integer, used for config-file values that
// mirror integer flags.
func parseIntFlag(s string) (int, bool) {
	n := 0
	if s == "" {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// importRDBInto reads a dump.rdb and loads it into the fresh keyspace, committing
// the result. It returns the key count and the function library sources the file
// carried so the caller can register them once the dispatcher exists. It is the
// startup half of --load-rdb.
func importRDBInto(ks *keyspace.Keyspace, path string, onlyDB int) (int, []string, error) {
	blob, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(blob) < 5 || string(blob[:5]) != "REDIS" {
		return 0, nil, fmt.Errorf("not an RDB file: %s", path)
	}
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		return 0, nil, fmt.Errorf("parse RDB %s: %w", path, err)
	}
	n, err := command.LoadSnapshot(ks, snap, onlyDB, true)
	if err != nil {
		return 0, nil, fmt.Errorf("load RDB %s: %w", path, err)
	}
	if err := ks.Commit(); err != nil {
		return 0, nil, fmt.Errorf("commit after load: %w", err)
	}
	return n, snap.Functions, nil
}

// openKeyspace opens the data file at path, creating it on first run, and
// returns the keyspace over it plus a close function. The pager picks the file
// format up from its header on reopen, so databases is used only at create time.
// poolPages is the buffer-pool capacity in frames; zero uses the pager default.
func openKeyspace(path string, databases, poolPages int, valueCacheBytes int64, engineOpt keyspace.Option) (*keyspace.Keyspace, func(), error) {
	osfs := vfs.NewOS()
	opts := pager.Options{CachePages: poolPages}
	var (
		pgr *pager.Pager
		err error
	)
	if osfs.Exists(path) {
		pgr, err = pager.Open(osfs, path, opts)
	} else {
		opts.DBCount = uint32(databases)
		pgr, err = pager.Create(osfs, path, opts)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("open data file %s: %w", path, err)
	}
	ksOpts := []keyspace.Option{keyspace.WithValueCacheBytes(valueCacheBytes)}
	if engineOpt != nil {
		ksOpts = append(ksOpts, engineOpt)
	}
	ks, err := keyspace.Open(pgr, ksOpts...)
	if err != nil {
		_ = pgr.Close()
		return nil, nil, fmt.Errorf("open keyspace: %w", err)
	}
	return ks, func() { _ = pgr.Close() }, nil
}

// defaultPageBytes is the pager's fixed page size, used to convert a page count
// to bytes when sizing the buffer pool and the value cache.
const defaultPageBytes = 16384

// parseBufPoolPages converts a human-readable size string (128mb, 512MiB, 65536)
// to a page count. It understands k/m/g suffixes (case-insensitive, with or
// without the trailing 'b' or 'ib'). A plain integer is treated as a byte count.
// The page size is fixed at the pager's default (16 KiB) for the conversion.
func parseBufPoolPages(s string) (int, error) {
	if s == "" || s == "0" {
		return 0, nil
	}
	// "auto" sizes the pool from the detected memory cap (cap-aware sizing, see
	// memlimit.go): a quarter of a cgroup limit so the page cache keeps the rest,
	// or the historical 128mb default on an uncapped host.
	if strings.EqualFold(s, "auto") {
		pages := int(autoBufferPoolBytes() / defaultPageBytes)
		if pages < 64 {
			pages = 64
		}
		return pages, nil
	}
	val, unit, _ := splitSuffix(s)
	if val <= 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	switch unit {
	case "k", "kb", "kib":
		val *= 1024
	case "m", "mb", "mib":
		val *= 1024 * 1024
	case "g", "gb", "gib":
		val *= 1024 * 1024 * 1024
	case "", "b":
		// already bytes
	default:
		return 0, fmt.Errorf("unknown unit in %q", s)
	}
	pages := val / defaultPageBytes
	if pages < 64 {
		pages = 64
	}
	return pages, nil
}

// splitSuffix splits a string like "128mb" into (128, "mb"). The suffix is
// lowercased; the numeric part must be a non-negative integer.
func splitSuffix(s string) (int, string, bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, "", false
	}
	n := 0
	for _, c := range s[:i] {
		n = n*10 + int(c-'0')
	}
	return n, strings.ToLower(s[i:]), true
}
