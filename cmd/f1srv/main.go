// Command f1srv runs the clean-room f1raw string server. It is the from-first-
// principles in-memory wire path the 2x claim is measured on, separate from the
// historical aki server binary. The flags it accepts are a subset of the aki/redis
// server flags so an existing benchmark harness can launch or connect to it without
// special casing.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"

	"github.com/tamnd/aki/f1srv"
)

func main() {
	// "server" subcommand is accepted and skipped so the harness's
	// `f1srv server --addr ...` invocation matches the aki binary's shape.
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "server" {
		args = args[1:]
	}

	fs := flag.NewFlagSet("f1srv", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:6390", "listen address host:port")
	dir := fs.String("dir", ".", "working directory; holds the cold value log when --ltm-cold is set")
	_ = fs.String("appendonly", "no", "append-only file (accepted, no durability yet)")
	_ = fs.String("appendfsync", "everysec", "fsync policy (accepted, no durability yet)")
	_ = fs.String("aki-engine", "f1raw", "engine name (accepted; this binary is always f1raw)")
	netMode := fs.String("aki-net", "auto", "net model: auto (reactor on Linux, goroutine elsewhere), go (goroutine-per-conn), or reactor (Linux epoll)")
	execModel := fs.String("exec-model", "shared", "command execution model: shared (stripe-locked shared store) or affinity (route each key to its owning shard worker, spec 2064/17)")
	reactorLoops := fs.Int("reactor-loops", 0, "number of epoll event loops for the reactor net model (0 = GOMAXPROCS); tune to find the throughput optimum on the deployment")
	indexBuckets := fs.Int("index-buckets", 1<<22, "f1raw index buckets")
	arenaBytes := fs.Int("arena-bytes", 2<<30, "f1raw arena size in bytes")
	stripes := fs.Int("incr-stripes", 1<<10, "INCR-family RMW lock stripes")
	// Adaptive intra-key set partitioning (spec 2064/f1_rewrite_ltm/19). Off by default:
	// --set-partition-max 1 leaves every set unpartitioned on its existing single-lock body.
	// Above 1 a hot set that reaches --set-partition-threshold members grows toward
	// min(max, roundUpPow2(card/target)) partitions so its single-key writes scale with cores.
	// The threshold and target default to the slice-6c sweep winners when left at 0.
	setPartMax := fs.Int("set-partition-max", 1, "cap on partitions one hot set can engage (1 = feature off); rounded up to a power of two")
	setPartThreshold := fs.Int("set-partition-threshold", 0, "cardinality at which a set first engages partitioning (0 = built-in default)")
	setPartTarget := fs.Int("set-partition-target", 0, "members-per-partition a grow aims for (0 = built-in default)")
	ltmCold := fs.Bool("ltm-cold", false, "engage the larger-than-memory string tier: separate large values to a cold log under --dir")
	sepThreshold := fs.Int("sep-threshold", 0, "inline-vs-separated value cutoff in bytes for --ltm-cold (0 = engine default)")
	arenaSegmented := fs.Bool("arena-segmented", false, "use the reclaimable segmented arena (spec 2064/21 M0); off keeps the grow-only bump arena")
	arenaSegmentBytes := fs.Int("arena-segment-bytes", 0, "segment size in bytes for --arena-segmented (0 = engine default 8 MiB, floored at the largest record)")
	arenaOverflowBytes := fs.Int("arena-overflow-bytes", 0, "never-reclaimed overflow-bucket region size for --arena-segmented (0 = an eighth of the arena)")
	ltmMigrator := fs.Bool("ltm-migrator", false, "engage the background migrator that sinks whole string records cold under arena fill pressure (spec 2064/21 M3); serves a string dataset larger than the arena. Implies --arena-segmented and opens a cold record region under --dir")
	pprofAddr := fs.String("pprof", "", "if set, serve net/http/pprof on this host:port (profiling only, off by default)")
	// --gogc and --gomemlimit are optional heap-pacing knobs for the LTM regime,
	// where a large arena and a cold value log grow the heap and an operator may
	// want to trade RSS against collection frequency or hold the process under a
	// hard byte ceiling. They are off by default: the in-memory path meets its
	// throughput and tail targets on the runtime default GC, so the default here
	// changes nothing and leaves an explicit GOGC/GOMEMLIMIT in the environment
	// untouched. Set --gogc to a positive percent to raise the target, and set
	// --gomemlimit to a positive byte count to engage the soft ceiling.
	gogc := fs.Int("gogc", 0, "GC target percent for the LTM regime (0 = leave the runtime default); higher trades RSS for fewer collections")
	gomemlimit := fs.Int64("gomemlimit", 0, "soft heap ceiling in bytes for the LTM regime (0 = off); the collector runs before the heap crosses it")
	// ExitOnError means a bad flag exits the process, so Parse never returns a
	// non-nil error here. Blank-assign to satisfy errcheck without dead handling.
	_ = fs.Parse(args)

	// Apply the optional GC knobs. Only touch the runtime when the operator asked
	// for it with a positive flag; a zero flag leaves whatever the runtime already
	// resolved from the environment in place. effGOGC is the value the banner
	// reports: the flag when we set it, otherwise the environment's GOGC, otherwise
	// the runtime default of 100.
	effGOGC := 100
	if envVal, envSet := os.LookupEnv("GOGC"); envSet {
		if n, err := strconv.Atoi(envVal); err == nil {
			effGOGC = n
		}
	}
	if *gogc > 0 {
		debug.SetGCPercent(*gogc)
		effGOGC = *gogc
	}
	if *gomemlimit > 0 {
		debug.SetMemoryLimit(*gomemlimit)
	}

	cfg := f1srv.DefaultConfig(*addr)
	cfg.IndexBuckets = *indexBuckets
	cfg.ArenaBytes = *arenaBytes
	cfg.IncrStripes = *stripes
	cfg.NetMode = *netMode
	cfg.ExecModel = *execModel
	cfg.ReactorLoops = *reactorLoops
	cfg.SetPartitionMax = *setPartMax
	cfg.SetPartitionThreshold = *setPartThreshold
	cfg.SetPartitionTarget = *setPartTarget
	cfg.ArenaSegmented = *arenaSegmented
	cfg.ArenaSegmentBytes = *arenaSegmentBytes
	cfg.ArenaOverflowBytes = *arenaOverflowBytes
	if *ltmCold {
		cfg.ColdPath = filepath.Join(*dir, "f1raw-cold.vlog")
		cfg.SepThreshold = *sepThreshold
	}
	if *ltmMigrator {
		// The migrator drains whole segments into the cold record region, so it needs the
		// segmented arena and an open record region. Force both on here rather than making the
		// operator remember to pair the flags: --ltm-migrator alone is the LTM string switch.
		cfg.ArenaSegmented = true
		cfg.ColdRecordsPath = filepath.Join(*dir, "f1raw-cold.recs")
		cfg.Migrator = true
	}

	if *pprofAddr != "" {
		go func() {
			log.Printf("f1srv: pprof on http://%s/debug/pprof/", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("f1srv: pprof server: %v", err)
			}
		}()
	}

	srv := f1srv.New(cfg)
	if err := srv.Listen(); err != nil {
		log.Fatalf("f1srv: listen %s: %v", *addr, err)
	}
	cold := "off"
	if *ltmCold {
		cold = cfg.ColdPath
	}
	memlimit := "off"
	if *gomemlimit > 0 {
		memlimit = fmt.Sprintf("%dMiB", *gomemlimit>>20)
	}
	setPart := "off"
	if *setPartMax > 1 {
		setPart = fmt.Sprintf("max=%d", *setPartMax)
	}
	migrator := "off"
	if *ltmMigrator {
		migrator = cfg.ColdRecordsPath
	}
	// Report the net model (and the reactor loop count when overridden) so a run self-documents
	// which network path it served on: the reactor and goroutine paths differ in throughput, so a
	// benchmark log that names the path leaves no doubt about what was measured.
	net := *netMode
	if *reactorLoops > 0 {
		net = fmt.Sprintf("%s/loops=%d", net, *reactorLoops)
	}
	fmt.Printf("f1srv listening on %s (net=%s index-buckets=%d arena=%dMiB cold=%s migrator=%s gogc=%d gomemlimit=%s set-partition=%s)\n",
		srv.Addr(), net, *indexBuckets, *arenaBytes>>20, cold, migrator, effGOGC, memlimit, setPart)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("f1srv: serve: %v", err)
	}
}
