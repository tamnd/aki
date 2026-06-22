package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/aki/command"
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/vfs"
)

// cmdServer starts the aki server: it opens (or creates) the data file, builds
// the keyspace and command dispatcher, and runs the network listener until
// interrupted.
func cmdServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:6379", "TCP listen address (host:port)")
	unixSocket := fs.String("unixsocket", "", "Unix socket path to listen on as well")
	maxClients := fs.Int("maxclients", 10000, "maximum number of connected clients")
	databases := fs.Int("databases", 16, "number of logical databases")
	requirePass := fs.String("requirepass", "", "password required for the default user")
	aclFile := fs.String("aclfile", "", "path to an external ACL file loaded at startup and written by ACL SAVE")
	logfile := fs.String("logfile", "", "path to the log file (empty logs to stderr)")
	loglevel := fs.String("loglevel", "", "minimum log level: debug, verbose, notice, warning")
	dbfile := fs.String("dbfile", "aki.db", "path to the .aki data file")
	loadRDB := fs.String("load-rdb", "", "import this dump.rdb on first open (only when the .aki file does not exist)")
	rdbDB := fs.Int("rdb-db", -1, "with --load-rdb, import only this source database")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fresh := !vfs.NewOS().Exists(*dbfile)
	if *loadRDB != "" && !fresh {
		return fmt.Errorf("--load-rdb only applies on first open; %s already exists", *dbfile)
	}

	ks, closeKS, err := openKeyspace(*dbfile, *databases)
	if err != nil {
		return err
	}
	defer closeKS()

	var importedFuncs []string
	if *loadRDB != "" && fresh {
		n, funcs, err := importRDBInto(ks, *loadRDB, *rdbDB)
		if err != nil {
			return err
		}
		importedFuncs = funcs
		fmt.Printf("loaded %d keys from %s\n", n, *loadRDB)
	}

	d := command.New(command.Config{
		Databases:   *databases,
		RequirePass: *requirePass,
		AclFile:     *aclFile,
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
		Addr:       *addr,
		UnixSocket: *unixSocket,
		MaxClients: *maxClients,
	}
	if *logfile != "" {
		if err := d.SetConfig("logfile", *logfile); err != nil {
			return fmt.Errorf("set logfile: %w", err)
		}
	}
	if *loglevel != "" {
		if err := d.SetConfig("loglevel", *loglevel); err != nil {
			return fmt.Errorf("set loglevel: %w", err)
		}
	}
	if err := d.LogStart(); err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer d.LogClose()

	srv := networking.New(cfg, d)
	d.SetServer(srv)
	d.StartBackground()
	defer d.StopBackground()

	if err := d.StartMetrics(); err != nil {
		return fmt.Errorf("start metrics endpoint: %w", err)
	}
	defer d.StopMetrics()

	d.LogNotice("Server started", "aki_version", Version, "addr", *addr)

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

	fmt.Printf("aki %s listening on %s\n", Version, *addr)
	if *unixSocket != "" {
		fmt.Printf("aki also listening on unix:%s\n", *unixSocket)
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
func openKeyspace(path string, databases int) (*keyspace.Keyspace, func(), error) {
	osfs := vfs.NewOS()
	var (
		pgr *pager.Pager
		err error
	)
	if osfs.Exists(path) {
		pgr, err = pager.Open(osfs, path, pager.Options{})
	} else {
		pgr, err = pager.Create(osfs, path, pager.Options{DBCount: uint32(databases)})
	}
	if err != nil {
		return nil, nil, fmt.Errorf("open data file %s: %w", path, err)
	}
	ks, err := keyspace.Open(pgr)
	if err != nil {
		_ = pgr.Close()
		return nil, nil, fmt.Errorf("open keyspace: %w", err)
	}
	return ks, func() { _ = pgr.Close() }, nil
}
