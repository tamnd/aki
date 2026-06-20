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
	dbfile := fs.String("dbfile", "aki.db", "path to the .aki data file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ks, closeKS, err := openKeyspace(*dbfile, *databases)
	if err != nil {
		return err
	}
	defer closeKS()

	d := command.New(command.Config{
		Databases:   *databases,
		RequirePass: *requirePass,
		Version:     fmt.Sprintf("7.2.0-aki-%s", Version),
		Engine:      command.NewEngine(ks),
	})

	cfg := networking.Config{
		Addr:       *addr,
		UnixSocket: *unixSocket,
		MaxClients: *maxClients,
	}
	srv := networking.New(cfg, d)

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(cfg) }()

	fmt.Printf("aki %s listening on %s\n", Version, *addr)
	if *unixSocket != "" {
		fmt.Printf("aki also listening on unix:%s\n", *unixSocket)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		return err
	case <-sig:
		fmt.Println("\naki shutting down")
		return srv.Close()
	}
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
