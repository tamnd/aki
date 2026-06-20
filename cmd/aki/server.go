package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/aki/command"
	"github.com/tamnd/aki/networking"
)

// cmdServer starts the aki server: it parses flags, builds the command
// dispatcher, and runs the network listener until interrupted. At this
// milestone the server answers the connection-group commands; the keyspace and
// data-type commands are wired in as later slices land.
func cmdServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:6379", "TCP listen address (host:port)")
	unixSocket := fs.String("unixsocket", "", "Unix socket path to listen on as well")
	maxClients := fs.Int("maxclients", 10000, "maximum number of connected clients")
	databases := fs.Int("databases", 16, "number of logical databases")
	requirePass := fs.String("requirepass", "", "password required for the default user")
	if err := fs.Parse(args); err != nil {
		return err
	}

	d := command.New(command.Config{
		Databases:   *databases,
		RequirePass: *requirePass,
		Version:     fmt.Sprintf("7.2.0-aki-%s", Version),
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
