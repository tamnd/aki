package command

import (
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
)

// This file implements the built-in admin endpoint from doc 21 section 10.1. When
// admin-port is set, aki serves Go's net/http/pprof handlers on a dedicated HTTP
// server bound to loopback by default, so an operator can pull a CPU profile,
// heap profile, goroutine dump, or execution trace from a running server with
// go tool pprof. The same server also mirrors /metrics and the health probes so
// everything diagnostic lives on one port (doc 21 section 10.4).

// adminServer holds the running endpoint so it can be shut down.
type adminServer struct {
	srv *http.Server
	ln  net.Listener
}

// StartAdmin starts the admin endpoint when admin-port is set. A port of 0 leaves
// it disabled and returns nil. The server command calls it once at startup,
// alongside StartMetrics. The server runs in its own goroutine.
func (d *Dispatcher) StartAdmin() error {
	port := d.confInt("admin-port", 6399)
	if port <= 0 {
		return nil
	}
	bind := d.confValue("admin-bind", "127.0.0.1")
	ln, err := net.Listen("tcp", net.JoinHostPort(bind, strconv.FormatInt(port, 10)))
	if err != nil {
		return err
	}
	d.serveAdminOn(ln)
	return nil
}

// serveAdminOn serves the admin handlers on an already-open listener. StartAdmin
// uses it after binding the configured port; tests use it with an ephemeral
// listener. It builds its own mux rather than relying on the default one, so
// importing net/http/pprof does not silently expose profiling anywhere else.
func (d *Dispatcher) serveAdminOn(ln net.Listener) {
	mux := http.NewServeMux()

	// pprof handlers. Index covers the per-profile pages (heap, goroutine, block,
	// mutex, allocs, threadcreate) by reading the last path segment, so the named
	// profiles do not each need their own route.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// The same diagnostic surface as the metrics endpoint, so an operator only
	// needs the one port.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(d.renderMetrics()))
	})
	mux.HandleFunc("/health", d.handleHealth)
	mux.HandleFunc("/ready", d.handleReady)

	srv := &http.Server{Handler: mux}
	d.admin.srv = srv
	d.admin.ln = ln
	go func() { _ = srv.Serve(ln) }()
}

// StopAdmin shuts the endpoint down. It is safe to call when the endpoint was
// never started.
func (d *Dispatcher) StopAdmin() {
	if d.admin.srv != nil {
		_ = d.admin.srv.Close()
	}
}

// AdminAddr returns the address the endpoint is listening on, or "" when it is not
// running. The server command prints it; tests read it to build request URLs.
func (d *Dispatcher) AdminAddr() string {
	if d.admin.ln == nil {
		return ""
	}
	return d.admin.ln.Addr().String()
}
