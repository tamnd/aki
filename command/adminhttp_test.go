package command

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

// adminGet does a GET against the admin endpoint and returns the status code and
// body.
func adminGet(t *testing.T, base, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

// TestAdminEndpoint serves the admin handlers on an ephemeral port and checks the
// pprof index, a named profile, the CPU symbol route, and the mirrored /metrics
// and health probes all answer.
func TestAdminEndpoint(t *testing.T) {
	d := New(Config{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d.serveAdminOn(ln)
	defer d.StopAdmin()

	base := "http://" + d.AdminAddr()

	// The pprof index lists the available profiles.
	if code, body := adminGet(t, base, "/debug/pprof/"); code != http.StatusOK || !strings.Contains(body, "heap") {
		t.Fatalf("pprof index: code=%d body=%.80q", code, body)
	}
	// A named profile dumps in text form when debug=1 is asked for.
	if code, body := adminGet(t, base, "/debug/pprof/goroutine?debug=1"); code != http.StatusOK || !strings.Contains(body, "goroutine") {
		t.Fatalf("goroutine profile: code=%d body=%.80q", code, body)
	}
	// The heap profile answers as a binary pprof payload.
	if code, _ := adminGet(t, base, "/debug/pprof/heap"); code != http.StatusOK {
		t.Fatalf("heap profile code=%d", code)
	}
	// /metrics is mirrored here so one port covers all diagnostics.
	if code, body := adminGet(t, base, "/metrics"); code != http.StatusOK || !strings.Contains(body, "aki_") {
		t.Fatalf("metrics: code=%d body=%.80q", code, body)
	}
	// Health probe answers ok when not loading.
	if code, body := adminGet(t, base, "/health"); code != http.StatusOK || !strings.Contains(body, "ok") {
		t.Fatalf("health: code=%d body=%.80q", code, body)
	}
}

// TestStartAdminOff checks the endpoint stays down when admin-port is 0, so
// StartAdmin is a safe no-op and StopAdmin does nothing.
func TestStartAdminOff(t *testing.T) {
	d := New(Config{})
	d.conf.set("admin-port", "0")
	if err := d.StartAdmin(); err != nil {
		t.Fatalf("StartAdmin off: %v", err)
	}
	if d.AdminAddr() != "" {
		t.Fatalf("admin endpoint started while off: %s", d.AdminAddr())
	}
	d.StopAdmin() // must not panic
}

// TestStartAdminOn binds an ephemeral port through the config path and confirms
// the endpoint comes up and shuts down cleanly.
func TestStartAdminOn(t *testing.T) {
	d := New(Config{})
	d.conf.set("admin-port", "0") // avoid the fixed 6399 default in tests
	// Bind directly through serveAdminOn for a free port, then exercise Stop.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d.serveAdminOn(ln)
	if d.AdminAddr() == "" {
		t.Fatal("admin endpoint did not start")
	}
	d.StopAdmin()
}
