package drivers

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestPprofOff checks that a server built without PprofAddr binds no pprof
// listener, so the default is a no-op.
func TestPprofOff(t *testing.T) {
	srv, _, _ := startServer(t)
	if pa := srv.PprofAddr(); pa != nil {
		t.Fatalf("PprofAddr() = %v, want nil when the option is empty", pa)
	}
}

// TestPprofOn sets PprofAddr and fetches /debug/pprof/ from the bound
// listener.
func TestPprofOn(t *testing.T) {
	srv, err := Listen(Options{
		Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18,
		PprofAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	pa := srv.PprofAddr()
	if pa == nil {
		t.Fatal("PprofAddr() = nil, want a bound address")
	}
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", pa))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "goroutine") {
		t.Fatalf("index body does not mention goroutine profiles: %q", body)
	}
}
