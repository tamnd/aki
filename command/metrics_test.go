package command

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newMetricsDispatcher builds a dispatcher over an in-memory keyspace and returns
// it directly so a test can call renderMetrics and serveMetricsOn without a real
// socket. Commands are driven through Handle on an offline connection.
func newMetricsDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	return New(Config{Engine: NewEngine(ks)})
}

// runOffline drives one command through the dispatcher on an offline connection so
// the statistics, error, and key counters fill in.
func runOffline(d *Dispatcher, args ...string) {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	d.Handle(networking.NewOfflineConn(), argv)
}

// TestRenderMetricsScalars checks the scalar INFO-backed metrics show up with both
// the aki_ name and the redis_ alias.
func TestRenderMetricsScalars(t *testing.T) {
	d := newMetricsDispatcher(t)
	out := d.renderMetrics()

	for _, want := range []string{
		"# TYPE aki_connected_clients gauge",
		"aki_uptime_in_seconds ",
		"# TYPE redis_uptime_in_seconds gauge",
		"redis_memory_used_bytes ",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("renderMetrics missing %q in:\n%s", want, out)
		}
	}
}

// TestRenderMetricsCommands checks per-command counters appear with a command
// label after a command runs.
func TestRenderMetricsCommands(t *testing.T) {
	d := newMetricsDispatcher(t)
	runOffline(d, "SET", "foo", "bar")
	runOffline(d, "GET", "foo")
	runOffline(d, "GET", "foo")

	out := d.renderMetrics()
	for _, want := range []string{
		`aki_commands_calls_total{command="get"} 2`,
		`aki_commands_calls_total{command="set"} 1`,
		`aki_commands_duration_usec_total{command="get"} `,
		`aki_command_latency_usec_bucket{command="get",le="+Inf"} 2`,
		`aki_command_latency_usec_count{command="get"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("renderMetrics missing %q in:\n%s", want, out)
		}
	}
}

// TestRenderMetricsErrors checks a failed command produces an errorstat metric
// labelled by the error code.
func TestRenderMetricsErrors(t *testing.T) {
	d := newMetricsDispatcher(t)
	runOffline(d, "SET", "k", "v")
	runOffline(d, "LPUSH", "k", "x") // WRONGTYPE

	out := d.renderMetrics()
	if !strings.Contains(out, `aki_errors_total{error="WRONGTYPE"} 1`) {
		t.Fatalf("renderMetrics missing WRONGTYPE error metric in:\n%s", out)
	}
	if !strings.Contains(out, `aki_commands_failed_calls_total{command="lpush"} 1`) {
		t.Fatalf("renderMetrics missing lpush failed metric in:\n%s", out)
	}
}

// TestRenderMetricsDBKeys checks the per-db key gauge reflects stored keys.
func TestRenderMetricsDBKeys(t *testing.T) {
	d := newMetricsDispatcher(t)
	runOffline(d, "SET", "a", "1")
	runOffline(d, "SET", "b", "2")

	out := d.renderMetrics()
	if !strings.Contains(out, `aki_db_keys{db="0"} 2`) {
		t.Fatalf("renderMetrics missing db0 key count in:\n%s", out)
	}
}

// TestMetricsEndpoint checks the HTTP endpoint serves the same exposition text on
// an ephemeral listener.
func TestMetricsEndpoint(t *testing.T) {
	d := newMetricsDispatcher(t)
	runOffline(d, "SET", "foo", "bar")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	d.serveMetricsOn(ln)
	defer d.StopMetrics()

	addr := d.MetricsAddr()
	if addr == "" {
		t.Fatalf("MetricsAddr empty after serveMetricsOn")
	}

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), `aki_commands_calls_total{command="set"} 1`) {
		t.Fatalf("endpoint body missing set counter:\n%s", body)
	}
}

// TestHealthEndpoint checks /health reports ok when operational and loading when
// the AOF is replaying.
func TestHealthEndpoint(t *testing.T) {
	d := newMetricsDispatcher(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	d.serveMetricsOn(ln)
	defer d.StopMetrics()
	base := "http://" + d.MetricsAddr()

	resp, err := http.Get(base + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health status = %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"status":"ok"`) || !strings.Contains(string(body), `"loading":0`) {
		t.Fatalf("/health body = %s", body)
	}

	d.loading.Store(true)
	resp, err = http.Get(base + "/health")
	if err != nil {
		t.Fatalf("GET /health loading: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/health loading status = %d want 503", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"loading":1`) {
		t.Fatalf("/health loading body = %s", body)
	}
}

// TestReadyEndpoint checks /ready flips from 503 to 200 once the server marks
// itself ready.
func TestReadyEndpoint(t *testing.T) {
	d := newMetricsDispatcher(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	d.serveMetricsOn(ln)
	defer d.StopMetrics()
	base := "http://" + d.MetricsAddr()

	resp, err := http.Get(base + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/ready before SetReady = %d want 503", resp.StatusCode)
	}

	d.SetReady(true)
	resp, err = http.Get(base + "/ready")
	if err != nil {
		t.Fatalf("GET /ready after ready: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ready after SetReady = %d want 200", resp.StatusCode)
	}
}

// TestStartMetricsDisabled checks StartMetrics is a no-op when metrics-port is 0.
func TestStartMetricsDisabled(t *testing.T) {
	d := newMetricsDispatcher(t)
	if err := d.StartMetrics(); err != nil {
		t.Fatalf("StartMetrics with port 0: %v", err)
	}
	if d.MetricsAddr() != "" {
		t.Fatalf("MetricsAddr = %q want empty when disabled", d.MetricsAddr())
	}
}
