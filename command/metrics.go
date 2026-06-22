package command

import (
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tamnd/aki/networking"
)

// This file implements the Prometheus metrics endpoint from doc 20 section 8.1.
// When metrics-port is set, aki serves the Prometheus text exposition format at
// /metrics on a dedicated HTTP server. The figures are read from the same INFO
// fields and the same per-command statistics the rest of observability uses, so
// the endpoint never duplicates state and never blocks the command path.

// metricsServer holds the running endpoint so it can be shut down.
type metricsServer struct {
	srv *http.Server
	ln  net.Listener
}

// StartMetrics starts the Prometheus endpoint when metrics-port is set. A port of
// 0 leaves it disabled and returns nil. The server runs in its own goroutine.
func (d *Dispatcher) StartMetrics() error {
	port := d.confInt("metrics-port", 0)
	if port <= 0 {
		return nil
	}
	bind := d.confValue("metrics-bind", "127.0.0.1")
	ln, err := net.Listen("tcp", net.JoinHostPort(bind, strconv.FormatInt(port, 10)))
	if err != nil {
		return err
	}
	d.serveMetricsOn(ln)
	return nil
}

// serveMetricsOn serves /metrics on an already-open listener. StartMetrics uses it
// after binding the configured port; tests use it with an ephemeral listener.
func (d *Dispatcher) serveMetricsOn(ln net.Listener) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(d.renderMetrics()))
	})
	mux.HandleFunc("/health", d.handleHealth)
	mux.HandleFunc("/ready", d.handleReady)
	srv := &http.Server{Handler: mux}
	d.metrics.srv = srv
	d.metrics.ln = ln
	go func() { _ = srv.Serve(ln) }()
}

// handleHealth answers GET /health from doc 20 section 9.5. It returns 200 with an
// ok body when the server is operational and 503 while the AOF is still replaying.
func (d *Dispatcher) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if d.loading.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"loading","loading":1}` + "\n"))
		return
	}
	_, _ = w.Write([]byte(`{"status":"ok","version":"` + d.cfg.Version + `","loading":0}` + "\n"))
}

// handleReady answers GET /ready. It returns 200 only when the server is accepting
// clients and not loading, and 503 otherwise. A load balancer uses it to decide
// when to send traffic.
func (d *Dispatcher) handleReady(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if d.ready.Load() && !d.loading.Load() {
		_, _ = w.Write([]byte(`{"status":"ready"}` + "\n"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"status":"not ready"}` + "\n"))
}

// SetReady marks the server as accepting clients. The server command calls it once
// the listener is up. The HTTP /ready endpoint reads it.
func (d *Dispatcher) SetReady(v bool) { d.ready.Store(v) }

// StopMetrics shuts the endpoint down. It is safe to call when the endpoint was
// never started.
func (d *Dispatcher) StopMetrics() {
	if d.metrics.srv != nil {
		_ = d.metrics.srv.Close()
	}
}

// MetricsAddr returns the address the endpoint is listening on, or "" when it is
// not running. The server command prints it; tests read it to build a scrape URL.
func (d *Dispatcher) MetricsAddr() string {
	if d.metrics.ln == nil {
		return ""
	}
	return d.metrics.ln.Addr().String()
}

// infoFieldMap names a Prometheus metric, its type, and the INFO field it mirrors.
type infoFieldMap struct {
	metric    string
	typ       string
	infoField string
	redisName string // non-empty when a redis_-prefixed alias is also emitted
}

// scalarMetrics lists the single-value metrics mirrored straight from INFO. The
// order is fixed so the output is stable.
var scalarMetrics = []infoFieldMap{
	{"aki_connected_clients", "gauge", "connected_clients", "redis_connected_clients"},
	{"aki_blocked_clients", "gauge", "blocked_clients", "redis_blocked_clients"},
	{"aki_tracking_clients", "gauge", "tracking_clients", ""},
	{"aki_used_memory_bytes", "gauge", "used_memory", "redis_memory_used_bytes"},
	{"aki_used_memory_rss_bytes", "gauge", "used_memory_rss", "redis_memory_used_rss_bytes"},
	{"aki_used_memory_peak_bytes", "gauge", "used_memory_peak", ""},
	{"aki_maxmemory_bytes", "gauge", "maxmemory", "redis_memory_max_bytes"},
	{"aki_mem_fragmentation_ratio", "gauge", "mem_fragmentation_ratio", ""},
	{"aki_total_connections_received_total", "counter", "total_connections_received", ""},
	{"aki_total_commands_processed_total", "counter", "total_commands_processed", "redis_commands_processed_total"},
	{"aki_instantaneous_ops_per_sec", "gauge", "instantaneous_ops_per_sec", ""},
	{"aki_total_net_input_bytes_total", "counter", "total_net_input_bytes", ""},
	{"aki_total_net_output_bytes_total", "counter", "total_net_output_bytes", ""},
	{"aki_rejected_connections_total", "counter", "rejected_connections", ""},
	{"aki_expired_keys_total", "counter", "expired_keys", "redis_expired_keys_total"},
	{"aki_evicted_keys_total", "counter", "evicted_keys", "redis_evicted_keys_total"},
	{"aki_keyspace_hits_total", "counter", "keyspace_hits", "redis_keyspace_hits_total"},
	{"aki_keyspace_misses_total", "counter", "keyspace_misses", "redis_keyspace_misses_total"},
	{"aki_rdb_changes_since_last_save", "gauge", "rdb_changes_since_last_save", ""},
	{"aki_rdb_last_save_time", "gauge", "rdb_last_save_time", ""},
	{"aki_loading", "gauge", "loading", ""},
	{"aki_replication_offset", "gauge", "master_repl_offset", ""},
	{"aki_connected_slaves", "gauge", "connected_slaves", "redis_connected_slaves"},
	{"aki_uptime_in_seconds", "gauge", "uptime_in_seconds", "redis_uptime_in_seconds"},
	{"aki_used_cpu_sys_total", "counter", "used_cpu_sys", ""},
	{"aki_used_cpu_user_total", "counter", "used_cpu_user", ""},
}

// latencyBuckets are the upper bounds for the per-command latency histogram in
// microseconds, matching the bucket list in the spec.
var latencyBuckets = []uint64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 65536}

// renderMetrics builds the full Prometheus exposition text from the current state.
func (d *Dispatcher) renderMetrics() string {
	fields := d.collectInfo()
	var b strings.Builder

	for _, m := range scalarMetrics {
		v, ok := fields[m.infoField]
		if !ok {
			continue
		}
		writeMetricHeader(&b, m.metric, m.typ)
		b.WriteString(m.metric + " " + promValue(v) + "\n")
		if m.redisName != "" {
			writeMetricHeader(&b, m.redisName, m.typ)
			b.WriteString(m.redisName + " " + promValue(v) + "\n")
		}
	}

	d.writeCommandMetrics(&b)
	d.writeErrorMetrics(&b)
	writeDBMetrics(&b, fields)
	d.writeLatencyHistogram(&b)
	return b.String()
}

// writeCommandMetrics emits the per-command counters with a command label.
func (d *Dispatcher) writeCommandMetrics(b *strings.Builder) {
	names := d.statNames()
	if len(names) == 0 {
		return
	}
	writeMetricHeader(b, "aki_commands_calls_total", "counter")
	for _, n := range names {
		b.WriteString("aki_commands_calls_total{command=\"" + n + "\"} " +
			strconv.FormatUint(d.cmdStatFor(n).calls.Load(), 10) + "\n")
	}
	writeMetricHeader(b, "aki_commands_duration_usec_total", "counter")
	for _, n := range names {
		b.WriteString("aki_commands_duration_usec_total{command=\"" + n + "\"} " +
			strconv.FormatUint(d.cmdStatFor(n).usec.Load(), 10) + "\n")
	}
	writeMetricHeader(b, "aki_commands_rejected_calls_total", "counter")
	for _, n := range names {
		b.WriteString("aki_commands_rejected_calls_total{command=\"" + n + "\"} " +
			strconv.FormatUint(d.cmdStatFor(n).rejected.Load(), 10) + "\n")
	}
	writeMetricHeader(b, "aki_commands_failed_calls_total", "counter")
	for _, n := range names {
		b.WriteString("aki_commands_failed_calls_total{command=\"" + n + "\"} " +
			strconv.FormatUint(d.cmdStatFor(n).failed.Load(), 10) + "\n")
	}
}

// writeErrorMetrics emits the per-error-code counter with an error label.
func (d *Dispatcher) writeErrorMetrics(b *strings.Builder) {
	var codes []string
	d.stats.errs.Range(func(k, _ any) bool {
		codes = append(codes, k.(string))
		return true
	})
	if len(codes) == 0 {
		return
	}
	sort.Strings(codes)
	writeMetricHeader(b, "aki_errors_total", "counter")
	for _, code := range codes {
		v, ok := d.stats.errs.Load(code)
		if !ok {
			continue
		}
		b.WriteString("aki_errors_total{error=\"" + code + "\"} " +
			strconv.FormatUint(v.(*atomic.Uint64).Load(), 10) + "\n")
	}
}

// writeDBMetrics emits the per-database key count from the keyspace INFO lines,
// which carry "keys=N,expires=M,avg_ttl=T" per db.
func writeDBMetrics(b *strings.Builder, fields map[string]string) {
	var dbs []string
	for k := range fields {
		if strings.HasPrefix(k, "db") {
			dbs = append(dbs, k)
		}
	}
	if len(dbs) == 0 {
		return
	}
	sort.Strings(dbs)
	writeMetricHeader(b, "aki_db_keys", "gauge")
	for _, db := range dbs {
		n := strings.TrimPrefix(db, "db")
		keys := fieldFromCSV(fields[db], "keys")
		b.WriteString("aki_db_keys{db=\"" + n + "\"} " + promValue(keys) + "\n")
	}
	writeMetricHeader(b, "aki_db_expires", "gauge")
	for _, db := range dbs {
		n := strings.TrimPrefix(db, "db")
		expires := fieldFromCSV(fields[db], "expires")
		b.WriteString("aki_db_expires{db=\"" + n + "\"} " + promValue(expires) + "\n")
	}
}

// writeLatencyHistogram emits the per-command latency histogram in the Prometheus
// histogram shape: one _bucket series per upper bound, plus _sum and _count.
func (d *Dispatcher) writeLatencyHistogram(b *strings.Builder) {
	names := d.statNames()
	if len(names) == 0 {
		return
	}
	writeMetricHeader(b, "aki_command_latency_usec", "histogram")
	for _, n := range names {
		cs := d.cmdStatFor(n)
		total := cs.hist.total()
		for _, le := range latencyBuckets {
			b.WriteString("aki_command_latency_usec_bucket{command=\"" + n +
				"\",le=\"" + strconv.FormatUint(le, 10) + "\"} " +
				strconv.FormatUint(cs.hist.countLE(le), 10) + "\n")
		}
		b.WriteString("aki_command_latency_usec_bucket{command=\"" + n +
			"\",le=\"+Inf\"} " + strconv.FormatUint(total, 10) + "\n")
		b.WriteString("aki_command_latency_usec_sum{command=\"" + n + "\"} " +
			strconv.FormatUint(cs.usec.Load(), 10) + "\n")
		b.WriteString("aki_command_latency_usec_count{command=\"" + n + "\"} " +
			strconv.FormatUint(total, 10) + "\n")
	}
}

// collectInfo runs the INFO section writers against an offline connection and
// returns every "key:value" field as a map. It reuses the exact INFO computation
// so the metrics never drift from what INFO reports.
func (d *Dispatcher) collectInfo() map[string]string {
	conn := networking.NewOfflineConn()
	ctx := &Ctx{Conn: conn, d: d, sess: &session{}}
	var b strings.Builder
	for _, sec := range infoSections() {
		sec.write(ctx, &b)
	}
	out := make(map[string]string)
	for _, ln := range strings.Split(b.String(), "\r\n") {
		if k, v, ok := strings.Cut(ln, ":"); ok {
			out[k] = v
		}
	}
	return out
}

// writeMetricHeader writes the HELP and TYPE lines Prometheus expects before a
// metric's samples. The HELP text is the metric name, which is enough for a
// scraper and keeps the table above as the single source of names.
func writeMetricHeader(b *strings.Builder, name, typ string) {
	b.WriteString("# HELP " + name + " " + name + "\n")
	b.WriteString("# TYPE " + name + " " + typ + "\n")
}

// promValue normalizes an INFO value for Prometheus. A bare number passes through;
// anything that is not numeric becomes 0 so the output stays valid.
func promValue(v string) string {
	if v == "" {
		return "0"
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return v
	}
	return "0"
}

// fieldFromCSV pulls one "name=value" out of a comma-separated INFO value such as
// the keyspace "keys=1,expires=0,avg_ttl=0" line.
func fieldFromCSV(csv, name string) string {
	for _, part := range strings.Split(csv, ",") {
		if k, v, ok := strings.Cut(part, "="); ok && k == name {
			return v
		}
	}
	return "0"
}
