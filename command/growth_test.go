package command

import (
	"strconv"
	"strings"
	"testing"
)

// TestInfoGrowthFields checks the file-growth fields show up in INFO with
// parseable values after some traffic.
func TestInfoGrowthFields(t *testing.T) {
	d := newMetricsDispatcher(t)
	for i := 0; i < 20; i++ {
		runOffline(d, "SET", "k"+strconv.Itoa(i), "v")
		runOffline(d, "GET", "k"+strconv.Itoa(i))
	}

	fields := d.collectInfo()

	file, ok := fields["aki_dataset_file_bytes"]
	if !ok {
		t.Fatalf("INFO missing aki_dataset_file_bytes")
	}
	if n, err := strconv.ParseInt(file, 10, 64); err != nil || n <= 0 {
		t.Fatalf("aki_dataset_file_bytes = %q, want positive int", file)
	}

	for _, name := range []string{
		"aki_wal_bytes", "aki_wal_frame_count", "aki_dirty_pages",
		"aki_buffer_pool_pages",
	} {
		v, ok := fields[name]
		if !ok {
			t.Fatalf("INFO missing %s", name)
		}
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			t.Fatalf("%s = %q, want int", name, v)
		}
	}

	for _, name := range []string{"aki_page_cache_hit_ratio", "aki_on_disk_vs_ram_ratio"} {
		v, ok := fields[name]
		if !ok {
			t.Fatalf("INFO missing %s", name)
		}
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			t.Fatalf("%s = %q, want float", name, v)
		}
	}

	// The reads above all served pages, so the buffer pool has resident frames and
	// the hit ratio is above zero.
	if fields["aki_buffer_pool_pages"] == "0" {
		t.Fatalf("aki_buffer_pool_pages should be non-zero after traffic")
	}
	if fields["aki_page_cache_hit_ratio"] == "0.0000" {
		t.Fatalf("aki_page_cache_hit_ratio should be non-zero after traffic")
	}
}

// TestMetricsGrowthFields checks the growth fields are exported on /metrics.
func TestMetricsGrowthFields(t *testing.T) {
	d := newMetricsDispatcher(t)
	runOffline(d, "SET", "foo", "bar")
	runOffline(d, "GET", "foo")

	out := d.renderMetrics()
	for _, want := range []string{
		"# TYPE aki_dataset_file_bytes gauge",
		"aki_dataset_file_bytes ",
		"# TYPE aki_page_cache_hit_ratio gauge",
		"aki_buffer_pool_pages ",
		"aki_on_disk_vs_ram_ratio ",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("renderMetrics missing %q in:\n%s", want, out)
		}
	}
}

// TestCacheRatioFormat checks the hit-ratio formatter handles the no-traffic case
// and a normal split.
func TestCacheRatioFormat(t *testing.T) {
	if got := fmtCacheRatio(0, 0); got != "0.0000" {
		t.Fatalf("fmtCacheRatio(0,0) = %q, want 0.0000", got)
	}
	if got := fmtCacheRatio(3, 1); got != "0.7500" {
		t.Fatalf("fmtCacheRatio(3,1) = %q, want 0.7500", got)
	}
}
