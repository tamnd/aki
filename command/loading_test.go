package command

import (
	"strings"
	"testing"
)

// TestLoadingBlocksNormalCommand checks a command without the loading flag is
// rejected with -LOADING while the dataset is loading.
func TestLoadingBlocksNormalCommand(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.loading.Store(true)

	out := runReply(d, "GET", "k")
	if !strings.Contains(out, "LOADING Redis is loading the dataset in memory") {
		t.Fatalf("GET during loading: want LOADING error, got %q", out)
	}
}

// TestLoadingAllowsFlaggedCommand checks INFO, which carries the loading flag,
// still runs during loading and reports loading:1.
func TestLoadingAllowsFlaggedCommand(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.loading.Store(true)

	out := runReply(d, "INFO", "persistence")
	if strings.Contains(out, "LOADING Redis") {
		t.Fatalf("INFO during loading should run, got %q", out)
	}
	if !strings.Contains(out, "loading:1") {
		t.Fatalf("INFO during loading: want loading:1, got %q", out)
	}
}

// TestLoadingClearedRunsCommand checks a normal command runs again once loading
// finishes, and INFO reports loading:0.
func TestLoadingClearedRunsCommand(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.loading.Store(false)

	out := runReply(d, "GET", "k")
	if strings.Contains(out, "LOADING Redis") {
		t.Fatalf("GET after loading: should run, got %q", out)
	}
	info := runReply(d, "INFO", "persistence")
	if !strings.Contains(info, "loading:0") {
		t.Fatalf("INFO after loading: want loading:0, got %q", info)
	}
}
