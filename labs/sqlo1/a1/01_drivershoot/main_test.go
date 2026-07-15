package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The smoke test runs the whole suite at tiny counts under whichever
// driver tag this binary carries; the no-tag build only proves the stub
// refuses loudly. CI runs all three tags plus the stub.
func TestRunAllQuick(t *testing.T) {
	dir := t.TempDir()
	if driverName == "none" {
		if _, err := openShootDB(filepath.Join(dir, "x.db"), 4096, 1024); err == nil {
			t.Fatal("no-tag build must refuse to open")
		}
		return
	}
	cfg := config{
		dir: dir, page: 8192, cacheKiB: 8192, val: 64,
		keys: 2000, ops: 5000, batch: 512, readers: 2, fields: 32,
		dist: "zipf", poolDur: 150 * time.Millisecond,
	}
	if err := runAll(cfg); err != nil {
		t.Fatal(err)
	}

	// The header must carry the swept page size, or the creation
	// pragmas silently did not land and every page-size arm measures
	// the default.
	path := filepath.Join(dir, fmt.Sprintf("shoot-%s-p%d.db", driverName, cfg.page))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := int(b[16])<<8 | int(b[17]); got != cfg.page {
		t.Fatalf("database header page size %d, want %d", got, cfg.page)
	}
}
