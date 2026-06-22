package command

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestInfoServerStorageFields checks the aki storage facts in the INFO server
// section: format string, page size, shard count, file path, WAL file, and the
// in-memory flag.
func TestInfoServerStorageFields(t *testing.T) {
	r, c := startData(t)

	if got := infoField(t, r, c, "server", "aki_storage_format"); got != "fmt001" {
		t.Fatalf("aki_storage_format = %q want fmt001", got)
	}
	// startData opens the pager with a 4096 byte page.
	if got := infoField(t, r, c, "server", "aki_page_size"); got != "4096" {
		t.Fatalf("aki_page_size = %q want 4096", got)
	}
	// The shard count defaults to 1 until the sharded writer model lands.
	if got := infoField(t, r, c, "server", "aki_shard_count"); got != "1" {
		t.Fatalf("aki_shard_count = %q want 1", got)
	}
	// The file path is absolute and ends with the name the pager was opened with.
	file := infoField(t, r, c, "server", "aki_file")
	if !filepath.IsAbs(file) || !strings.HasSuffix(file, "data.aki") {
		t.Fatalf("aki_file = %q want absolute path ending in data.aki", file)
	}
	// The WAL sidecar is not open, so its path is empty.
	if got := infoField(t, r, c, "server", "aki_wal_file"); got != "" {
		t.Fatalf("aki_wal_file = %q want empty", got)
	}
	if got := infoField(t, r, c, "server", "aki_in_memory"); got != "0" {
		t.Fatalf("aki_in_memory = %q want 0", got)
	}
}

// TestInfoServerStorageNoEngine checks the storage fields stay sane on a
// connection-only server with no keyspace behind it.
func TestInfoServerStorageNoEngine(t *testing.T) {
	r, c := start(t, Config{})

	if got := infoField(t, r, c, "server", "aki_page_size"); got != "0" {
		t.Fatalf("aki_page_size = %q want 0", got)
	}
	if got := infoField(t, r, c, "server", "aki_file"); got != "" {
		t.Fatalf("aki_file = %q want empty", got)
	}
}
