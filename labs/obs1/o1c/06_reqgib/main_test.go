package main

import (
	"testing"
	"time"
)

// The whole pipeline at shrunken constants, under race in CI: write log,
// chain, folder, publisher, and the counting bucket all agree that
// requests were spent and segments published.
func TestReqGibSmoke(t *testing.T) {
	r, err := run(cfg{
		payloadBytes: 8 << 20, groups: 2, valBytes: 500,
		flushSize: 1 << 20, segTarget: 2 << 20, foldAge: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.puts == 0 || r.total() == 0 {
		t.Fatalf("no requests counted: %+v", r)
	}
	if r.walFlushes == 0 || r.chainBatches == 0 {
		t.Fatalf("write log rows missing from INFO: %+v", r)
	}
	if r.segPuts == 0 || r.published == 0 {
		t.Fatalf("fold pipeline idle: %+v", r)
	}
	if r.perGiB() <= 0 {
		t.Fatalf("per-GiB rate %f", r.perGiB())
	}
}

func TestInfoRow(t *testing.T) {
	info := "# Durability\r\nwal_flushes:42\r\nchain_commit_batches:7\r\n"
	if infoRow(info, "wal_flushes") != 42 || infoRow(info, "chain_commit_batches") != 7 {
		t.Fatal("info parse")
	}
	if infoRow(info, "absent_row") != 0 {
		t.Fatal("absent row should read zero")
	}
}
