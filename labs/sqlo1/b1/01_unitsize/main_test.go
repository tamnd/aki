package main

import (
	"bytes"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs a tiny sweep without direct IO (tmpfs in CI
// rejects O_DIRECT) and checks the CSV shape: one row per cell, 11
// columns, ops and iops positive.
func TestRunAllSmoke(t *testing.T) {
	cfg := config{
		dir:    t.TempDir(),
		fileMB: 16,
		secs:   0.05,
		units:  []int64{4096, 8192},
		qds:    []int{1, 2},
		ops:    []string{"randread", "randwrite"},
		seed:   1,
		direct: false,
	}
	var out bytes.Buffer
	if err := runAll(cfg, &out); err != nil {
		t.Fatalf("runAll: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	want := 1 + len(cfg.ops)*len(cfg.units)*len(cfg.qds)
	if len(lines) != want {
		t.Fatalf("got %d lines, want %d:\n%s", len(lines), want, out.String())
	}
	for _, line := range lines[1:] {
		f := strings.Split(line, ",")
		if len(f) != 11 {
			t.Fatalf("row has %d fields, want 11: %q", len(f), line)
		}
		ops, err := strconv.ParseInt(f[4], 10, 64)
		if err != nil || ops <= 0 {
			t.Fatalf("bad ops column in %q: %v", line, err)
		}
		iops, err := strconv.ParseFloat(f[5], 64)
		if err != nil || iops <= 0 {
			t.Fatalf("bad iops column in %q: %v", line, err)
		}
		if f[10] != "0" {
			t.Fatalf("direct column %q, want 0 in the no-direct smoke", f[10])
		}
	}
}

// TestRunAllRejectsBadUnit pins the 4096-multiple guard.
func TestRunAllRejectsBadUnit(t *testing.T) {
	cfg := config{dir: t.TempDir(), fileMB: 1, secs: 0.01,
		units: []int64{512}, qds: []int{1}, ops: []string{"randread"}}
	if err := runAll(cfg, &bytes.Buffer{}); err == nil {
		t.Fatal("unit 512 accepted, want an error")
	}
}

// TestDirectOpen proves the direct-IO open path works on a real
// filesystem, or records that this one refuses it.
func TestDirectOpen(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("no direct IO on this platform")
	}
	path := t.TempDir() + "/d.dat"
	if err := fillFile(path, 1<<20, 1); err != nil {
		t.Fatal(err)
	}
	f, direct, err := openIO(path, false, true)
	if err != nil {
		t.Skipf("filesystem refused direct IO: %v", err)
	}
	defer f.Close()
	if !direct {
		t.Fatal("openIO reported direct=false on a direct open")
	}
	buf := alignedBuf(4096, 4096)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("direct read: %v", err)
	}
}

// TestAlignedBuf pins the O_DIRECT buffer contract: n bytes, base
// address a multiple of align.
func TestAlignedBuf(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("plain buffers on this platform")
	}
	for _, n := range []int{4096, 8192, 16384} {
		b := alignedBuf(n, 4096)
		if len(b) != n {
			t.Fatalf("len %d, want %d", len(b), n)
		}
		if addr := bufAddr(b); addr%4096 != 0 {
			t.Fatalf("base address %#x not 4096-aligned", addr)
		}
	}
}

// TestPercentiles pins the index math on a known slice.
func TestPercentiles(t *testing.T) {
	lats := make([]int64, 100)
	for i := range lats {
		lats[i] = int64(i + 1)
	}
	p50, p99, mx := percentiles(lats)
	if p50 != 51 || p99 != 100 || mx != 100 {
		t.Fatalf("got %d/%d/%d, want 51/100/100", p50, p99, mx)
	}
	if a, b, c := percentiles(nil); a != 0 || b != 0 || c != 0 {
		t.Fatal("empty slice must yield zeros")
	}
}

// TestFillFile checks the fill writes exactly the requested bytes.
func TestFillFile(t *testing.T) {
	path := t.TempDir() + "/f.dat"
	if err := fillFile(path, 3<<20, 7); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 3<<20 {
		t.Fatalf("size %d, want %d", st.Size(), 3<<20)
	}
}
