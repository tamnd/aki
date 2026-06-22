package command

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestWriteProfiles checks one snapshot pass writes a heap and mutex file. CPU is
// skipped with a zero window so the test does not spend real time sampling.
func TestWriteProfiles(t *testing.T) {
	d := New(Config{})
	dir := t.TempDir()
	if err := d.writeProfiles(dir, "20060102_150405", 0); err != nil {
		t.Fatalf("writeProfiles: %v", err)
	}
	for _, kind := range []string{"heap", "mutex"} {
		path := filepath.Join(dir, kind+"_20060102_150405.prof")
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("missing %s snapshot: %v", kind, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s snapshot is empty", kind)
		}
	}
	// A zero CPU window writes no cpu snapshot.
	if matches, _ := filepath.Glob(filepath.Join(dir, "cpu_*.prof")); len(matches) != 0 {
		t.Fatalf("cpu snapshot written for zero window: %v", matches)
	}
}

// TestPruneProfiles checks rotation keeps only the newest snapshots of each kind
// and leaves other files alone.
func TestPruneProfiles(t *testing.T) {
	dir := t.TempDir()
	for _, kind := range profileKinds {
		for i := range 5 {
			// Zero-padded so the lexical sort matches chronological order.
			name := kind + "_2026010100000" + strconv.Itoa(i) + ".prof"
			if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	// An unrelated file must survive pruning.
	keepFile := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(keepFile, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	pruneProfiles(dir, 2)

	for _, kind := range profileKinds {
		matches, _ := filepath.Glob(filepath.Join(dir, kind+"_*.prof"))
		if len(matches) != 2 {
			t.Fatalf("%s left %d files, want 2: %v", kind, len(matches), matches)
		}
		// The two newest (highest suffix) must be the survivors.
		want := map[string]bool{
			kind + "_20260101000003.prof": true,
			kind + "_20260101000004.prof": true,
		}
		for _, m := range matches {
			if !want[filepath.Base(m)] {
				t.Fatalf("%s kept unexpected file %s", kind, m)
			}
		}
	}
	if _, err := os.Stat(keepFile); err != nil {
		t.Fatalf("pruning removed unrelated file: %v", err)
	}
}

// TestStartProfilerOff checks the profiler stays idle when the feature is off, so
// StartProfiler is a safe no-op on a default config and StopProfiler does nothing.
func TestStartProfilerOff(t *testing.T) {
	d := New(Config{})
	if err := d.StartProfiler(); err != nil {
		t.Fatalf("StartProfiler off: %v", err)
	}
	if d.profiler.stop != nil {
		t.Fatal("profiler started while feature off")
	}
	d.StopProfiler() // must not panic or block
}

// TestStartProfilerOn turns the feature on with a short interval pointed at a temp
// directory, lets one snapshot pass run, then stops it cleanly.
func TestStartProfilerOn(t *testing.T) {
	d := New(Config{})
	dir := t.TempDir()
	d.conf.set("continuous-profiling", "yes")
	d.conf.set("profiling-dir", dir)
	d.conf.set("profiling-interval", "1")
	d.conf.set("profiling-keep", "3")

	if err := d.StartProfiler(); err != nil {
		t.Fatalf("StartProfiler on: %v", err)
	}
	if d.profiler.stop == nil {
		t.Fatal("profiler did not start")
	}
	d.StopProfiler()
	if d.profiler.stop != nil {
		t.Fatal("StopProfiler left state behind")
	}
}
