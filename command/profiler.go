package command

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"time"
)

// This file implements continuous profiling from doc 21 section 10.2. When
// continuous-profiling is on, a background goroutine writes a cpu, heap, and
// mutex pprof snapshot to profiling-dir every profiling-interval seconds and
// rotates out the oldest so only profiling-keep of each kind remain. This gives
// post-hoc analysis of a latency spike or memory growth without a live profiling
// session attached.

// profileKinds are the snapshot kinds written each interval, in the order they
// are produced. The filenames are <kind>_<timestamp>.prof.
var profileKinds = []string{"cpu", "heap", "mutex"}

// profilerMaxCPUWindow caps how long a single snapshot spends sampling CPU, so a
// long interval does not hold a CPU profile open for minutes at a time.
const profilerMaxCPUWindow = 10 * time.Second

// profilerState holds the running profiler goroutine so it can be stopped.
type profilerState struct {
	stop chan struct{}
	done chan struct{}
}

// StartProfiler launches continuous profiling when continuous-profiling is on. It
// is a no-op returning nil when the feature is off. The server command calls it
// once at startup, alongside StartMetrics.
func (d *Dispatcher) StartProfiler() error {
	if !d.confBool("continuous-profiling", false) {
		return nil
	}
	dir := d.confValue("profiling-dir", "./profiles")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	interval := d.confInt("profiling-interval", 60)
	if interval <= 0 {
		interval = 60
	}
	keep := int(d.confInt("profiling-keep", 10))
	if keep <= 0 {
		keep = 10
	}
	// A snapshot spends half the interval sampling CPU, capped, so it always
	// finishes well before the next tick.
	cpuWindow := time.Duration(interval) * time.Second / 2
	if cpuWindow > profilerMaxCPUWindow {
		cpuWindow = profilerMaxCPUWindow
	}
	// Without a mutex profiling fraction the mutex snapshot is always empty, so
	// turn it on while continuous profiling runs.
	runtime.SetMutexProfileFraction(1)

	d.profiler.stop = make(chan struct{})
	d.profiler.done = make(chan struct{})
	go func() {
		defer close(d.profiler.done)
		t := time.NewTicker(time.Duration(interval) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-d.profiler.stop:
				return
			case <-t.C:
				_ = d.writeProfiles(dir, profileStamp(), cpuWindow)
				pruneProfiles(dir, keep)
			}
		}
	}()
	return nil
}

// StopProfiler stops the profiler goroutine and waits for it to exit. It is safe
// to call when the profiler was never started.
func (d *Dispatcher) StopProfiler() {
	if d.profiler.stop == nil {
		return
	}
	close(d.profiler.stop)
	<-d.profiler.done
	d.profiler.stop = nil
	d.profiler.done = nil
	runtime.SetMutexProfileFraction(0)
}

// profileStamp formats the current time the way the snapshot filenames use it,
// matching the layout in the spec.
func profileStamp() string {
	return time.Now().Format("20060102_150405")
}

// writeProfiles writes one cpu, heap, and mutex snapshot into dir, stamped with
// stamp. cpuWindow is how long to sample CPU; a zero window skips the cpu
// snapshot, which the tests use to stay fast. A failure on one snapshot is
// returned but does not stop the others from being attempted.
func (d *Dispatcher) writeProfiles(dir, stamp string, cpuWindow time.Duration) error {
	var firstErr error
	note := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if cpuWindow > 0 {
		note(writeCPUProfile(filepath.Join(dir, "cpu_"+stamp+".prof"), cpuWindow))
	}
	// A GC before the heap snapshot makes it reflect live memory rather than
	// garbage not yet collected.
	runtime.GC()
	note(writeLookupProfile(filepath.Join(dir, "heap_"+stamp+".prof"), "heap"))
	note(writeLookupProfile(filepath.Join(dir, "mutex_"+stamp+".prof"), "mutex"))
	return firstErr
}

// writeCPUProfile samples the CPU for window and writes the profile to path.
func writeCPUProfile(path string, window time.Duration) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := pprof.StartCPUProfile(f); err != nil {
		return err
	}
	time.Sleep(window)
	pprof.StopCPUProfile()
	return nil
}

// writeLookupProfile writes the named pprof lookup profile (heap, mutex, and so
// on) to path.
func writeLookupProfile(path, name string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	p := pprof.Lookup(name)
	if p == nil {
		return nil
	}
	return p.WriteTo(f, 0)
}

// pruneProfiles keeps only the newest keep snapshots of each kind in dir and
// removes the rest. The timestamp in the filename sorts chronologically, so a
// lexical sort puts the oldest first.
func pruneProfiles(dir string, keep int) {
	for _, kind := range profileKinds {
		matches, err := filepath.Glob(filepath.Join(dir, kind+"_*.prof"))
		if err != nil || len(matches) <= keep {
			continue
		}
		sort.Strings(matches)
		for _, old := range matches[:len(matches)-keep] {
			_ = os.Remove(old)
		}
	}
}
