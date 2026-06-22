package command

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"
)

// This file implements the crash report from doc 20 section 8.3. When a command
// goroutine panics, aki writes a bug report to the log and then exits with code
// 255, the same way Redis turns an assertion or segfault into a crash log before
// dying. The report is gated on crash-log-enabled, and the memory section on
// crash-memlog-enabled.

// crashReportExitCode is the process exit code after a crash, matching Redis.
const crashReportExitCode = 255

// OnPanic is the panic hook the network server calls from its per-connection
// recover. It writes the crash report and exits the process, because a panic
// leaves engine state mid-mutation and aki cannot safely keep serving. The
// network layer holds a PanicHandler reference to this method.
func (d *Dispatcher) OnPanic(cause any, stack []byte) {
	d.WriteCrashReport(cause, stack)
	os.Exit(crashReportExitCode)
}

// WriteCrashReport formats a bug report for a panic and writes it to the log
// sink. It does nothing when crash-log-enabled is off. The memory section is
// included only when crash-memlog-enabled is on. The report goes to the log
// file, or to stderr when no log file is configured, and is also mirrored to
// stderr when the log file is elsewhere so an operator watching the console
// still sees the crash.
func (d *Dispatcher) WriteCrashReport(cause any, stack []byte) {
	if !d.confBool("crash-log-enabled", true) {
		return
	}
	report := d.formatCrashReport(cause, stack, d.confBool("crash-memlog-enabled", true), time.Now())

	d.log.mu.Lock()
	out := d.log.out
	toFile := d.log.file != nil
	d.log.mu.Unlock()

	if out != nil {
		_, _ = io.WriteString(out, report)
	}
	// When the log lives in a file, the operator at the console saw nothing, so
	// echo the report to stderr as well.
	if toFile {
		_, _ = io.WriteString(os.Stderr, report)
	}
}

// formatCrashReport builds the report text. It is split out from WriteCrashReport
// so a test can check the content without writing to a real sink.
func (d *Dispatcher) formatCrashReport(cause any, stack []byte, withMem bool, now time.Time) string {
	var b strings.Builder
	role := "master"
	if !d.roleMaster.Load() {
		role = "slave"
	}
	ts := now.Format("02 Jan 2006 15:04:05.000")

	b.WriteString("\n=== AKI BUG REPORT START: Cut & paste starting from here ===\n")
	fmt.Fprintf(&b, "%d:%s %s # aki %s crashed by signal: panic\n", os.Getpid(), role, ts, d.cfg.Version)
	fmt.Fprintf(&b, "%d:%s %s # Crash report follows. Please open an issue with the text below.\n", os.Getpid(), role, ts)

	b.WriteString("\n------ PANIC ------\n")
	fmt.Fprintf(&b, "panic: %v\n", cause)

	b.WriteString("\n------ RUNTIME ------\n")
	fmt.Fprintf(&b, "aki_version:%s\n", d.cfg.Version)
	fmt.Fprintf(&b, "go_version:%s\n", runtime.Version())
	fmt.Fprintf(&b, "os:%s\n", runtime.GOOS)
	fmt.Fprintf(&b, "arch:%s\n", runtime.GOARCH)
	fmt.Fprintf(&b, "uptime_in_seconds:%d\n", int64(now.Sub(d.startTime).Seconds()))

	if withMem {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		b.WriteString("\n------ MEMORY ------\n")
		fmt.Fprintf(&b, "goroutines:%d\n", runtime.NumGoroutine())
		fmt.Fprintf(&b, "alloc_bytes:%d\n", m.Alloc)
		fmt.Fprintf(&b, "sys_bytes:%d\n", m.Sys)
		fmt.Fprintf(&b, "heap_inuse_bytes:%d\n", m.HeapInuse)
		fmt.Fprintf(&b, "heap_objects:%d\n", m.HeapObjects)
		fmt.Fprintf(&b, "stack_inuse_bytes:%d\n", m.StackInuse)
		fmt.Fprintf(&b, "num_gc:%d\n", m.NumGC)
	}

	b.WriteString("\n------ STACK TRACE ------\n")
	b.Write(stack)
	if len(stack) == 0 || stack[len(stack)-1] != '\n' {
		b.WriteByte('\n')
	}

	b.WriteString("\n=== AKI BUG REPORT END. Make sure to include from START to END. ===\n")
	return b.String()
}
