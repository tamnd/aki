package command

import (
	"strings"
	"testing"
	"time"
)

// TestCrashReportFormat checks the report carries the panic cause, the runtime
// section, the stack trace, and the bug-report markers.
func TestCrashReportFormat(t *testing.T) {
	d := newMetricsDispatcher(t)
	stack := []byte("goroutine 1 [running]:\nmain.boom()\n")

	report := d.formatCrashReport("boom", stack, true, time.Now())

	for _, want := range []string{
		"=== AKI BUG REPORT START",
		"=== AKI BUG REPORT END",
		"------ PANIC ------",
		"panic: boom",
		"------ RUNTIME ------",
		"go_version:",
		"------ MEMORY ------",
		"goroutines:",
		"------ STACK TRACE ------",
		"main.boom()",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n%s", want, report)
		}
	}
}

// TestCrashReportNoMemSection checks crash-memlog-enabled no drops the memory
// section while the rest of the report still renders.
func TestCrashReportNoMemSection(t *testing.T) {
	d := newMetricsDispatcher(t)

	report := d.formatCrashReport("boom", []byte("stack\n"), false, time.Now())

	if strings.Contains(report, "------ MEMORY ------") {
		t.Fatalf("memory section should be absent:\n%s", report)
	}
	if !strings.Contains(report, "------ PANIC ------") {
		t.Fatalf("panic section should still be present:\n%s", report)
	}
}

// TestWriteCrashReportEnabled checks the report lands on the log sink when
// crash-log-enabled is on.
func TestWriteCrashReportEnabled(t *testing.T) {
	d := newMetricsDispatcher(t)
	buf := captureLog(d)

	d.WriteCrashReport("kaboom", []byte("stack\n"))

	if !strings.Contains(buf.String(), "panic: kaboom") {
		t.Fatalf("crash report not written to log: %q", buf.String())
	}
}

// TestWriteCrashReportDisabled checks crash-log-enabled no suppresses the report.
func TestWriteCrashReportDisabled(t *testing.T) {
	d := newMetricsDispatcher(t)
	buf := captureLog(d)
	d.conf.set("crash-log-enabled", "no")

	d.WriteCrashReport("kaboom", []byte("stack\n"))

	if buf.Len() != 0 {
		t.Fatalf("crash report should be suppressed: %q", buf.String())
	}
}
