package command

import (
	"os"
	"strings"
	"testing"
)

// TestAOFTimestampAnnotated checks that aof-timestamp-enabled writes a #TS comment
// line ahead of the command in the incr file.
func TestAOFTimestampAnnotated(t *testing.T) {
	d := aofDispatcherForFsync(t)
	if err := d.SetConfig("aof-timestamp-enabled", "yes"); err != nil {
		t.Fatalf("set aof-timestamp-enabled: %v", err)
	}

	writeAOF(d)

	d.aof.mu.Lock()
	path := d.aof.incrPath
	d.aof.mu.Unlock()
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read incr file: %v", err)
	}
	content := string(blob)
	if !strings.Contains(content, "#TS:") {
		t.Fatalf("incr file has no timestamp annotation: %q", content)
	}
	if !strings.Contains(content, "SET") {
		t.Fatalf("incr file is missing the command: %q", content)
	}
	if strings.Index(content, "#TS:") > strings.Index(content, "SET") {
		t.Fatalf("annotation should come before the command: %q", content)
	}
}

// TestAOFTimestampOffByDefault checks the annotation is absent when the directive
// is left at its default.
func TestAOFTimestampOffByDefault(t *testing.T) {
	d := aofDispatcherForFsync(t)

	writeAOF(d)

	d.aof.mu.Lock()
	path := d.aof.incrPath
	d.aof.mu.Unlock()
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read incr file: %v", err)
	}
	if strings.Contains(string(blob), "#TS:") {
		t.Fatalf("annotation present with the directive off: %q", string(blob))
	}
}

// TestAOFReplaySkipsAnnotations checks the loader skips a #TS line and still
// applies the command after it.
func TestAOFReplaySkipsAnnotations(t *testing.T) {
	d := newMetricsDispatcher(t)
	blob := "#TS:1700000000123\r\n" + "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"

	if err := d.replayAOF(replayCtx(d), []byte(blob)); err != nil {
		t.Fatalf("replayAOF with annotation: %v", err)
	}
	if got := runReply(d, "GET", "k"); got != "$1\r\nv\r\n" {
		t.Fatalf("GET k after load = %q want the annotated command applied", got)
	}
}

// TestAOFReplayTruncatedAnnotation checks a #TS line with no terminating newline
// follows the aof-load-truncated rule: tolerated by default, an error when off.
func TestAOFReplayTruncatedAnnotation(t *testing.T) {
	blob := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n" + "#TS:1700000000"

	d := newMetricsDispatcher(t)
	if err := d.replayAOF(replayCtx(d), []byte(blob)); err != nil {
		t.Fatalf("default policy should tolerate a truncated annotation: %v", err)
	}
	if got := runReply(d, "GET", "k"); got != "$1\r\nv\r\n" {
		t.Fatalf("GET k = %q want the complete command before the annotation", got)
	}

	strict := newMetricsDispatcher(t)
	if err := strict.SetConfig("aof-load-truncated", "no"); err != nil {
		t.Fatalf("set aof-load-truncated: %v", err)
	}
	if err := strict.replayAOF(replayCtx(strict), []byte(blob)); err == nil {
		t.Fatal("strict policy should reject a truncated annotation")
	}
}
