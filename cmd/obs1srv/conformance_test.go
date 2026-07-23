// The hot conformance corpus (spec 2064/obs1 doc 10, suite conformance,
// T-I1 arm): every registered command runs at least once against the real
// obs1srv binary over real TCP, persistence off, and the reply must match
// exactly. Deep per-command semantics stay pinned by the type-package and
// drivers suites; this corpus proves the end-to-end wiring from a parsed
// flag through main, the net driver, dispatch, the shard runtime, and back
// out the reply writer. The corpus checks itself against dispatch.Commands,
// so a verb added to the table without an entry here fails the suite.
package main

import (
	"bufio"
	"net"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/obs1srv/conformance"
	"github.com/tamnd/aki/obs1srv/dispatch"
)

// doStep runs one corpus command, failing the test on transport errors.
func doStep(t *testing.T, rc *conformance.Conn, cmd []string) string {
	t.Helper()
	v, err := rc.Do(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return conformance.Render(v)
}

// buildServer compiles the real binary once per test run.
func buildServer(t *testing.T) string {
	t.Helper()
	bin := t.TempDir() + "/obs1srv"
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

var servingRE = regexp.MustCompile(`serving on (\S+) with`)

// startServer runs the binary on an ephemeral port and reads the served
// address off its stderr banner.
func startServer(t *testing.T, bin, driver string) string {
	t.Helper()
	cmd := exec.Command(bin, "-addr", "127.0.0.1:0", "-net", driver, "-shards", "4")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	sc := bufio.NewScanner(stderr)
	deadline := time.Now().Add(10 * time.Second)
	for sc.Scan() {
		if m := servingRE.FindStringSubmatch(sc.Text()); m != nil {
			return m[1]
		}
		if time.Now().After(deadline) {
			break
		}
	}
	t.Fatalf("no serving banner from %s -net %s", bin, driver)
	return ""
}

// TestConformanceHot runs the per-command corpus against the built binary,
// once per net driver this platform offers. The uring arm serves through
// the probed fallback where the kernel lacks io_uring; the replies must be
// identical either way, which is the point.
func TestConformanceHot(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the real binary")
	}
	bin := buildServer(t)
	drivers := []string{"goroutine"}
	if runtime.GOOS == "linux" {
		drivers = append(drivers, "reactor", "uring")
	}
	for _, d := range drivers {
		t.Run(d, func(t *testing.T) {
			addr := startServer(t, bin, d)
			nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatalf("dial %s: %v", addr, err)
			}
			t.Cleanup(func() { _ = nc.Close() })
			rc := conformance.NewConn(nc)
			seen := map[string]bool{}
			for i, s := range conformance.Hot {
				if msg := conformance.Check(s, doStep(t, rc, s.Cmd)); msg != "" {
					t.Fatalf("step %d %s", i, msg)
				}
				seen[strings.ToUpper(s.Cmd[0])] = true
			}
			var missing []string
			for _, name := range dispatch.Commands() {
				if !seen[name] {
					missing = append(missing, name)
				}
			}
			if len(missing) > 0 {
				t.Fatalf("registered commands with no corpus entry: %v", missing)
			}
		})
	}
}
