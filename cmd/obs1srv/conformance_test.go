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
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/obs1srv/dispatch"
)

// step is one corpus entry: a command and its rendered expected reply. A
// want beginning with "~" is a substring match, for replies that carry
// counters or version-shaped text (INFO, XINFO).
type step struct {
	cmd  []string
	want string
}

// c is shorthand for a corpus step.
func c(want string, cmd ...string) step { return step{cmd: cmd, want: want} }

// render flattens one decoded reply into the corpus's comparable form:
// status and bulk as their text, integers as digits, nil as (nil), arrays
// bracketed and space-joined. Bulk "3" and integer 3 render alike on
// purpose; reply-type exactness is the package suites' job.
func render(v any) string {
	switch x := v.(type) {
	case nil:
		return "(nil)"
	case string:
		return x
	case int64:
		return fmt.Sprintf("%d", x)
	case []any:
		parts := make([]string, len(x))
		for i := range x {
			parts[i] = render(x[i])
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return fmt.Sprintf("%v", v)
}

// respConn is a minimal RESP2 client for the corpus: array-of-bulk out,
// recursive reply in.
type respConn struct {
	c net.Conn
	r *bufio.Reader
}

func dialResp(t *testing.T, addr string) *respConn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	return &respConn{c: nc, r: bufio.NewReader(nc)}
}

func (rc *respConn) do(t *testing.T, args []string) any {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	_ = rc.c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := rc.c.Write([]byte(b.String())); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
	v, err := rc.read()
	if err != nil {
		t.Fatalf("read reply to %v: %v", args, err)
	}
	return v
}

func (rc *respConn) read() (any, error) {
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, fmt.Errorf("empty reply line")
	}
	body := line[1:]
	switch line[0] {
	case '+', '-':
		return body, nil
	case ':':
		var n int64
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return nil, fmt.Errorf("bad integer %q", body)
		}
		return n, nil
	case '$':
		var n int
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return nil, fmt.Errorf("bad bulk length %q", body)
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := ioReadFull(rc.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		var n int
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return nil, fmt.Errorf("bad array length %q", body)
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := range out {
			v, err := rc.read()
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("unknown reply type %q", line)
}

// ioReadFull avoids importing io for one call.
func ioReadFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
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
			rc := dialResp(t, addr)
			seen := map[string]bool{}
			for i, s := range hotCorpus {
				got := render(rc.do(t, s.cmd))
				if strings.HasPrefix(s.want, "~") {
					if !strings.Contains(got, s.want[1:]) {
						t.Fatalf("step %d %v: got %q, want it to contain %q", i, s.cmd, got, s.want[1:])
					}
				} else if got != s.want {
					t.Fatalf("step %d %v: got %q, want %q", i, s.cmd, got, s.want)
				}
				seen[strings.ToUpper(s.cmd[0])] = true
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
