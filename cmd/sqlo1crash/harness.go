package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// config is one crash-loop run. bin plus args launch the server; the
// harness appends -addr 127.0.0.1:0 and reads the bound address back from
// the listen line, so a thousand iterations never fight over a port.
type config struct {
	bin        string
	args       []string
	env        []string // extra environment, nil means inherit only
	iterations int
	workers    int
	keys       int // keys per worker
	killMin    time.Duration
	killMax    time.Duration
	durable    bool
	seed       int64
}

// iterationResult is the verdict tally for one kill cycle plus the two
// checksums and the measured recovery time.
type iterationResult struct {
	ops            int
	killAfter      time.Duration
	recovery       time.Duration
	matched        int
	pendingApplied int
	lost           int
	corrupt        int
	oracleDigest   string
	observedDigest string
	corruptKeys    []string
}

// pass applies the pluggable criterion: corruption is fatal always, lost
// acked writes are fatal only when the store claims durability. That is
// what lets the same harness gate the S0 memory placeholder today and the
// A2 SQLite store later without editing the verifier.
func (r iterationResult) pass(durable bool) bool {
	if r.corrupt > 0 {
		return false
	}
	if durable && r.lost > 0 {
		return false
	}
	return true
}

// serverProc is one launched server: the process and the address it
// reported on its listen line.
type serverProc struct {
	cmd  *exec.Cmd
	addr string
}

func startServer(cfg config) (*serverProc, error) {
	args := append(append([]string{}, cfg.args...), "-addr", "127.0.0.1:0")
	cmd := exec.Command(cfg.bin, args...)
	if cfg.env != nil {
		cmd.Env = cfg.env
	}
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	addrCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if _, after, ok := strings.Cut(sc.Text(), "listening on "); ok {
				addrCh <- strings.TrimSpace(after)
				// Keep draining so the child can never block on a full
				// stdout pipe.
				for sc.Scan() {
				}
				return
			}
		}
		errCh <- fmt.Errorf("server exited before printing a listen line")
	}()

	select {
	case addr := <-addrCh:
		return &serverProc{cmd: cmd, addr: addr}, nil
	case err := <-errCh:
		cmd.Wait()
		return nil, err
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		return nil, fmt.Errorf("server printed no listen line within 10s")
	}
}

// kill is the whole point: SIGKILL, no shutdown path, no flush.
func (p *serverProc) kill() {
	p.cmd.Process.Kill()
	p.cmd.Wait()
}

// waitReady dials and pings until the restarted server answers, bounded so
// a hung recovery fails the iteration instead of the harness.
func waitReady(addr string) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		rc, err := dialRESP(addr)
		if err == nil {
			rep, err := rc.do(opDeadline, []byte("PING"))
			rc.close()
			if err == nil && rep.kind == '+' && rep.s == "PONG" {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("server at %s not answering PING within 30s of restart", addr)
}

// runIteration is one kill cycle: start, load, kill -9 at a random point
// inside the window, restart, verify every key against the shadow oracle.
func runIteration(cfg config, iter int) (iterationResult, error) {
	var res iterationResult
	rng := rand.New(rand.NewSource(cfg.seed + int64(iter)))

	srv, err := startServer(cfg)
	if err != nil {
		return res, fmt.Errorf("iteration %d start: %w", iter, err)
	}

	workers := make([]*worker, cfg.workers)
	conns := make([]*respConn, cfg.workers)
	for i := range workers {
		workers[i] = newWorker(i, cfg.keys, cfg.seed+int64(iter*cfg.workers+i))
		conns[i], err = dialRESP(srv.addr)
		if err != nil {
			srv.kill()
			return res, fmt.Errorf("iteration %d dial: %w", iter, err)
		}
	}

	errs := make([]error, cfg.workers)
	var wg sync.WaitGroup
	for i, w := range workers {
		wg.Go(func() {
			errs[i] = w.run(conns[i])
		})
	}

	res.killAfter = cfg.killMin + time.Duration(rng.Int63n(int64(cfg.killMax-cfg.killMin)+1))
	time.Sleep(res.killAfter)
	srv.kill()
	wg.Wait()
	for _, c := range conns {
		c.close()
	}
	for _, e := range errs {
		if e != nil {
			return res, fmt.Errorf("iteration %d: oracle mismatch while the server was alive: %w", iter, e)
		}
	}
	for _, w := range workers {
		res.ops += w.ops
	}

	// Restart and time the recovery from process launch to the first PING.
	// The S0 store is memory only, so this proves the loop mechanics; a
	// store with a data dir passes it through cfg.args and this same clock
	// becomes the WAL-tail-linearity measurement.
	t0 := time.Now()
	srv2, err := startServer(cfg)
	if err != nil {
		return res, fmt.Errorf("iteration %d restart: %w", iter, err)
	}
	defer srv2.kill()
	if err := waitReady(srv2.addr); err != nil {
		return res, fmt.Errorf("iteration %d: %w", iter, err)
	}
	res.recovery = time.Since(t0)

	rc, err := dialRESP(srv2.addr)
	if err != nil {
		return res, fmt.Errorf("iteration %d verify dial: %w", iter, err)
	}
	defer rc.close()
	if err := verifyKeyspace(rc, workers, &res); err != nil {
		return res, fmt.Errorf("iteration %d verify: %w", iter, err)
	}
	return res, nil
}

// verifyKeyspace walks every oracle key, classifies the observed state,
// and computes the two checksums. The oracle digest is what the server
// must hold given the acked history with each in-flight op resolved to
// whichever legal outcome was observed; the observed digest is what the
// server actually holds. On a durable store they must be equal; the diff
// is the corrupt plus lost key sets, and corrupt keys are named for the
// failure report.
func verifyKeyspace(rc *respConn, workers []*worker, res *iterationResult) error {
	oracle := map[string][]byte{}
	observed := map[string][]byte{}
	for _, w := range workers {
		for i := range w.keys {
			key := string(w.keys[i])
			st := w.states[i]
			rep, err := rc.do(opDeadline, []byte("GET"), w.keys[i])
			if err != nil {
				return fmt.Errorf("GET %s: %w", key, err)
			}
			if rep.kind != '$' {
				return fmt.Errorf("GET %s: reply %+v, want a bulk string", key, rep)
			}
			found := !rep.null
			if found {
				observed[key] = rep.b
			}
			switch classify(st, rep.b, found) {
			case verdictMatch:
				res.matched++
				if st.acked != nil {
					oracle[key] = st.acked
				}
			case verdictPendingApplied:
				res.pendingApplied++
				if found {
					oracle[key] = rep.b
				}
			case verdictLost:
				res.lost++
				oracle[key] = st.acked
			case verdictCorrupt:
				res.corrupt++
				if st.acked != nil {
					oracle[key] = st.acked
				}
				res.corruptKeys = append(res.corruptKeys, key)
			}
		}
	}
	res.oracleDigest = digest(oracle)
	res.observedDigest = digest(observed)
	return nil
}
