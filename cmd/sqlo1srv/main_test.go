package main

import (
	"bufio"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestSeedScript is the G5 seed: build the real binary, start it with one
// command and no config, and drive the seven-command surface over a raw
// socket. If this passes, "download and run" is true for the placeholder
// build and stays pinned for every store that follows.
func TestSeedScript(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "sqlo1srv")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	cmd := exec.Command(bin, "-addr", "127.0.0.1:0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	// The server prints its bound address on the first line.
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("reading listen line: %v", err)
	}
	addr := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "sqlo1srv listening on "))

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(c)

	seed := []struct{ cmd, want string }{
		{"*1\r\n$4\r\nPING\r\n", "+PONG\r\n"},
		{"*2\r\n$4\r\nECHO\r\n$4\r\nseed\r\n", "$4\r\nseed\r\n"},
		{"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nsqlo1\r\n", "+OK\r\n"},
		{"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n", "$5\r\nsqlo1\r\n"},
		{"*3\r\n$6\r\nEXPIRE\r\n$4\r\nname\r\n$3\r\n100\r\n", ":1\r\n"},
		{"*2\r\n$3\r\nTTL\r\n$4\r\nname\r\n", ":100\r\n"},
		{"*2\r\n$3\r\nDEL\r\n$4\r\nname\r\n", ":1\r\n"},
		{"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n", "$-1\r\n"},
	}
	for _, step := range seed {
		if _, err := c.Write([]byte(step.cmd)); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(step.want))
		if _, err := readFull(r, got); err != nil {
			t.Fatalf("reading reply to %q: %v", step.cmd, err)
		}
		if string(got) != step.want {
			t.Fatalf("reply to %q = %q, want %q", step.cmd, got, step.want)
		}
	}
}

// TestFileStoreLifecycle is the single-file half of G5: start the
// binary on a fresh -store file path, write through the wire, shut
// down with SIGTERM, and start a second process on the same file. The
// value must survive, which proves create, clean shutdown (flush,
// checkpoint, close), and reopen with recovery all hang together from
// the command line.
func TestFileStoreLifecycle(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "sqlo1srv")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	data := filepath.Join(dir, "data.aki")

	run := func(steps []struct{ cmd, want string }) {
		cmd := exec.Command(bin, "-addr", "127.0.0.1:0", "-store", "file", "-path", data)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { cmd.Process.Kill(); cmd.Wait() }()

		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil {
			t.Fatalf("reading listen line: %v", err)
		}
		addr := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "sqlo1srv listening on "))

		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		r := bufio.NewReader(c)
		for _, step := range steps {
			if _, err := c.Write([]byte(step.cmd)); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(step.want))
			if _, err := readFull(r, got); err != nil {
				t.Fatalf("reading reply to %q: %v", step.cmd, err)
			}
			if string(got) != step.want {
				t.Fatalf("reply to %q = %q, want %q", step.cmd, got, step.want)
			}
		}
		c.Close()

		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Wait(); err != nil {
			t.Fatalf("clean shutdown: %v", err)
		}
	}

	run([]struct{ cmd, want string }{
		{"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nsqlo1\r\n", "+OK\r\n"},
		{"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n", "$5\r\nsqlo1\r\n"},
	})
	run([]struct{ cmd, want string }{
		{"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n", "$5\r\nsqlo1\r\n"},
	})
}

func readFull(r *bufio.Reader, p []byte) (int, error) {
	n := 0
	for n < len(p) {
		m, err := r.Read(p[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
