package sqlo1_test

// The hard-evict opt-in end to end over the real Track B store: with a
// disk cap tight enough that the free-extent gauge reads positive, an
// armed server deletes policy-ranked victims through the command path,
// and an unarmed server under the same pressure deletes nothing
// (E-I6). Doc 11 section 5.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

func expectReply(t *testing.T, r *bufio.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("reading reply (want %q): %v", want, err)
	}
	if string(got) != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

// startHardEvictServer serves over a fresh sqlo1b store whose byte cap
// sits under the pressure reserve, so Pressure().Extent is positive
// from the first write.
func startHardEvictServer(t *testing.T, armed bool) (*sqlo1.Server, *sqlo1b.Store, net.Conn, *bufio.Reader) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hardevict.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// 16 MiB sits between the shed floor (one drain plus checkpoint
	// slack, 12 extents) and the pressure reserve (twice that), so the
	// free-extent gauge reads positive while writes still land.
	db.SetMaxBytes(16 << 20)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	srv, err := sqlo1.NewServer(db)
	if err != nil {
		t.Fatal(err)
	}
	srv.SetPolicy(sqlo1.PolicyAllkeysRandom)
	if armed {
		srv.EnableHardEvict()
	}
	go srv.Serve(l)

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(30 * time.Second))
	return srv, db, c, bufio.NewReader(c)
}

func hardEvictScenario(t *testing.T, armed bool) (gone int) {
	t.Helper()
	srv, db, c, r := startHardEvictServer(t, armed)

	const keys = 32
	for i := range keys {
		cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$6\r\nhk-%03d\r\n$5\r\nvalue\r\n", i)
		if _, err := c.Write([]byte(cmd)); err != nil {
			t.Fatal(err)
		}
		expectReply(t, r, "+OK\r\n")
	}
	// Victims must be resident: hard evict never touches a dirty record.
	if err := srv.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p := db.Pressure(); p.Extent <= 0 {
		t.Fatalf("free-extent gauge %f, the scenario needs positive pressure", p.Extent)
	}

	srv.HardEvictStepForTest(context.Background())

	for i := range keys {
		cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$6\r\nhk-%03d\r\n", i)
		if _, err := c.Write([]byte(cmd)); err != nil {
			t.Fatal(err)
		}
		head, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if head == "$-1\r\n" {
			gone++
			continue
		}
		if head != "$5\r\n" {
			t.Fatalf("GET reply header %q", head)
		}
		expectReply(t, r, "value\r\n")
	}
	// The server's own maintenance ticker may run more steps behind the
	// explicit one, so the counter can only be at least what the GET
	// sweep saw missing.
	if got := srv.HardEvictedForTest(); got < int64(gone) {
		t.Fatalf("hard_evicted counter %d, but %d keys are gone", got, gone)
	}
	return gone
}

func TestHardEvictArmedDeletes(t *testing.T) {
	if gone := hardEvictScenario(t, true); gone == 0 {
		t.Fatal("armed hard evict under extent pressure deleted nothing")
	}
}

func TestHardEvictUnarmedNeverDeletes(t *testing.T) {
	if gone := hardEvictScenario(t, false); gone != 0 {
		t.Fatalf("unarmed server deleted %d keys under the same pressure", gone)
	}
}
