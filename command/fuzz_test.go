package command

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newFuzzDispatcher builds a dispatcher over an in-memory keyspace for fuzzing.
func newFuzzDispatcher(tb testing.TB) *Dispatcher {
	tb.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		tb.Fatalf("create pager: %v", err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		tb.Fatalf("open keyspace: %v", err)
	}
	return New(Config{Engine: NewEngine(ks)})
}

// FuzzDispatch sends arbitrary argument vectors to the dispatcher. No input may
// panic the dispatcher, and the server must still answer PING afterward, which
// catches a command that wedges shared state (doc 23 §7.4).
func FuzzDispatch(f *testing.F) {
	seeds := [][]byte{
		[]byte("GET\x00k"),
		[]byte("SET\x00k\x00v"),
		[]byte("INCR\x00n"),
		[]byte("LPUSH\x00l\x00a\x00b"),
		[]byte("EXPIRE\x00k\x00100"),
		[]byte("HSET\x00h\x00f\x00v"),
		[]byte("ZADD\x00z\x001\x00m"),
		[]byte("DEBUG\x00JMAP"),
		[]byte(""),
		[]byte("\x00\x00\x00"),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		d := newFuzzDispatcher(t)
		// Split the input into argv on NUL, capping the count so one input can not
		// allocate without bound.
		fields := bytes.SplitN(data, []byte{0}, 33)
		argv := make([][]byte, 0, len(fields))
		for _, fld := range fields {
			if len(fld) == 0 {
				continue
			}
			argv = append(argv, fld)
		}
		if len(argv) == 0 {
			return
		}
		d.Handle(networking.NewOfflineConn(), argv)

		// The dispatcher must still be alive: a fresh PING should reply +PONG.
		conn := networking.NewOfflineConn()
		d.Handle(conn, [][]byte{[]byte("PING")})
		if out := conn.OutBytes(); !bytes.Contains(out, []byte("PONG")) {
			t.Fatalf("PING after %q did not reply PONG: %q", data, out)
		}
	})
}
