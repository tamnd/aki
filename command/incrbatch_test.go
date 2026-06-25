package command

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// startIncrBatchServer brings up a real TCP server whose engine has the shard
// workers running, so increments from an online connection take the batched
// pipeline hand-off rather than the inline path (an offline connection never
// batches). The default everysec policy makes the engine deferred, and AOF is
// left off so the test exercises the data path without the append log.
func startIncrBatchServer(t *testing.T) (*Dispatcher, string) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	d := New(Config{Engine: NewEngine(ks)})
	d.engine.StartWorker()
	t.Cleanup(d.engine.StopWorker)

	srv := networking.New(networking.Config{Addr: "127.0.0.1:0"}, d)
	d.SetServer(srv)
	go func() { _ = srv.ListenAndServe(networking.Config{Addr: "127.0.0.1:0"}) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	return d, srv.Addr().String()
}

// encodeCmd renders one command as a RESP array.
func encodeCmd(args ...string) string {
	out := "*" + strconv.Itoa(len(args)) + "\r\n"
	for _, a := range args {
		out += "$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n"
	}
	return out
}

// readReply reads one RESP reply, returning a tagged string: "int:N", "str:...",
// "nil", "err:...", or "ok:..." for a simple string. It supports just the reply
// shapes the increment tests produce.
func readTaggedReply(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if len(line) < 3 {
		t.Fatalf("short reply line %q", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case ':':
		return "int:" + body
	case '+':
		return "ok:" + body
	case '-':
		return "err:" + body
	case '$':
		n, _ := strconv.Atoi(body)
		if n < 0 {
			return "nil"
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			t.Fatalf("read bulk body: %v", err)
		}
		return "str:" + string(buf[:n])
	default:
		t.Fatalf("unexpected reply type %q", line)
		return ""
	}
}

// A pipeline of INCRs on one key over an online connection runs through the
// batched hand-off and must still return 1..N in order.
func TestINCRBatchPipelineOneKey(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	const n = 200
	var pipe string
	for range n {
		pipe += encodeCmd("INCR", "k")
	}
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		got := readTaggedReply(t, r)
		want := "int:" + strconv.Itoa(i)
		if got != want {
			t.Fatalf("reply %d = %q want %q", i, got, want)
		}
	}
}

// A pipeline of increments over distinct keys spread across all shards must
// return the right per-key value, exercising the per-shard sub-batch grouping.
func TestINCRBatchDistinctKeys(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	const n = 256
	var pipe string
	for i := range n {
		pipe += encodeCmd("INCRBY", fmt.Sprintf("key:%d", i), "7")
	}
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		got := readTaggedReply(t, r)
		if got != "int:7" {
			t.Fatalf("reply %d = %q want int:7", i, got)
		}
	}
}

// A mixed pipeline must keep reply order and read-after-write within the batch:
// the SET is visible to the following INCR, and a GET between increments sees the
// most recent applied value, because every non-increment flushes the pending
// batch ahead of itself.
func TestINCRBatchMixedOrder(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	pipe := encodeCmd("SET", "m", "10") +
		encodeCmd("INCR", "m") +
		encodeCmd("GET", "m") +
		encodeCmd("INCRBY", "m", "5") +
		encodeCmd("DECR", "m") +
		encodeCmd("GET", "m")
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	want := []string{"ok:OK", "int:11", "str:11", "int:16", "int:15", "str:15"}
	for i, w := range want {
		got := readTaggedReply(t, r)
		if got != w {
			t.Fatalf("reply %d = %q want %q", i, got, w)
		}
	}
}

// A wrong-type increment in the middle of a deferred batch must report its error
// at its own position and not disturb the surrounding increments.
func TestINCRBatchWrongType(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	// RPUSH makes "lst" a list; the INCR on it must be a wrong-type error while the
	// INCRs on "c" still count cleanly. RPUSH is non-deferrable, so it flushes any
	// pending batch and runs inline; "lst" then exists as a list when the deferred
	// INCR on it applies on the owner.
	pipe := encodeCmd("INCR", "c") +
		encodeCmd("RPUSH", "lst", "x") +
		encodeCmd("INCR", "lst") +
		encodeCmd("INCR", "c")
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	got := []string{readTaggedReply(t, r), readTaggedReply(t, r), readTaggedReply(t, r), readTaggedReply(t, r)}
	if got[0] != "int:1" {
		t.Fatalf("reply 0 = %q want int:1", got[0])
	}
	if got[1] != "int:1" {
		t.Fatalf("RPUSH reply = %q want int:1", got[1])
	}
	if len(got[2]) < 4 || got[2][:4] != "err:" {
		t.Fatalf("wrong-type INCR reply = %q want an error", got[2])
	}
	if got[3] != "int:2" {
		t.Fatalf("reply 3 = %q want int:2", got[3])
	}
}

// Many online connections each pipelining INCR on one shared key must observe
// exactly 1..N*M with no gaps or duplicates: the batched owner is the sole writer
// of the key's shard, so it serializes the read-modify-write the way the rmwLock
// did on the inline path. Running under -race also exercises the concurrent
// owner/reader hand-off.
func TestINCRBatchNoLostUpdateOnline(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	const (
		clients = 8
		perCli  = 300
		total   = clients * perCli
	)
	var pipe string
	for range perCli {
		pipe += encodeCmd("INCR", "shared")
	}

	results := make([][]int64, clients)
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("client %d dial: %v", id, err)
				return
			}
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			r := bufio.NewReader(conn)
			if _, err := conn.Write([]byte(pipe)); err != nil {
				t.Errorf("client %d write: %v", id, err)
				return
			}
			got := make([]int64, 0, perCli)
			for range perCli {
				rep := readTaggedReply(t, r)
				if len(rep) < 4 || rep[:4] != "int:" {
					t.Errorf("client %d got %q", id, rep)
					return
				}
				v, _ := strconv.ParseInt(rep[4:], 10, 64)
				got = append(got, v)
			}
			results[id] = got
		}(c)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	seen := make([]bool, total+1)
	for _, rs := range results {
		for _, v := range rs {
			if v < 1 || v > total {
				t.Fatalf("value %d out of range 1..%d", v, total)
			}
			if seen[v] {
				t.Fatalf("duplicate value %d: an update was lost", v)
			}
			seen[v] = true
		}
	}
	for i := 1; i <= total; i++ {
		if !seen[i] {
			t.Fatalf("missing value %d: an update was lost", i)
		}
	}
}
