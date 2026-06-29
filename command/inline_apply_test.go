package command

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
)

// TestInlineApplyConcurrentSingleKey hammers one collection key from many
// connections at once, the shape the redis-benchmark collection workloads drive
// (one fixed key, fifty clients). It exercises the inline-apply path in
// updateShard: with every writer serializing on the same keyspace shard mutex the
// hand-off queue stays empty, so each push applies on its own connection goroutine
// instead of routing through the shard worker. The test asserts no update is lost:
// after G goroutines push N elements each, the list holds exactly G*N elements and
// the hash holds exactly the expected distinct fields. A lost update or a torn
// read-modify-write under the inline path would drop the count.
func TestInlineApplyConcurrentSingleKey(t *testing.T) {
	_, addr := startIncrBatchServer(t)

	const (
		goroutines = 16
		perG       = 400
	)

	t.Run("rpush", func(t *testing.T) {
		const key = "inline:list"
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				conn, br := dialInline(t, addr)
				defer conn.Close()
				for i := 0; i < perG; i++ {
					member := fmt.Sprintf("g%02d-%05d", g, i)
					writeRaw(t, conn, encodeCmd("RPUSH", key, member))
					if r := readTaggedReply(t, br); !isIntReply(r) {
						t.Errorf("RPUSH reply = %q, want integer", r)
						return
					}
				}
			}(g)
		}
		wg.Wait()

		conn, br := dialInline(t, addr)
		defer conn.Close()
		writeRaw(t, conn, encodeCmd("LLEN", key))
		got := readTaggedReply(t, br)
		want := "int:" + strconv.Itoa(goroutines*perG)
		if got != want {
			t.Fatalf("LLEN after concurrent RPUSH = %s, want %s (lost updates)", got, want)
		}
	})

	t.Run("hset_distinct_fields", func(t *testing.T) {
		const key = "inline:hash"
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				conn, br := dialInline(t, addr)
				defer conn.Close()
				for i := 0; i < perG; i++ {
					field := fmt.Sprintf("f-%02d-%05d", g, i)
					writeRaw(t, conn, encodeCmd("HSET", key, field, "v"))
					if r := readTaggedReply(t, br); !isIntReply(r) {
						t.Errorf("HSET reply = %q, want integer", r)
						return
					}
				}
			}(g)
		}
		wg.Wait()

		conn, br := dialInline(t, addr)
		defer conn.Close()
		writeRaw(t, conn, encodeCmd("HLEN", key))
		got := readTaggedReply(t, br)
		want := "int:" + strconv.Itoa(goroutines*perG)
		if got != want {
			t.Fatalf("HLEN after concurrent HSET = %s, want %s (lost fields)", got, want)
		}
	})

	t.Run("sadd_distinct_members", func(t *testing.T) {
		const key = "inline:set"
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				conn, br := dialInline(t, addr)
				defer conn.Close()
				for i := 0; i < perG; i++ {
					member := fmt.Sprintf("m-%02d-%05d", g, i)
					writeRaw(t, conn, encodeCmd("SADD", key, member))
					if r := readTaggedReply(t, br); !isIntReply(r) {
						t.Errorf("SADD reply = %q, want integer", r)
						return
					}
				}
			}(g)
		}
		wg.Wait()

		conn, br := dialInline(t, addr)
		defer conn.Close()
		writeRaw(t, conn, encodeCmd("SCARD", key))
		got := readTaggedReply(t, br)
		want := "int:" + strconv.Itoa(goroutines*perG)
		if got != want {
			t.Fatalf("SCARD after concurrent SADD = %s, want %s (lost members)", got, want)
		}
	})
}

// dialInline opens one TCP connection for a testing.T test.
func dialInline(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, bufio.NewReader(conn)
}

// writeRaw writes a fully-encoded RESP command verbatim, with no extra framing.
func writeRaw(t *testing.T, conn net.Conn, cmd string) {
	t.Helper()
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// isIntReply reports whether a readTaggedReply value is an integer reply.
func isIntReply(r string) bool {
	return len(r) > 4 && r[:4] == "int:"
}
