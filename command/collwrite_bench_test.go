package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
)

// pregrowColl pushes n elements onto key so the list leaves the listpack/blob
// form and becomes a btree-backed (coll) collection, the steady state the
// benchmark below measures.
func pregrowColl(b *testing.B, conn net.Conn, br *bufio.Reader, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		key := "{g6}:bench:list"
		val := fmt.Sprintf("element-%08d", i)
		cmd := fmt.Sprintf("*3\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		if _, err := conn.Write([]byte(cmd)); err != nil {
			b.Fatal(err)
		}
		if _, err := br.ReadString('\n'); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCollRPushSteady measures one RPUSH per op onto a list already in the
// btree-backed form, the path CollUpdateRouted shortened by dropping the
// redundant metadata Peek. Sequential, single connection, so ns/op is the per-op
// CPU on the write path.
func BenchmarkCollRPushSteady(b *testing.B) {
	addr := startG5Server(b)
	conn, br := dialG5(b, addr)
	defer conn.Close()
	pregrowColl(b, conn, br, 600)

	key := "{g6}:bench:list"
	val := "steady-value-payload"
	cmd := fmt.Sprintf("*3\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
	b.ResetTimer()
	for b.Loop() {
		if _, err := conn.Write([]byte(cmd)); err != nil {
			b.Fatal(err)
		}
		reply, err := br.ReadString('\n')
		if err != nil {
			b.Fatal(err)
		}
		if !strings.HasPrefix(reply, ":") {
			b.Fatalf("RPUSH reply = %q want integer", reply)
		}
	}
}

// BenchmarkCollHSetSteady measures one HSET per op onto a hash already in the
// btree-backed form, the same shortened path for the hash add.
func BenchmarkCollHSetSteady(b *testing.B) {
	addr := startG5Server(b)
	conn, br := dialG5(b, addr)
	defer conn.Close()
	key := "{g6}:bench:hash"
	for i := 0; i < 600; i++ {
		f := fmt.Sprintf("f%08d", i)
		cmd := fmt.Sprintf("*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$3\r\nval\r\n", len(key), key, len(f), f)
		if _, err := conn.Write([]byte(cmd)); err != nil {
			b.Fatal(err)
		}
		if _, err := br.ReadString('\n'); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	i := 0
	for b.Loop() {
		f := fmt.Sprintf("f%08d", i%600)
		cmd := fmt.Sprintf("*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$4\r\nval2\r\n", len(key), key, len(f), f)
		if _, err := conn.Write([]byte(cmd)); err != nil {
			b.Fatal(err)
		}
		if _, err := br.ReadString('\n'); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
