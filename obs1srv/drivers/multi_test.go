package drivers

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
)

// TestMultiKeyFanOut walks the tier-one multi-key surface over a socket on a
// two-shard runtime: MSET and MGET in argument order across shards, counts
// summed for DEL, UNLINK, and EXISTS.
func TestMultiKeyFanOut(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSET", "a", "1", "b", "2", "c", "3", "d", "4")
	expect(t, br, "+OK\r\n")

	// MGET keeps argument order, absent keys answer nil, duplicates repeat.
	send(t, nc, "MGET", "c", "missing", "a", "d", "a")
	expect(t, br, "*5\r\n$1\r\n3\r\n$-1\r\n$1\r\n1\r\n$1\r\n4\r\n$1\r\n1\r\n")

	// EXISTS counts every occurrence, duplicates included.
	send(t, nc, "EXISTS", "a", "missing", "b", "a")
	expect(t, br, ":3\r\n")

	// DEL sums per-shard counts; a second pass finds nothing.
	send(t, nc, "DEL", "a", "b", "missing")
	expect(t, br, ":2\r\n")
	send(t, nc, "DEL", "a", "b")
	expect(t, br, ":0\r\n")

	// UNLINK is DEL under the tier-one contract.
	send(t, nc, "UNLINK", "c", "d", "missing")
	expect(t, br, ":2\r\n")
	send(t, nc, "MGET", "a", "b", "c", "d")
	expect(t, br, "*4\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n")

	// Single-key MGET still answers the array form.
	send(t, nc, "SET", "solo", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "MGET", "solo")
	expect(t, br, "*1\r\n$1\r\nv\r\n")

	// MSET arity: pairs only.
	send(t, nc, "MSET", "k1", "v1", "k2")
	expect(t, br, "-ERR wrong number of arguments for 'mset' command\r\n")
}

// TestFanOutPipelined interleaves fan-outs with single-key commands in one
// write and expects every reply in request order: the gather must hold each
// fan-out's slot in the reorder ring while later single-key replies park
// behind it.
func TestFanOutPipelined(t *testing.T) {
	_, nc, br := startServer(t)

	req := cmd("MSET", "p", "1", "q", "2") +
		cmd("SET", "r", "3") +
		cmd("MGET", "p", "q", "r") +
		cmd("GET", "p") +
		cmd("DEL", "p", "q", "r") +
		cmd("GET", "p") +
		cmd("EXISTS", "p", "q", "r")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n"+
		"+OK\r\n"+
		"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"+
		"$1\r\n1\r\n"+
		":3\r\n"+
		"$-1\r\n"+
		":0\r\n")
}

// TestFanOutManyKeys pushes one MSET and one MGET past the per-sub-command
// key cap, so each shard's slice splits into several sub-commands and the
// gather merges more partials than shards.
func TestFanOutManyKeys(t *testing.T) {
	_, nc, br := startServer(t)

	const n = 500
	args := make([]string, 0, 1+2*n)
	args = append(args, "MSET")
	for i := 0; i < n; i++ {
		args = append(args, "key:"+strconv.Itoa(i), "val:"+strconv.Itoa(i))
	}
	send(t, nc, args...)
	expect(t, br, "+OK\r\n")

	get := make([]string, 0, 1+n)
	get = append(get, "MGET")
	want := fmt.Sprintf("*%d\r\n", n)
	for i := 0; i < n; i++ {
		get = append(get, "key:"+strconv.Itoa(i))
		v := "val:" + strconv.Itoa(i)
		want += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
	}
	send(t, nc, get...)
	expect(t, br, want)

	del := append([]string{"DEL"}, get[1:]...)
	send(t, nc, del...)
	expect(t, br, fmt.Sprintf(":%d\r\n", n))
}

// TestFanOutOrderingUnderTraffic runs pipelined fan-outs on several
// connections while other connections hammer single-key traffic, asserting
// every connection sees its replies in exact request order. With -race this
// is the fan-out ordering proof against concurrent point traffic.
func TestFanOutOrderingUnderTraffic(t *testing.T) {
	srv, seed, sbr := startServer(t)

	send(t, seed, "MSET", "s0", "x", "s1", "y", "s2", "z")
	expect(t, sbr, "+OK\r\n")

	var wg sync.WaitGroup
	errs := make(chan error, 8)

	// Point-traffic connections: pipelined SET/GET on their own keys.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			nc, err := net.Dial("tcp", srv.Addr().String())
			if err != nil {
				errs <- err
				return
			}
			defer nc.Close()
			br := bufio.NewReader(nc)
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("pt:%d:%d", w, i)
				req := cmd("SET", k, "v") + cmd("GET", k) + cmd("DEL", k)
				if _, err := nc.Write([]byte(req)); err != nil {
					errs <- err
					return
				}
				if err := expectErr(br, "+OK\r\n$1\r\nv\r\n:1\r\n"); err != nil {
					errs <- fmt.Errorf("point conn %d iter %d: %w", w, i, err)
					return
				}
			}
		}(w)
	}

	// Fan-out connections: pipelines that interleave fan-outs with point ops
	// whose replies must never overtake or trail out of order.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			nc, err := net.Dial("tcp", srv.Addr().String())
			if err != nil {
				errs <- err
				return
			}
			defer nc.Close()
			br := bufio.NewReader(nc)
			for i := 0; i < 200; i++ {
				a := fmt.Sprintf("fk:%d:%d:a", w, i)
				b := fmt.Sprintf("fk:%d:%d:b", w, i)
				req := cmd("MSET", a, "1", b, "2") +
					cmd("GET", a) +
					cmd("MGET", a, "s0", b, "nope:"+a) +
					cmd("EXISTS", a, b, "s1") +
					cmd("DEL", a, b)
				if _, err := nc.Write([]byte(req)); err != nil {
					errs <- err
					return
				}
				want := "+OK\r\n" +
					"$1\r\n1\r\n" +
					"*4\r\n$1\r\n1\r\n$1\r\nx\r\n$1\r\n2\r\n$-1\r\n" +
					":3\r\n" +
					":2\r\n"
				if err := expectErr(br, want); err != nil {
					errs <- fmt.Errorf("fan conn %d iter %d: %w", w, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// expectErr is expect for goroutines: it returns instead of failing the test.
func expectErr(br *bufio.Reader, want string) error {
	got := make([]byte, len(want))
	for n := 0; n < len(got); {
		m, err := br.Read(got[n:])
		if err != nil {
			return fmt.Errorf("read after %q: %v", got[:n], err)
		}
		n += m
	}
	if string(got) != want {
		return fmt.Errorf("reply = %q, want %q", got, want)
	}
	return nil
}
