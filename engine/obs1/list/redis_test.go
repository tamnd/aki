package list

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// A throwaway RESP client for the live-replay tests, the same shape the set and
// zset slices use: it speaks just enough of the protocol to send an inline-array
// command and read one reply. It never runs unless AKI_REDIS_ADDR is set, so it
// stays out of the default test path and adds no dependency.
type redisConn struct {
	c net.Conn
	r *bufio.Reader
}

func dialRedis(addr string) (*redisConn, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &redisConn{c: c, r: bufio.NewReader(c)}, nil
}

func (rc *redisConn) close() { rc.c.Close() }

func (rc *redisConn) write(args ...string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	rc.c.SetDeadline(time.Now().Add(2 * time.Second))
	_, err := rc.c.Write([]byte(b.String()))
	return err
}

// cmd sends a command and reads one flat reply (status, error, integer, or
// bulk). An error reply comes back as an error carrying the Redis text.
func (rc *redisConn) cmd(args ...string) (string, error) {
	if err := rc.write(args...); err != nil {
		return "", err
	}
	return rc.read()
}

func (rc *redisConn) read() (string, error) {
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("empty reply")
	}
	switch line[0] {
	case '+', ':':
		return line[1:], nil
	case '-':
		return "", fmt.Errorf("%s", line[1:])
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "", nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rc.r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	default:
		return "", fmt.Errorf("unexpected reply %q", line)
	}
}

// cmdReply sends a command and reads one full reply into a recursive form: nil
// for a null bulk or null array, a string for a status/integer/bulk, an []any
// for an array, and an errReply for an error line, so a differential can compare
// error text without unwinding the read.
func (rc *redisConn) cmdReply(args ...string) (any, error) {
	if err := rc.write(args...); err != nil {
		return nil, err
	}
	return rc.readReply()
}

type errReply struct{ msg string }

func (rc *redisConn) readReply() (any, error) {
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, fmt.Errorf("empty reply")
	}
	switch line[0] {
	case '+', ':':
		return line[1:], nil
	case '-':
		return errReply{line[1:]}, nil
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rc.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := 0; i < n; i++ {
			el, err := rc.readReply()
			if err != nil {
				return nil, err
			}
			out[i] = el
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected reply %q", line)
	}
}

// blockMoveDiffer pairs the BLMOVE and BLMPOP harness with a live Redis for a
// byte-exact replay, the same shape the BLPOP parity suite uses. Beyond the cases
// one connection resolves (an immediate serve, a self-move, a finite timeout, and
// the parse errors), it runs the two-client wake through a second connection on
// each backend: A parks the blocking command, B pushes, and the woken reply plus
// every touched list must agree byte-for-byte.
type blockMoveDiffer struct {
	t      *testing.T
	a, b   *shard.Conn // harness: a blocks, b pushes
	ra, rb *redisConn  // redis: ra blocks, rb pushes
}

func newBlockMoveDiffer(t *testing.T) *blockMoveDiffer {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay BLMOVE and BLMPOP against a live Redis")
	}
	ra, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(ra.close)
	rb, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(rb.close)
	rt := newBlockHarness(t)
	return &blockMoveDiffer{t: t, a: rt.NewConn(), b: rt.NewConn(), ra: ra, rb: rb}
}

// agree runs one single-connection-resolvable command on both backends and pins
// the replies equal.
func (d *blockMoveDiffer) agree(op byte, verb string, args ...string) {
	d.t.Helper()
	mine := decodeReply(d.t, do(d.t, d.a, op, args...))
	theirs, err := d.ra.cmdReply(append([]string{verb}, args...)...)
	if err != nil {
		d.t.Fatalf("%s %v: redis transport error: %v", verb, args, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("%s %v: aki %v, redis %v", verb, args, render(mine), render(theirs))
	}
}

// freshList clears a key on the shared Redis and, when seeded, pushes the same
// values onto both backends so a following move or pop starts from equal state.
func (d *blockMoveDiffer) freshList(name string, vals ...string) string {
	k := "aki:blmove:" + name
	d.ra.cmd("DEL", k)
	if len(vals) > 0 {
		d.agree(bkRpush, "RPUSH", append([]string{k}, vals...)...)
	}
	return k
}

// parkWake blocks connection A on both backends with the same command, pushes
// with connection B, reads the woken reply, and pins it plus every listed key
// equal. On Redis the block is written without reading so the push on B can wake
// it; whether the server parks then serves or the push lands first, the moved
// result and the final lists are identical, so the assertion holds either way.
func (d *blockMoveDiffer) parkWake(op byte, verb string, blockArgs []string, pushOp byte, pushVerb string, pushArgs []string, checkKeys ...string) {
	d.t.Helper()
	park(d.t, d.a, op, blockArgs...)
	do(d.t, d.b, pushOp, pushArgs...)
	mine := decodeReply(d.t, drainOne(d.t, d.a))

	if err := d.ra.write(append([]string{verb}, blockArgs...)...); err != nil {
		d.t.Fatalf("%s %v: redis write: %v", verb, blockArgs, err)
	}
	if _, err := d.rb.cmd(append([]string{pushVerb}, pushArgs...)...); err != nil {
		d.t.Fatalf("%s %v: redis push: %v", pushVerb, pushArgs, err)
	}
	theirs, err := d.ra.readReply()
	if err != nil {
		d.t.Fatalf("%s %v: redis read: %v", verb, blockArgs, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("wake %s %v: aki %v, redis %v", verb, blockArgs, render(mine), render(theirs))
	}
	for _, k := range checkKeys {
		d.agree(bkLrange, "LRANGE", k, "0", "-1")
	}
}

func TestBlmoveAgainstRedis(t *testing.T) {
	d := newBlockMoveDiffer(t)

	// Immediate serve, every direction pair, with the resulting source and dest.
	for _, dir := range []struct{ from, to string }{
		{"LEFT", "LEFT"}, {"LEFT", "RIGHT"}, {"RIGHT", "LEFT"}, {"RIGHT", "RIGHT"},
	} {
		src := d.freshList("src_"+dir.from+dir.to, "a", "b", "c")
		dst := d.freshList("dst_"+dir.from+dir.to, "d", "e")
		d.agree(bkBlmove, "BLMOVE", src, dst, dir.from, dir.to, "0")
		d.agree(bkLrange, "LRANGE", src, "0", "-1")
		d.agree(bkLrange, "LRANGE", dst, "0", "-1")
	}

	// BRPOPLPUSH immediate, then a self-move that rotates one list.
	s := d.freshList("brpl", "a", "b", "c")
	d.agree(bkBrpoplpush, "BRPOPLPUSH", s, d.freshList("brpl_dst"), "0")
	d.agree(bkBrpoplpush, "BRPOPLPUSH", s, s, "0")
	d.agree(bkLrange, "LRANGE", s, "0", "-1")

	// A string source is WRONGTYPE at once.
	str := d.freshList("str")
	d.agree(bkSet, "SET", str, "v")
	d.agree(bkBlmove, "BLMOVE", str, d.freshList("str_dst"), "LEFT", "RIGHT", "0")

	// Finite timeout on an empty source: the null bulk.
	e1 := d.freshList("empty1")
	e2 := d.freshList("empty2")
	d.agree(bkBlmove, "BLMOVE", e1, e2, "LEFT", "RIGHT", "0.05")

	// Timeout and direction errors, checked before any side effect.
	d.agree(bkBlmove, "BLMOVE", e1, e2, "LEFT", "RIGHT", "-1")
	d.agree(bkBlmove, "BLMOVE", e1, e2, "LEFT", "RIGHT", "notafloat")
	d.agree(bkBlmove, "BLMOVE", e1, e2, "UP", "RIGHT", "0")

	// Two clients: A parks BLMOVE on an empty source, B pushes, the served bulk and
	// both keys agree byte-for-byte.
	ws := d.freshList("wake_src")
	wd := d.freshList("wake_dst")
	d.parkWake(bkBlmove, "BLMOVE", []string{ws, wd, "LEFT", "RIGHT", "0"},
		bkRpush, "RPUSH", []string{ws, "x"}, ws, wd)
}

func TestBlmpopAgainstRedis(t *testing.T) {
	d := newBlockMoveDiffer(t)

	// Immediate serve: a COUNT that clamps, both ends, and the leftover.
	k := d.freshList("mpop", "a", "b", "c", "d")
	d.agree(bkBlmpop, "BLMPOP", "0", "1", k, "LEFT", "COUNT", "2")
	d.agree(bkBlmpop, "BLMPOP", "0", "1", k, "RIGHT")
	d.agree(bkLrange, "LRANGE", k, "0", "-1")

	// First non-empty across a missing key.
	k1 := d.freshList("mp1")
	k2 := d.freshList("mp2", "x", "y")
	d.agree(bkBlmpop, "BLMPOP", "0", "2", k1, k2, "LEFT", "COUNT", "5")

	// Finite timeout with every key empty: the null array.
	m1 := d.freshList("mm1")
	m2 := d.freshList("mm2")
	d.agree(bkBlmpop, "BLMPOP", "0.05", "2", m1, m2, "LEFT")

	// The timeout and tail parse errors.
	d.agree(bkBlmpop, "BLMPOP", "-1", "1", k, "LEFT")
	d.agree(bkBlmpop, "BLMPOP", "notafloat", "1", k, "LEFT")
	d.agree(bkBlmpop, "BLMPOP", "0", "0", k, "LEFT")
	d.agree(bkBlmpop, "BLMPOP", "0", "1", k, "UP")
	d.agree(bkBlmpop, "BLMPOP", "0", "1", k, "LEFT", "COUNT", "0")

	// Two clients: A parks BLMPOP, B pushes three, the served [key,[elems]] and the
	// leftover key agree byte-for-byte.
	ws := d.freshList("mwake")
	d.parkWake(bkBlmpop, "BLMPOP", []string{"0", "1", ws, "LEFT", "COUNT", "2"},
		bkRpush, "RPUSH", []string{ws, "p", "q", "r"}, ws)
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
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
