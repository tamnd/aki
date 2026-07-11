package zset

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// A throwaway RESP client for the live-replay tests: it speaks just enough of
// the protocol to send an inline-array command and read one flat reply (status,
// error, integer, or bulk). It never runs unless AKI_REDIS_ADDR is set, so it
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

func (rc *redisConn) cmd(args ...string) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	rc.c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := rc.c.Write([]byte(b.String())); err != nil {
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
		return "", fmt.Errorf("redis: %s", line[1:])
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

// cmdArray sends a command and reads a flat array reply, returning the elements
// (integers rendered as their decimal string, bulks as their bytes) and whether
// the array was non-null. It covers the shapes this slice needs: ZRANGE's bulk
// list, ZRANGE WITHSCORES's flat member/score list, and ZRANK WITHSCORE's
// [rank, score]. A null array (absent member with WITHSCORE) reports notNull
// false.
func (rc *redisConn) cmdArray(args ...string) (elems []string, notNull bool, err error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	rc.c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = rc.c.Write([]byte(b.String())); err != nil {
		return nil, false, err
	}
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return nil, false, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, false, fmt.Errorf("want array reply, got %q", line)
	}
	n, _ := strconv.Atoi(line[1:])
	if n < 0 {
		return nil, false, nil
	}
	elems = make([]string, n)
	for i := 0; i < n; i++ {
		el, err := rc.readElem()
		if err != nil {
			return nil, false, err
		}
		elems[i] = el
	}
	return elems, true, nil
}

// readElem reads one array element, an integer or a bulk string.
func (rc *redisConn) readElem() (string, error) {
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("empty element")
	}
	switch line[0] {
	case ':':
		return line[1:], nil
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
		return "", fmt.Errorf("unexpected element %q", line)
	}
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
