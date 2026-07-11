package list

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
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
