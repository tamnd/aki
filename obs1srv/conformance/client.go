package conformance

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// Conn is the minimal RESP2 client the corpus replays through:
// array-of-bulk out, recursive reply in.
type Conn struct {
	C net.Conn
	R *bufio.Reader
}

func NewConn(nc net.Conn) *Conn { return &Conn{C: nc, R: bufio.NewReader(nc)} }

// Do sends one command and decodes its reply.
func (rc *Conn) Do(args []string) (any, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	_ = rc.C.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := rc.C.Write([]byte(b.String())); err != nil {
		return nil, fmt.Errorf("write %v: %w", args, err)
	}
	v, err := rc.read()
	if err != nil {
		return nil, fmt.Errorf("reply to %v: %w", args, err)
	}
	return v, nil
}

func (rc *Conn) read() (any, error) {
	line, err := rc.R.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, fmt.Errorf("empty reply line")
	}
	body := line[1:]
	switch line[0] {
	case '+', '-':
		return body, nil
	case ':':
		var n int64
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return nil, fmt.Errorf("bad integer %q", body)
		}
		return n, nil
	case '$':
		var n int
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return nil, fmt.Errorf("bad bulk length %q", body)
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(rc.R, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		var n int
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return nil, fmt.Errorf("bad array length %q", body)
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := range out {
			v, err := rc.read()
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("unknown reply type %q", line)
}
