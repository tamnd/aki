package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// respConn is the harness side of the wire: build one command array, read
// one reply, never pipeline. One op in flight per connection is what keeps
// the shadow oracle exact: when the server dies mid-run, at most one op per
// worker has an unknown outcome, and that op is the only ambiguity the
// verifier has to accept.
type respConn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func dialRESP(addr string) (*respConn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &respConn{c: c, r: bufio.NewReader(c), w: bufio.NewWriter(c)}, nil
}

func (rc *respConn) close() { rc.c.Close() }

// reply is a decoded RESP2 answer. kind picks the meaningful field: '+'
// simple string, '-' error, ':' integer, '$' bulk, with null set when the
// bulk length was -1.
type reply struct {
	kind byte
	s    string
	n    int64
	b    []byte
	null bool
}

func (rc *respConn) do(deadline time.Duration, args ...[]byte) (reply, error) {
	if err := rc.c.SetDeadline(time.Now().Add(deadline)); err != nil {
		return reply{}, err
	}
	buf := make([]byte, 0, 64)
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, a := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(a)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, a...)
		buf = append(buf, '\r', '\n')
	}
	if _, err := rc.w.Write(buf); err != nil {
		return reply{}, err
	}
	if err := rc.w.Flush(); err != nil {
		return reply{}, err
	}
	return rc.readReply()
}

func (rc *respConn) readReply() (reply, error) {
	line, err := rc.r.ReadBytes('\n')
	if err != nil {
		return reply{}, err
	}
	line = bytes.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return reply{}, fmt.Errorf("empty reply line")
	}
	rest := string(line[1:])
	switch line[0] {
	case '+':
		return reply{kind: '+', s: rest}, nil
	case '-':
		return reply{kind: '-', s: rest}, nil
	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return reply{}, fmt.Errorf("bad integer reply %q", rest)
		}
		return reply{kind: ':', n: n}, nil
	case '$':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return reply{}, fmt.Errorf("bad bulk length %q", rest)
		}
		if n == -1 {
			return reply{kind: '$', null: true}, nil
		}
		b := make([]byte, n+2)
		if _, err := io.ReadFull(rc.r, b); err != nil {
			return reply{}, err
		}
		return reply{kind: '$', b: b[:n]}, nil
	}
	return reply{}, fmt.Errorf("unexpected reply type %q", line[0])
}
