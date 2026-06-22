// Package respclient is a minimal blocking RESP client for talking to a running
// aki or Redis instance over the wire. MIGRATE uses it on the server side to
// push keys to a target, and the CLI uses it for networked dump and import. It
// writes requests as RESP arrays and parses replies with the shared decoder,
// holding one read buffer across calls.
package respclient

import (
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/tamnd/aki/resp"
)

// Client is one connection to a remote instance. It is not safe for concurrent
// use: each call writes a command and reads its reply before returning.
type Client struct {
	conn net.Conn
	buf  []byte
}

// Dial connects to addr. A non-zero timeout arms a single deadline that bounds
// the whole conversation on the connection, which is how MIGRATE enforces its
// timeout. A zero timeout means no deadline.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	var (
		conn net.Conn
		err  error
	)
	if timeout > 0 {
		conn, err = net.DialTimeout("tcp", addr, timeout)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	if timeout > 0 {
		if derr := conn.SetDeadline(time.Now().Add(timeout)); derr != nil {
			_ = conn.Close()
			return nil, derr
		}
	}
	return &Client{conn: conn}, nil
}

// Close shuts the connection.
func (c *Client) Close() { _ = c.conn.Close() }

// Call writes one command as a RESP array of bulk strings and reads a single
// reply. Byte-slice arguments keep binary payloads such as a DUMP blob intact.
func (c *Client) Call(args ...[]byte) (resp.RESPValue, error) {
	if err := c.Send(args); err != nil {
		return resp.RESPValue{}, err
	}
	return c.ReadReply()
}

// CallStr is Call for commands whose arguments are all plain strings.
func (c *Client) CallStr(args ...string) (resp.RESPValue, error) {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	return c.Call(raw...)
}

// Send writes a command as a RESP array of bulk strings without reading a reply.
// It is exported so a caller can pipeline several commands and read the replies
// in order afterwards.
func (c *Client) Send(args [][]byte) error {
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	_, err := c.conn.Write(b)
	return err
}

// ReadReply decodes the next complete value from the connection, reading more
// bytes whenever the buffer holds only a partial value.
func (c *Client) ReadReply() (resp.RESPValue, error) {
	tmp := make([]byte, 4096)
	for {
		v, n, err := resp.Decode(c.buf, 0)
		if err == nil {
			c.buf = c.buf[n:]
			return v, nil
		}
		if !errors.Is(err, resp.ErrNeedMore) {
			return resp.RESPValue{}, err
		}
		m, rerr := c.conn.Read(tmp)
		if m > 0 {
			c.buf = append(c.buf, tmp[:m]...)
		}
		if rerr != nil {
			return resp.RESPValue{}, rerr
		}
	}
}
