package main

import (
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/tamnd/aki/resp"
)

// netClient is the CLI's outbound RESP client. It dials a running aki or Redis
// instance and runs commands over the wire so dump and import can read from or
// write to a live server instead of an offline .aki file. It writes requests as
// RESP arrays and parses replies with the shared decoder, holding one read
// buffer across calls.
type netClient struct {
	conn net.Conn
	buf  []byte
}

// dialServer connects to addr. A non-zero timeout arms a single deadline that
// bounds every call made on the connection.
func dialServer(addr string, timeout time.Duration) (*netClient, error) {
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
	return &netClient{conn: conn}, nil
}

// close shuts the connection.
func (c *netClient) close() { _ = c.conn.Close() }

// call runs a command given as strings and returns the reply.
func (c *netClient) call(args ...string) (resp.RESPValue, error) {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	return c.callRaw(raw)
}

// callRaw runs a command given as byte slices, which keeps binary payloads such
// as a DUMP blob intact.
func (c *netClient) callRaw(args [][]byte) (resp.RESPValue, error) {
	if err := c.send(args); err != nil {
		return resp.RESPValue{}, err
	}
	return c.readReply()
}

// send writes a command as a RESP array of bulk strings.
func (c *netClient) send(args [][]byte) error {
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

// readReply decodes the next complete value from the connection, reading more
// bytes whenever the buffer holds only a partial value.
func (c *netClient) readReply() (resp.RESPValue, error) {
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
