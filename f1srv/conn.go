package f1srv

import (
	"bufio"
	"net"
	"strconv"
)

// connState is one connection's parse-dispatch-reply state. rbuf holds bytes read
// from the socket that have not yet been consumed into a complete command; argv is
// reused across commands and points directly into rbuf, so a command costs no
// per-argument allocation. The write buffer batches replies and flushes once per read
// of the socket, so a pipeline of N commands is one read and one write.
type connState struct {
	srv  *Server
	conn net.Conn
	w    *bufio.Writer
	rbuf []byte
	argv [][]byte
	vbuf []byte  // reused destination for GET/MGET value copies
	num  [24]byte // scratch for formatting integer replies
}

// loop reads from the socket, drains every complete command in the buffer, and
// flushes the batched replies, until the peer closes or a protocol error ends it.
func (c *connState) loop() {
	for {
		if !c.fill() {
			return
		}
		if !c.drain() {
			return
		}
		if c.w.Buffered() > 0 {
			if err := c.w.Flush(); err != nil {
				return
			}
		}
	}
}

// fill reads one chunk from the socket into rbuf, growing the buffer when it is full
// so a value larger than the initial buffer still parses. It returns false on EOF or
// error.
func (c *connState) fill() bool {
	if len(c.rbuf) == cap(c.rbuf) {
		grown := make([]byte, len(c.rbuf), cap(c.rbuf)*2)
		copy(grown, c.rbuf)
		c.rbuf = grown
	}
	n, err := c.conn.Read(c.rbuf[len(c.rbuf):cap(c.rbuf)])
	if n > 0 {
		c.rbuf = c.rbuf[:len(c.rbuf)+n]
	}
	return err == nil
}

// drain parses and dispatches every complete command currently in rbuf, then
// compacts any partial trailing bytes to the front. It returns false on a protocol
// error that should close the connection.
func (c *connState) drain() bool {
	pos := 0
	for {
		argv, consumed, status := c.parse(c.rbuf[pos:])
		switch status {
		case parseOK:
			c.argv = argv
			pos += consumed
			c.dispatch(argv)
		case parseNeedMore:
			if pos > 0 {
				c.rbuf = append(c.rbuf[:0], c.rbuf[pos:]...)
			}
			return true
		case parseErr:
			c.writeErr("ERR Protocol error")
			return false
		}
	}
}

type parseStatus int

const (
	parseOK parseStatus = iota
	parseNeedMore
	parseErr
)

// parse reads one command from the front of b. A RESP multibulk (*N then N bulk
// strings) is the client path; a bare line is the inline path for a hand-typed
// client. argv slices point into b. consumed is the byte count of the parsed command.
func (c *connState) parse(b []byte) (argv [][]byte, consumed int, status parseStatus) {
	if len(b) == 0 {
		return nil, 0, parseNeedMore
	}
	if b[0] != '*' {
		return c.parseInline(b)
	}
	count, i, ok := readIntLine(b, 1)
	if !ok {
		return nil, 0, parseNeedMore
	}
	if count <= 0 {
		return c.argv[:0], i, parseOK // empty or null array: a no-op command
	}
	argv = c.argv[:0]
	for k := 0; k < count; k++ {
		if i >= len(b) {
			return nil, 0, parseNeedMore
		}
		if b[i] != '$' {
			return nil, 0, parseErr
		}
		blen, ni, ok := readIntLine(b, i+1)
		if !ok {
			return nil, 0, parseNeedMore
		}
		i = ni
		if blen < 0 {
			argv = append(argv, nil)
			continue
		}
		if i+blen+2 > len(b) {
			return nil, 0, parseNeedMore
		}
		argv = append(argv, b[i:i+blen])
		i += blen + 2 // bulk bytes plus trailing CRLF
	}
	return argv, i, parseOK
}

// parseInline handles a single space-separated line, enough for redis-cli's inline
// PING and manual probing. It is not the benchmark path.
func (c *connState) parseInline(b []byte) (argv [][]byte, consumed int, status parseStatus) {
	nl := indexByte(b, '\n')
	if nl < 0 {
		return nil, 0, parseNeedMore
	}
	line := b[:nl]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	argv = c.argv[:0]
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}
		start := i
		for i < len(line) && line[i] != ' ' {
			i++
		}
		argv = append(argv, line[start:i])
	}
	return argv, nl + 1, parseOK
}

// readIntLine parses an optionally-negative base-10 integer terminated by CRLF,
// starting at b[i]. It returns the value and the index just past the CRLF. ok is
// false when the terminator is not yet in the buffer, which the caller treats as
// "need more data".
func readIntLine(b []byte, i int) (val int, next int, ok bool) {
	neg := false
	if i < len(b) && b[i] == '-' {
		neg = true
		i++
	}
	v := 0
	digits := 0
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		v = v*10 + int(b[i]-'0')
		i++
		digits++
	}
	if i+1 >= len(b) {
		return 0, 0, false
	}
	if b[i] != '\r' || b[i+1] != '\n' {
		return 0, 0, false
	}
	if digits == 0 && !neg {
		return 0, 0, false
	}
	if neg {
		v = -v
	}
	return v, i + 2, true
}

func indexByte(b []byte, c byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// --- reply writers, all appending to the batched write buffer ---

func (c *connState) writeSimple(s string) {
	c.w.WriteByte('+')
	c.w.WriteString(s)
	c.w.WriteString("\r\n")
}

func (c *connState) writeErr(s string) {
	c.w.WriteByte('-')
	c.w.WriteString(s)
	c.w.WriteString("\r\n")
}

func (c *connState) writeInt(n int64) {
	c.w.WriteByte(':')
	c.w.Write(strconv.AppendInt(c.num[:0], n, 10))
	c.w.WriteString("\r\n")
}

func (c *connState) writeBulk(b []byte) {
	c.w.WriteByte('$')
	c.w.Write(strconv.AppendInt(c.num[:0], int64(len(b)), 10))
	c.w.WriteString("\r\n")
	c.w.Write(b)
	c.w.WriteString("\r\n")
}

func (c *connState) writeNil() {
	c.w.WriteString("$-1\r\n")
}

func (c *connState) writeArrayHeader(n int) {
	c.w.WriteByte('*')
	c.w.Write(strconv.AppendInt(c.num[:0], int64(n), 10))
	c.w.WriteString("\r\n")
}
