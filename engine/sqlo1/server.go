package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server is the S0 RESP endpoint: seven commands over a Store, one
// goroutine per connection, replies batched per read so pipelining works.
// It exists so the harness, the seed script, and the crash loop have a
// real server to talk to before the shard runtime lands; nothing in it is
// a performance statement.
type Server struct {
	st Store

	mu  sync.Mutex // serializes command execution against st
	seq int64      // drain sequence feeding ApplyBatch

	// now is the clock, swappable by tests that exercise expiry.
	now func() int64 // wall milliseconds
}

// NewServer wraps a Store in the S0 command surface.
func NewServer(st Store) *Server {
	return &Server{st: st, now: func() int64 { return time.Now().UnixMilli() }}
}

// Serve accepts connections until the listener closes.
func (s *Server) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(c)
	}
}

func (s *Server) handleConn(c net.Conn) {
	defer c.Close()
	buf := make([]byte, 0, 16<<10)
	reply := make([]byte, 0, 4<<10)
	for {
		// Parse everything buffered, then write all replies in one call, so
		// a pipelined burst costs one read and one write.
		reply = reply[:0]
		consumed := 0
		for {
			args, n, err := ParseCommand(buf[consumed:])
			if errors.Is(err, ErrIncomplete) {
				break
			}
			if pe, ok := errors.AsType[*ProtoError](err); ok {
				reply = AppendError(reply, pe.Error())
				c.Write(reply)
				return
			}
			consumed += n
			if len(args) == 0 {
				continue
			}
			reply = s.dispatch(reply, args)
		}
		if len(reply) > 0 {
			if _, err := c.Write(reply); err != nil {
				return
			}
		}
		buf = append(buf[:0], buf[consumed:]...)

		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := c.Read(buf[len(buf):cap(buf)])
		if err != nil {
			return
		}
		buf = buf[:len(buf)+n]
	}
}

func (s *Server) dispatch(reply []byte, args [][]byte) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "PING":
		switch len(args) {
		case 1:
			return AppendSimple(reply, "PONG")
		case 2:
			return AppendBulk(reply, args[1])
		}
		return arityErr(reply, cmd)
	case "ECHO":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		return AppendBulk(reply, args[1])
	case "SET":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		s.apply(Op{Rec: Record{Key: cloneBytes(args[1]), Value: cloneBytes(args[2])}})
		return AppendSimple(reply, "OK")
	case "GET":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		r, ok := s.getLive(args[1])
		if !ok {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, r.Value)
	case "DEL":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		n := int64(0)
		for _, k := range args[1:] {
			if _, ok := s.getLive(k); ok {
				s.apply(Op{Del: true, Rec: Record{Key: cloneBytes(k)}})
				n++
			}
		}
		return AppendInt(reply, n)
	case "EXPIRE":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		sec, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		r, ok := s.getLive(args[1])
		if !ok {
			return AppendInt(reply, 0)
		}
		if sec <= 0 {
			s.apply(Op{Del: true, Rec: Record{Key: cloneBytes(args[1])}})
			return AppendInt(reply, 1)
		}
		r.ExpireMs = s.now() + sec*1000
		s.apply(Op{Rec: r})
		return AppendInt(reply, 1)
	case "TTL":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		r, ok := s.getLive(args[1])
		switch {
		case !ok:
			return AppendInt(reply, -2)
		case r.ExpireMs == 0:
			return AppendInt(reply, -1)
		default:
			// Round up, so a key with 1ms left still reports 1.
			return AppendInt(reply, (r.ExpireMs-s.now()+999)/1000)
		}
	}
	return AppendError(reply, fmt.Sprintf("ERR unknown command '%s'", args[0]))
}

// getLive reads a key and applies lazy expiry: a record whose expiry has
// passed is deleted on sight and reported missing.
func (s *Server) getLive(key []byte) (Record, bool) {
	r, err := s.st.Get(context.Background(), key)
	if err != nil {
		return Record{}, false
	}
	if r.ExpireMs > 0 && r.ExpireMs <= s.now() {
		s.apply(Op{Del: true, Rec: Record{Key: cloneBytes(key)}})
		return Record{}, false
	}
	return r, true
}

// apply pushes one op at the next drain sequence. One op per batch is
// placeholder-grade; real batching arrives with the S1 write-behind queue.
func (s *Server) apply(op Op) {
	s.seq++
	s.st.ApplyBatch(context.Background(), &DrainBatch{Seq: s.seq, Ops: []Op{op}})
}

func arityErr(reply []byte, cmd string) []byte {
	return AppendError(reply, fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd)))
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
