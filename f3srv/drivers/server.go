package drivers

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/dispatch"
	"github.com/tamnd/aki/f3srv/resp"
)

// defaultArenaBytes is the per-shard arena when the caller leaves it zero,
// generous for the M0 surface; the product sizing rides the maxmemory work.
const defaultArenaBytes = 256 << 20

const (
	// readBufSize is a connection's initial read buffer. It grows only when a
	// single command outsizes it.
	readBufSize = 64 << 10

	// compactMax is the doc 08 section 2.1 tail rule: a leftover smaller than
	// this is copied to the buffer head after a parse pass, and a bigger one
	// stays put until the buffer fills, so a large half-arrived bulk is not
	// re-copied on every read.
	compactMax = 512
)

// Options configures a server.
type Options struct {
	// Addr is the TCP listen address; ":0" picks a free port.
	Addr string
	// Shards is the owner worker count; non-positive takes shard.DefaultShards.
	Shards int
	// ArenaBytes is the arena size per shard; non-positive takes the default.
	ArenaBytes int
	// SegBytes is the arena segment size per shard; non-positive takes the
	// store default.
	SegBytes int
	// VlogDir, when set, gives every shard its own value-log file under this
	// directory; empty keeps the stores memory-only.
	VlogDir string
	// ResidentCapBytes is each shard's resident byte budget when VlogDir is
	// set; past it, separated and chunked value bytes spill to the shard's
	// log. 0 means uncapped.
	ResidentCapBytes uint64
}

// Server is the goroutine-per-connection driver over the shard runtime: a
// reader goroutine parses RESP2 and routes through the dispatch table, a
// writer goroutine drains replies in request order onto the socket.
type Server struct {
	rt     *shard.Runtime
	ln     net.Listener
	closed atomic.Bool
	conns  sync.WaitGroup
}

// Listen builds the runtime, registers the command table, starts the workers,
// and binds the listener. Serve must be called to accept.
func Listen(o Options) (*Server, error) {
	if o.Shards <= 0 {
		o.Shards = shard.DefaultShards()
	}
	if o.ArenaBytes <= 0 {
		o.ArenaBytes = defaultArenaBytes
	}
	ln, err := net.Listen("tcp", o.Addr)
	if err != nil {
		return nil, err
	}
	rt, err := shard.Open(shard.Config{
		Shards:           o.Shards,
		ArenaBytes:       o.ArenaBytes,
		SegBytes:         o.SegBytes,
		VlogDir:          o.VlogDir,
		ResidentCapBytes: o.ResidentCapBytes,
	})
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	s := &Server{rt: rt, ln: ln}
	s.rt.Use(dispatch.Handlers())
	s.rt.Start()
	return s, nil
}

// Addr reports the bound listen address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until Close. It returns nil after a Close and the
// accept error otherwise.
func (s *Server) Serve() error {
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			return err
		}
		s.conns.Add(1)
		go func() {
			defer s.conns.Done()
			s.handle(nc)
		}()
	}
}

// Close stops accepting, waits for the connection handlers, and stops the
// shard workers.
func (s *Server) Close() error {
	s.closed.Store(true)
	err := s.ln.Close()
	s.conns.Wait()
	s.rt.Stop()
	return err
}

// handle runs one connection: this goroutine reads, parses, and routes; a
// writer goroutine drains the reply queue in request order onto the socket.
//
// Buffer discipline is doc 08 section 2.1: parse every complete command the
// buffer holds after each read (the parser hands out views, and Do copies
// them into the hop node before returning, so the buffer is free again by the
// time the loop advances), flush at the drained boundary, and either recycle
// the buffer whole or fold a small tail to the head.
func (s *Server) handle(nc net.Conn) {
	defer func() { _ = nc.Close() }()
	c := s.rt.NewConn()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		bw := bufio.NewWriter(nc)
		emit := func(rep []byte) { _, _ = bw.Write(rep) }
		for c.Wait() {
			c.DrainReplies(emit)
			if c.Failed() {
				// A streamed reply died after its header went out; nothing
				// coherent can follow, so the connection drops.
				return
			}
			if bw.Flush() != nil {
				return
			}
		}
		c.DrainReplies(emit)
		_ = bw.Flush()
	}()
	defer func() {
		c.Close()
		<-writerDone
	}()

	var p resp.Parser
	buf := make([]byte, readBufSize)
	n, pos := 0, 0
	for {
		if n == len(buf) {
			if pos > 0 {
				n = copy(buf, buf[pos:n])
				pos = 0
			} else {
				// One command larger than the buffer: grow, there is no other
				// way to see its end.
				bigger := make([]byte, 2*len(buf))
				n = copy(bigger, buf[:n])
				buf = bigger
			}
		}
		m, err := nc.Read(buf[n:])
		n += m
		if m > 0 {
			for pos < n {
				args, consumed, st := p.Next(buf[pos:n])
				if st == resp.NeedMore {
					break
				}
				if st == resp.ProtoErr {
					// Answer in pipeline order, then close: the stream cannot
					// be resynced after a framing error.
					_ = c.Do(shard.OpError, false, [][]byte{[]byte("ERR Protocol error: " + p.LastError())})
					c.Flush()
					return
				}
				pos += consumed
				if len(args) == 0 {
					continue
				}
				if derr := dispatch.Dispatch(c, args); derr != nil {
					return
				}
			}
			if pos == n {
				pos, n = 0, 0
				// A giant inbound bulk grew the buffer; once drained, shrink
				// back so an idle connection does not pin megabytes.
				if len(buf) > 16*readBufSize {
					buf = make([]byte, readBufSize)
				}
			} else if pos > 0 && n-pos <= compactMax {
				n = copy(buf, buf[pos:n])
				pos = 0
			}
			// A drained read is the pipeline boundary: publish everything
			// batched so far, one atomic push per touched shard.
			c.Flush()
		}
		if err != nil {
			return
		}
	}
}
