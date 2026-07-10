package drivers

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/dispatch"
	"github.com/tamnd/aki/f3srv/resp"
)

// The connection goroutine shapes Options.ConnShape selects between.
const (
	// ShapeSingle is one goroutine per connection, the doc 08 section 4.1
	// shape: read, parse, dispatch, wait on the batch's completions, drain
	// the replies, flush, read again. The default.
	ShapeSingle = "single"

	// ShapePair is the reader/writer pair the M0 transport shipped with: a
	// reader goroutine parses and dispatches, a writer goroutine drains
	// replies onto the socket. It costs one goroutine, one channel, and a
	// worker-to-writer wake plus a writer park per request-reply round; it
	// stays selectable for the labs/f3/m0/15_conn_single A/B and leaves with
	// the M10 driver decision.
	ShapePair = "pair"
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

	// replyBufSize is the writer goroutine's socket buffer. The sizing rule is
	// pipeline depth times typical reply size: the buffer must hold one full
	// round of replies so a pipelined burst amortizes into about one write()
	// per drained boundary instead of one per buffer-full mid-drain flush. At
	// the gate shape (P16, separated-band values to 4KiB) that is 16 x 4KiB =
	// 64KiB; the labs/f3/m0/08_reply_buffer sweep froze it there. Cost: 64KiB
	// of writer buffer per connection, 32MiB at the 512-connection point.
	replyBufSize = 64 << 10
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
	// ReplyBufBytes overrides the per-connection reply writer buffer;
	// non-positive takes replyBufSize. The knob exists for the lab sweep.
	ReplyBufBytes int
	// FlushEveryDrain restores the pre-boundary flush discipline: the writer
	// flushes its socket buffer after every drain pass instead of waiting for
	// the pipeline boundary. The knob exists for the labs/f3/m0/11_transport
	// A/B; production keeps it off.
	FlushEveryDrain bool
	// PinWorkers locks each shard worker to an OS thread; see
	// shard.Config.PinWorkers. Default off.
	PinWorkers bool
	// PprofAddr, when set, binds a second listener serving net/http/pprof
	// under /debug/pprof/. Empty keeps it off. The endpoint has no auth, so
	// bind it to loopback (for example "127.0.0.1:6060"). This is a server
	// layer concern; the engine stays free of net/http.
	PprofAddr string
	// ConnShape picks the per-connection goroutine shape: ShapeSingle (also
	// the empty default) or ShapePair. The pair shape exists for the lab 15
	// A/B until the M10 driver decision.
	ConnShape string
}

// Server is the goroutine-per-connection driver over the shard runtime. In
// the default single shape, one goroutine per connection parses RESP2, routes
// through the dispatch table, waits on the batch's completions, and drains
// the replies in request order onto the socket itself; the pair shape splits
// the drain onto a second writer goroutine.
type Server struct {
	rt         *shard.Runtime
	ln         net.Listener
	pprofLn    net.Listener
	replyBuf   int
	flushEvery bool
	shape      string
	closed     atomic.Bool
	conns      sync.WaitGroup

	// flushes counts writer socket flushes across all connections. With the
	// reply buffer at its default every flush is one write syscall (a P16
	// round of point replies never fills 64KiB), so the counter is the labs'
	// writes-per-round evidence. One relaxed add per flush, nothing per reply.
	flushes atomic.Uint64

	// The akinet counter registry (doc 08 section 9.5): live connections'
	// counter homes, and the folded totals of closed ones. The mutex guards
	// only registration, unregistration, and the aggregation walk; the
	// counters themselves are single-writer per-connection state.
	netMu   sync.Mutex
	netLive map[*connState]struct{}
	netDone NetStats
}

// Flushes reports the total writer socket flushes since start, the lab and
// test surface for syscalls-per-round accounting.
func (s *Server) Flushes() uint64 { return s.flushes.Load() }

// Listen builds the runtime, registers the command table, starts the workers,
// and binds the listener. Serve must be called to accept.
func Listen(o Options) (*Server, error) {
	switch o.ConnShape {
	case "":
		o.ConnShape = ShapeSingle
	case ShapeSingle, ShapePair:
	default:
		return nil, fmt.Errorf("drivers: unknown conn shape %q", o.ConnShape)
	}
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
		PinWorkers:       o.PinWorkers,
	})
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	if o.ReplyBufBytes <= 0 {
		o.ReplyBufBytes = replyBufSize
	}
	s := &Server{
		rt:         rt,
		ln:         ln,
		replyBuf:   o.ReplyBufBytes,
		flushEvery: o.FlushEveryDrain,
		shape:      o.ConnShape,
		netLive:    make(map[*connState]struct{}),
	}
	rt.SetNetInfo(s.appendNetInfo)
	if o.PprofAddr != "" {
		pln, err := net.Listen("tcp", o.PprofAddr)
		if err != nil {
			_ = ln.Close()
			rt.Stop()
			return nil, err
		}
		s.pprofLn = pln
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		go func() { _ = http.Serve(pln, mux) }()
	}
	s.rt.Use(dispatch.Handlers())
	s.rt.Start()
	return s, nil
}

// PprofAddr reports the bound pprof listen address, nil when disabled.
func (s *Server) PprofAddr() net.Addr {
	if s.pprofLn == nil {
		return nil
	}
	return s.pprofLn.Addr()
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
	if s.pprofLn != nil {
		_ = s.pprofLn.Close()
	}
	s.conns.Wait()
	s.rt.Stop()
	return err
}

// handle runs one connection in the configured shape.
func (s *Server) handle(nc net.Conn) {
	if s.shape == ShapePair {
		s.handlePair(nc)
		return
	}
	s.handleSingle(nc)
}

// handleSingle runs one connection on one goroutine, the doc 08 section 4.1
// shape: read, parse, dispatch, wait on the batch's completions, drain the
// replies, flush the socket, read again. Request-reply traffic never needs a
// second goroutine: every reply answers a command this goroutine already
// dispatched and published, so the boundary below drains to Owes() == false
// before the next read and nothing can complete while the goroutine is
// blocked in Read. The M0 surface has no push mode or blocking command yet,
// so the section 4.1 flush-from-the-completing-owner try-lock is not needed
// either; when those land, their completions arrive unsolicited and that path
// gets built with them. Streamed replies already work here: emitStream runs
// on this goroutine inside the boundary drain (or the Do throttle's inline
// drain), consuming the worker's chunk ring like the pair's writer did.
//
// The waker machinery and its #564 skip rules are unchanged; Wait's
// spin-then-park just runs on this goroutine, so a round costs the worker
// wake alone where the pair also paid a worker-to-writer channel handoff.
func (s *Server) handleSingle(nc net.Conn) {
	defer func() { _ = nc.Close() }()
	c := s.rt.NewConn()
	cs := &connState{sc: c}
	s.register(cs)
	// LIFO defers: close first, then fold the final counts. In-flight replies
	// for a gone client are dropped by Close's contract, nobody drains them.
	defer s.unregister(cs)
	defer c.Close()

	bw := bufio.NewWriterSize(&countedWriter{w: nc, n: &cs.writes}, s.replyBuf)
	emit := func(rep []byte) { _, _ = bw.Write(rep) }
	// The pipeline-window throttle in Do drains through the same emit when
	// the parse pass runs a full reorder ring ahead; without it a burst
	// deeper than the ring in one read pass would deadlock this goroutine.
	c.SetInlineDrain(emit)

	s.readLoop(nc, c, cs, func() bool {
		// Publish everything batched so far, one atomic push per touched
		// shard, then serve the round: wait, drain, and hold the socket flush
		// to the boundary so a P16 round stays one write syscall. A lone
		// command zeroes the owed count on its first drain and flushes at
		// once, which is the P1 immediate-flush contract.
		c.Flush()
		for c.Owes() {
			if !c.Wait() {
				return false
			}
			c.DrainReplies(emit)
			if c.Failed() {
				// A streamed reply died after its header went out; nothing
				// coherent can follow, so the connection drops.
				return false
			}
			if s.flushEvery && bw.Buffered() > 0 {
				s.flushes.Add(1)
				if bw.Flush() != nil {
					return false
				}
			}
		}
		if bw.Buffered() > 0 {
			s.flushes.Add(1)
			if bw.Flush() != nil {
				return false
			}
		}
		return true
	})
}

// handlePair runs one connection as the M0 reader/writer pair: this goroutine
// reads, parses, and routes; a writer goroutine drains the reply queue in
// request order onto the socket. Kept behind Options.ConnShape for the lab 15
// A/B against handleSingle.
func (s *Server) handlePair(nc net.Conn) {
	defer func() { _ = nc.Close() }()
	c := s.rt.NewConn()
	cs := &connState{sc: c}
	s.register(cs)
	// LIFO defers put the fold after the writer join below, so it reads final
	// counts: the reader is this goroutine and the writer is gone by then.
	defer s.unregister(cs)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		bw := bufio.NewWriterSize(&countedWriter{w: nc, n: &cs.writes}, s.replyBuf)
		emit := func(rep []byte) { _, _ = bw.Write(rep) }
		for c.Wait() {
			c.DrainReplies(emit)
			if c.Failed() {
				// A streamed reply died after its header went out; nothing
				// coherent can follow, so the connection drops.
				return
			}
			if !s.flushEvery && c.Owes() {
				// This drain ended mid-pipeline: replies are still due for
				// commands the reader has already handed to the shards, so
				// the rest of the round lands in the buffer and the flush
				// waits for the boundary. The worker wakes this goroutine
				// again for every owed reply, so the deferral is bounded by
				// the round, and bufio flushes on its own if the buffer
				// fills first. A lone command never gets here: its reply
				// zeroes the owed count and flushes immediately below.
				continue
			}
			if bw.Buffered() > 0 {
				// An empty Flush is a no-op without a syscall; count only
				// the ones that write.
				s.flushes.Add(1)
			}
			if bw.Flush() != nil {
				return
			}
		}
		c.DrainReplies(emit)
		if bw.Buffered() > 0 {
			s.flushes.Add(1)
		}
		_ = bw.Flush()
	}()
	defer func() {
		c.Close()
		<-writerDone
	}()

	s.readLoop(nc, c, cs, func() bool {
		// A drained read is the pipeline boundary: publish everything batched
		// so far, one atomic push per touched shard. The writer goroutine
		// owns the drain and the socket flush.
		c.Flush()
		return true
	})
}

// readLoop is the read-parse-dispatch loop both shapes share. boundary runs
// after every read pass (the section 2.2 pipeline boundary) and must publish
// the batch; returning false tears the connection down. The protocol-error
// reply goes through boundary too, so a single-goroutine connection drains
// and flushes the in-order error before closing.
//
// Buffer discipline is doc 08 section 2.1: parse every complete command the
// buffer holds after each read (the parser hands out views, and Do copies
// them into the hop node before returning, so the buffer is free again by the
// time the loop advances), and either recycle the buffer whole or fold a
// small tail to the head.
func (s *Server) readLoop(nc net.Conn, c *shard.Conn, cs *connState, boundary func() bool) {
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
		// One bump per blocking Read call: each is one read(2) as the akinet
		// counters see it (a netpoller EAGAIN retry hides inside, but the
		// counter's job is reads per op, and those move together).
		cs.reads.bump()
		n += m
		if m > 0 {
			cmds := uint64(0)
			for pos < n {
				args, consumed, st := p.Next(buf[pos:n])
				if st == resp.NeedMore {
					break
				}
				if st == resp.ProtoErr {
					// Answer in pipeline order, then close: the stream cannot
					// be resynced after a framing error.
					_ = c.Do(shard.OpError, false, [][]byte{[]byte("ERR Protocol error: " + p.LastError())})
					boundary()
					return
				}
				pos += consumed
				if len(args) == 0 {
					continue
				}
				cmds++
				if derr := dispatch.Dispatch(c, args); derr != nil {
					return
				}
			}
			if cmds > 0 {
				// The pass's command count folds into one store, and a pass
				// that completed commands is one batch boundary (section 2.2):
				// commands/batch is the pipeline depth the server actually saw.
				cs.commands.add(cmds)
				cs.batches.bump()
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
			if !boundary() {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
