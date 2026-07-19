package drivers

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

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

// The network drivers Options.NetDriver selects between.
const (
	// NetGoroutine is the goroutine-per-connection driver, the default on
	// every platform and the only driver on non-Linux.
	NetGoroutine = "goroutine"

	// NetReactor is the raw-epoll event-loop driver (doc 08 section 4.2),
	// Linux only, pulled forward from M10 as a correctness-only skeleton
	// behind this knob. On other platforms it falls back to the goroutine
	// driver with a logged notice. It becomes a default nowhere until it wins
	// its regime A/B under the section 4.4 rule.
	NetReactor = "reactor"

	// NetURing is the io_uring event-loop driver (doc 08 section 4.3), the
	// reactor's architecture with the per-op syscalls replaced by ring
	// submissions: one io_uring_enter per loop pass covers the recv re-arms
	// and the batched sends. Linux only, and probed at Listen (the syscalls,
	// IORING_FEAT_NODROP, and the RECV/SEND/READ/ASYNC_CANCEL opcodes, kernel
	// 5.6+); anywhere the probe fails it falls back to the goroutine driver
	// with a logged notice. Same section 4.4 rule: a default nowhere until it
	// wins its regime A/B.
	NetURing = "uring"
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

	// blockPollSpin and blockPollSleep tune the handlePair reader's wait for a
	// block to clear. That shape's writer drains on another goroutine, so the
	// reader cannot drain the barrier itself and polls instead: it spins a short
	// window (a serving push usually lands fast) before it sleeps, keeping a long
	// block off a busy core. handleSingle never uses these; it drains the block
	// in its boundary. ShapePair is the lab-only A/B shape, so the values favor
	// simplicity over a tuned backoff.
	blockPollSpin  = 512
	blockPollSleep = 100 * time.Microsecond

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
	// ReadBufBytes overrides a connection's initial read buffer;
	// non-positive takes readBufSize. Like ReplyBufBytes this is a lab knob:
	// the read buffer grows on demand for a command that outsizes it, so a
	// smaller initial size only trims idle/point-op connections (the 64B
	// memory-bar cell) and lets large-command connections grow as before.
	// Lab 24 (labs/f3/m0/24_conn_buffers) sweeps it against the gate shapes.
	ReadBufBytes int
	// BatchDataCap, RepCap, ReplyRing, and FreeListCap override the
	// per-connection hop-transport sizes (shard.Config fields of the same name);
	// non-positive takes the tuning.go default. They are the M0 memory-bar lever
	// swept by labs/f3/m0/25_conn_caps and 27_rep_headroom: at high fan-out the
	// pooled hopBatch buffers and the reply reorder ring dominate resident
	// footprint. BatchDataCap and RepCap both grow on demand for a larger command
	// or reply, so a smaller start only trims the steady small-value path; RepCap
	// in particular lets a write-heavy load skip the batchDataCap+64*batchCap
	// default reply headroom (labs/f3/m0/27 measured 15 MiB off the c512 SET cell
	// at no throughput cost).
	BatchDataCap int
	RepCap       int
	ReplyRing    int
	FreeListCap  int
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
	// NetDriver picks the network driver: NetGoroutine (also the empty
	// default), NetReactor, or NetURing. The reactor and uring drivers run on
	// Linux only (uring additionally needs a kernel that passes the probe);
	// elsewhere they fall back to the goroutine driver with a logged notice,
	// and NetStats reports the driver actually running, never the one asked
	// for.
	NetDriver string
	// NetLoops is the event-loop count for the reactor and uring drivers;
	// non-positive takes defaultNetLoops, GOMAXPROCS/2 floored (min 1). Lab 19
	// first froze the 2/5 network share off the pre-M10 8-cpu mask, but a
	// re-sweep of the current surface (labs/f3/m0/26_loop_knee) moves the knee
	// up to half the cores at both 8 and 14 cpu: the M10 batched wakes and
	// buffer leasing flattened the oversubscription a loop past the knee used
	// to pay, and the P16 point-op gate is loop-bound, so a loop outearns the
	// shard core it borrows. The 2/5 default held the reactor to 1.67x redis
	// GET (short of the 2x gate); half the cores clears 2x on both ops.
	NetLoops int
	// OutBufLimitBytes is the hard cap on one connection's pending reply
	// bytes buffered driver-side (the client-output-buffer-limit lineage,
	// doc 08 section 3.5): a client whose unread backlog passes it is
	// disconnected, that connection only, and the event is counted in INFO as
	// net_disconnects_outbuf. 0, the default, means no cap, matching Redis
	// normal-class clients. The reactor and uring drivers enforce it; the
	// goroutine driver's writer blocks on the socket instead of buffering, so
	// it has no equivalent backlog to cap.
	OutBufLimitBytes int
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
	readBuf    int
	outLimit   int
	flushEvery bool
	shape      string
	conns      sync.WaitGroup

	// driver names the network driver actually running (NetGoroutine or
	// NetReactor), fixed at Listen after the platform fallback has resolved,
	// so NetStats and INFO report the running config, not the requested one.
	driver string

	// backend is the non-goroutine driver when one is active, nil otherwise.
	// It is built at Listen (so the driver name above is settled before any
	// traffic), serves in place of the accept loop, and is stopped by Close.
	backend netBackend

	// closeMu orders the accept path against Close. A queued connection can
	// win the Accept race with ln.Close, so without the gate Serve's
	// conns.Add(1) can land while Close is already in conns.Wait (the
	// TestPprofOff race), and the handler would then run against a stopped
	// runtime. track takes the lock to check closed and Add as one step;
	// Close flips closed under the same lock before it waits, so every Add
	// happens before the Wait or never happens at all.
	closeMu sync.Mutex
	closed  bool

	// outbufDrops counts connections disconnected at the OutBufLimitBytes
	// hard cap, the doc 08 section 3.5 write-side discipline event; INFO
	// renders it as net_disconnects_outbuf.
	outbufDrops atomic.Uint64

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

	// pubsub is the network-layer channel registry (doc 08, doc 17 section 13):
	// pub/sub lives here, not in the shard workers, so a PUBLISH storm never
	// slows a GET. Built at Listen, walked by publishers and mutated by
	// subscribers under its own mutex.
	pubsub *pubsubRegistry

	// monitors is the network-layer MONITOR set (monitor.go): the connections
	// streaming the command feed, kept here for the same reason as pub/sub. Its
	// atomic count gates the command path so a server with no monitor pays one
	// relaxed load per command and never the mutex.
	monitors *monitorRegistry

	// nextConnID hands each connection its CLIENT ID / HELLO id. A connection's
	// identity is network-layer state (client.go), like pub/sub, so it lives
	// here and not in the shard workers. register is the one place every driver
	// admits a connection, so the id is stamped there. It starts at zero and the
	// first connection gets 1, matching redis (no connection is id 0).
	nextConnID atomic.Uint64
}

// Flushes reports the total writer socket flushes since start, the lab and
// test surface for syscalls-per-round accounting.
func (s *Server) Flushes() uint64 { return s.flushes.Load() }

// netBackend is the seam a non-goroutine network driver plugs into: serve
// owns the accept loop and everything behind it, stop tears the driver's
// resources down and joins its threads. The goroutine driver is not a backend;
// it is the code in this file, and stays the fallback whenever a backend
// cannot be built.
type netBackend interface {
	serve() error
	stop()
	// wakes reports the owner-to-loop wake deliveries the driver has paid
	// (the reactor's eventfd writes), the NetStats.LoopWakes source.
	wakes() uint64
}

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
	switch o.NetDriver {
	case "":
		o.NetDriver = NetGoroutine
	case NetGoroutine, NetReactor, NetURing:
	default:
		return nil, fmt.Errorf("drivers: unknown net driver %q", o.NetDriver)
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
		BatchDataCap:     o.BatchDataCap,
		RepCap:           o.RepCap,
		ReplyRing:        o.ReplyRing,
		FreeListCap:      o.FreeListCap,
	})
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	if o.ReplyBufBytes <= 0 {
		o.ReplyBufBytes = replyBufSize
	}
	if o.ReadBufBytes <= 0 {
		o.ReadBufBytes = readBufSize
	}
	s := &Server{
		rt:         rt,
		ln:         ln,
		replyBuf:   o.ReplyBufBytes,
		readBuf:    o.ReadBufBytes,
		outLimit:   o.OutBufLimitBytes,
		flushEvery: o.FlushEveryDrain,
		shape:      o.ConnShape,
		driver:     NetGoroutine,
		netLive:    make(map[*connState]struct{}),
		pubsub:     newPubsubRegistry(),
		monitors:   newMonitorRegistry(),
	}
	switch o.NetDriver {
	case NetReactor:
		// Platform-resolved: on Linux this builds the epoll loops and flips
		// the driver name; elsewhere (or on a setup failure) it logs the
		// fallback notice and the goroutine driver serves as usual. Resolved
		// here rather than in Serve so the driver name is settled before the
		// listener sees a byte.
		if b, err := newReactorBackend(s, o); err != nil {
			log.Printf("drivers: reactor driver unavailable (%v), falling back to the goroutine driver", err)
		} else {
			s.backend = b
			s.driver = NetReactor
		}
	case NetURing:
		// Same resolution shape, one more way to fail: the kernel probe. The
		// probe result folds into the error, so the notice says why (ENOSYS,
		// sysctl, seccomp, missing ops) and the goroutine driver serves.
		if b, err := newURingBackend(s, o); err != nil {
			log.Printf("drivers: uring driver unavailable (%v), falling back to the goroutine driver", err)
		} else {
			s.backend = b
			s.driver = NetURing
		}
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
	s.rt.UseDemoter(dispatch.Demoter())
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
	if s.backend != nil {
		return s.backend.serve()
	}
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			if s.isClosed() {
				return nil
			}
			return err
		}
		if !s.track() {
			_ = nc.Close()
			return nil
		}
		go func() {
			defer s.conns.Done()
			s.handle(nc)
		}()
	}
}

// track admits an accepted connection into the handler WaitGroup, refusing
// once Close has begun. The check and the Add are one critical section; see
// closeMu.
func (s *Server) track() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return false
	}
	s.conns.Add(1)
	return true
}

func (s *Server) isClosed() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.closed
}

// Close stops accepting, waits for the connection handlers, and stops the
// shard workers.
func (s *Server) Close() error {
	s.closeMu.Lock()
	s.closed = true
	s.closeMu.Unlock()
	err := s.ln.Close()
	if s.pprofLn != nil {
		_ = s.pprofLn.Close()
	}
	s.conns.Wait()
	if s.backend != nil {
		// The backend's loops close every adopted fd exactly once and join
		// before this returns, so the runtime stop below never races a loop
		// still driving shard connections.
		s.backend.stop()
	}
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
	cs := &connState{sc: c, addr: connAddr(nc.RemoteAddr()), laddr: connAddr(nc.LocalAddr())}
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
	}, func() bool {
		// This shape's boundary already drained the block to completion (the
		// for c.Owes() loop above waits for the parked reply), so on return the
		// emit watermark has crossed the barrier and Blocked is already false.
		// The readLoop's for c.Blocked() guard therefore never calls this; it
		// exists to satisfy the shared signature.
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
	cs := &connState{sc: c, addr: connAddr(nc.RemoteAddr()), laddr: connAddr(nc.LocalAddr())}
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
	}, func() bool {
		// This shape's writer drains on its own goroutine, so the reader polls
		// the barrier instead of draining it: spin a bounded window, then sleep
		// so a long block does not burn a core. A closed connection surfaces as
		// the writer goroutine finishing, which ends the wait so the reader can
		// tear down. Returning true just re-checks Blocked in the caller.
		for i := 0; i < blockPollSpin; i++ {
			if !c.Blocked() {
				return true
			}
		}
		select {
		case <-writerDone:
			return false
		default:
		}
		time.Sleep(blockPollSleep)
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
//
// A blocking verb (BLPOP and kin) adds one gate: after it is dispatched the
// connection's reader barrier is armed (Blocked), and the parse loop stops there
// so a command pipelined behind an unresolved block does not run until the
// block's reply goes out. The buffered tail is held, not dropped, and re-parsed
// the moment the block clears. handleSingle drains the block inside boundary and
// returns with the barrier already down, so its unblock is a no-op; handlePair's
// writer drains on its own goroutine, so its unblock polls the barrier. On a
// connection that never blocks the only added cost is one relaxed Blocked load
// per parse iteration.
func (s *Server) readLoop(nc net.Conn, c *shard.Conn, cs *connState, boundary func() bool, unblock func() bool) {
	var p resp.Parser
	buf := make([]byte, s.readBuf)
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
			// The inner loop re-runs when a parse pass stopped on an unresolved
			// block: once the block clears, the buffered tail is re-parsed
			// without a fresh read. A pass that runs to the buffer's end or to a
			// partial command breaks out and reads more.
			for {
				stoppedOnBlock := false
				cmds := uint64(0)
				for pos < n {
					// A blocking command already dispatched on this connection
					// holds the barrier: stop here so a pipelined command behind
					// it does not run until its reply goes out. The tail stays
					// buffered for the re-parse after the block clears.
					if c.Blocked() {
						stoppedOnBlock = true
						break
					}
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
					// Feed the command to any MONITOR connections before it runs
					// (monitor.go). The gate is a single relaxed atomic load, so a
					// server with no monitor pays nothing here, and a connection's
					// own commands are excluded so a monitor never echoes itself.
					// The args views are still valid: the parser hands them out for
					// this pass and the feed copies them into each monitor's node.
					if s.monitors.active() && !cs.monitoring {
						s.monitors.feed(time.Now(), 0, cs.addr, args)
					}
					// CLIENT, HELLO, QUIT, and RESET are answered here, in the
					// network layer, from per-connection state (client.go); they
					// never reach the shard hop. This sits ahead of the pub/sub
					// intercept so the lifecycle verbs run even in subscribe mode.
					if s.connIntercept(c, cs, args) {
						// QUIT queued its +OK and asked to close: stop parsing this
						// pass so nothing pipelined behind QUIT runs, let the
						// boundary flush the acknowledgement, then return below.
						if cs.quit {
							break
						}
						continue
					}
					// Pub/sub is answered here, in the network layer, and never
					// reaches the shard hop (doc 17 section 13). The intercept owns
					// the command when it returns true, including a command refused
					// because the connection is in subscribe mode.
					if s.pubsubIntercept(c, cs, args) {
						continue
					}
					if derr := dispatch.Dispatch(c, args); derr != nil {
						return
					}
				}
				if cmds > 0 {
					// The pass's command count folds into one store, and a pass
					// that completed commands is one batch boundary (section
					// 2.2): commands/batch is the pipeline depth the server saw.
					cs.commands.add(cmds)
					cs.batches.bump()
				}
				if pos == n {
					pos, n = 0, 0
					// A giant inbound bulk grew the buffer; once drained, shrink
					// back so an idle connection does not pin megabytes.
					if len(buf) > 16*s.readBuf {
						buf = make([]byte, s.readBuf)
					}
				} else if pos > 0 && n-pos <= compactMax {
					n = copy(buf, buf[pos:n])
					pos = 0
				}
				if !boundary() {
					return
				}
				// QUIT's +OK has now flushed at the boundary; close the connection.
				if cs.quit {
					return
				}
				if !stoppedOnBlock {
					break
				}
				// The pass stopped on an unresolved block. Wait for its reply to
				// go out, then re-parse the buffered tail so the pipelined
				// command behind it runs the moment the block clears.
				for c.Blocked() {
					if !unblock() {
						return
					}
				}
				if pos >= n {
					break
				}
			}
		}
		if err != nil {
			return
		}
	}
}
