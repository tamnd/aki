package drivers

import (
	"io"
	"strconv"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The akinet counters (spec 2064/f3/08 section 9.5): per-connection transport
// event counters, incremented on their owner goroutine and aggregated only on
// read. Each counter is single-writer: the bump is a load and a store on the
// atomic type, never an atomic add, so the owner pays no lock prefix and the
// atomic type exists only to give the aggregating reader (INFO, NetStats) a
// defined value under the race detector. Bumps happen once per syscall-scale
// event or once per read pass, never one atomic per command, which keeps the
// F7 discipline on the wire.
type eventCounter struct{ v atomic.Uint64 }

func (c *eventCounter) add(n uint64) { c.v.Store(c.v.Load() + n) }

func (c *eventCounter) bump() { c.add(1) }

func (c *eventCounter) load() uint64 { return c.v.Load() }

// connState is one connection's counter home: the transport counters owned by
// the reader and writer goroutines, plus the shard connection whose waker
// counters (worker wakes sent, writer parks taken) belong to the same
// per-connection aggregation. The server keeps live connections in a registry
// and folds a connection's final counts into the closed-connection totals when
// its handler exits, so NetStats never loses history to churn.
type connState struct {
	reads    eventCounter // reader: nc.Read calls, one blocking read each
	batches  eventCounter // reader: read passes that completed >= 1 command
	commands eventCounter // reader: commands handed to dispatch
	writes   eventCounter // writer: socket Write calls under the reply buffer
	sc       *shard.Conn

	// subs is the set of exact channels this connection is subscribed to, the
	// per-connection half of the pub/sub registry (pubsub.go). It is nil until
	// the first SUBSCRIBE and touched only by this connection's reader goroutine,
	// so its own access needs no lock; the registry's reverse index is what the
	// shared mutex guards. A non-empty set means the connection is in subscribe
	// mode, which restricts the commands it may run.
	subs map[string]struct{}
}

// inSubscribeMode reports whether the connection holds any subscription, so the
// RESP2 subscribe-context command restriction applies (doc 17 section 13). Later
// slices fold pattern and shard subscriptions into the same test.
func (cs *connState) inSubscribeMode() bool { return len(cs.subs) > 0 }

// countedWriter counts the writer goroutine's socket writes: it sits between
// the reply bufio.Writer and the connection, so every buffer flush, mid-fill
// spill, and large direct write is one bump. Each Write on a healthy socket
// is one write(2) (net.Conn retries partial writes internally, and a short
// write is rare off a slow client), so the counter reads as
// net_write_syscalls.
type countedWriter struct {
	w io.Writer
	n *eventCounter
}

func (cw *countedWriter) Write(p []byte) (int, error) {
	cw.n.bump()
	return cw.w.Write(p)
}

// NetStats is the doc 08 section 9.5 akinet counter snapshot: the transport's
// verifiable run-time facts, aggregated across every connection this server
// has handled plus the shard workers' waker counters. The ratios the campaign
// reads fall straight out: Commands/Batches is pipeline depth as the server
// saw it, Commands/WriteSyscalls is the flush discipline's yield, and the
// wake and park columns are the cross-goroutine handoff traffic the reactor
// slices must beat.
type NetStats struct {
	// Driver is the active driver's name, recorded so a harness run can
	// verify the running config instead of trusting launch flags.
	Driver string

	// Shape is the goroutine driver's connection shape (ShapeSingle or
	// ShapePair), recorded for the same reason: the lab 15 A/B reads it back
	// off the wire instead of trusting the flag it launched with.
	Shape string

	// ReadSyscalls counts the reader goroutines' blocking socket reads;
	// WriteSyscalls the writer goroutines' socket writes.
	ReadSyscalls  uint64
	WriteSyscalls uint64

	// Batches counts read passes that completed at least one command (the
	// section 2.2 batch boundary); Commands counts commands dispatched.
	Batches  uint64
	Commands uint64

	// WorkerWakes counts wake tokens connection readers sent to parked shard
	// workers; ConnWakes counts tokens workers sent to parked connection
	// writers. Only real sends count, never the common single-load wake path.
	WorkerWakes uint64
	ConnWakes   uint64

	// WorkerParks and ConnParks count real parks: blocks taken on the waker
	// channel after the spin window, not spin turns.
	WorkerParks uint64
	ConnParks   uint64

	// LoopWakes counts the owner-to-loop wake deliveries the reactor driver
	// paid: eventfd writes, batched to one per touched loop per worker drain
	// pass (M10 pull-forward slice 4). ConnWakes still counts the per-
	// connection claims, so LoopWakes/ConnWakes is the batching yield the
	// slice exists for; the goroutine driver reports zero here because its
	// per-connection channel token is the delivery.
	LoopWakes uint64

	// OutbufDisconnects counts connections dropped at the OutBufLimitBytes
	// hard cap (doc 08 section 3.5's client-output-buffer-limit discipline):
	// the client's unread reply backlog passed the configured bound and the
	// driver disconnected it, that connection only. Zero when the cap is off
	// (the default) or never hit.
	OutbufDisconnects uint64
}

// addConn folds one connection's counters into the snapshot.
func (ns *NetStats) addConn(cs *connState) {
	ns.ReadSyscalls += cs.reads.load()
	ns.WriteSyscalls += cs.writes.load()
	ns.Batches += cs.batches.load()
	ns.Commands += cs.commands.load()
	ww, cp := cs.sc.NetWakes()
	ns.WorkerWakes += ww
	ns.ConnParks += cp
}

// NetStats aggregates the akinet counters: live connections, folded totals of
// closed ones, and the shard workers' waker counters. It is the Go-side
// snapshot the labs and tests read; INFO renders the same figures through
// appendNetInfo.
func (s *Server) NetStats() NetStats {
	s.netMu.Lock()
	ns := s.netDone
	for cs := range s.netLive {
		ns.addConn(cs)
	}
	s.netMu.Unlock()
	ns.ConnWakes, ns.WorkerParks = s.rt.NetWakes()
	ns.OutbufDisconnects = s.outbufDrops.Load()
	if s.backend != nil {
		ns.LoopWakes = s.backend.wakes()
	}
	ns.Driver = s.driver
	ns.Shape = s.shape
	return ns
}

// register puts a connection into the live registry. It also bumps the runtime
// live-connection count that drives the connection-writer park-immediately
// switch (shard.connSpinHighWater); every driver admits a connection through
// here, so this is the one place the count needs to move on open.
func (s *Server) register(cs *connState) {
	s.netMu.Lock()
	s.netLive[cs] = struct{}{}
	s.netMu.Unlock()
	s.rt.ConnOpened()
}

// unregister folds a finished connection's counters into the closed totals.
// The caller must have joined both connection goroutines first, so the loads
// see the final values.
func (s *Server) unregister(cs *connState) {
	// Drop the connection from every channel it subscribed to before folding its
	// counters; the pub/sub registry has its own mutex, taken outside netMu.
	s.pubsub.removeConn(cs)
	s.netMu.Lock()
	delete(s.netLive, cs)
	s.netDone.addConn(cs)
	s.netMu.Unlock()
	s.rt.ConnClosed()
}

// appendNetInfo renders the "# Net" INFO section from a fresh snapshot. It is
// registered with the shard runtime through SetNetInfo and runs on the INFO
// connection's writer goroutine during the stats gather; everything it reads
// is either under the registry mutex or an atomic load, so the walk is safe
// against live traffic.
func (s *Server) appendNetInfo(text []byte) []byte {
	ns := s.NetStats()
	text = append(text, "\r\n# Net\r\nnet_driver:"...)
	text = append(text, ns.Driver...)
	text = append(text, "\r\nnet_conn_shape:"...)
	text = append(text, ns.Shape...)
	text = append(text, '\r', '\n')
	stat := func(name string, v uint64) {
		text = append(text, name...)
		text = append(text, ':')
		text = strconv.AppendUint(text, v, 10)
		text = append(text, '\r', '\n')
	}
	stat("net_read_syscalls", ns.ReadSyscalls)
	stat("net_write_syscalls", ns.WriteSyscalls)
	stat("net_batches", ns.Batches)
	stat("net_commands", ns.Commands)
	stat("net_worker_wakes", ns.WorkerWakes)
	stat("net_worker_parks", ns.WorkerParks)
	stat("net_conn_wakes", ns.ConnWakes)
	stat("net_conn_parks", ns.ConnParks)
	stat("net_loop_wakes", ns.LoopWakes)
	stat("net_disconnects_outbuf", ns.OutbufDisconnects)
	return text
}
