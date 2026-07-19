package drivers

import (
	"io"
	"strconv"
	"sync/atomic"
	"time"

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

	// psubs is the set of glob patterns this connection is subscribed to through
	// PSUBSCRIBE, the pattern twin of subs. ssubs is the set of shard channels it
	// holds through SSUBSCRIBE, a third independent namespace. All three are
	// reader-owned, nil until first use. A connection is in subscribe mode when any
	// of the three is non-empty.
	psubs map[string]struct{}
	ssubs map[string]struct{}

	// subCount mirrors len(subs), psubCount mirrors len(psubs), and ssubCount
	// mirrors len(ssubs), all as single-writer atomics so CLIENT LIST can read
	// another connection's channel, pattern, and shard-channel counts without
	// touching its maps. The owning reader goroutine restamps them after every
	// subs/psubs/ssubs mutation (pubsub.go); a cross-connection reader loads them.
	// This is the eventCounter discipline applied to a size gauge: single writer,
	// atomic only so the race detector sees a defined value.
	subCount  atomic.Int64
	psubCount atomic.Int64
	ssubCount atomic.Int64

	// id is this connection's CLIENT ID, stamped in register from the server's
	// monotonic counter. It is network-layer identity (client.go), immutable after
	// admission, so any goroutine may read it. The library tags a client advertises
	// through CLIENT SETINFO are not retained: f3 validates the option and answers
	// OK without storing it, matching the f1srv precedent.
	id uint64

	// name holds this connection's CLIENT SETNAME label, nil when unnamed. It is
	// an atomic pointer, not a plain slice, so CLIENT LIST can read another
	// connection's name without racing the owner's SETNAME. The owning reader
	// goroutine publishes a fresh immutable copy on each change (setName); every
	// reader loads through loadName. Writes are rare (SETNAME, HELLO SETNAME,
	// RESET), so the per-write copy costs nothing on the command path.
	name atomic.Pointer[[]byte]

	// addr and laddr are the connection's remote and local "ip:port" endpoints,
	// the addr= and laddr= fields CLIENT INFO reports. They are stamped once when
	// the connection is admitted (server.go from the net.Conn, the event-loop
	// drivers from the accepted fd) and never change, so a cross-connection read
	// (a later CLIENT LIST slice) needs no lock for them. connUnix is the connect
	// time in unix seconds, the base CLIENT INFO subtracts from now for the age=
	// field; also immutable after admission.
	addr     string
	laddr    string
	connUnix int64

	// quit is set when the connection ran QUIT: its +OK is queued, and the read
	// loop returns after the boundary flush so the acknowledgement lands before
	// the socket closes. Reader-owned like the fields above.
	quit bool

	// monitoring is set when the connection ran MONITOR: it has joined the
	// monitor set (monitor.go) and its own commands are excluded from the feed,
	// so a monitor never echoes itself. Reader-owned like quit, and gated on in
	// the command path through the registry's atomic count, not this flag.
	monitoring bool
}

// setName publishes a new CLIENT SETNAME label. An empty name clears it back to
// unnamed. The bytes are copied out of the caller's buffer (the parse buffer is
// reused for the next command) into a fresh slice, then stored atomically, so a
// concurrent CLIENT LIST reader on another goroutine never sees a half-written
// name. Only the owning reader goroutine calls this.
func (cs *connState) setName(name []byte) {
	if len(name) == 0 {
		cs.name.Store(nil)
		return
	}
	cp := append([]byte(nil), name...)
	cs.name.Store(&cp)
}

// loadName returns the connection's current CLIENT SETNAME label, nil when
// unnamed. Safe from any goroutine: the stored slice is immutable once published.
func (cs *connState) loadName() []byte {
	if p := cs.name.Load(); p != nil {
		return *p
	}
	return nil
}

// inSubscribeMode reports whether the connection holds any subscription of any
// kind (channel, pattern, or shard channel), so the RESP2 subscribe-context
// command restriction applies (doc 17 section 13).
func (cs *connState) inSubscribeMode() bool {
	return len(cs.subs) > 0 || len(cs.psubs) > 0 || len(cs.ssubs) > 0
}

// subTotal is the connection's regular subscription count, channels plus patterns,
// the number redis reports in every (P)SUBSCRIBE and (P)UNSUBSCRIBE confirmation.
// Shard subscriptions are a separate namespace with their own count, so they are
// not folded in here. Reader-owned, so it is read on the owning goroutine without
// a lock.
func (cs *connState) subTotal() int { return len(cs.subs) + len(cs.psubs) }

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
	// Stamp the connection's CLIENT ID here, the one admission choke point, so
	// every driver hands out ids from the same monotonic sequence. redis numbers
	// from 1, so the pre-increment counter's first value is 1.
	cs.id = s.nextConnID.Add(1)
	// Stamp the connect time for CLIENT INFO's age= field. Admission is a rare,
	// cold path (one per connection), so the wall-clock read costs nothing on the
	// command path.
	cs.connUnix = time.Now().Unix()
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
	// Drop it from the monitor set too, on the same teardown path, so a departed
	// monitor never lingers as a feed target.
	s.monitors.remove(cs)
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
