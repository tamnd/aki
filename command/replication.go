package command

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/resp"
)

// This file implements the replication subsystem (spec 2064 doc 18 sections 2
// through 17): the master side that serves PSYNC and streams writes to replicas,
// and the replica side that runs the handshake, loads the RDB snapshot, and
// applies the command stream. aki speaks the Redis replication protocol so it can
// replicate to and from real Redis and to and from itself.

// replBacklog is the circular buffer of recently propagated replication bytes. A
// reconnecting replica can resume from its offset without a full resync as long
// as its offset still falls inside the window the backlog holds.
type replBacklog struct {
	buf      []byte
	off      int64 // master_repl_offset of the oldest valid byte
	histlen  int64 // number of valid bytes currently in buf
	writeCur int64 // running write position, taken mod len(buf)
}

func newReplBacklog(size int, startOff int64) *replBacklog {
	if size <= 0 {
		size = 1 << 20
	}
	return &replBacklog{buf: make([]byte, size), off: startOff}
}

// feed appends data to the ring, advancing the base offset once the buffer is
// full and old bytes start being overwritten.
func (b *replBacklog) feed(data []byte) {
	n := int64(len(b.buf))
	for _, c := range data {
		b.buf[b.writeCur%n] = c
		b.writeCur++
		if b.histlen < n {
			b.histlen++
		} else {
			b.off++
		}
	}
}

// copyFrom returns the backlog bytes from startOff to the current end, used to
// serve a partial resync. The bool is false when startOff falls outside the
// window the backlog still holds, in which case the master must fall back to a
// full resync. startOff is in the same offset space as master_repl_offset: it is
// the offset of the first byte the replica still needs. The caller holds repl.mu.
func (b *replBacklog) copyFrom(startOff int64) ([]byte, bool) {
	if startOff < b.off || startOff > b.off+b.histlen {
		return nil, false
	}
	n := int64(len(b.buf))
	rel := startOff - b.off
	length := b.histlen - rel
	out := make([]byte, length)
	// The oldest retained byte sits at writeCur-histlen in the running write
	// counter, so the byte at startOff is rel positions further along.
	start := b.writeCur - b.histlen + rel
	for i := int64(0); i < length; i++ {
		out[i] = b.buf[(start+i)%n]
	}
	return out, true
}

// replicaHandle is the master's record of one connected replica.
type replicaHandle struct {
	conn      *networking.Conn
	addr      string
	port      int
	ackOffset int64
	ackTime   time.Time
	state     string // "online"
}

// replState holds all replication bookkeeping for one instance, both its master
// role (serving replicas) and its replica role (following a master).
type replState struct {
	mu sync.Mutex
	on atomic.Bool // a backlog exists, so writes must be sequenced and propagated

	// Master role.
	replid       string
	replid2      string
	offset       int64 // master_repl_offset
	secondOffset int64
	backlog      *replBacklog
	replicas     map[uint64]*replicaHandle
	lastDB       int // last SELECT propagated, -1 forces one

	// Replica role.
	role         string // "master" or "slave"
	masterHost   string
	masterPort   int
	link         string // "connect", "connecting", "sync", "connected"
	slaveOff     int64
	masterReplid string
	stop         chan struct{}
	loopDone     chan struct{} // closed when the replica client goroutine exits
	gen          int

	// Manual failover (the FAILOVER command). foActive is true while a coordinated
	// handoff runs; foStop cancels it when FAILOVER ABORT is issued.
	foActive bool
	foStop   chan struct{}

	lastPing time.Time
}

// replInit sets the replication defaults when the dispatcher is built.
func (d *Dispatcher) replInit() {
	d.repl.replid = d.runID
	d.repl.replid2 = strings.Repeat("0", 40)
	d.repl.secondOffset = -1
	d.repl.lastDB = -1
	d.repl.role = "master"
	d.roleMaster.Store(true)
	d.repl.link = "connect"
	d.repl.replicas = map[uint64]*replicaHandle{}
}

// propagateRepl appends one write command to the backlog and streams it to every
// connected replica. The caller holds repl.mu (runCommand takes it for writes
// while replication is active). A SELECT is emitted first when the database
// changed, matching the Redis propagation format.
func (d *Dispatcher) propagateRepl(db int, argv [][]byte) {
	if d.repl.backlog == nil {
		return
	}
	var buf []byte
	if db != d.repl.lastDB {
		buf = appendRESPCommand(buf, [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))})
		d.repl.lastDB = db
	}
	buf = appendRESPCommand(buf, argv)
	d.feedStream(buf)
}

// feedStream advances the offset, fills the backlog, and delivers the bytes to
// every replica. The caller holds repl.mu.
func (d *Dispatcher) feedStream(buf []byte) {
	d.repl.offset += int64(len(buf))
	d.repl.backlog.feed(buf)
	for id, h := range d.repl.replicas {
		if err := h.conn.Deliver(buf); err != nil {
			delete(d.repl.replicas, id)
		}
	}
}

// replActive reports whether writes must be sequenced through the replication
// lock and propagated, which is true once the backlog has been created.
func (d *Dispatcher) replActive() bool { return d.repl.on.Load() }

// ensureBacklog creates the backlog on first replica attach and turns on write
// sequencing. The caller holds repl.mu.
func (d *Dispatcher) ensureBacklog() {
	if d.repl.backlog != nil {
		return
	}
	size := int(d.confInt("repl-backlog-size", 1<<20))
	d.repl.backlog = newReplBacklog(size, d.repl.offset)
	d.repl.on.Store(true)
}

// handlePsync serves PSYNC and SYNC. A PSYNC carrying a known replid and an
// offset still inside the backlog window is answered with +CONTINUE and the
// missing bytes (a partial resync). Otherwise it falls back to a full resync:
// +FULLRESYNC, the current dataset as an RDB payload, and registration as a
// replica so future writes stream to it. SYNC is always a full resync.
func (d *Dispatcher) handlePsync(ctx *Ctx, psync bool) {
	if d.engine == nil {
		ctx.enc().WriteError("ERR This instance has no keyspace to replicate")
		return
	}
	if psync && len(ctx.Argv) >= 3 {
		reqReplid := string(ctx.Argv[1])
		reqOff, ok := parseInteger(ctx.Argv[2])
		if ok && reqReplid != "?" && reqOff >= 0 && d.tryPartialResync(ctx, reqReplid, reqOff) {
			return
		}
	}
	d.fullResync(ctx, psync)
}

// tryPartialResync attempts a +CONTINUE resume from reqOff. It succeeds only when
// a backlog exists, the replica's replid matches the current replid (or the
// previous replid within its failover window), and reqOff still falls inside the
// backlog. On success it writes +CONTINUE, the backlog tail, and registers the
// replica. It reports whether it handled the request.
func (d *Dispatcher) tryPartialResync(ctx *Ctx, reqReplid string, reqOff int64) bool {
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	if d.repl.backlog == nil {
		return false
	}
	// The replid in +CONTINUE is always the current replid. A replica that was
	// following the old master before a failover presents replid2; we bridge it as
	// long as its offset is within the second-replid window.
	switch {
	case reqReplid == d.repl.replid:
	case reqReplid == d.repl.replid2 && d.repl.secondOffset >= 0 && reqOff <= d.repl.secondOffset:
	default:
		return false
	}
	data, ok := d.repl.backlog.copyFrom(reqOff)
	if !ok {
		return false
	}
	hdr := append([]byte("+CONTINUE "+d.repl.replid+"\r\n"), data...)
	if err := ctx.Conn.Deliver(hdr); err != nil {
		return false
	}
	d.repl.replicas[ctx.Conn.ID()] = &replicaHandle{
		conn:      ctx.Conn,
		addr:      replicaIP(ctx.Conn.RemoteAddr()),
		port:      ctx.replPort(),
		ackOffset: reqOff + int64(len(data)),
		ackTime:   time.Now(),
		state:     "online",
	}
	if sess, ok := ctx.Conn.Session().(*session); ok {
		sess.isReplica = true
	}
	return true
}

// fullResync sends a full snapshot: +FULLRESYNC (for PSYNC), the RDB payload, and
// registers the connection so later writes stream to it.
func (d *Dispatcher) fullResync(ctx *Ctx, psync bool) {
	snap, err := d.snapshotForRDB()
	if err != nil {
		ctx.enc().WriteError("ERR Failed to produce snapshot for SYNC")
		return
	}
	blob, err := rdb.MarshalFile(snap)
	if err != nil {
		ctx.enc().WriteError("ERR Failed to serialize snapshot for SYNC")
		return
	}

	d.repl.mu.Lock()
	d.ensureBacklog()
	startOff := d.repl.offset
	replid := d.repl.replid
	var hdr []byte
	if psync {
		hdr = append(hdr, []byte("+FULLRESYNC "+replid+" "+strconv.FormatInt(startOff, 10)+"\r\n")...)
	}
	hdr = append(hdr, []byte("$"+strconv.Itoa(len(blob))+"\r\n")...)
	hdr = append(hdr, blob...)

	h := &replicaHandle{
		conn:      ctx.Conn,
		addr:      replicaIP(ctx.Conn.RemoteAddr()),
		port:      ctx.replPort(),
		ackOffset: startOff,
		ackTime:   time.Now(),
		state:     "online",
	}
	if err := ctx.Conn.Deliver(hdr); err != nil {
		d.repl.mu.Unlock()
		return
	}
	d.repl.replicas[ctx.Conn.ID()] = h
	if sess, ok := ctx.Conn.Session().(*session); ok {
		sess.isReplica = true
	}
	d.repl.mu.Unlock()
}

// handleReplconf serves the REPLCONF sub-commands used during the handshake and
// the live stream: listening-port and capa during setup, ACK from a replica
// reporting its offset, and GETACK asking a replica to ACK now.
func (d *Dispatcher) handleReplconf(ctx *Ctx) {
	if len(ctx.Argv) < 2 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'replconf' command")
		return
	}
	sub := strings.ToLower(string(ctx.Argv[1]))
	switch sub {
	case "listening-port":
		if len(ctx.Argv) >= 3 {
			if p, ok := parseInteger(ctx.Argv[2]); ok {
				if sess, ok := ctx.Conn.Session().(*session); ok {
					sess.replListenPort = int(p)
				}
			}
		}
		ctx.Conn.WriteRaw(resp.ReplyOK)
	case "capa", "ip-address", "client-id", "version", "rdb-filter-only", "rdb-only":
		ctx.Conn.WriteRaw(resp.ReplyOK)
	case "ack":
		if len(ctx.Argv) >= 3 {
			if off, ok := parseInteger(ctx.Argv[2]); ok {
				d.repl.mu.Lock()
				if h, ok := d.repl.replicas[ctx.Conn.ID()]; ok {
					h.ackOffset = off
					h.ackTime = time.Now()
				}
				d.repl.mu.Unlock()
			}
		}
		// ACK gets no reply.
	case "getack":
		// A master sends GETACK to replicas; a replica answers with ACK. When a
		// client sends it to us we have nothing to do.
	default:
		ctx.Conn.WriteRaw(resp.ReplyOK)
	}
}

// replPort returns the listening port the replica announced with REPLCONF
// listening-port, or the connection's remote port if it never announced one.
func (ctx *Ctx) replPort() int {
	if sess, ok := ctx.Conn.Session().(*session); ok && sess.replListenPort != 0 {
		return sess.replListenPort
	}
	return remotePort(ctx.Conn.RemoteAddr())
}

func replicaIP(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

func remotePort(addr string) int {
	if _, p, err := net.SplitHostPort(addr); err == nil {
		if n, err := strconv.Atoi(p); err == nil {
			return n
		}
	}
	return 0
}

// handleReplicaOf serves REPLICAOF and SLAVEOF. "NO ONE" promotes this instance
// to a standalone master; "host port" tears down any current replication and
// starts following the named master.
func (d *Dispatcher) handleReplicaOf(ctx *Ctx) {
	host := string(ctx.Argv[1])
	portArg := string(ctx.Argv[2])
	if strings.EqualFold(host, "no") && strings.EqualFold(portArg, "one") {
		d.promoteToMaster()
		ctx.Conn.WriteRaw(resp.ReplyOK)
		return
	}
	port, err := strconv.Atoi(portArg)
	if err != nil || port < 0 || port > 65535 {
		ctx.enc().WriteError("ERR Invalid master port")
		return
	}

	d.repl.mu.Lock()
	if d.repl.role == "slave" && d.repl.masterHost == host && d.repl.masterPort == port {
		d.repl.mu.Unlock()
		ctx.Conn.WriteRaw(resp.ReplyOK)
		return
	}
	d.repl.mu.Unlock()

	d.startFollowing(host, port)
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// startFollowing tears down any current replica link and starts following the
// named master, spawning the replica client goroutine. It is the shared core of
// REPLICAOF host port and the demotion leg of a manual failover.
func (d *Dispatcher) startFollowing(host string, port int) {
	d.repl.mu.Lock()
	d.stopReplicaLocked()
	d.repl.role = "slave"
	d.roleMaster.Store(false)
	d.repl.masterHost = host
	d.repl.masterPort = port
	d.repl.link = "connect"
	d.repl.gen++
	gen := d.repl.gen
	d.repl.stop = make(chan struct{})
	stop := d.repl.stop
	d.repl.loopDone = make(chan struct{})
	loopDone := d.repl.loopDone
	d.repl.mu.Unlock()

	go d.replicaClientLoop(gen, stop, loopDone, host, port)
}

// promoteToMaster stops following any master and becomes a fresh replication
// origin. The old replid is remembered as replid2 so existing sub-replicas could
// resume across the promotion under psync2.
func (d *Dispatcher) promoteToMaster() {
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	d.stopReplicaLocked()
	if d.repl.role == "slave" {
		d.repl.replid2 = d.repl.replid
		d.repl.secondOffset = d.repl.offset + 1
		d.repl.replid = newRunID()
	}
	d.repl.role = "master"
	d.roleMaster.Store(true)
	d.repl.masterHost = ""
	d.repl.masterPort = 0
	d.repl.link = "connect"
}

// stopReplicaLocked signals the current replica client goroutine to exit. The
// caller holds repl.mu.
func (d *Dispatcher) stopReplicaLocked() {
	if d.repl.stop != nil {
		close(d.repl.stop)
		d.repl.stop = nil
	}
	// The exiting goroutine owns closing loopDone, so just drop our reference.
	// Anyone who needs to join captured the channel before unlocking.
	d.repl.loopDone = nil
}

// StopReplication signals the replica client goroutine to exit and waits for it
// to return. It must run before the keyspace and pager close on shutdown, so the
// apply loop never touches a closed pager. Safe to call on a master: it returns
// at once when no replica link is active.
func (d *Dispatcher) StopReplication() {
	d.repl.mu.Lock()
	loopDone := d.repl.loopDone
	if d.repl.stop != nil {
		close(d.repl.stop)
		d.repl.stop = nil
	}
	d.repl.loopDone = nil
	d.repl.mu.Unlock()

	if loopDone != nil {
		<-loopDone
	}
}

// isReadonlyReplica reports whether external writes must be refused because this
// instance is a read-only replica.
func (d *Dispatcher) isReadonlyReplica() bool {
	d.repl.mu.Lock()
	slave := d.repl.role == "slave"
	d.repl.mu.Unlock()
	if !slave {
		return false
	}
	return !strings.EqualFold(d.confValue("replica-read-only", "yes"), "no")
}

// replicaClientLoop is the replica-side driver. It connects to the master, runs
// the handshake, loads the RDB, and applies the command stream, retrying on
// failure until REPLICAOF changes the target or the server stops.
func (d *Dispatcher) replicaClientLoop(gen int, stop, loopDone chan struct{}, host string, port int) {
	defer close(loopDone)
	for {
		select {
		case <-stop:
			return
		default:
		}
		if d.currentGen() != gen {
			return
		}
		err := d.replicaSession(gen, stop, host, port)
		if d.currentGen() != gen {
			return
		}
		_ = err
		d.setLink(gen, "connect")
		select {
		case <-stop:
			return
		case <-time.After(time.Second):
		}
	}
}

func (d *Dispatcher) currentGen() int {
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	return d.repl.gen
}

func (d *Dispatcher) setLink(gen int, link string) {
	d.repl.mu.Lock()
	if d.repl.gen == gen {
		d.repl.link = link
	}
	d.repl.mu.Unlock()
}

// replicaSession runs one full attempt: connect, handshake, RDB load, apply loop.
// It returns when the connection drops or the generation changes.
func (d *Dispatcher) replicaSession(gen int, stop chan struct{}, host string, port int) error {
	d.setLink(gen, "connecting")
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 5*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-stop:
			_ = conn.Close()
		case <-done:
		}
	}()

	br := bufio.NewReader(conn)
	if err := d.replHandshake(conn, br); err != nil {
		return err
	}
	d.setLink(gen, "sync")
	continued, startOff, err := d.replSendPsync(conn, br)
	if err != nil {
		return err
	}
	if !continued {
		if err := d.replLoadRDB(br); err != nil {
			return err
		}
		d.repl.mu.Lock()
		d.repl.slaveOff = startOff
		d.repl.offset = startOff
		d.repl.mu.Unlock()
	}
	d.setLink(gen, "connected")

	return d.replApplyLoop(gen, conn, br)
}

// replHandshake sends AUTH (when masterauth is set), REPLCONF listening-port and
// REPLCONF capa, reading the master's reply to each.
func (d *Dispatcher) replHandshake(conn net.Conn, br *bufio.Reader) error {
	if auth := d.confValue("masterauth", ""); auth != "" {
		user := d.confValue("masteruser", "")
		if user != "" {
			if err := replCommand(conn, br, "AUTH", user, auth); err != nil {
				return err
			}
		} else if err := replCommand(conn, br, "AUTH", auth); err != nil {
			return err
		}
	}
	if err := replCommand(conn, br, "PING"); err != nil {
		return err
	}
	if err := replCommand(conn, br, "REPLCONF", "listening-port", strconv.Itoa(d.listenPort())); err != nil {
		return err
	}
	if err := replCommand(conn, br, "REPLCONF", "capa", "eof", "capa", "psync2"); err != nil {
		return err
	}
	return nil
}

// replSendPsync sends PSYNC and parses the master's decision. On the first
// connect, or when no usable cached replid is held, it sends PSYNC ? -1 and a
// +FULLRESYNC follows. On reconnect it presents the cached replid and the next
// offset it needs; the master answers +CONTINUE to resume from the backlog or
// +FULLRESYNC to start over. continued is true for the +CONTINUE case, in which
// the caller skips the RDB load and keeps its current offset.
func (d *Dispatcher) replSendPsync(conn net.Conn, br *bufio.Reader) (continued bool, startOff int64, err error) {
	d.repl.mu.Lock()
	cachedReplid := d.repl.masterReplid
	cachedOff := d.repl.slaveOff
	d.repl.mu.Unlock()

	replid, off := "?", "-1"
	if cachedReplid != "" && cachedReplid != strings.Repeat("0", 40) {
		// slaveOff is the offset just past the last byte applied, which is exactly
		// the first byte still needed, so it goes on the wire as-is.
		replid, off = cachedReplid, strconv.FormatInt(cachedOff, 10)
	}
	if err := writeRESPCommand(conn, "PSYNC", replid, off); err != nil {
		return false, 0, err
	}
	line, err := readLine(br)
	if err != nil {
		return false, 0, err
	}
	fields := strings.Fields(strings.TrimPrefix(line, "+"))
	if len(fields) == 0 {
		return false, 0, errString("unexpected PSYNC reply: " + line)
	}
	switch {
	case strings.EqualFold(fields[0], "CONTINUE"):
		if len(fields) >= 2 {
			d.repl.mu.Lock()
			d.repl.masterReplid = fields[1]
			d.repl.replid = fields[1]
			d.repl.mu.Unlock()
		}
		return true, 0, nil
	case strings.EqualFold(fields[0], "FULLRESYNC") && len(fields) >= 3:
		d.repl.mu.Lock()
		d.repl.masterReplid = fields[1]
		d.repl.replid = fields[1]
		d.repl.mu.Unlock()
		startOff, _ = strconv.ParseInt(fields[2], 10, 64)
		return false, startOff, nil
	}
	return false, 0, errString("unexpected PSYNC reply: " + line)
}

// replLoadRDB reads the RDB bulk payload that follows +FULLRESYNC and loads it
// into the keyspace, flushing whatever was there first. It handles both the
// length-prefixed form and the EOF-marker diskless form.
func (d *Dispatcher) replLoadRDB(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "$") {
		return errString("expected RDB bulk, got: " + line)
	}
	var blob []byte
	if strings.HasPrefix(line, "$EOF:") {
		marker := []byte(strings.TrimPrefix(line, "$EOF:"))
		blob, err = readUntilMarker(br, marker)
		if err != nil {
			return err
		}
	} else {
		n, perr := strconv.Atoi(strings.TrimPrefix(line, "$"))
		if perr != nil {
			return perr
		}
		blob = make([]byte, n)
		if _, err := readFull(br, blob); err != nil {
			return err
		}
	}
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		return err
	}
	if err := d.flushAllDatabases(); err != nil {
		return err
	}
	if err := d.engine.updateKeyspace(func(ks *keyspace.Keyspace) error {
		_, lerr := LoadSnapshot(ks, snap, -1, true)
		return lerr
	}); err != nil {
		return err
	}
	// The master's snapshot carries its function libraries in FUNCTION2 records.
	// Replace whatever this replica had so the two nodes agree on the functions.
	d.functions.mu.Lock()
	d.functions.libs = map[string]*funcLib{}
	d.functions.fnIndex = map[string]string{}
	d.functions.mu.Unlock()
	d.loadFunctionLibraries(snap.Functions)
	return nil
}

// replApplyLoop reads the command stream from the master, applies each command,
// advances the replica offset by the raw byte length, and sends a REPLCONF ACK
// every second and whenever the master asks with GETACK.
func (d *Dispatcher) replApplyLoop(gen int, conn net.Conn, br *bufio.Reader) error {
	apply := networking.NewOfflineConn()
	sess := &session{authenticated: true, fromMaster: true}
	apply.SetSession(sess)
	ctx := &Ctx{Conn: apply, d: d, sess: sess}

	var writeMu sync.Mutex
	ackStop := make(chan struct{})
	defer close(ackStop)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ackStop:
				return
			case <-t.C:
				d.repl.mu.Lock()
				off := d.repl.slaveOff
				d.repl.mu.Unlock()
				writeMu.Lock()
				_ = writeRESPCommand(conn, "REPLCONF", "ACK", strconv.FormatInt(off, 10))
				writeMu.Unlock()
			}
		}
	}()

	buf := make([]byte, 0, 16384)
	tmp := make([]byte, 16384)
	pos := 0
	for {
		if d.currentGen() != gen {
			return nil
		}
		for {
			val, next, err := resp.Decode(buf, pos)
			if errors.Is(err, resp.ErrNeedMore) {
				break
			}
			if err != nil {
				return err
			}
			raw := next - pos
			pos = next
			d.applyFromMaster(ctx, sess, val, raw, conn, &writeMu)
		}
		if pos > 0 {
			buf = append(buf[:0], buf[pos:]...)
			pos = 0
		}
		nr, err := br.Read(tmp)
		if nr > 0 {
			buf = append(buf, tmp[:nr]...)
		}
		if err != nil {
			return err
		}
	}
}

// applyFromMaster handles one decoded command from the replication stream. SELECT
// switches the apply database, PING and REPLCONF advance the offset only, GETACK
// triggers an immediate ACK, and every other command is dispatched against the
// keyspace.
func (d *Dispatcher) applyFromMaster(ctx *Ctx, sess *session, val resp.RESPValue, raw int, conn net.Conn, writeMu *sync.Mutex) {
	d.repl.mu.Lock()
	d.repl.slaveOff += int64(raw)
	d.repl.offset = d.repl.slaveOff
	off := d.repl.slaveOff
	d.repl.mu.Unlock()

	if val.Type != resp.TypeArray || len(val.Elems) == 0 {
		return
	}
	argv := make([][]byte, len(val.Elems))
	for i, e := range val.Elems {
		argv[i] = e.Str
	}
	name := strings.ToLower(string(argv[0]))
	switch name {
	case "ping":
		return
	case "select":
		if len(argv) >= 2 {
			if db, ok := parseInteger(argv[1]); ok {
				ctx.Conn.SetDB(int(db))
			}
		}
		return
	case "replconf":
		if len(argv) >= 2 && strings.EqualFold(string(argv[1]), "getack") {
			writeMu.Lock()
			_ = writeRESPCommand(conn, "REPLCONF", "ACK", strconv.FormatInt(off, 10))
			writeMu.Unlock()
		}
		return
	}
	cmd, err := d.table.lookup(name, argv)
	if err != nil || !checkArity(cmd, len(argv)) {
		return
	}
	ctx.Argv = argv
	d.runCommand(ctx, cmd)
}

// handleWait serves WAIT numreplicas timeout. It blocks until at least
// numreplicas replicas have acknowledged the master offset reached at call time,
// or the timeout elapses, then replies with the number that did. A timeout of 0
// means wait forever. Like real Redis it keeps waiting for the whole timeout even
// when fewer replicas are connected, since a replica may attach during the wait.
func (d *Dispatcher) handleWait(ctx *Ctx) {
	numReplicas, ok := parseInteger(ctx.Argv[1])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	timeoutMs, ok := parseInteger(ctx.Argv[2])
	if !ok || timeoutMs < 0 {
		ctx.enc().WriteError("ERR timeout is not an integer or out of range")
		return
	}

	ctx.enc().WriteInteger(int64(d.waitReplicas(numReplicas, timeoutMs)))
}

// handleWaitAOF implements WAITAOF numlocal numreplicas timeout (Redis 7.2). It
// waits until the local copy is durable (numlocal, the WAL/AOF fsynced) and until
// numreplicas replicas have acknowledged, then replies with a two-element array
// [local_acked, replicas_acked]. aki has a single local copy, so numlocal is 0 or
// 1.
func (d *Dispatcher) handleWaitAOF(ctx *Ctx) {
	numLocal, ok := parseInteger(ctx.Argv[1])
	if !ok || numLocal < 0 || numLocal > 1 {
		ctx.enc().WriteError("ERR WAITAOF numlocal must be 0 or 1.")
		return
	}
	numReplicas, ok := parseInteger(ctx.Argv[2])
	if !ok || numReplicas < 0 {
		ctx.enc().WriteError("ERR value is out of range, must be positive")
		return
	}
	timeoutMs, ok := parseInteger(ctx.Argv[3])
	if !ok || timeoutMs < 0 {
		ctx.enc().WriteError("ERR timeout is not an integer or out of range")
		return
	}

	if !d.aofEnabled() && numLocal != 0 {
		ctx.enc().WriteError("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
		return
	}

	localAcked := 0
	if numLocal >= 1 {
		if d.forceSyncAOF() {
			localAcked = 1
		}
	}

	replicasAcked := 0
	if numReplicas > 0 {
		replicasAcked = d.waitReplicas(int64(numReplicas), timeoutMs)
	}

	ctx.enc().WriteArrayLen(2)
	ctx.enc().WriteInteger(int64(localAcked))
	ctx.enc().WriteInteger(int64(replicasAcked))
}

// waitReplicas blocks until at least numReplicas replicas acknowledge the current
// offset or the timeout passes, and returns the count reached. It is the shared
// core behind WAIT and the replica leg of WAITAOF.
func (d *Dispatcher) waitReplicas(numReplicas, timeoutMs int64) int {
	d.repl.mu.Lock()
	target := d.repl.offset
	d.repl.mu.Unlock()

	if n := d.countReplicasAtOffset(target); int64(n) >= numReplicas {
		return n
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	lastAck := time.Time{}
	for {
		now := time.Now()
		if now.Sub(lastAck) >= 100*time.Millisecond {
			d.broadcastGetAck()
			lastAck = now
		}
		n := d.countReplicasAtOffset(target)
		if int64(n) >= numReplicas {
			return n
		}
		if timeoutMs > 0 && time.Now().After(deadline) {
			return n
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// listenPort returns the TCP port this server listens on, for REPLCONF
// listening-port. It falls back to the configured port and then 0.
func (d *Dispatcher) listenPort() int {
	if d.srv != nil {
		if a := d.srv.Addr(); a != nil {
			if p := remotePort(a.String()); p != 0 {
				return p
			}
		}
	}
	return int(d.confInt("port", 0))
}

// countReplicasAtOffset returns how many replicas have acknowledged at least the
// given offset, used by WAIT.
func (d *Dispatcher) countReplicasAtOffset(target int64) int {
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	n := 0
	for _, h := range d.repl.replicas {
		if h.ackOffset >= target {
			n++
		}
	}
	return n
}

// isReplica reports whether this instance is following a master.
func (d *Dispatcher) isReplica() bool {
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	return d.repl.role == "slave"
}

// denyStaleData reports whether a command must be refused because this replica
// has lost its master link and replica-serve-stale-data is off. Redis turns away
// every command except the ones flagged stale-safe (INFO, CONFIG, the connection
// and pub/sub commands) so a client cannot read data that may have drifted from
// the master. The default is yes, which serves the stale data, so the gate is off
// unless the operator turned it off. A master is never affected.
func (d *Dispatcher) denyStaleData(cmd *CmdDesc) bool {
	if cmd.Flags.Has(FlagStale) {
		return false
	}
	if d.confBool("replica-serve-stale-data", true) {
		return false
	}
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	return d.repl.role == "slave" && d.repl.link != "connected"
}

// goodReplicaCount returns how many connected replicas acked within maxLag, the
// "good replica" notion min-replicas-to-write counts against. A replica that has
// gone quiet longer than maxLag does not count.
func (d *Dispatcher) goodReplicaCount(maxLag time.Duration) int {
	now := time.Now()
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	n := 0
	for _, h := range d.repl.replicas {
		if now.Sub(h.ackTime) <= maxLag {
			n++
		}
	}
	return n
}

// enoughGoodReplicas reports whether a write may proceed under
// min-replicas-to-write and min-replicas-max-lag. The gate is off unless both
// are positive, matching Redis, and a replica never applies it since its master
// already enforced the rule before sending the write.
func (d *Dispatcher) enoughGoodReplicas() bool {
	minReplicas := int(d.confInt("min-replicas-to-write", 0))
	maxLag := int(d.confInt("min-replicas-max-lag", 10))
	if minReplicas <= 0 || maxLag <= 0 {
		return true
	}
	if d.isReplica() {
		return true
	}
	return d.goodReplicaCount(time.Duration(maxLag)*time.Second) >= minReplicas
}

// broadcastGetAck asks every replica to send a REPLCONF ACK now, so WAIT sees
// fresh offsets rather than waiting for the one-second ACK tick.
func (d *Dispatcher) broadcastGetAck() {
	getack := appendRESPCommand(nil, [][]byte{[]byte("REPLCONF"), []byte("GETACK"), []byte("*")})
	d.repl.mu.Lock()
	if d.repl.backlog != nil {
		d.feedStream(getack)
	}
	d.repl.mu.Unlock()
}

// replPingReplicas sends a PING down the stream to keep replica links alive. It
// runs from the background cron.
func (d *Dispatcher) replPingReplicas() {
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	if d.repl.backlog == nil || len(d.repl.replicas) == 0 {
		return
	}
	period := time.Duration(d.confInt("repl-ping-replica-period", 10)) * time.Second
	if period <= 0 {
		period = 10 * time.Second
	}
	now := time.Now()
	if !d.repl.lastPing.IsZero() && now.Sub(d.repl.lastPing) < period {
		return
	}
	d.repl.lastPing = now
	d.feedStream(appendRESPCommand(nil, [][]byte{[]byte("PING")}))
}

// dropReplica removes a replica record when its connection closes.
func (d *Dispatcher) dropReplica(id uint64) {
	d.repl.mu.Lock()
	delete(d.repl.replicas, id)
	d.repl.mu.Unlock()
}

// writeRESPCommand encodes a command as a RESP array and writes it to conn.
func writeRESPCommand(conn net.Conn, parts ...string) error {
	argv := make([][]byte, len(parts))
	for i, p := range parts {
		argv[i] = []byte(p)
	}
	_, err := conn.Write(appendRESPCommand(nil, argv))
	return err
}

// replCommand sends a command and reads one reply line, returning an error when
// the master answers with a RESP error. It is used for the handshake steps whose
// reply is a simple status.
func replCommand(conn net.Conn, br *bufio.Reader, parts ...string) error {
	if err := writeRESPCommand(conn, parts...); err != nil {
		return err
	}
	line, err := readLine(br)
	if err != nil {
		return err
	}
	if strings.HasPrefix(line, "-") {
		return errString(strings.TrimPrefix(line, "-"))
	}
	return nil
}

// readLine reads one CRLF-terminated line and returns it without the CRLF.
func readLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// readFull reads exactly len(p) bytes into p.
func readFull(br *bufio.Reader, p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := br.Read(p[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// readUntilMarker reads bytes until it sees the EOF marker that terminates a
// diskless RDB transfer, returning the payload before the marker.
func readUntilMarker(br *bufio.Reader, marker []byte) ([]byte, error) {
	var out []byte
	for {
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		out = append(out, b)
		if len(out) >= len(marker) {
			tail := out[len(out)-len(marker):]
			if string(tail) == string(marker) {
				return out[:len(out)-len(marker)], nil
			}
		}
	}
}
