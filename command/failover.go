package command

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/resp"
)

// This file implements the FAILOVER command (spec 2064 doc 18 section 12): a
// coordinated, manual master/replica handoff. The master picks a target replica
// (the one named with TO, or the one with the highest acked offset), waits for it
// to catch up unless FORCE is given, promotes it over a control connection, then
// demotes itself to follow the new master.
//
// aki has no client write-pause machinery, so writes are not frozen during the
// wait the way real Redis freezes them. Under a steady write load the target may
// never reach the master offset and the failover times out and aborts, which is
// the safe outcome. With writes quiesced the handoff completes cleanly. FORCE
// skips the wait entirely.

// failoverDefaultTimeoutMs is the catch-up wait used when FAILOVER is issued
// without an explicit TIMEOUT.
const failoverDefaultTimeoutMs = 5000

// handleFailover parses FAILOVER [TO host port] [FORCE] [ABORT] [TIMEOUT ms],
// validates the request, and starts the handoff in the background. It replies
// when the failover is initiated, not when it completes.
func (d *Dispatcher) handleFailover(ctx *Ctx) {
	var (
		toSet      bool
		toHost     string
		toPort     int
		force      bool
		abort      bool
		timeoutMs  int64 = failoverDefaultTimeoutMs
		timeoutSet bool
	)
	argv := ctx.Argv
	for i := 1; i < len(argv); i++ {
		switch strings.ToUpper(string(argv[i])) {
		case "TO":
			if i+2 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			toHost = string(argv[i+1])
			p, err := strconv.Atoi(string(argv[i+2]))
			if err != nil || p < 0 || p > 65535 {
				ctx.enc().WriteError("ERR Invalid target port")
				return
			}
			toPort = p
			toSet = true
			i += 2
		case "FORCE":
			force = true
		case "ABORT":
			abort = true
		case "TIMEOUT":
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			ms, err := strconv.ParseInt(string(argv[i+1]), 10, 64)
			if err != nil || ms < 0 {
				ctx.enc().WriteError("ERR Invalid timeout")
				return
			}
			timeoutMs = ms
			timeoutSet = true
			i++
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	if abort {
		if toSet || force || timeoutSet {
			ctx.enc().WriteError("ERR FAILOVER ABORT is not valid with other arguments.")
			return
		}
		d.repl.mu.Lock()
		if !d.repl.foActive {
			d.repl.mu.Unlock()
			ctx.enc().WriteError("ERR No failover in progress.")
			return
		}
		if d.repl.foStop != nil {
			close(d.repl.foStop)
			d.repl.foStop = nil
		}
		d.repl.foActive = false
		d.repl.mu.Unlock()
		ctx.Conn.WriteRaw(resp.ReplyOK)
		return
	}
	if timeoutMs == 0 {
		timeoutMs = failoverDefaultTimeoutMs
	}

	d.repl.mu.Lock()
	if d.repl.foActive {
		d.repl.mu.Unlock()
		ctx.enc().WriteError("ERR FAILOVER already in progress.")
		return
	}
	if d.repl.role == "slave" || len(d.repl.replicas) == 0 {
		d.repl.mu.Unlock()
		ctx.enc().WriteError("ERR FAILOVER requires connected replicas.")
		return
	}
	var target *replicaHandle
	if toSet {
		for _, h := range d.repl.replicas {
			if strings.EqualFold(h.addr, toHost) && h.port == toPort {
				target = h
				break
			}
		}
		if target == nil {
			d.repl.mu.Unlock()
			ctx.enc().WriteError("ERR FAILOVER target " + toHost + " " + strconv.Itoa(toPort) + " is not a replica.")
			return
		}
	} else {
		var best int64 = -1
		for _, h := range d.repl.replicas {
			if h.ackOffset > best {
				best = h.ackOffset
				target = h
			}
		}
	}
	targetHost := target.addr
	targetPort := target.port
	masterOffset := d.repl.offset
	stop := make(chan struct{})
	d.repl.foActive = true
	d.repl.foStop = stop
	d.repl.mu.Unlock()

	ctx.Conn.WriteRaw(resp.ReplyOK)
	go d.runFailover(targetHost, targetPort, masterOffset, timeoutMs, force, stop)
}

// runFailover drives the handoff after FAILOVER replies. Without FORCE it waits
// for the target to reach the master offset, then promotes the target and demotes
// this node. A timeout or an ABORT leaves this node as master.
func (d *Dispatcher) runFailover(host string, port int, masterOffset, timeoutMs int64, force bool, stop chan struct{}) {
	defer d.clearFailover(stop)

	if !force {
		if !d.waitReplicaOffset(host, port, masterOffset, timeoutMs, stop) {
			d.logWarning("failover aborted: target did not catch up in time")
			return
		}
	}
	select {
	case <-stop:
		return
	default:
	}
	if err := d.promoteReplica(host, port); err != nil {
		d.logWarning("failover aborted: promoting target failed: " + err.Error())
		return
	}
	d.startFollowing(host, port)
	d.logNotice("failover complete: now following " + net.JoinHostPort(host, strconv.Itoa(port)))
}

// waitReplicaOffset blocks until the named replica acknowledges at least target,
// or the timeout passes, or the failover is aborted. It nudges replicas with
// GETACK so fresh offsets arrive without waiting for the one-second ACK tick.
func (d *Dispatcher) waitReplicaOffset(host string, port int, target, timeoutMs int64, stop chan struct{}) bool {
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	lastAck := time.Time{}
	for {
		select {
		case <-stop:
			return false
		default:
		}
		if off, ok := d.replicaAck(host, port); ok && off >= target {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		now := time.Now()
		if now.Sub(lastAck) >= 100*time.Millisecond {
			d.broadcastGetAck()
			lastAck = now
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// replicaAck returns the acked offset of the replica at host:port and whether it
// is still connected.
func (d *Dispatcher) replicaAck(host string, port int) (int64, bool) {
	d.repl.mu.Lock()
	defer d.repl.mu.Unlock()
	for _, h := range d.repl.replicas {
		if strings.EqualFold(h.addr, host) && h.port == port {
			return h.ackOffset, true
		}
	}
	return 0, false
}

// promoteReplica opens a short-lived control connection to the target replica and
// sends REPLICAOF NO ONE so it becomes a master.
func (d *Dispatcher) promoteReplica(host string, port int) error {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 5*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	return replCommand(conn, br, "REPLICAOF", "NO", "ONE")
}

// clearFailover marks the failover finished, but only if the stop channel still
// matches, so a later FAILOVER started after an ABORT is not cleared by the old
// goroutine.
func (d *Dispatcher) clearFailover(stop chan struct{}) {
	d.repl.mu.Lock()
	if d.repl.foStop == stop {
		d.repl.foActive = false
		d.repl.foStop = nil
	}
	d.repl.mu.Unlock()
}
