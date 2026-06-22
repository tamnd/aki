package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/resp"
)

// This file implements the SENTINEL command family (spec 2064 doc 18 section 29).
// aki is not a Sentinel, but it answers the SENTINEL subcommands deeply enough
// that a Sentinel-aware client can discover the current master address from an
// aki instance without a separate Sentinel process. The data all comes from the
// live replication state, so there is no Sentinel config file and no quorum
// voting: aki reports itself (or its upstream master when it is a replica) as the
// single monitored master.

// sentinelDisabledErr is the reply every SENTINEL subcommand gets when
// sentinel-compat-mode is off.
const sentinelDisabledErr = "ERR SENTINEL command disabled (sentinel-compat-mode no)"

// sentinelNoMaster is the reply when the name argument does not match the
// configured monitor name.
const sentinelNoMaster = "ERR No such master with that name"

// sentinelEnabled reports whether the SENTINEL command family is served.
func (d *Dispatcher) sentinelEnabled() bool {
	return !strings.EqualFold(d.confValue("sentinel-compat-mode", "yes"), "no")
}

// monitorName is the name aki reports for the master in SENTINEL queries.
func (d *Dispatcher) monitorName() string {
	return d.confValue("sentinel-monitor-name", "mymaster")
}

// sentinelMaster returns the address and run id of the master this instance
// reports to Sentinel clients. Running as a master or standalone it is this
// instance; running as a replica it is the upstream master. linkUp is true when
// the master is reachable, which is always so for our own address and follows the
// replication link state when we are a replica.
func (d *Dispatcher) sentinelMaster() (ip string, port int, runid string, linkUp bool) {
	d.repl.mu.Lock()
	role := d.repl.role
	mhost := d.repl.masterHost
	mport := d.repl.masterPort
	link := d.repl.link
	mrunid := d.repl.masterReplid
	selfRunid := d.repl.replid
	d.repl.mu.Unlock()

	if role == "slave" {
		if mrunid == "" {
			mrunid = strings.Repeat("0", 40)
		}
		return mhost, mport, mrunid, link == "connected"
	}
	return d.announceIP(), d.announcePort(), selfRunid, true
}

// handleSentinel dispatches the SENTINEL subcommands. The subcommands have
// hyphens and varied argument counts, and the spec fixes the error strings, so
// they are routed here by hand rather than through the container framework.
func handleSentinel(ctx *Ctx) {
	d := ctx.d
	if !d.sentinelEnabled() {
		ctx.enc().WriteError(sentinelDisabledErr)
		return
	}
	sub := strings.ToLower(string(ctx.Argv[1]))
	switch sub {
	case "masters":
		handleSentinelMasters(ctx)
	case "master":
		handleSentinelMaster(ctx)
	case "replicas", "slaves":
		handleSentinelReplicas(ctx)
	case "sentinels":
		handleSentinelSentinels(ctx)
	case "get-master-addr-by-name":
		handleSentinelGetMasterAddr(ctx)
	case "is-master-down-by-addr":
		handleSentinelIsMasterDown(ctx)
	case "ckquorum":
		handleSentinelCkquorum(ctx)
	case "reset":
		handleSentinelReset(ctx)
	case "failover":
		handleSentinelFailover(ctx)
	case "flushconfig":
		ctx.enc().WriteStatus("OK")
	case "myid":
		handleSentinelMyID(ctx)
	case "monitor", "remove", "set":
		// Accepted for compatibility. aki derives all Sentinel state from the live
		// replication topology, so these have no durable effect.
		ctx.enc().WriteStatus("OK")
	case "simulate-failure":
		ctx.enc().WriteError("ERR SENTINEL SIMULATE-FAILURE is not supported")
	case "help":
		handleSentinelHelp(ctx)
	default:
		ctx.enc().WriteError("ERR unknown subcommand or wrong number of arguments for 'sentinel' command")
	}
}

// requireMasterName checks the name argument at argv[idx] matches the monitor
// name, writing the not-found error and returning false when it does not.
func (ctx *Ctx) requireMasterName(idx int) bool {
	if len(ctx.Argv) <= idx {
		ctx.enc().WriteError("ERR unknown subcommand or wrong number of arguments for 'sentinel' command")
		return false
	}
	if !strings.EqualFold(string(ctx.Argv[idx]), ctx.d.monitorName()) {
		ctx.enc().WriteError(sentinelNoMaster)
		return false
	}
	return true
}

// handleSentinelMasters replies with an array holding the one master info map.
func handleSentinelMasters(ctx *Ctx) {
	enc := ctx.enc()
	enc.WriteArrayLen(1)
	ctx.d.writeMasterInfo(enc)
}

// handleSentinelMaster replies with the info map for the named master.
func handleSentinelMaster(ctx *Ctx) {
	if !ctx.requireMasterName(2) {
		return
	}
	ctx.d.writeMasterInfo(ctx.enc())
}

// writeMasterInfo writes the master info map described in spec 29.3.
func (d *Dispatcher) writeMasterInfo(enc *resp.Encoder) {
	ip, port, runid, linkUp := d.sentinelMaster()
	d.repl.mu.Lock()
	numSlaves := len(d.repl.replicas)
	d.repl.mu.Unlock()
	d.cluster.mu.Lock()
	epoch := d.cluster.myEpoch
	d.cluster.mu.Unlock()

	flags := "master"
	if !linkUp {
		flags = "master,disconnected"
	}
	now := time.Now().UnixMilli()
	downAfter := d.confInt("sentinel-down-after-milliseconds", 30000)
	failoverTimeout := d.confInt("sentinel-failover-timeout", 180000)

	enc.WriteMapLen(20)
	pair(enc, "name", d.monitorName())
	pair(enc, "ip", ip)
	pair(enc, "port", strconv.Itoa(port))
	pair(enc, "runid", runid)
	pair(enc, "flags", flags)
	pair(enc, "link-pending-commands", "0")
	pair(enc, "link-refcount", "1")
	pair(enc, "last-ping-sent", strconv.FormatInt(now, 10))
	pair(enc, "last-ok-ping-reply", strconv.FormatInt(now, 10))
	pair(enc, "last-ping-reply", strconv.FormatInt(now, 10))
	pair(enc, "down-after-milliseconds", strconv.FormatInt(downAfter, 10))
	pair(enc, "info-refresh", strconv.FormatInt(now, 10))
	pair(enc, "role-reported", "master")
	pair(enc, "role-reported-time", strconv.FormatInt(now, 10))
	pair(enc, "config-epoch", strconv.FormatInt(epoch, 10))
	pair(enc, "num-slaves", strconv.Itoa(numSlaves))
	pair(enc, "num-other-sentinels", "0")
	pair(enc, "quorum", "1")
	pair(enc, "failover-timeout", strconv.FormatInt(failoverTimeout, 10))
	pair(enc, "parallel-syncs", "1")
}

// handleSentinelReplicas replies with an array of replica info maps for the named
// master. The replicas are the ones currently attached to this instance.
func handleSentinelReplicas(ctx *Ctx) {
	if !ctx.requireMasterName(2) {
		return
	}
	d := ctx.d
	masterIP := d.announceIP()
	masterPort := d.announcePort()

	type rep struct {
		ip     string
		port   int
		offset int64
		state  string
	}
	d.repl.mu.Lock()
	reps := make([]rep, 0, len(d.repl.replicas))
	for _, h := range d.repl.replicas {
		reps = append(reps, rep{ip: h.addr, port: h.port, offset: h.ackOffset, state: h.state})
	}
	d.repl.mu.Unlock()

	enc := ctx.enc()
	enc.WriteArrayLen(len(reps))
	for _, r := range reps {
		enc.WriteMapLen(12)
		pair(enc, "name", r.ip+":"+strconv.Itoa(r.port))
		pair(enc, "ip", r.ip)
		pair(enc, "port", strconv.Itoa(r.port))
		pair(enc, "runid", strings.Repeat("0", 40))
		flags := "slave"
		if r.state != "online" {
			flags = "slave,disconnected"
		}
		pair(enc, "flags", flags)
		pair(enc, "master-link-down-time", "0")
		pair(enc, "master-link-status", "ok")
		pair(enc, "master-host", masterIP)
		pair(enc, "master-port", strconv.Itoa(masterPort))
		pair(enc, "slave-priority", d.confValue("replica-priority", "100"))
		pair(enc, "slave-repl-offset", strconv.FormatInt(r.offset, 10))
		pair(enc, "replica-announced", "1")
	}
}

// handleSentinelSentinels replies with the other sentinels for the named master.
// aki has no peer sentinels, so the list is empty.
func handleSentinelSentinels(ctx *Ctx) {
	if !ctx.requireMasterName(2) {
		return
	}
	ctx.enc().WriteArrayLen(0)
}

// handleSentinelGetMasterAddr replies with the [ip, port] of the named master,
// the query most client libraries use to discover the master.
func handleSentinelGetMasterAddr(ctx *Ctx) {
	if !ctx.requireMasterName(2) {
		return
	}
	ip, port, _, _ := ctx.d.sentinelMaster()
	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr(ip)
	enc.WriteBulkStringStr(strconv.Itoa(port))
}

// handleSentinelIsMasterDown always votes abstain: aki does not participate in
// Sentinel quorum, so it reports the master as not down and casts no leader vote.
func handleSentinelIsMasterDown(ctx *Ctx) {
	enc := ctx.enc()
	enc.WriteArrayLen(3)
	enc.WriteInteger(0)
	enc.WriteBulkStringStr("*")
	enc.WriteInteger(0)
}

// handleSentinelCkquorum reports that quorum is reachable, which is trivially
// true since aki is the sole sentinel in this compat mode.
func handleSentinelCkquorum(ctx *Ctx) {
	if !ctx.requireMasterName(2) {
		return
	}
	ctx.enc().WriteStatus("OK 1 usable Sentinels. Quorum and failover authorization can be reached")
}

// handleSentinelReset replies with the number of masters matched by the pattern.
// aki keeps no per-master Sentinel state to reset, so it reports 0.
func handleSentinelReset(ctx *Ctx) {
	if len(ctx.Argv) < 3 {
		ctx.enc().WriteError("ERR unknown subcommand or wrong number of arguments for 'sentinel' command")
		return
	}
	ctx.enc().WriteInteger(0)
}

// handleSentinelFailover triggers a failover for the named master. aki has no
// peer to fail over to, so it accepts the command and reports OK without changing
// the topology.
func handleSentinelFailover(ctx *Ctx) {
	if !ctx.requireMasterName(2) {
		return
	}
	ctx.enc().WriteStatus("OK")
}

// handleSentinelMyID replies with this instance's sentinel id, which is the
// master replid.
func handleSentinelMyID(ctx *Ctx) {
	ctx.d.repl.mu.Lock()
	id := ctx.d.repl.replid
	ctx.d.repl.mu.Unlock()
	ctx.enc().WriteBulkStringStr(id)
}

// pair writes one key/value bulk-string pair of a map reply.
func pair(enc *resp.Encoder, key, val string) {
	enc.WriteBulkStringStr(key)
	enc.WriteBulkStringStr(val)
}

// handleSentinelHelp lists the SENTINEL subcommands.
func handleSentinelHelp(ctx *Ctx) {
	lines := []string{
		"SENTINEL <subcommand> [<arg> ...]. Subcommands are:",
		"MASTERS",
		"    Show a list of monitored masters and their state.",
		"MASTER <master-name>",
		"    Show the state of the named master.",
		"REPLICAS <master-name>",
		"    Show a list of replicas for the named master.",
		"SENTINELS <master-name>",
		"    Show a list of other sentinels for the named master.",
		"GET-MASTER-ADDR-BY-NAME <master-name>",
		"    Return the ip and port of the current master for the name.",
		"IS-MASTER-DOWN-BY-ADDR <ip> <port> <epoch> <runid>",
		"    Quorum check (always abstains in aki).",
		"CKQUORUM <master-name>",
		"    Check whether quorum is reachable.",
		"RESET <pattern>",
		"    Reset masters matching the pattern.",
		"FAILOVER <master-name>",
		"    Force a failover for the named master.",
		"FLUSHCONFIG",
		"    Rewrite the config file (no-op in aki).",
		"MYID",
		"    Return the sentinel id.",
		"HELP",
		"    Print this help.",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}
