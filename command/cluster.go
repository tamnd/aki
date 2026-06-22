package command

import (
	"strconv"
	"strings"
	"sync"

	"github.com/tamnd/aki/keyspace"
)

// This file implements the CLUSTER command family (spec 2064 doc 18 sections 20
// through 28). aki runs single-node by default (cluster-enabled no), where all
// 16384 slots are served implicitly, no MOVED or ASK redirects are emitted, and
// CROSSSLOT is not enforced. The reporting subcommands work in any mode; the
// slot-management subcommands need cluster-enabled yes. The gossip bus, MOVED and
// ASK redirection across nodes, and automatic failover are not part of this
// slice, so MEET, FORGET, REPLICATE and FAILOVER report that they need a peer.

// clusterDisabledErr is the reply a state-changing CLUSTER subcommand gets when
// cluster mode is off, matching real Redis byte for byte.
const clusterDisabledErr = "ERR This instance has cluster support disabled"

// clusterState holds this node's cluster bookkeeping. With cluster mode off it
// stays mostly empty; the node id is still generated so CLUSTER MYID has a stable
// answer.
type clusterState struct {
	mu           sync.Mutex
	nodeID       string
	myEpoch      int64
	currentEpoch int64
	slots        []bool // owned[slot]; len numSlots once initialised
	importing    map[int]string
	migrating    map[int]string
}

// clusterInit sets the cluster defaults when the dispatcher is built.
func (d *Dispatcher) clusterInit() {
	d.cluster.nodeID = newRunID()
	d.cluster.slots = make([]bool, numSlots)
	d.cluster.importing = map[int]string{}
	d.cluster.migrating = map[int]string{}
}

// clusterEnabled reports whether cluster mode is on.
func (d *Dispatcher) clusterEnabled() bool {
	return strings.EqualFold(d.confValue("cluster-enabled", "no"), "yes")
}

// announceIP is the address aki reports for itself in CLUSTER NODES, SLOTS and
// SHARDS. It prefers cluster-announce-ip, then the bound address, then loopback.
func (d *Dispatcher) announceIP() string {
	if ip := d.confValue("cluster-announce-ip", ""); ip != "" {
		return ip
	}
	if d.srv != nil {
		if a := d.srv.Addr(); a != nil {
			if h := replicaIP(a.String()); h != "" && h != "::" {
				return h
			}
		}
	}
	return "127.0.0.1"
}

// announcePort is the client port aki reports for itself, honouring
// cluster-announce-port when set.
func (d *Dispatcher) announcePort() int {
	if p := int(d.confInt("cluster-announce-port", 0)); p != 0 {
		return p
	}
	return d.listenPort()
}

// busPort is the cluster bus port, the client port plus 10000 unless overridden.
func (d *Dispatcher) busPort() int {
	if p := int(d.confInt("cluster-announce-bus-port", 0)); p != 0 {
		return p
	}
	return d.announcePort() + 10000
}

// handleClusterKeyslot replies with the slot a key hashes to. It works whether or
// not cluster mode is enabled.
func handleClusterKeyslot(ctx *Ctx) {
	ctx.enc().WriteInteger(int64(hashSlot(ctx.Argv[2])))
}

// handleClusterMyID replies with this node's 40-hex id.
func handleClusterMyID(ctx *Ctx) {
	ctx.d.cluster.mu.Lock()
	id := ctx.d.cluster.nodeID
	ctx.d.cluster.mu.Unlock()
	ctx.enc().WriteBulkStringStr(id)
}

// ownedRanges returns the slot ranges this node owns as sorted (start, end)
// pairs. The caller holds cluster.mu.
func (d *Dispatcher) ownedRanges() [][2]int {
	var ranges [][2]int
	start := -1
	for s := 0; s < numSlots; s++ {
		if d.cluster.slots[s] {
			if start < 0 {
				start = s
			}
		} else if start >= 0 {
			ranges = append(ranges, [2]int{start, s - 1})
			start = -1
		}
	}
	if start >= 0 {
		ranges = append(ranges, [2]int{start, numSlots - 1})
	}
	return ranges
}

// countOwnedSlots returns how many slots this node owns. The caller holds
// cluster.mu.
func (d *Dispatcher) countOwnedSlots() int {
	n := 0
	for _, owned := range d.cluster.slots {
		if owned {
			n++
		}
	}
	return n
}

// handleClusterInfo replies with the cluster_* key:value block.
func handleClusterInfo(ctx *Ctx) {
	d := ctx.d
	d.cluster.mu.Lock()
	enabled := d.clusterEnabled()
	assigned := d.countOwnedSlots()
	myEpoch := d.cluster.myEpoch
	curEpoch := d.cluster.currentEpoch
	d.cluster.mu.Unlock()

	var b strings.Builder
	enabledVal := 0
	known := 0
	size := 0
	if enabled {
		enabledVal = 1
		known = 1
		if assigned > 0 {
			size = 1
		}
	} else {
		assigned = 0
	}
	state := "ok"
	if enabled && strings.EqualFold(d.confValue("cluster-require-full-coverage", "yes"), "yes") && assigned < numSlots {
		state = "fail"
	}
	b.WriteString("cluster_enabled:" + strconv.Itoa(enabledVal) + "\r\n")
	b.WriteString("cluster_state:" + state + "\r\n")
	b.WriteString("cluster_slots_assigned:" + strconv.Itoa(assigned) + "\r\n")
	b.WriteString("cluster_slots_ok:" + strconv.Itoa(assigned) + "\r\n")
	b.WriteString("cluster_slots_pfail:0\r\n")
	b.WriteString("cluster_slots_fail:0\r\n")
	b.WriteString("cluster_known_nodes:" + strconv.Itoa(known) + "\r\n")
	b.WriteString("cluster_size:" + strconv.Itoa(size) + "\r\n")
	b.WriteString("cluster_current_epoch:" + strconv.FormatInt(curEpoch, 10) + "\r\n")
	b.WriteString("cluster_my_epoch:" + strconv.FormatInt(myEpoch, 10) + "\r\n")
	b.WriteString("cluster_stats_messages_sent:0\r\n")
	b.WriteString("cluster_stats_messages_received:0\r\n")
	b.WriteString("total_cluster_links_buffer_limit_exceeded:0\r\n")
	ctx.enc().WriteBulkStringStr(b.String())
}

// nodeLine builds this node's CLUSTER NODES / nodes.conf line. The caller holds
// cluster.mu.
func (d *Dispatcher) nodeLine() string {
	ip := d.announceIP()
	port := d.announcePort()
	bus := d.busPort()
	var b strings.Builder
	b.WriteString(d.cluster.nodeID)
	b.WriteString(" ")
	b.WriteString(ip + ":" + strconv.Itoa(port) + "@" + strconv.Itoa(bus))
	b.WriteString(" myself,master - 0 0 ")
	b.WriteString(strconv.FormatInt(d.cluster.myEpoch, 10))
	b.WriteString(" connected")
	for _, r := range d.ownedRanges() {
		b.WriteString(" ")
		if r[0] == r[1] {
			b.WriteString(strconv.Itoa(r[0]))
		} else {
			b.WriteString(strconv.Itoa(r[0]) + "-" + strconv.Itoa(r[1]))
		}
	}
	return b.String()
}

// handleClusterNodes replies with one nodes.conf line per known node. Single-node
// aki knows only itself.
func handleClusterNodes(ctx *Ctx) {
	ctx.d.cluster.mu.Lock()
	line := ctx.d.nodeLine()
	ctx.d.cluster.mu.Unlock()
	ctx.enc().WriteBulkStringStr(line + "\n")
}

// handleClusterSlots replies with the slot-range array. With no slots owned it is
// an empty array, which is what a cluster-disabled instance reports.
func handleClusterSlots(ctx *Ctx) {
	d := ctx.d
	d.cluster.mu.Lock()
	ranges := d.ownedRanges()
	ip := d.announceIP()
	port := d.announcePort()
	id := d.cluster.nodeID
	d.cluster.mu.Unlock()

	enc := ctx.enc()
	enc.WriteArrayLen(len(ranges))
	for _, r := range ranges {
		enc.WriteArrayLen(3)
		enc.WriteInteger(int64(r[0]))
		enc.WriteInteger(int64(r[1]))
		enc.WriteArrayLen(3)
		enc.WriteBulkStringStr(ip)
		enc.WriteInteger(int64(port))
		enc.WriteBulkStringStr(id)
	}
}

// handleClusterShards replies with the shard array (Redis 7+). A node with no
// slots produces an empty array.
func handleClusterShards(ctx *Ctx) {
	d := ctx.d
	d.cluster.mu.Lock()
	ranges := d.ownedRanges()
	ip := d.announceIP()
	port := d.announcePort()
	id := d.cluster.nodeID
	d.cluster.mu.Unlock()
	d.repl.mu.Lock()
	off := d.repl.offset
	d.repl.mu.Unlock()

	enc := ctx.enc()
	if len(ranges) == 0 {
		enc.WriteArrayLen(0)
		return
	}
	enc.WriteArrayLen(1)
	enc.WriteMapLen(2)
	enc.WriteBulkStringStr("slots")
	enc.WriteArrayLen(len(ranges) * 2)
	for _, r := range ranges {
		enc.WriteInteger(int64(r[0]))
		enc.WriteInteger(int64(r[1]))
	}
	enc.WriteBulkStringStr("nodes")
	enc.WriteArrayLen(1)
	enc.WriteMapLen(9)
	enc.WriteBulkStringStr("id")
	enc.WriteBulkStringStr(id)
	enc.WriteBulkStringStr("port")
	enc.WriteInteger(int64(port))
	enc.WriteBulkStringStr("tls-port")
	enc.WriteInteger(0)
	enc.WriteBulkStringStr("ip")
	enc.WriteBulkStringStr(ip)
	enc.WriteBulkStringStr("endpoint")
	enc.WriteBulkStringStr(ip)
	enc.WriteBulkStringStr("hostname")
	enc.WriteBulkStringStr("")
	enc.WriteBulkStringStr("role")
	enc.WriteBulkStringStr("master")
	enc.WriteBulkStringStr("replication-offset")
	enc.WriteInteger(off)
	enc.WriteBulkStringStr("health")
	enc.WriteBulkStringStr("online")
}

// keysInSlot returns up to limit keys in the current database that hash to slot.
// A limit below zero means no cap. It scans the current database keyspace.
func (ctx *Ctx) keysInSlot(slot, limit int) ([][]byte, int) {
	var matched [][]byte
	count := 0
	ctx.view(func(db *keyspace.DB) error {
		all, err := db.Keys()
		if err != nil {
			return err
		}
		for _, e := range all {
			if hashSlot(e.Key) != slot {
				continue
			}
			count++
			if limit < 0 || len(matched) < limit {
				matched = append(matched, e.Key)
			}
		}
		return nil
	})
	return matched, count
}

// handleClusterCountKeysInSlot replies with the number of keys in the slot.
func handleClusterCountKeysInSlot(ctx *Ctx) {
	slot, ok := parseSlot(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR Invalid slot")
		return
	}
	_, count := ctx.keysInSlot(slot, 0)
	ctx.enc().WriteInteger(int64(count))
}

// handleClusterGetKeysInSlot replies with up to count keys in the slot.
func handleClusterGetKeysInSlot(ctx *Ctx) {
	slot, ok := parseSlot(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR Invalid slot")
		return
	}
	limit, ok := parseInteger(ctx.Argv[3])
	if !ok || limit < 0 {
		ctx.enc().WriteError("ERR Invalid number of keys")
		return
	}
	keys, _ := ctx.keysInSlot(slot, int(limit))
	enc := ctx.enc()
	enc.WriteArrayLen(len(keys))
	for _, k := range keys {
		enc.WriteBulkString(k)
	}
}

// handleClusterLinks replies with the cluster bus link list, which is empty on a
// single node.
func handleClusterLinks(ctx *Ctx) {
	ctx.enc().WriteArrayLen(0)
}

// handleClusterReplicas replies with the replicas of the given node. Single-node
// aki has no peers, so an unknown id is an error and the local id has no cluster
// replicas to report.
func handleClusterReplicas(ctx *Ctx) {
	if !ctx.d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	id := string(ctx.Argv[2])
	ctx.d.cluster.mu.Lock()
	self := ctx.d.cluster.nodeID
	ctx.d.cluster.mu.Unlock()
	if id != self {
		ctx.enc().WriteError("ERR Unknown node " + id)
		return
	}
	ctx.enc().WriteArrayLen(0)
}

// parseSlot parses a slot argument and checks it is in 0..16383.
func parseSlot(arg []byte) (int, bool) {
	n, err := strconv.Atoi(string(arg))
	if err != nil || n < 0 || n >= numSlots {
		return 0, false
	}
	return n, true
}

// --- Slot management (cluster mode only) ---

// handleClusterAddSlots assigns individual slots to this node.
func handleClusterAddSlots(ctx *Ctx) {
	ctx.d.changeSlots(ctx, ctx.Argv[2:], true, false)
}

// handleClusterDelSlots removes individual slots from this node.
func handleClusterDelSlots(ctx *Ctx) {
	ctx.d.changeSlots(ctx, ctx.Argv[2:], false, false)
}

// handleClusterAddSlotsRange assigns slot ranges to this node.
func handleClusterAddSlotsRange(ctx *Ctx) {
	ctx.d.changeSlots(ctx, ctx.Argv[2:], true, true)
}

// handleClusterDelSlotsRange removes slot ranges from this node.
func handleClusterDelSlotsRange(ctx *Ctx) {
	ctx.d.changeSlots(ctx, ctx.Argv[2:], false, true)
}

// changeSlots applies an add or delete of slots, parsing either the individual or
// the range argument form. It validates everything before mutating so the command
// is atomic: a bad slot or a busy or unassigned slot fails the whole call.
func (d *Dispatcher) changeSlots(ctx *Ctx, args [][]byte, add, ranges bool) {
	if !d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	slots, errMsg := parseSlotArgs(args, ranges)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}

	d.cluster.mu.Lock()
	defer d.cluster.mu.Unlock()
	for _, s := range slots {
		if add && d.cluster.slots[s] {
			ctx.enc().WriteError("ERR Slot " + strconv.Itoa(s) + " is already busy")
			return
		}
		if !add && !d.cluster.slots[s] {
			ctx.enc().WriteError("ERR Slot " + strconv.Itoa(s) + " is already unassigned")
			return
		}
	}
	for _, s := range slots {
		d.cluster.slots[s] = add
	}
	ctx.enc().WriteStatus("OK")
}

// parseSlotArgs turns the slot arguments into a list of slot numbers. With ranges
// true the args are (start end) pairs; otherwise each arg is one slot.
func parseSlotArgs(args [][]byte, ranges bool) ([]int, string) {
	const badSlot = "ERR Invalid or out of range slot"
	if ranges {
		if len(args)%2 != 0 {
			return nil, "ERR wrong number of arguments for 'cluster|addslotsrange' command"
		}
		var out []int
		for i := 0; i < len(args); i += 2 {
			start, ok1 := parseSlot(args[i])
			end, ok2 := parseSlot(args[i+1])
			if !ok1 || !ok2 || start > end {
				return nil, badSlot
			}
			for s := start; s <= end; s++ {
				out = append(out, s)
			}
		}
		return out, ""
	}
	var out []int
	for _, a := range args {
		s, ok := parseSlot(a)
		if !ok {
			return nil, badSlot
		}
		out = append(out, s)
	}
	return out, ""
}

// handleClusterSetSlot marks a slot importing, migrating, stable, or assigned to a
// node, the states a resharding tool drives.
func handleClusterSetSlot(ctx *Ctx) {
	d := ctx.d
	if !d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	slot, ok := parseSlot(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR Invalid or out of range slot")
		return
	}
	action := strings.ToUpper(string(ctx.Argv[3]))

	d.cluster.mu.Lock()
	defer d.cluster.mu.Unlock()
	switch action {
	case "IMPORTING":
		if len(ctx.Argv) != 5 {
			ctx.enc().WriteError("ERR Invalid CLUSTER SETSLOT action")
			return
		}
		d.cluster.importing[slot] = string(ctx.Argv[4])
	case "MIGRATING":
		if len(ctx.Argv) != 5 {
			ctx.enc().WriteError("ERR Invalid CLUSTER SETSLOT action")
			return
		}
		d.cluster.migrating[slot] = string(ctx.Argv[4])
	case "STABLE":
		delete(d.cluster.importing, slot)
		delete(d.cluster.migrating, slot)
	case "NODE":
		if len(ctx.Argv) != 5 {
			ctx.enc().WriteError("ERR Invalid CLUSTER SETSLOT action")
			return
		}
		delete(d.cluster.importing, slot)
		delete(d.cluster.migrating, slot)
		d.cluster.slots[slot] = string(ctx.Argv[4]) == d.cluster.nodeID
	default:
		ctx.enc().WriteError("ERR Invalid CLUSTER SETSLOT action")
		return
	}
	ctx.enc().WriteStatus("OK")
}

// handleClusterFlushSlots removes every slot assignment from this node.
func handleClusterFlushSlots(ctx *Ctx) {
	d := ctx.d
	if !d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	d.cluster.mu.Lock()
	for s := range d.cluster.slots {
		d.cluster.slots[s] = false
	}
	d.cluster.mu.Unlock()
	ctx.enc().WriteStatus("OK")
}

// handleClusterBumpEpoch raises this node's config epoch and reports the result.
func handleClusterBumpEpoch(ctx *Ctx) {
	d := ctx.d
	if !d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	d.cluster.mu.Lock()
	d.cluster.currentEpoch++
	d.cluster.myEpoch = d.cluster.currentEpoch
	epoch := d.cluster.myEpoch
	d.cluster.mu.Unlock()
	ctx.enc().WriteStatus("BUMPED " + strconv.FormatInt(epoch, 10))
}

// handleClusterSetConfigEpoch sets this node's config epoch.
func handleClusterSetConfigEpoch(ctx *Ctx) {
	d := ctx.d
	if !d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	epoch, err := strconv.ParseInt(string(ctx.Argv[2]), 10, 64)
	if err != nil || epoch < 0 {
		ctx.enc().WriteError("ERR Invalid config epoch specified: " + string(ctx.Argv[2]))
		return
	}
	d.cluster.mu.Lock()
	d.cluster.myEpoch = epoch
	if epoch > d.cluster.currentEpoch {
		d.cluster.currentEpoch = epoch
	}
	d.cluster.mu.Unlock()
	ctx.enc().WriteStatus("OK")
}

// handleClusterReset clears slot assignments and node state. HARD also mints a new
// node id. It refuses when slots are still assigned, matching Redis.
func handleClusterReset(ctx *Ctx) {
	d := ctx.d
	if !d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	hard := false
	if len(ctx.Argv) == 3 {
		mode := strings.ToUpper(string(ctx.Argv[2]))
		switch mode {
		case "HARD":
			hard = true
		case "SOFT":
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	d.cluster.mu.Lock()
	for s := range d.cluster.slots {
		d.cluster.slots[s] = false
	}
	d.cluster.importing = map[int]string{}
	d.cluster.migrating = map[int]string{}
	d.cluster.myEpoch = 0
	d.cluster.currentEpoch = 0
	if hard {
		d.cluster.nodeID = newRunID()
	}
	d.cluster.mu.Unlock()
	ctx.enc().WriteStatus("OK")
}

// handleClusterPeerOp serves the subcommands that need a peer over the cluster
// bus: MEET, FORGET, REPLICATE and FAILOVER. The gossip bus is not part of this
// build, so with cluster mode off they report support disabled and with it on
// they report that no peer link is available.
func handleClusterPeerOp(ctx *Ctx) {
	if !ctx.d.clusterEnabled() {
		ctx.enc().WriteError(clusterDisabledErr)
		return
	}
	ctx.enc().WriteError("ERR This operation requires a running cluster bus, which is not available in this build")
}
