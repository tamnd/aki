package command

import (
	"bytes"
	"sort"
	"sync"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// pubsubRegistry maps channels, patterns and shard channels to the connections
// subscribed to them. PUBLISH reads it to find delivery targets; the subscribe
// commands write to it. It carries its own lock because PUBLISH on one
// connection touches the subscriptions of another.
type pubsubRegistry struct {
	mu       sync.RWMutex
	channels map[string]map[uint64]*networking.Conn
	patterns map[string]map[uint64]*networking.Conn
	shards   map[string]map[uint64]*networking.Conn
}

func newPubsubRegistry() *pubsubRegistry {
	return &pubsubRegistry{
		channels: map[string]map[uint64]*networking.Conn{},
		patterns: map[string]map[uint64]*networking.Conn{},
		shards:   map[string]map[uint64]*networking.Conn{},
	}
}

// add registers conn under name in m. It is idempotent: a second add for the
// same connection leaves the set unchanged.
func psAdd(m map[string]map[uint64]*networking.Conn, name string, conn *networking.Conn) {
	set := m[name]
	if set == nil {
		set = map[uint64]*networking.Conn{}
		m[name] = set
	}
	set[conn.ID()] = conn
}

// remove drops a connection from name in m, deleting the entry when it empties.
func psRemove(m map[string]map[uint64]*networking.Conn, name string, id uint64) {
	if set := m[name]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(m, name)
		}
	}
}

// dropClient removes every subscription a disconnecting client held. It reads the
// names from the session so it touches only that client's entries.
func (r *pubsubRegistry) dropClient(id uint64, sess *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for ch := range sess.subChannels {
		psRemove(r.channels, ch, id)
	}
	for p := range sess.subPatterns {
		psRemove(r.patterns, p, id)
	}
	for ch := range sess.subShards {
		psRemove(r.shards, ch, id)
	}
}

// pubsubCommands returns the Pub/Sub command group.
func pubsubCommands() []*CmdDesc {
	pubsub := &CmdDesc{
		Name: "pubsub", Group: GroupPubSub, Since: "2.8.0",
		Arity: -2, Flags: FlagLoading | FlagStale,
		Handler: handlePubsubHelp,
		SubCmds: []*CmdDesc{
			{Name: "channels", SubName: "pubsub|channels", Group: GroupPubSub, Since: "2.8.0",
				Arity: -2, Flags: FlagLoading | FlagStale, Handler: handlePubsubChannels},
			{Name: "numsub", SubName: "pubsub|numsub", Group: GroupPubSub, Since: "2.8.0",
				Arity: -2, Flags: FlagLoading | FlagStale, Handler: handlePubsubNumSub},
			{Name: "numpat", SubName: "pubsub|numpat", Group: GroupPubSub, Since: "2.8.0",
				Arity: 2, Flags: FlagLoading | FlagStale | FlagFast, Handler: handlePubsubNumPat},
			{Name: "shardchannels", SubName: "pubsub|shardchannels", Group: GroupPubSub, Since: "7.0.0",
				Arity: -2, Flags: FlagLoading | FlagStale, Handler: handlePubsubShardChannels},
			{Name: "shardnumsub", SubName: "pubsub|shardnumsub", Group: GroupPubSub, Since: "7.0.0",
				Arity: -2, Flags: FlagLoading | FlagStale, Handler: handlePubsubShardNumSub},
			{Name: "help", SubName: "pubsub|help", Group: GroupPubSub, Since: "6.2.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handlePubsubHelp},
		},
	}
	return []*CmdDesc{
		{Name: "subscribe", Group: GroupPubSub, Since: "2.0.0",
			Arity: -2, Flags: FlagPubSub | FlagLoading | FlagStale, Handler: handleSubscribe},
		{Name: "unsubscribe", Group: GroupPubSub, Since: "2.0.0",
			Arity: -1, Flags: FlagPubSub | FlagLoading | FlagStale, Handler: handleUnsubscribe},
		{Name: "psubscribe", Group: GroupPubSub, Since: "2.0.0",
			Arity: -2, Flags: FlagPubSub | FlagLoading | FlagStale, Handler: handlePSubscribe},
		{Name: "punsubscribe", Group: GroupPubSub, Since: "2.0.0",
			Arity: -1, Flags: FlagPubSub | FlagLoading | FlagStale, Handler: handlePUnsubscribe},
		{Name: "ssubscribe", Group: GroupPubSub, Since: "7.0.0",
			Arity: -2, Flags: FlagPubSub | FlagLoading | FlagStale, Handler: handleSSubscribe},
		{Name: "sunsubscribe", Group: GroupPubSub, Since: "7.0.0",
			Arity: -1, Flags: FlagPubSub | FlagLoading | FlagStale, Handler: handleSUnsubscribe},
		// PUBLISH and SPUBLISH are not FlagPubSub. They queue normally inside MULTI
		// and are blocked in RESP2 subscriber mode, so they must not carry the
		// subscriber-mode allowance the subscribe family has.
		{Name: "publish", Group: GroupPubSub, Since: "2.0.0",
			Arity: 3, Flags: FlagLoading | FlagStale | FlagFast, Handler: handlePublish},
		{Name: "spublish", Group: GroupPubSub, Since: "7.0.0",
			Arity: 3, Flags: FlagLoading | FlagStale | FlagFast, Handler: handleSPublish},
		pubsub,
	}
}

// allowedInSubMode reports whether a command may run while a RESP2 client is in
// subscriber mode. The subscribe family is allowed, plus PING, QUIT and RESET.
func allowedInSubMode(name string) bool {
	switch name {
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe",
		"ssubscribe", "sunsubscribe", "ping", "quit", "reset":
		return true
	default:
		return false
	}
}

// writeSubConfirm writes a subscribe or unsubscribe confirmation: a three-element
// push of the kind, the channel or pattern name, and the running count. A nil
// name writes a null, the form an empty UNSUBSCRIBE uses.
func writeSubConfirm(enc *resp.Encoder, kind string, name []byte, count int) {
	enc.WritePushLen(3)
	enc.WriteBulkStringStr(kind)
	if name == nil {
		enc.WriteNull()
	} else {
		enc.WriteBulkString(name)
	}
	enc.WriteInteger(int64(count))
}

func (s *session) ensurePubsubMaps() {
	if s.subChannels == nil {
		s.subChannels = map[string]bool{}
	}
	if s.subPatterns == nil {
		s.subPatterns = map[string]bool{}
	}
	if s.subShards == nil {
		s.subShards = map[string]bool{}
	}
}

// handleSubscribe subscribes the connection to each named channel and confirms
// each one with the running subscription count.
func handleSubscribe(ctx *Ctx) {
	ctx.sess.ensurePubsubMaps()
	enc := ctx.enc()
	for _, ch := range ctx.Argv[1:] {
		name := string(ch)
		ctx.d.ps.mu.Lock()
		psAdd(ctx.d.ps.channels, name, ctx.Conn)
		ctx.d.ps.mu.Unlock()
		ctx.sess.subChannels[name] = true
		writeSubConfirm(enc, "subscribe", ch, ctx.sess.subCount())
	}
}

// handlePSubscribe subscribes the connection to each glob pattern.
func handlePSubscribe(ctx *Ctx) {
	ctx.sess.ensurePubsubMaps()
	enc := ctx.enc()
	for _, p := range ctx.Argv[1:] {
		name := string(p)
		ctx.d.ps.mu.Lock()
		psAdd(ctx.d.ps.patterns, name, ctx.Conn)
		ctx.d.ps.mu.Unlock()
		ctx.sess.subPatterns[name] = true
		writeSubConfirm(enc, "psubscribe", p, ctx.sess.subCount())
	}
}

// handleSSubscribe subscribes the connection to each shard channel. In standalone
// mode every shard channel is local, so this mirrors SUBSCRIBE.
func handleSSubscribe(ctx *Ctx) {
	ctx.sess.ensurePubsubMaps()
	enc := ctx.enc()
	for _, ch := range ctx.Argv[1:] {
		name := string(ch)
		ctx.d.ps.mu.Lock()
		psAdd(ctx.d.ps.shards, name, ctx.Conn)
		ctx.d.ps.mu.Unlock()
		ctx.sess.subShards[name] = true
		writeSubConfirm(enc, "ssubscribe", ch, ctx.sess.subCount())
	}
}

// handleUnsubscribe removes the named channels, or all of them when none are
// given. An empty UNSUBSCRIBE with no current channels sends one null
// confirmation.
func handleUnsubscribe(ctx *Ctx) {
	ctx.sess.ensurePubsubMaps()
	unsubscribe(ctx, ctx.d.ps.channels, ctx.sess.subChannels, "unsubscribe")
}

// handlePUnsubscribe is the pattern mirror of UNSUBSCRIBE.
func handlePUnsubscribe(ctx *Ctx) {
	ctx.sess.ensurePubsubMaps()
	unsubscribe(ctx, ctx.d.ps.patterns, ctx.sess.subPatterns, "punsubscribe")
}

// handleSUnsubscribe is the shard-channel mirror of UNSUBSCRIBE.
func handleSUnsubscribe(ctx *Ctx) {
	ctx.sess.ensurePubsubMaps()
	unsubscribe(ctx, ctx.d.ps.shards, ctx.sess.subShards, "sunsubscribe")
}

// unsubscribe drops the requested names from one namespace, or all held names
// when the command had no arguments, sending one confirmation each.
func unsubscribe(ctx *Ctx, reg map[string]map[uint64]*networking.Conn, held map[string]bool, kind string) {
	enc := ctx.enc()
	id := ctx.Conn.ID()

	var names []string
	if len(ctx.Argv) > 1 {
		for _, a := range ctx.Argv[1:] {
			names = append(names, string(a))
		}
	} else {
		for n := range held {
			names = append(names, n)
		}
		sort.Strings(names)
	}

	if len(names) == 0 {
		writeSubConfirm(enc, kind, nil, ctx.sess.subCount())
		return
	}
	for _, n := range names {
		ctx.d.ps.mu.Lock()
		psRemove(reg, n, id)
		ctx.d.ps.mu.Unlock()
		delete(held, n)
		writeSubConfirm(enc, kind, []byte(n), ctx.sess.subCount())
	}
}

// handlePublish delivers a message to every channel subscriber and to every
// pattern subscriber whose pattern matches, then returns the total delivery
// count. A client subscribed both ways is counted once per delivery.
func handlePublish(ctx *Ctx) {
	channel := ctx.Argv[1]
	msg := ctx.Argv[2]

	type target struct {
		conn    *networking.Conn
		pattern []byte
	}
	var targets []target

	ctx.d.ps.mu.RLock()
	for _, conn := range ctx.d.ps.channels[string(channel)] {
		targets = append(targets, target{conn: conn})
	}
	for pat, conns := range ctx.d.ps.patterns {
		if !stringMatch([]byte(pat), channel, false) {
			continue
		}
		pb := []byte(pat)
		for _, conn := range conns {
			targets = append(targets, target{conn: conn, pattern: pb})
		}
	}
	ctx.d.ps.mu.RUnlock()

	var count int64
	for _, t := range targets {
		var frame []byte
		if t.pattern != nil {
			frame = framePMessage(t.conn.Proto(), t.pattern, channel, msg)
		} else {
			frame = frameMessage(t.conn.Proto(), "message", channel, msg)
		}
		if err := t.conn.Deliver(frame); err == nil {
			count++
		}
	}
	ctx.enc().WriteInteger(count)
}

// handleSPublish delivers a message only to shard-channel subscribers and returns
// the delivery count.
func handleSPublish(ctx *Ctx) {
	channel := ctx.Argv[1]
	msg := ctx.Argv[2]

	var conns []*networking.Conn
	ctx.d.ps.mu.RLock()
	for _, conn := range ctx.d.ps.shards[string(channel)] {
		conns = append(conns, conn)
	}
	ctx.d.ps.mu.RUnlock()

	var count int64
	for _, conn := range conns {
		if err := conn.Deliver(frameMessage(conn.Proto(), "smessage", channel, msg)); err == nil {
			count++
		}
	}
	ctx.enc().WriteInteger(count)
}

// frameMessage builds a three-element message or smessage push for one connection
// at its protocol version.
func frameMessage(proto int, kind string, channel, msg []byte) []byte {
	var b bytes.Buffer
	e := resp.NewEncoder(&b, proto)
	e.WritePushLen(3)
	e.WriteBulkStringStr(kind)
	e.WriteBulkString(channel)
	e.WriteBulkString(msg)
	return b.Bytes()
}

// framePMessage builds a four-element pmessage push carrying the pattern that
// matched.
func framePMessage(proto int, pattern, channel, msg []byte) []byte {
	var b bytes.Buffer
	e := resp.NewEncoder(&b, proto)
	e.WritePushLen(4)
	e.WriteBulkStringStr("pmessage")
	e.WriteBulkString(pattern)
	e.WriteBulkString(channel)
	e.WriteBulkString(msg)
	return b.Bytes()
}

// handlePubsubChannels lists active channels, optionally filtered by a glob.
func handlePubsubChannels(ctx *Ctx) {
	pubsubList(ctx, ctx.d.ps.channels)
}

// handlePubsubShardChannels lists active shard channels.
func handlePubsubShardChannels(ctx *Ctx) {
	pubsubList(ctx, ctx.d.ps.shards)
}

// pubsubList writes the active names of a namespace as a bulk-string array, with
// an optional glob filter at argv[2].
func pubsubList(ctx *Ctx, reg map[string]map[uint64]*networking.Conn) {
	var pattern []byte
	if len(ctx.Argv) > 2 {
		pattern = ctx.Argv[2]
	}
	var names []string
	ctx.d.ps.mu.RLock()
	for n := range reg {
		if pattern == nil || stringMatch(pattern, []byte(n), false) {
			names = append(names, n)
		}
	}
	ctx.d.ps.mu.RUnlock()
	sort.Strings(names)
	enc := ctx.enc()
	enc.WriteArrayLen(len(names))
	for _, n := range names {
		enc.WriteBulkStringStr(n)
	}
}

// handlePubsubNumSub reports the subscriber count of each named channel.
func handlePubsubNumSub(ctx *Ctx) {
	pubsubNumSub(ctx, ctx.d.ps.channels)
}

// handlePubsubShardNumSub reports the subscriber count of each named shard
// channel.
func handlePubsubShardNumSub(ctx *Ctx) {
	pubsubNumSub(ctx, ctx.d.ps.shards)
}

// pubsubNumSub writes alternating name and subscriber count for each argument.
func pubsubNumSub(ctx *Ctx, reg map[string]map[uint64]*networking.Conn) {
	names := ctx.Argv[2:]
	enc := ctx.enc()
	if enc.Proto() == 3 {
		enc.WriteMapLen(len(names))
	} else {
		enc.WriteArrayLen(len(names) * 2)
	}
	ctx.d.ps.mu.RLock()
	defer ctx.d.ps.mu.RUnlock()
	for _, n := range names {
		enc.WriteBulkString(n)
		enc.WriteInteger(int64(len(reg[string(n)])))
	}
}

// handlePubsubNumPat reports the number of distinct patterns with a subscriber.
func handlePubsubNumPat(ctx *Ctx) {
	ctx.d.ps.mu.RLock()
	n := len(ctx.d.ps.patterns)
	ctx.d.ps.mu.RUnlock()
	ctx.enc().WriteInteger(int64(n))
}

// handlePubsubHelp prints the PUBSUB subcommand summary.
func handlePubsubHelp(ctx *Ctx) {
	lines := []string{
		"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CHANNELS [<pattern>]",
		"    Return the currently active channels matching a <pattern> (default: all).",
		"NUMSUB [<channel> [<channel> ...]]",
		"    Return the number of subscribers for the specified channels, excluding pattern subscriptions(default: no channels).",
		"NUMPAT",
		"    Return number of subscriptions to patterns.",
		"SHARDCHANNELS [<pattern>]",
		"    Return the currently active shard level channels matching a <pattern> (default: all).",
		"SHARDNUMSUB [<shardchannel> [<shardchannel> ...]]",
		"    Return the number of subscribers for the specified shard level channel(s)",
		"HELP",
		"    Print this help.",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteBulkStringStr(l)
	}
}
