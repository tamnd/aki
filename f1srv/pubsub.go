package f1srv

import "strconv"

// This file implements RESP pub/sub on f1srv, the M12 pub/sub slice of the f1_rewrite_ltm
// spec: SUBSCRIBE/UNSUBSCRIBE, PSUBSCRIBE/PUNSUBSCRIBE, SSUBSCRIBE/SUNSUBSCRIBE, PUBLISH,
// SPUBLISH, and PUBSUB introspection. Pub/sub owns no keyspace: a channel is not a key, a
// message is not stored, and nothing here touches the f1raw store. The state is entirely in
// the server registry (server.go: psChan/psPat/psShard) plus each connection's own subscription
// sets (conn.go: psChannels/psPatterns/psShard).
//
// Two namespaces are kept apart, matching Redis. Regular channels and patterns share one
// subscribe count (a client's SUBSCRIBE count reply is channels+patterns), and PUBLISH reaches
// a channel's direct subscribers plus every subscriber whose pattern matches. Shard channels
// are separate: SSUBSCRIBE has its own count, SPUBLISH reaches only shard subscribers, and a
// regular PUBLISH never reaches a shard channel.
//
// Delivery of a pushed message frame to a subscriber is the one cross-connection write in the
// server, and the two network drivers handle it differently through the connState.deliver hook.
// On the goroutine driver deliver writes the frame straight to the subscriber's socket under its
// writeMu, so a publisher on one goroutine and the subscriber's own per-batch flush cannot
// interleave a half frame. On the reactor driver deliver posts the frame to the subscriber's
// owning event loop, which serializes every write to that connection and so needs no lock. A
// publisher that is itself subscribed to the channel it publishes to appends to its own out
// buffer rather than calling deliver.

// pub/sub subscription kinds: a regular channel, a glob pattern, or a shard channel. The kind
// selects which per-connection set and which server registry map a subscribe or unsubscribe
// touches, and which running count its confirmation reports.
const (
	psKindChannel = iota
	psKindPattern
	psKindShard
)

// connSubMap returns this connection's current subscription set for the kind, which may be nil
// when the connection has never subscribed to that kind (the sets are allocated lazily so a
// connection that never subscribes carries no map).
func (c *connState) connSubMap(kind int) map[string]struct{} {
	switch kind {
	case psKindPattern:
		return c.psPatterns
	case psKindShard:
		return c.psShard
	default:
		return c.psChannels
	}
}

// ensureConnSubMap returns this connection's subscription set for the kind, allocating it on
// first use and storing it back on the connection.
func (c *connState) ensureConnSubMap(kind int) map[string]struct{} {
	switch kind {
	case psKindPattern:
		if c.psPatterns == nil {
			c.psPatterns = make(map[string]struct{})
		}
		return c.psPatterns
	case psKindShard:
		if c.psShard == nil {
			c.psShard = make(map[string]struct{})
		}
		return c.psShard
	default:
		if c.psChannels == nil {
			c.psChannels = make(map[string]struct{})
		}
		return c.psChannels
	}
}

// srvSubMap returns the server registry map for the kind. The maps are created in New, so this
// never returns nil.
func (c *connState) srvSubMap(kind int) map[string]map[*connState]struct{} {
	switch kind {
	case psKindPattern:
		return c.srv.psPat
	case psKindShard:
		return c.srv.psShard
	default:
		return c.srv.psChan
	}
}

// subCount is the running subscription count a confirmation reports. Regular channels and
// patterns share one count (channels plus patterns), the way Redis reports the SUBSCRIBE and
// PSUBSCRIBE reply; shard channels count on their own.
func (c *connState) subCount(kind int) int {
	if kind == psKindShard {
		return len(c.psShard)
	}
	return len(c.psChannels) + len(c.psPatterns)
}

func subVerb(kind int) string {
	switch kind {
	case psKindPattern:
		return "psubscribe"
	case psKindShard:
		return "ssubscribe"
	default:
		return "subscribe"
	}
}

func unsubVerb(kind int) string {
	switch kind {
	case psKindPattern:
		return "punsubscribe"
	case psKindShard:
		return "sunsubscribe"
	default:
		return "unsubscribe"
	}
}

func subVerbUpper(kind int) string {
	switch kind {
	case psKindPattern:
		return "PSUBSCRIBE"
	case psKindShard:
		return "SSUBSCRIBE"
	default:
		return "SUBSCRIBE"
	}
}

// stillSubscribed reports whether this connection holds any subscription of any kind, which is
// what keeps it in subscribe context.
func (c *connState) stillSubscribed() bool {
	return len(c.psChannels) > 0 || len(c.psPatterns) > 0 || len(c.psShard) > 0
}

// doSubscribe handles SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE. Each named channel or pattern is added to
// the connection's own set and the server registry, then confirmed with one array reply carrying
// the verb, the name, and the running count after adding it, matching Redis: one reply per name,
// count = the total after that name. A name already subscribed is idempotent but still confirmed
// with the unchanged count. The subscribe verbs are refused inside a transaction, which flags it
// so EXEC aborts, the way Redis refuses them at queue time.
func (c *connState) doSubscribe(argv [][]byte, kind int) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for '" + subVerb(kind) + "' command")
		return
	}
	if c.inMulti {
		c.multiAbort = true
		c.writeErr("ERR " + subVerbUpper(kind) + " is not allowed in transactions")
		return
	}
	s := c.srv
	verb := []byte(subVerb(kind))
	for _, name := range argv[1:] {
		ns := string(name)
		if _, ok := c.connSubMap(kind)[ns]; !ok {
			c.ensureConnSubMap(kind)[ns] = struct{}{}
			s.psMu.Lock()
			sm := c.srvSubMap(kind)[ns]
			if sm == nil {
				sm = make(map[*connState]struct{})
				c.srvSubMap(kind)[ns] = sm
			}
			sm[c] = struct{}{}
			s.psMu.Unlock()
			c.psMode = true
		}
		c.writeArrayHeader(3)
		c.writeBulk(verb)
		c.writeBulk(name)
		c.writeInt(int64(c.subCount(kind)))
	}
}

// doUnsubscribe handles UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE. Given explicit names it drops each
// and confirms it with the running count after removal. The bare form (no names) drops every
// subscription of this kind, one confirmation each; with nothing subscribed it still sends one
// reply carrying a null channel and a zero count, matching Redis.
func (c *connState) doUnsubscribe(argv [][]byte, kind int) {
	verb := []byte(unsubVerb(kind))
	if len(argv) < 2 {
		cm := c.connSubMap(kind)
		if len(cm) == 0 {
			c.writeArrayHeader(3)
			c.writeBulk(verb)
			c.writeNil()
			c.writeInt(int64(c.subCount(kind)))
			return
		}
		names := make([]string, 0, len(cm))
		for n := range cm {
			names = append(names, n)
		}
		for _, ns := range names {
			c.removeSub(kind, ns)
			c.writeArrayHeader(3)
			c.writeBulk(verb)
			c.writeBulk([]byte(ns))
			c.writeInt(int64(c.subCount(kind)))
		}
		return
	}
	for _, name := range argv[1:] {
		c.removeSub(kind, string(name))
		c.writeArrayHeader(3)
		c.writeBulk(verb)
		c.writeBulk(name)
		c.writeInt(int64(c.subCount(kind)))
	}
}

// removeSub drops one subscription from both the connection set and the server registry, deleting
// the registry entry when its last subscriber leaves so the table stays bounded to channels under
// active subscription. Removing a name the connection never held is a no-op, matching Redis's
// unsubscribe-from-nothing (the caller still confirms with the unchanged count). psMode is
// recomputed so the connection leaves subscribe context once its last subscription is gone.
func (c *connState) removeSub(kind int, ns string) {
	cm := c.connSubMap(kind)
	if _, ok := cm[ns]; ok {
		delete(cm, ns)
		s := c.srv
		s.psMu.Lock()
		if sm := c.srvSubMap(kind)[ns]; sm != nil {
			delete(sm, c)
			if len(sm) == 0 {
				delete(c.srvSubMap(kind), ns)
			}
		}
		s.psMu.Unlock()
	}
	c.psMode = c.stillSubscribed()
}

// cmdPublish handles PUBLISH and SPUBLISH. It builds the message frame, snapshots the set of
// target connections under the registry lock, then delivers outside the lock and replies with the
// number of deliveries. A regular PUBLISH reaches the channel's direct subscribers plus every
// subscriber whose pattern matches; SPUBLISH reaches only the shard channel's subscribers. The
// count is per-delivery, so a client subscribed to a channel and to two matching patterns counts
// three, matching Redis's receiver count.
func (c *connState) cmdPublish(argv [][]byte, shard bool) {
	verbName := "publish"
	if shard {
		verbName = "spublish"
	}
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for '" + verbName + "' command")
		return
	}
	channel, msg := argv[1], argv[2]
	msgVerb := "message"
	if shard {
		msgVerb = "smessage"
	}
	// The direct-channel frame is identical for every direct subscriber, so build it once.
	chFrame := pubFrame([]byte(msgVerb), channel, msg)

	type target struct {
		t     *connState
		frame []byte
	}
	var targets []target
	s := c.srv
	s.psMu.Lock()
	if shard {
		for t := range s.psShard[string(channel)] {
			targets = append(targets, target{t, chFrame})
		}
	} else {
		for t := range s.psChan[string(channel)] {
			targets = append(targets, target{t, chFrame})
		}
		// Each matching pattern produces a pmessage frame naming the pattern that matched, shared
		// by that pattern's subscribers.
		for pat, subs := range s.psPat {
			if globMatch([]byte(pat), channel) {
				pf := pubFrame([]byte("pmessage"), []byte(pat), channel, msg)
				for t := range subs {
					targets = append(targets, target{t, pf})
				}
			}
		}
	}
	s.psMu.Unlock()

	for _, d := range targets {
		if d.t == c {
			// The publisher is itself subscribed: its own out buffer carries the frame, flushed
			// by its own driver, so it never cross-writes to itself.
			c.out = append(c.out, d.frame...)
		} else {
			d.t.deliver(d.frame)
		}
	}
	c.writeInt(int64(len(targets)))
}

// cmdPubSub handles the PUBSUB introspection subcommands. CHANNELS/SHARDCHANNELS list active
// channels (those with at least one subscriber), optionally glob-filtered; NUMSUB/SHARDNUMSUB
// report exact-channel subscriber counts (patterns are not counted); NUMPAT reports the number of
// distinct patterns with at least one subscriber.
func (c *connState) cmdPubSub(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'pubsub' command")
		return
	}
	sub := argv[1]
	switch {
	case eqFold(sub, "CHANNELS"):
		c.pubsubChannels(argv, c.srv.psChan)
	case eqFold(sub, "SHARDCHANNELS"):
		c.pubsubChannels(argv, c.srv.psShard)
	case eqFold(sub, "NUMSUB"):
		c.pubsubNumSub(argv, c.srv.psChan)
	case eqFold(sub, "SHARDNUMSUB"):
		c.pubsubNumSub(argv, c.srv.psShard)
	case eqFold(sub, "NUMPAT"):
		s := c.srv
		s.psMu.Lock()
		n := len(s.psPat)
		s.psMu.Unlock()
		c.writeInt(int64(n))
	default:
		c.writeErr("ERR Unknown PUBSUB subcommand or wrong number of arguments for '" + string(sub) + "'")
	}
}

// pubsubChannels lists the active channels of a registry map (those with at least one
// subscriber), applying the optional glob filter in argv[2]. It snapshots the names under the
// lock, then writes the reply outside it.
func (c *connState) pubsubChannels(argv [][]byte, m map[string]map[*connState]struct{}) {
	var pat []byte
	if len(argv) >= 3 {
		pat = argv[2]
	}
	s := c.srv
	s.psMu.Lock()
	names := make([][]byte, 0, len(m))
	for ch, subs := range m {
		if len(subs) == 0 {
			continue
		}
		if pat != nil && !globMatch(pat, []byte(ch)) {
			continue
		}
		names = append(names, []byte(ch))
	}
	s.psMu.Unlock()
	c.writeArrayHeader(len(names))
	for _, n := range names {
		c.writeBulk(n)
	}
}

// pubsubNumSub reports the exact-channel subscriber count for each channel named in argv[2:], as
// a flat array of channel, count, channel, count. Patterns are never counted here.
func (c *connState) pubsubNumSub(argv [][]byte, m map[string]map[*connState]struct{}) {
	chans := argv[2:]
	counts := make([]int, len(chans))
	s := c.srv
	s.psMu.Lock()
	for i, ch := range chans {
		counts[i] = len(m[string(ch)])
	}
	s.psMu.Unlock()
	c.writeArrayHeader(len(chans) * 2)
	for i, ch := range chans {
		c.writeBulk(ch)
		c.writeInt(int64(counts[i]))
	}
}

// unsubscribeAll drops every subscription this connection holds, in all three namespaces, without
// sending confirmations. It is the pub/sub teardown behind RESET and a disconnect, mirroring
// unwatchAll for the watch table. Each registry entry is removed when its last subscriber leaves.
func (c *connState) unsubscribeAll() {
	if c.psChannels == nil && c.psPatterns == nil && c.psShard == nil {
		return
	}
	s := c.srv
	s.psMu.Lock()
	for ch := range c.psChannels {
		if sm := s.psChan[ch]; sm != nil {
			delete(sm, c)
			if len(sm) == 0 {
				delete(s.psChan, ch)
			}
		}
	}
	for p := range c.psPatterns {
		if sm := s.psPat[p]; sm != nil {
			delete(sm, c)
			if len(sm) == 0 {
				delete(s.psPat, p)
			}
		}
	}
	for sh := range c.psShard {
		if sm := s.psShard[sh]; sm != nil {
			delete(sm, c)
			if len(sm) == 0 {
				delete(s.psShard, sh)
			}
		}
	}
	s.psMu.Unlock()
	c.psChannels = nil
	c.psPatterns = nil
	c.psShard = nil
	c.psMode = false
}

// writeToConn is the goroutine driver's deliver hook: it writes a message frame straight to the
// subscriber's socket under writeMu, so a publisher on another goroutine and this connection's own
// per-batch flush serialize and never interleave a partial frame. A write error is ignored here;
// the connection's own loop observes the same error on its next read or flush and tears down.
func (c *connState) writeToConn(frame []byte) {
	c.writeMu.Lock()
	_, _ = c.conn.Write(frame)
	c.writeMu.Unlock()
}

// allowedInSubscribeMode reports whether a command is permitted while the connection is in
// subscribe context under RESP2. Redis restricts a subscribed connection to the subscribe and
// unsubscribe verbs plus PING, QUIT, and RESET; every other command is refused.
func allowedInSubscribeMode(cmd []byte) bool {
	switch {
	case eqFold(cmd, "SUBSCRIBE"), eqFold(cmd, "UNSUBSCRIBE"),
		eqFold(cmd, "PSUBSCRIBE"), eqFold(cmd, "PUNSUBSCRIBE"),
		eqFold(cmd, "SSUBSCRIBE"), eqFold(cmd, "SUNSUBSCRIBE"),
		eqFold(cmd, "PING"), eqFold(cmd, "QUIT"), eqFold(cmd, "RESET"):
		return true
	}
	return false
}

// lowerName returns the command name lower-cased, for the subscribe-context refusal message,
// which names the command the way Redis does ("Can't execute 'get': ...").
func lowerName(cmd []byte) string {
	b := make([]byte, len(cmd))
	for i := 0; i < len(cmd); i++ {
		x := cmd[i]
		if x >= 'A' && x <= 'Z' {
			x += 32
		}
		b[i] = x
	}
	return string(b)
}

// pubFrame builds a RESP2 array of bulk strings, the shape every pushed pub/sub frame takes
// (message, pmessage, smessage). The bytes of each part are copied into the returned frame, so it
// is self-contained and safe to hand to another goroutine or hold in a loop's delivery queue after
// the source argv buffer is reused.
func pubFrame(parts ...[]byte) []byte {
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(parts)), 10)
	b = append(b, '\r', '\n')
	for _, p := range parts {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(p)), 10)
		b = append(b, '\r', '\n')
		b = append(b, p...)
		b = append(b, '\r', '\n')
	}
	return b
}
