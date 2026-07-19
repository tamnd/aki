package drivers

import (
	"sync"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// pubsubRegistry is the network-layer channel directory (spec 2064/f3/17 section
// 13): a channel name maps to the set of connections subscribed to it. Channels
// are not keys; they have no owner shard, no size band, and no LTM story, and the
// shard workers never see them, so a PUBLISH storm and a GET contend on nothing.
// One mutex guards the map because PUBLISH runs on one connection's reader
// goroutine while SUBSCRIBE on another's mutates the same map, unlike the
// per-connection waiter set the blocking path uses, which a single owner
// serializes. It keeps the exact-channel registry, the glob-pattern registry
// (PSUBSCRIBE), and the shard-channel registry (SSUBSCRIBE). The three are
// independent namespaces: a message published to one never crosses to another.
// f3 is a single node, so a shard channel is a plain channel in its own directory
// rather than a slot-routed one, the standalone shape of the cluster surface.
type pubsubRegistry struct {
	mu            sync.Mutex
	channels      map[string]map[*connState]struct{}
	patterns      map[string]map[*connState]struct{}
	shardChannels map[string]map[*connState]struct{}
}

func newPubsubRegistry() *pubsubRegistry {
	return &pubsubRegistry{
		channels:      make(map[string]map[*connState]struct{}),
		patterns:      make(map[string]map[*connState]struct{}),
		shardChannels: make(map[string]map[*connState]struct{}),
	}
}

// subscribe records cs as a subscriber of channel and returns the connection's
// total subscription count, channels plus patterns, the number redis's confirmation
// carries. A duplicate subscribe to a channel the connection already holds
// re-confirms without double-adding, matching redis. cs.subs is reader-owned so it
// moves outside the lock; only the shared reverse index takes the mutex.
func (r *pubsubRegistry) subscribe(cs *connState, channel string) int {
	if cs.subs == nil {
		cs.subs = make(map[string]struct{})
	}
	if _, dup := cs.subs[channel]; !dup {
		cs.subs[channel] = struct{}{}
		r.mu.Lock()
		subs := r.channels[channel]
		if subs == nil {
			subs = make(map[*connState]struct{})
			r.channels[channel] = subs
		}
		subs[cs] = struct{}{}
		r.mu.Unlock()
		cs.subCount.Store(int64(len(cs.subs)))
	}
	return cs.subTotal()
}

// unsubscribe drops cs from channel and returns the remaining total subscription
// count. An unsubscribe from a channel the connection does not hold is a no-op
// that still reports the current count, matching redis.
func (r *pubsubRegistry) unsubscribe(cs *connState, channel string) int {
	if _, ok := cs.subs[channel]; ok {
		delete(cs.subs, channel)
		r.mu.Lock()
		r.dropLocked(r.channels, channel, cs)
		r.mu.Unlock()
		cs.subCount.Store(int64(len(cs.subs)))
	}
	return cs.subTotal()
}

// psubscribe records cs as a subscriber of a glob pattern and returns the total
// subscription count. The pattern registry mirrors the channel registry: a
// pattern maps to the connections that PSUBSCRIBEd it, and PUBLISH fans a message
// out to every pattern whose glob matches the published channel. A duplicate
// re-confirms without double-adding.
func (r *pubsubRegistry) psubscribe(cs *connState, pattern string) int {
	if cs.psubs == nil {
		cs.psubs = make(map[string]struct{})
	}
	if _, dup := cs.psubs[pattern]; !dup {
		cs.psubs[pattern] = struct{}{}
		r.mu.Lock()
		subs := r.patterns[pattern]
		if subs == nil {
			subs = make(map[*connState]struct{})
			r.patterns[pattern] = subs
		}
		subs[cs] = struct{}{}
		r.mu.Unlock()
		cs.psubCount.Store(int64(len(cs.psubs)))
	}
	return cs.subTotal()
}

// punsubscribe drops cs from a pattern and returns the remaining total count. A
// punsubscribe from a pattern the connection does not hold is a no-op that still
// reports the current count.
func (r *pubsubRegistry) punsubscribe(cs *connState, pattern string) int {
	if _, ok := cs.psubs[pattern]; ok {
		delete(cs.psubs, pattern)
		r.mu.Lock()
		r.dropLocked(r.patterns, pattern, cs)
		r.mu.Unlock()
		cs.psubCount.Store(int64(len(cs.psubs)))
	}
	return cs.subTotal()
}

// ssubscribe records cs as a subscriber of a shard channel and returns the shard
// subscription count. Shard channels are a separate namespace from regular
// channels and patterns, so the count is len(ssubs) alone, matching redis's
// SSUBSCRIBE confirmation.
func (r *pubsubRegistry) ssubscribe(cs *connState, channel string) int {
	if cs.ssubs == nil {
		cs.ssubs = make(map[string]struct{})
	}
	if _, dup := cs.ssubs[channel]; !dup {
		cs.ssubs[channel] = struct{}{}
		r.mu.Lock()
		subs := r.shardChannels[channel]
		if subs == nil {
			subs = make(map[*connState]struct{})
			r.shardChannels[channel] = subs
		}
		subs[cs] = struct{}{}
		r.mu.Unlock()
		cs.ssubCount.Store(int64(len(cs.ssubs)))
	}
	return len(cs.ssubs)
}

// sunsubscribe drops cs from a shard channel and returns the remaining shard
// subscription count. A no-op still reports the current count.
func (r *pubsubRegistry) sunsubscribe(cs *connState, channel string) int {
	if _, ok := cs.ssubs[channel]; ok {
		delete(cs.ssubs, channel)
		r.mu.Lock()
		r.dropLocked(r.shardChannels, channel, cs)
		r.mu.Unlock()
		cs.ssubCount.Store(int64(len(cs.ssubs)))
	}
	return len(cs.ssubs)
}

// dropLocked removes cs from one directory's subscriber set for a name (a channel
// or a pattern) and forgets an emptied name. The caller holds the mutex.
func (r *pubsubRegistry) dropLocked(dir map[string]map[*connState]struct{}, name string, cs *connState) {
	subs := dir[name]
	if subs == nil {
		return
	}
	delete(subs, cs)
	if len(subs) == 0 {
		delete(dir, name)
	}
}

// publish fans a message out to every subscriber of channel and returns the
// number of connections it reached. The subscriber set is snapshotted under the
// lock and the deliveries run outside it, so a slow wake never stalls a
// concurrent SUBSCRIBE and the registry mutex never nests under a connection
// waker. The message wire form is built once and every subscriber copies the
// same bytes into its own node; the reactor's vectored write shares the one
// buffer instead, the PUBLISH gate row's refinement (doc 17 section 13).
func (r *pubsubRegistry) publish(channel string, message []byte) int {
	// Snapshot the exact-channel targets and the matched-pattern targets under one
	// lock hold, then deliver outside it. Pattern matching runs the same glob the
	// channel introspection uses, once per live pattern, so a publish to a channel
	// with no pattern subscribers pays only a map walk. A connection subscribed both
	// to the channel and to a matching pattern is delivered to twice, a message and
	// a pmessage, and counted twice, matching redis.
	r.mu.Lock()
	var chanTargets []*shard.Conn
	if subs := r.channels[channel]; len(subs) > 0 {
		chanTargets = make([]*shard.Conn, 0, len(subs))
		for cs := range subs {
			chanTargets = append(chanTargets, cs.sc)
		}
	}
	type patTarget struct {
		sc      *shard.Conn
		pattern string
	}
	var patTargets []patTarget
	if len(r.patterns) > 0 {
		ch := []byte(channel)
		for pattern, subs := range r.patterns {
			if !globMatch([]byte(pattern), ch) {
				continue
			}
			for cs := range subs {
				patTargets = append(patTargets, patTarget{sc: cs.sc, pattern: pattern})
			}
		}
	}
	r.mu.Unlock()

	// The subscriber set can mix RESP2 and RESP3 connections, so each frame is
	// built at most once per protocol and reused: the RESP2 wire is the *3 array,
	// the RESP3 wire the >3 push, picked per subscriber from its negotiated
	// version. A single-protocol fan-out (the common case) still builds one wire.
	if len(chanTargets) > 0 {
		var wire2, wire3 []byte
		for _, sc := range chanTargets {
			if sc.Resp3() {
				if wire3 == nil {
					wire3 = appendMessage(nil, channel, message, true)
				}
				sc.DeliverOOB(wire3)
			} else {
				if wire2 == nil {
					wire2 = appendMessage(nil, channel, message, false)
				}
				sc.DeliverOOB(wire2)
			}
		}
	}
	for _, t := range patTargets {
		t.sc.DeliverOOB(appendPMessage(nil, t.pattern, channel, message, t.sc.Resp3()))
	}
	return len(chanTargets) + len(patTargets)
}

// channelList returns the channels with at least one subscriber, filtered by the
// optional glob pattern. A nil pattern returns every active channel.
func (r *pubsubRegistry) channelList(pattern []byte) []string {
	r.mu.Lock()
	out := make([]string, 0, len(r.channels))
	for ch := range r.channels {
		if pattern == nil || globMatch(pattern, []byte(ch)) {
			out = append(out, ch)
		}
	}
	r.mu.Unlock()
	return out
}

// numSub returns the subscriber count of one channel, zero for an unknown one.
func (r *pubsubRegistry) numSub(channel string) int {
	r.mu.Lock()
	n := len(r.channels[channel])
	r.mu.Unlock()
	return n
}

// numPat returns the number of distinct patterns with at least one subscriber,
// the PUBSUB NUMPAT figure.
func (r *pubsubRegistry) numPat() int {
	r.mu.Lock()
	n := len(r.patterns)
	r.mu.Unlock()
	return n
}

// spublish fans a message out to every subscriber of a shard channel and returns
// the number of connections it reached. Shard channels are an independent
// namespace, so a regular PUBLISH never lands here and pattern subscribers are
// not consulted; the push carries the "smessage" kind, not "message". The
// snapshot-then-deliver shape matches publish so a slow wake never stalls a
// concurrent SSUBSCRIBE.
func (r *pubsubRegistry) spublish(channel string, message []byte) int {
	r.mu.Lock()
	var targets []*shard.Conn
	if subs := r.shardChannels[channel]; len(subs) > 0 {
		targets = make([]*shard.Conn, 0, len(subs))
		for cs := range subs {
			targets = append(targets, cs.sc)
		}
	}
	r.mu.Unlock()

	if len(targets) > 0 {
		var wire2, wire3 []byte
		for _, sc := range targets {
			if sc.Resp3() {
				if wire3 == nil {
					wire3 = appendSMessage(nil, channel, message, true)
				}
				sc.DeliverOOB(wire3)
			} else {
				if wire2 == nil {
					wire2 = appendSMessage(nil, channel, message, false)
				}
				sc.DeliverOOB(wire2)
			}
		}
	}
	return len(targets)
}

// shardChannelList returns the shard channels with at least one subscriber,
// filtered by the optional glob pattern. A nil pattern returns every active
// shard channel. It is the PUBSUB SHARDCHANNELS figure.
func (r *pubsubRegistry) shardChannelList(pattern []byte) []string {
	r.mu.Lock()
	out := make([]string, 0, len(r.shardChannels))
	for ch := range r.shardChannels {
		if pattern == nil || globMatch(pattern, []byte(ch)) {
			out = append(out, ch)
		}
	}
	r.mu.Unlock()
	return out
}

// shardNumSub returns the subscriber count of one shard channel, zero for an
// unknown one, the PUBSUB SHARDNUMSUB figure.
func (r *pubsubRegistry) shardNumSub(channel string) int {
	r.mu.Lock()
	n := len(r.shardChannels[channel])
	r.mu.Unlock()
	return n
}

// removeConn drops a departing connection from every channel it held. It runs
// from the connection teardown (unregister), after both connection goroutines
// have joined, so cs.subs is this goroutine's alone; the reverse index still
// takes the mutex against live publishers.
func (r *pubsubRegistry) removeConn(cs *connState) {
	if cs.subs == nil && cs.psubs == nil && cs.ssubs == nil {
		return
	}
	r.mu.Lock()
	for channel := range cs.subs {
		r.dropLocked(r.channels, channel, cs)
	}
	for pattern := range cs.psubs {
		r.dropLocked(r.patterns, pattern, cs)
	}
	for channel := range cs.ssubs {
		r.dropLocked(r.shardChannels, channel, cs)
	}
	r.mu.Unlock()
	cs.subs = nil
	cs.psubs = nil
	cs.ssubs = nil
	cs.subCount.Store(0)
	cs.psubCount.Store(0)
	cs.ssubCount.Store(0)
}

// pubsubIntercept handles the pub/sub command family in the network layer,
// before dispatch.Dispatch, so it never enters the shard hop. It returns true
// when it owned the command (a pub/sub verb, or a command refused because the
// connection is in subscribe mode) and false to let the normal dispatch run.
// The confirmations it writes are solicited replies, so they go through
// InlineReply and keep their pipeline order; a wire failure there tears the
// connection down, surfaced as a false from the caller's boundary.
func (s *Server) pubsubIntercept(c *shard.Conn, cs *connState, args [][]byte) bool {
	switch {
	case eqFold(args[0], "SUBSCRIBE"):
		s.doSubscribe(c, cs, args)
		return true
	case eqFold(args[0], "UNSUBSCRIBE"):
		s.doUnsubscribe(c, cs, args)
		return true
	case eqFold(args[0], "PSUBSCRIBE"):
		s.doPsubscribe(c, cs, args)
		return true
	case eqFold(args[0], "PUNSUBSCRIBE"):
		s.doPunsubscribe(c, cs, args)
		return true
	case eqFold(args[0], "SSUBSCRIBE"):
		s.doSsubscribe(c, cs, args)
		return true
	case eqFold(args[0], "SUNSUBSCRIBE"):
		s.doSunsubscribe(c, cs, args)
		return true
	case eqFold(args[0], "PUBLISH"):
		s.doPublish(c, args)
		return true
	case eqFold(args[0], "SPUBLISH"):
		s.doSpublish(c, args)
		return true
	case eqFold(args[0], "PUBSUB"):
		s.doPubsub(c, args)
		return true
	}
	// Not a pub/sub verb. In subscribe mode a RESP2 connection may only run the
	// subscribe family plus PING/QUIT/RESET; anything else is refused in order.
	// PING is allowed and falls through to the shard hop, which answers it
	// normally. QUIT and RESET are not registered in f3, so they fall through to
	// the unknown-command answer they already give, a pre-existing gap. A RESP3
	// connection carries pushes out of band, so it stays fully usable for ordinary
	// commands while subscribed and the restriction does not apply, matching redis.
	if !c.Resp3() && cs.inSubscribeMode() && !subscribeModeAllowed(args[0]) {
		_ = c.InlineReply(resp.AppendError(nil,
			"ERR Can't execute '"+lowerVerb(args[0])+"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"))
		return true
	}
	return false
}

// subscribeModeAllowed reports whether a non-pub/sub command may run while the
// connection is in subscribe mode. The subscribe verbs (P/S variants) are all
// handled by the intercept above and never reach this test; they stay on the
// list as documentation of the allowed set.
func subscribeModeAllowed(verb []byte) bool {
	switch {
	case eqFold(verb, "PING"), eqFold(verb, "QUIT"), eqFold(verb, "RESET"),
		eqFold(verb, "PSUBSCRIBE"), eqFold(verb, "PUNSUBSCRIBE"),
		eqFold(verb, "SSUBSCRIBE"), eqFold(verb, "SUNSUBSCRIBE"):
		return true
	}
	return false
}

// doSubscribe subscribes the connection to each named channel and confirms each
// in order. Arity is at least one channel.
func (s *Server) doSubscribe(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) < 2 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'subscribe' command"))
		return
	}
	for _, ch := range args[1:] {
		n := s.pubsub.subscribe(cs, string(ch))
		_ = c.InlineReply(appendSubConfirm(nil, "subscribe", ch, n, c.Resp3()))
	}
}

// doUnsubscribe drops the named channels, or every held channel when none are
// named, confirming each in order. Unsubscribing from all on a connection that
// holds none answers one confirmation with a nil channel and count zero, the
// redis shape.
func (s *Server) doUnsubscribe(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) >= 2 {
		for _, ch := range args[1:] {
			n := s.pubsub.unsubscribe(cs, string(ch))
			_ = c.InlineReply(appendSubConfirm(nil, "unsubscribe", ch, n, c.Resp3()))
		}
		return
	}
	if len(cs.subs) == 0 {
		_ = c.InlineReply(appendUnsubNil(nil, c.Resp3()))
		return
	}
	held := make([]string, 0, len(cs.subs))
	for ch := range cs.subs {
		held = append(held, ch)
	}
	for _, ch := range held {
		n := s.pubsub.unsubscribe(cs, ch)
		_ = c.InlineReply(appendSubConfirm(nil, "unsubscribe", []byte(ch), n, c.Resp3()))
	}
}

// doPsubscribe subscribes the connection to each named glob pattern and confirms
// each in order. Arity is at least one pattern.
func (s *Server) doPsubscribe(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) < 2 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'psubscribe' command"))
		return
	}
	for _, pat := range args[1:] {
		n := s.pubsub.psubscribe(cs, string(pat))
		_ = c.InlineReply(appendSubConfirm(nil, "psubscribe", pat, n, c.Resp3()))
	}
}

// doPunsubscribe drops the named patterns, or every held pattern when none are
// named, confirming each in order. Punsubscribing from all on a connection that
// holds none answers one confirmation with a nil pattern and count zero, the
// redis shape.
func (s *Server) doPunsubscribe(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) >= 2 {
		for _, pat := range args[1:] {
			n := s.pubsub.punsubscribe(cs, string(pat))
			_ = c.InlineReply(appendSubConfirm(nil, "punsubscribe", pat, n, c.Resp3()))
		}
		return
	}
	if len(cs.psubs) == 0 {
		_ = c.InlineReply(appendPunsubNil(nil, c.Resp3()))
		return
	}
	held := make([]string, 0, len(cs.psubs))
	for pat := range cs.psubs {
		held = append(held, pat)
	}
	for _, pat := range held {
		n := s.pubsub.punsubscribe(cs, pat)
		_ = c.InlineReply(appendSubConfirm(nil, "punsubscribe", []byte(pat), n, c.Resp3()))
	}
}

// doSsubscribe subscribes the connection to each named shard channel and confirms
// each in order. The confirmation count is the shard subscription count alone,
// separate from the regular channel-plus-pattern total, matching redis. Arity is
// at least one shard channel.
func (s *Server) doSsubscribe(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) < 2 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'ssubscribe' command"))
		return
	}
	for _, ch := range args[1:] {
		n := s.pubsub.ssubscribe(cs, string(ch))
		_ = c.InlineReply(appendSubConfirm(nil, "ssubscribe", ch, n, c.Resp3()))
	}
}

// doSunsubscribe drops the named shard channels, or every held shard channel when
// none are named, confirming each in order. Sunsubscribing from all on a
// connection that holds none answers one confirmation with a nil channel and
// count zero, the redis shape.
func (s *Server) doSunsubscribe(c *shard.Conn, cs *connState, args [][]byte) {
	if len(args) >= 2 {
		for _, ch := range args[1:] {
			n := s.pubsub.sunsubscribe(cs, string(ch))
			_ = c.InlineReply(appendSubConfirm(nil, "sunsubscribe", ch, n, c.Resp3()))
		}
		return
	}
	if len(cs.ssubs) == 0 {
		_ = c.InlineReply(appendSunsubNil(nil, c.Resp3()))
		return
	}
	held := make([]string, 0, len(cs.ssubs))
	for ch := range cs.ssubs {
		held = append(held, ch)
	}
	for _, ch := range held {
		n := s.pubsub.sunsubscribe(cs, ch)
		_ = c.InlineReply(appendSubConfirm(nil, "sunsubscribe", []byte(ch), n, c.Resp3()))
	}
}

// doPublish delivers the message to the channel's subscribers and answers the
// receiver count. Arity is exactly a channel and a message.
func (s *Server) doPublish(c *shard.Conn, args [][]byte) {
	if len(args) != 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'publish' command"))
		return
	}
	n := s.pubsub.publish(string(args[1]), args[2])
	_ = c.InlineReply(resp.AppendInt(nil, int64(n)))
}

// doSpublish delivers the message to the shard channel's subscribers and answers
// the receiver count. Arity is exactly a shard channel and a message.
func (s *Server) doSpublish(c *shard.Conn, args [][]byte) {
	if len(args) != 3 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'spublish' command"))
		return
	}
	n := s.pubsub.spublish(string(args[1]), args[2])
	_ = c.InlineReply(resp.AppendInt(nil, int64(n)))
}

// doPubsub answers the introspection subcommands. CHANNELS lists active channels
// under an optional pattern, NUMSUB reports subscriber counts for named
// channels, and NUMPAT reports the number of distinct subscribed patterns.
func (s *Server) doPubsub(c *shard.Conn, args [][]byte) {
	if len(args) < 2 {
		_ = c.InlineReply(resp.AppendError(nil, "ERR wrong number of arguments for 'pubsub' command"))
		return
	}
	switch {
	case eqFold(args[1], "CHANNELS"):
		var pattern []byte
		if len(args) >= 3 {
			pattern = args[2]
		}
		chans := s.pubsub.channelList(pattern)
		out := resp.AppendArrayHeader(nil, len(chans))
		for _, ch := range chans {
			out = resp.AppendBulk(out, []byte(ch))
		}
		_ = c.InlineReply(out)
	case eqFold(args[1], "NUMSUB"):
		names := args[2:]
		out := resp.AppendArrayHeader(nil, len(names)*2)
		for _, ch := range names {
			out = resp.AppendBulk(out, ch)
			out = resp.AppendInt(out, int64(s.pubsub.numSub(string(ch))))
		}
		_ = c.InlineReply(out)
	case eqFold(args[1], "NUMPAT"):
		_ = c.InlineReply(resp.AppendInt(nil, int64(s.pubsub.numPat())))
	case eqFold(args[1], "SHARDCHANNELS"):
		var pattern []byte
		if len(args) >= 3 {
			pattern = args[2]
		}
		chans := s.pubsub.shardChannelList(pattern)
		out := resp.AppendArrayHeader(nil, len(chans))
		for _, ch := range chans {
			out = resp.AppendBulk(out, []byte(ch))
		}
		_ = c.InlineReply(out)
	case eqFold(args[1], "SHARDNUMSUB"):
		names := args[2:]
		out := resp.AppendArrayHeader(nil, len(names)*2)
		for _, ch := range names {
			out = resp.AppendBulk(out, ch)
			out = resp.AppendInt(out, int64(s.pubsub.shardNumSub(string(ch))))
		}
		_ = c.InlineReply(out)
	default:
		_ = c.InlineReply(resp.AppendError(nil, "ERR Unknown PUBSUB subcommand or wrong number of arguments"))
	}
}

// pubsubHeader writes the header of a pub/sub frame of n elements: a RESP3 push
// header (>n) when the connection negotiated RESP3, else the RESP2 array header
// (*n) the same n elements carry otherwise. Under RESP3 both the unsolicited
// messages and the subscribe confirmations ride push frames so a subscribed
// connection stays usable for ordinary command replies, matching redis's
// addReplyPushLen call sites; under RESP2 they are indistinguishable arrays, the
// pre-RESP3 shape.
func pubsubHeader(dst []byte, n int, resp3 bool) []byte {
	if resp3 {
		return resp.AppendPushHeader(dst, n)
	}
	return resp.AppendArrayHeader(dst, n)
}

// pubsubNil writes the nil channel slot of a from-nothing confirmation: the
// RESP3 null (_) under RESP3, else the RESP2 null bulk ($-1).
func pubsubNil(dst []byte, resp3 bool) []byte {
	if resp3 {
		return resp.AppendNull3(dst)
	}
	return resp.AppendNull(dst)
}

// appendMessage builds the unsolicited message push: a three-element frame of
// "message", the channel, and the payload.
func appendMessage(dst []byte, channel string, message []byte, resp3 bool) []byte {
	dst = pubsubHeader(dst, 3, resp3)
	dst = resp.AppendBulk(dst, []byte("message"))
	dst = resp.AppendBulk(dst, []byte(channel))
	dst = resp.AppendBulk(dst, message)
	return dst
}

// appendPMessage builds the unsolicited pattern message push: a four-element
// frame of "pmessage", the pattern that matched, the channel, and the payload.
func appendPMessage(dst []byte, pattern, channel string, message []byte, resp3 bool) []byte {
	dst = pubsubHeader(dst, 4, resp3)
	dst = resp.AppendBulk(dst, []byte("pmessage"))
	dst = resp.AppendBulk(dst, []byte(pattern))
	dst = resp.AppendBulk(dst, []byte(channel))
	dst = resp.AppendBulk(dst, message)
	return dst
}

// appendSMessage builds the unsolicited shard message push: a three-element
// frame of "smessage", the shard channel, and the payload. It mirrors the
// regular message push with the shard-specific kind.
func appendSMessage(dst []byte, channel string, message []byte, resp3 bool) []byte {
	dst = pubsubHeader(dst, 3, resp3)
	dst = resp.AppendBulk(dst, []byte("smessage"))
	dst = resp.AppendBulk(dst, []byte(channel))
	dst = resp.AppendBulk(dst, message)
	return dst
}

// appendSubConfirm builds a subscribe or unsubscribe confirmation: the kind, the
// channel, and the connection's running subscription count.
func appendSubConfirm(dst []byte, kind string, channel []byte, count int, resp3 bool) []byte {
	dst = pubsubHeader(dst, 3, resp3)
	dst = resp.AppendBulk(dst, []byte(kind))
	dst = resp.AppendBulk(dst, channel)
	dst = resp.AppendInt(dst, int64(count))
	return dst
}

// appendUnsubNil builds the UNSUBSCRIBE-from-nothing confirmation: "unsubscribe",
// a nil channel, and count zero.
func appendUnsubNil(dst []byte, resp3 bool) []byte {
	dst = pubsubHeader(dst, 3, resp3)
	dst = resp.AppendBulk(dst, []byte("unsubscribe"))
	dst = pubsubNil(dst, resp3)
	dst = resp.AppendInt(dst, 0)
	return dst
}

// appendPunsubNil builds the PUNSUBSCRIBE-from-nothing confirmation:
// "punsubscribe", a nil pattern, and count zero.
func appendPunsubNil(dst []byte, resp3 bool) []byte {
	dst = pubsubHeader(dst, 3, resp3)
	dst = resp.AppendBulk(dst, []byte("punsubscribe"))
	dst = pubsubNil(dst, resp3)
	dst = resp.AppendInt(dst, 0)
	return dst
}

// appendSunsubNil builds the SUNSUBSCRIBE-from-nothing confirmation:
// "sunsubscribe", a nil channel, and count zero.
func appendSunsubNil(dst []byte, resp3 bool) []byte {
	dst = pubsubHeader(dst, 3, resp3)
	dst = resp.AppendBulk(dst, []byte("sunsubscribe"))
	dst = pubsubNil(dst, resp3)
	dst = resp.AppendInt(dst, 0)
	return dst
}

// eqFold reports whether an argument token equals an ASCII command word, case
// insensitively, without allocating. It is the drivers-package twin of the
// dispatch table's tokenIs.
func eqFold(arg []byte, word string) bool {
	if len(arg) != len(word) {
		return false
	}
	for i := 0; i < len(arg); i++ {
		ch := arg[i]
		if ch >= 'a' && ch <= 'z' {
			ch -= 'a' - 'A'
		}
		if ch != word[i] {
			return false
		}
	}
	return true
}

// lowerVerb renders a command token in lower case for an error message, the form
// redis prints the offending verb in.
func lowerVerb(arg []byte) string {
	b := make([]byte, len(arg))
	for i, ch := range arg {
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		b[i] = ch
	}
	return string(b)
}

// globMatch reports whether str matches the glob pattern, the same operators
// redis's stringmatchlen implements and SCAN's MATCH uses (set/scan.go): * any
// run, ? one byte, [...] a class with ranges and a leading ^ negation, and \
// escaping the next byte. Byte-oriented and case sensitive. Kept local to the
// network layer the way each keyspace keeps its own copy.
func globMatch(pattern, str []byte) bool {
	p, sIdx := 0, 0
	for p < len(pattern) {
		switch pattern[p] {
		case '*':
			for p+1 < len(pattern) && pattern[p+1] == '*' {
				p++
			}
			if p+1 == len(pattern) {
				return true
			}
			for i := sIdx; i <= len(str); i++ {
				if globMatch(pattern[p+1:], str[i:]) {
					return true
				}
			}
			return false
		case '?':
			if sIdx == len(str) {
				return false
			}
			sIdx++
			p++
		case '[':
			if sIdx == len(str) {
				return false
			}
			p++
			neg := false
			if p < len(pattern) && pattern[p] == '^' {
				neg = true
				p++
			}
			match := false
			for p < len(pattern) && pattern[p] != ']' {
				if pattern[p] == '\\' && p+1 < len(pattern) {
					p++
					if pattern[p] == str[sIdx] {
						match = true
					}
				} else if p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']' {
					lo, hi := pattern[p], pattern[p+2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if str[sIdx] >= lo && str[sIdx] <= hi {
						match = true
					}
					p += 2
				} else if pattern[p] == str[sIdx] {
					match = true
				}
				p++
			}
			if p < len(pattern) {
				p++ // consume ']'
			}
			if match == neg {
				return false
			}
			sIdx++
		case '\\':
			if p+1 < len(pattern) {
				p++
			}
			if sIdx == len(str) || pattern[p] != str[sIdx] {
				return false
			}
			sIdx++
			p++
		default:
			if sIdx == len(str) || pattern[p] != str[sIdx] {
				return false
			}
			sIdx++
			p++
		}
	}
	return sIdx == len(str)
}
