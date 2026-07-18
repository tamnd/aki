package stream

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The stream group and PEL durability round trips (spec 2064/f3/M8-collection-durability-
// plan, the second stream slice): stand up consumer groups, their consumers, and a pending-
// entries list against a stream store backed by a real .aki, close the file so only its
// durable bytes remain, reopen into a fresh registry, replay, and assert every group reads
// back with the exact cursor, consumer ordinals (nil holes kept), clocks, and pending slabs
// the first run left. It is the group arm of the entry-level round trips in durable_test.go.
//
// The command handlers write their reply through a shard.Reply a package unit test cannot
// build, so the xgroup, xreadgroup, xack, xclaim, xnack driver helpers below mirror the
// handlers' mutate-and-log bodies exactly, the driver convention the entry arm set. The
// clocks are stamped from cx.NowMs, varied per op so the round trip carries distinct delivery
// and seen and active times, not one uniform value a bug could mask.

// consumerDump is one consumer's recoverable state, or a nil hole a DELCONSUMER left.
type consumerDump struct {
	name     string
	ord      uint32
	seen     int64
	active   int64
	pelCount uint32
	present  bool
}

// pelDump is one pending entry's recoverable slab.
type pelDump struct {
	id            string
	deliveryTime  int64
	deliveryCount uint16
	consumerOrd   uint32
}

// groupDump is one group's whole recoverable state: its cursor and lag basis, its consumers
// in ordinal order (holes kept), and its pending list in id order.
type groupDump struct {
	lastDelivered string
	entriesRead   uint64
	readValid     bool
	pelCount      uint32
	consumers     []consumerDump
	pel           []pelDump
}

// dumpGroups renders a stream's groups to a comparable map, the whole state recovery must
// rebuild. A stream with no groups dumps nil, so a destroyed or never-grouped stream compares
// equal to a recovered one that rebuilt no groups.
func dumpGroups(s *stream) map[string]groupDump {
	if s == nil || len(s.groups) == 0 {
		return nil
	}
	out := make(map[string]groupDump, len(s.groups))
	for name, grp := range s.groups {
		gd := groupDump{
			lastDelivered: grp.lastDeliveredID.String(),
			entriesRead:   grp.entriesRead,
			readValid:     grp.readValid,
			pelCount:      grp.pelCount,
		}
		for _, con := range grp.consumerByOrd {
			if con == nil {
				gd.consumers = append(gd.consumers, consumerDump{})
				continue
			}
			gd.consumers = append(gd.consumers, consumerDump{
				name:     string(con.name),
				ord:      con.ord,
				seen:     con.seenTime,
				active:   con.activeTime,
				pelCount: con.pelCount,
				present:  true,
			})
		}
		if grp.pel != nil {
			grp.pel.walkFrom(streamID{}, func(pe *pelEntry) bool {
				gd.pel = append(gd.pel, pelDump{
					id:            pe.id.String(),
					deliveryTime:  pe.deliveryTime,
					deliveryCount: pe.deliveryCount,
					consumerOrd:   pe.consumerOrd,
				})
				return true
			})
		}
		out[name] = gd
	}
	return out
}

// --- group driver helpers, mirroring the handler mutate-and-log bodies ---------

// xgroupCreateD mirrors xgroupCreate: resolve the start cursor, add the group (creating the
// stream under it when it is absent), and cut the group-set effect.
func xgroupCreateD(cx *shard.Ctx, g *reg, key, name, idArg string) {
	k := []byte(key)
	s := g.live(cx, k)
	created := false
	if s == nil {
		s = newStream()
		created = true
	}
	start, read, valid, _ := groupStartID(s, []byte(idArg))
	s.addGroup([]byte(name), newGroup(start, read, valid))
	if created {
		g.m[key] = s
	}
	g.note(s)
	logGroupSet(cx, k, []byte(name), s.group([]byte(name)))
}

// xgroupCreateConsumerD mirrors xgroupCreateConsumer: add the consumer if absent and cut the
// consumer-set effect that pins its ordinal.
func xgroupCreateConsumerD(cx *shard.Ctx, g *reg, key, name, con string) {
	k := []byte(key)
	s := g.live(cx, k)
	grp := s.group([]byte(name))
	if grp.createConsumer([]byte(con), cx.NowMs) {
		g.note(s)
		logConsumerSet(cx, k, []byte(name), grp.consumer([]byte(con)))
	}
}

// xreadgroupDeliverD mirrors the `>` form of Xreadgroup: deliver the entries above the group
// cursor to the named consumer and cut the delivery effects.
func xreadgroupDeliverD(cx *shard.Ctx, g *reg, key, name, con string, count int, noack bool) []deliveredEntry {
	k := []byte(key)
	s := g.live(cx, k)
	grp := s.group([]byte(name))
	newCon := grp.consumer([]byte(con)) == nil
	c := grp.ensureConsumer([]byte(con), cx.NowMs)
	c.seenTime = cx.NowMs
	entries := grp.deliverNew(s, c, count, noack, cx.NowMs)
	g.note(s)
	logGroupDelivery(cx, k, []byte(name), grp, c, entries, noack, newCon)
	return entries
}

// xackD mirrors Xack: retire each pending id and cut a pel-del per removal.
func xackGrpD(cx *shard.Ctx, g *reg, key, name string, ids ...streamID) int {
	k := []byte(key)
	s := g.live(cx, k)
	grp := s.group([]byte(name))
	n := 0
	for _, id := range ids {
		if grp.ackOne(id) {
			n++
			logPelDel(cx, k, []byte(name), id)
		}
	}
	g.note(s)
	return n
}

// xclaimD mirrors Xclaim with default options: reassign each pending id to the target
// consumer and cut the reassigned slabs (and the drops for entries whose log entry is gone).
func xclaimGrpD(cx *shard.Ctx, g *reg, key, name, con string, minIdle int64, ids ...streamID) []streamID {
	k := []byte(key)
	s := g.live(cx, k)
	grp := s.group([]byte(name))
	newCon := grp.consumer([]byte(con)) == nil
	c := grp.ensureConsumer([]byte(con), cx.NowMs)
	c.seenTime = cx.NowMs
	var claimed []streamID
	for _, id := range ids {
		res := grp.claimOne(s, id, c, cx.NowMs, minIdle, xclaimOpts{})
		switch {
		case res.claimed:
			claimed = append(claimed, res.id)
		case res.deleted:
			logPelDel(cx, k, []byte(name), res.id)
		}
	}
	if len(claimed) > 0 {
		c.activeTime = cx.NowMs
	}
	g.note(s)
	logClaimResults(cx, k, []byte(name), grp, c, claimed, newCon)
	return claimed
}

// xnackD mirrors Xnack: release each pending id back to the group PEL and cut its resolved
// slab.
func xnackGrpD(cx *shard.Ctx, g *reg, key, name string, mode nackMode, ids ...streamID) int {
	k := []byte(key)
	s := g.live(cx, k)
	grp := s.group([]byte(name))
	n := 0
	for _, id := range ids {
		if grp.nack(s, id, mode, -1, false, false) {
			n++
			if pe, ok := grp.pel.find(id); ok {
				logPelSet(cx, k, []byte(name), pe)
			}
		}
	}
	g.note(s)
	return n
}

// xgroupSetIDD mirrors xgroupSetID: reposition the cursor and cut the group-set effect.
func xgroupSetIDD(cx *shard.Ctx, g *reg, key, name, idArg string) {
	k := []byte(key)
	s := g.live(cx, k)
	grp := s.group([]byte(name))
	start, read, valid, _ := groupStartID(s, []byte(idArg))
	grp.lastDeliveredID = start
	grp.entriesRead = read
	grp.readValid = valid
	logGroupSet(cx, k, []byte(name), grp)
}

// xgroupDelConsumerD mirrors xgroupDelConsumer: remove the consumer, draining its pending
// entries, and cut the consumer-del effect.
func xgroupDelConsumerD(cx *shard.Ctx, g *reg, key, name, con string) int64 {
	k := []byte(key)
	s := g.live(cx, k)
	grp := s.group([]byte(name))
	n := grp.delConsumer([]byte(con))
	g.note(s)
	logConsumerDel(cx, k, []byte(name), []byte(con))
	return n
}

// xgroupDestroyD mirrors xgroupDestroy: drop the group wholesale and cut the destroy effect.
func xgroupDestroyD(cx *shard.Ctx, g *reg, key, name string) {
	k := []byte(key)
	s := g.live(cx, k)
	if s.group([]byte(name)) == nil {
		return
	}
	delete(s.groups, name)
	g.note(s)
	logGroupDestroy(cx, k, []byte(name))
}

// buildGroupScenario runs the shared pre-snapshot phase both round trips use: an orders stream
// with a group that delivers, acks, claims across consumers, and nacks; a queue stream whose
// only consumer is created then deleted (leaving an ordinal hole); and a temp stream with a
// group and a live PEL. Clocks are stamped from cx.NowMs, varied per op.
func buildGroupScenario(cx *shard.Ctx, g *reg) {
	cx.NowMs = 1000
	for i := uint64(1); i <= 5; i++ {
		xaddD(cx, g, "orders", 1, i, "f", string(rune('0'+i)))
	}
	xgroupCreateD(cx, g, "orders", "g1", "0-0")
	cx.NowMs = 1100
	xgroupCreateConsumerD(cx, g, "orders", "g1", "alice") // ord 0
	cx.NowMs = 2000
	xreadgroupDeliverD(cx, g, "orders", "g1", "alice", 2, false) // 1-1, 1-2 to alice; cursor 1-2
	cx.NowMs = 3000
	xreadgroupDeliverD(cx, g, "orders", "g1", "bob", 1, false) // bob ord 1; 1-3; cursor 1-3
	cx.NowMs = 4000
	xackGrpD(cx, g, "orders", "g1", streamID{1, 1}) // retire 1-1
	cx.NowMs = 5000
	xclaimGrpD(cx, g, "orders", "g1", "alice", 0, streamID{1, 3}) // 1-3 bob -> alice, count 2
	cx.NowMs = 6000
	xnackGrpD(cx, g, "orders", "g1", nackSilent, streamID{1, 2}) // 1-2 unowned, count 0

	// queue: a group at the tail whose only consumer is created by a `>` read that delivers
	// nothing, then removed, leaving an ordinal hole the snapshot must keep.
	cx.NowMs = 1000
	xaddD(cx, g, "queue", 2, 1, "q", "1")
	xgroupCreateD(cx, g, "queue", "g2", "$")
	cx.NowMs = 1200
	xreadgroupDeliverD(cx, g, "queue", "g2", "carol", 10, false) // creates carol ord 0, delivers nothing
	cx.NowMs = 1300
	xgroupDelConsumerD(cx, g, "queue", "g2", "carol") // hole at ord 0

	// temp: a group with a live PEL, destroyed later.
	cx.NowMs = 1000
	xaddD(cx, g, "temp", 7, 1, "t", "1")
	xaddD(cx, g, "temp", 7, 2, "t", "2")
	xgroupCreateD(cx, g, "temp", "gtmp", "0-0")
	cx.NowMs = 1500
	xreadgroupDeliverD(cx, g, "temp", "gtmp", "dave", 10, false) // 7-1, 7-2 to dave
}

// TestStreamGroupEffectLogRecovers rebuilds groups and PELs from the effect log alone, no
// snapshot: every XGROUP, XREADGROUP, XACK, XCLAIM, XNACK, DELCONSUMER, and DESTROY effect
// replays onto a fresh registry and reproduces the exact group state, including the ordinal
// hole a DELCONSUMER left and a group a DESTROY dropped.
func TestStreamGroupEffectLogRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streamgroupeffect.aki")
	f, s := streamDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	buildGroupScenario(cx, g)
	// No snapshot: continue mutating so the whole state is effect-log only.
	cx.NowMs = 8000
	xgroupSetIDD(cx, g, "orders", "g1", "1-4")
	cx.NowMs = 9000
	xreadgroupDeliverD(cx, g, "orders", "g1", "alice", 10, false) // 1-5 to alice
	cx.NowMs = 10000
	xackGrpD(cx, g, "orders", "g1", streamID{1, 3}) // retire 1-3
	cx.NowMs = 11000
	xgroupDestroyD(cx, g, "temp", "gtmp") // temp keeps its entries, loses its group

	wantOrders := dumpGroups(g.m["orders"])
	wantQueue := dumpGroups(g.m["queue"])
	wantTemp := dumpGroups(g.m["temp"]) // nil, group destroyed
	wantOrdersEntries := entriesOf(g.m["orders"])

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := streamDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := dumpGroups(g2.m["orders"]); !reflect.DeepEqual(got, wantOrders) {
		t.Fatalf("orders groups after recovery =\n%+v\nwant\n%+v", got, wantOrders)
	}
	if got := dumpGroups(g2.m["queue"]); !reflect.DeepEqual(got, wantQueue) {
		t.Fatalf("queue groups after recovery =\n%+v\nwant\n%+v", got, wantQueue)
	}
	if got := dumpGroups(g2.m["temp"]); !reflect.DeepEqual(got, wantTemp) {
		t.Fatalf("temp groups after recovery = %+v, want %+v (destroy must survive)", got, wantTemp)
	}
	if got := entriesOf(g2.m["orders"]); !reflect.DeepEqual(got, wantOrdersEntries) {
		t.Fatalf("orders entries after recovery = %v, want %v", got, wantOrdersEntries)
	}
}

// TestStreamGroupSnapshotRecovers is the composition round trip across a checkpoint: the
// pre-snapshot group state is folded into the snapshot header section, then a tail of further
// effects (an XGROUP SETID, a fresh delivery, an XACK, and an XGROUP DESTROY) replays on top,
// so the recovered state is snapshot plus tail, the ordinal hole survives the fold, and a
// group the tail destroyed does not leak back.
func TestStreamGroupSnapshotRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streamgroupsnap.aki")
	f, s := streamDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	buildGroupScenario(cx, g)

	cx.NowMs = 7000
	Snapshot(cx) // fold every live stream and its groups to a snapshot frame

	// Tail after the snapshot.
	cx.NowMs = 8000
	xgroupSetIDD(cx, g, "orders", "g1", "1-4")
	cx.NowMs = 9000
	xreadgroupDeliverD(cx, g, "orders", "g1", "alice", 10, false) // 1-5 to alice
	cx.NowMs = 10000
	xackGrpD(cx, g, "orders", "g1", streamID{1, 3}) // retire the claimed 1-3
	cx.NowMs = 11000
	xgroupDestroyD(cx, g, "temp", "gtmp")

	wantOrders := dumpGroups(g.m["orders"])
	wantQueue := dumpGroups(g.m["queue"])
	wantTemp := dumpGroups(g.m["temp"])

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := streamDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := dumpGroups(g2.m["orders"]); !reflect.DeepEqual(got, wantOrders) {
		t.Fatalf("orders groups after recovery =\n%+v\nwant\n%+v", got, wantOrders)
	}
	if got := dumpGroups(g2.m["queue"]); !reflect.DeepEqual(got, wantQueue) {
		t.Fatalf("queue groups after recovery =\n%+v\nwant\n%+v", got, wantQueue)
	}
	if got := dumpGroups(g2.m["temp"]); !reflect.DeepEqual(got, wantTemp) {
		t.Fatalf("temp groups after recovery = %+v, want %+v", got, wantTemp)
	}
}
