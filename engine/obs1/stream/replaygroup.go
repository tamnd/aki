package stream

// Boot replay's entry points for the consumer-group vocabulary (spec
// 2064/obs1 doc 04 section 2), under the same contract replay.go
// states. Group frames are post-decision throughout: the option soup
// XCLAIM, XAUTOCLAIM, and XNACK resolve (IDLE, TIME, RETRYCOUNT,
// FORCE, JUSTID) never reaches a frame, delivery times ride as framed
// values, and the ids listed are the ids that actually moved, so
// replay is arithmetic-free and clock-free. Consumer clocks come from
// the frames too: a delivery stamps its consumer at the framed
// delivery time, and a claim stamps at the greatest resulting delivery
// time it carries, the closest clock-free reading of the serve-time
// stamp.

import (
	"fmt"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// ReplayGNew applies XGROUP CREATE's resulting cursor and lag basis.
// create is true when a collnew led the frame, the MKSTREAM form: the
// stream is rebuilt empty first, the reset-to-empty rule. The serve
// path refuses a duplicate group (BUSYGROUP), so one here is
// divergence. Adding a group upgrades the stream to the native band
// exactly as serve does.
func ReplayGNew(cx *shard.Ctx, key, group []byte, lastMs, lastSeq, entriesRead uint64, readValid, create bool) error {
	g := registry(cx)
	var s *stream
	if create {
		g.dropStream(key)
		s = newStream()
		g.m[string(key)] = s
	} else if s = g.m[string(key)]; s == nil {
		return fmt.Errorf("stream replay: gnew names key %q but no stream exists", key)
	}
	if s.group(group) != nil {
		return fmt.Errorf("stream replay: gnew group %q already exists on %q", group, key)
	}
	s.addGroup(group, newGroup(streamID{ms: lastMs, seq: lastSeq}, entriesRead, readValid))
	g.note(s)
	return nil
}

// ReplayGSetID applies XGROUP SETID's resulting cursor and lag basis
// unconditionally; the owner already merged the optional arguments.
func ReplayGSetID(cx *shard.Ctx, key, group []byte, lastMs, lastSeq, entriesRead uint64, readValid bool) error {
	_, grp, err := replayGroup(cx, key, group, "gsetid")
	if err != nil {
		return err
	}
	grp.lastDeliveredID = streamID{ms: lastMs, seq: lastSeq}
	grp.entriesRead = entriesRead
	grp.readValid = readValid
	return nil
}

// ReplayGDrop applies XGROUP DESTROY of a group that existed; its
// consumers and PEL leave with it.
func ReplayGDrop(cx *shard.Ctx, key, group []byte) error {
	s, _, err := replayGroup(cx, key, group, "gdrop")
	if err != nil {
		return err
	}
	delete(s.groups, string(group))
	registry(cx).note(s)
	return nil
}

// ReplayGConsumerNew applies XGROUP CREATECONSUMER when it created
// one, seen at the framed time with no activity yet. The serve path
// only frames a creation, so an existing consumer is divergence.
func ReplayGConsumerNew(cx *shard.Ctx, key, group, consumer []byte, seenMs int64) error {
	s, grp, err := replayGroup(cx, key, group, "gconsumernew")
	if err != nil {
		return err
	}
	if !grp.createConsumer(consumer, seenMs) {
		return fmt.Errorf("stream replay: gconsumernew consumer %q already exists in group %q of %q", consumer, group, key)
	}
	registry(cx).note(s)
	return nil
}

// ReplayGConsumerDel applies XGROUP DELCONSUMER of a consumer that
// existed, draining its pending entries by owner, the same walk the
// command ran; no id list rides the frame.
func ReplayGConsumerDel(cx *shard.Ctx, key, group, consumer []byte) error {
	s, grp, err := replayGroup(cx, key, group, "gconsumerdel")
	if err != nil {
		return err
	}
	if grp.consumer(consumer) == nil {
		return fmt.Errorf("stream replay: gconsumerdel names consumer %q absent from group %q of %q", consumer, group, key)
	}
	grp.delConsumer(consumer)
	registry(cx).note(s)
	return nil
}

// ReplayGAck removes the framed ids from the group's PEL. The frame
// carries only ids that actually left, XACK's acknowledgments and the
// claim-path removals of deleted entries alike, so a miss is
// divergence.
func ReplayGAck(cx *shard.Ctx, key, group []byte, ms, seqs []uint64) error {
	s, grp, err := replayGroup(cx, key, group, "gack")
	if err != nil {
		return err
	}
	if grp.pel == nil {
		return fmt.Errorf("stream replay: gack on group %q of %q but the group has no pending entries", group, key)
	}
	for i := range ms {
		if !grp.ackOne(streamID{ms: ms[i], seq: seqs[i]}) {
			return fmt.Errorf("stream replay: gack id %d-%d framed as removed is not pending in group %q of %q", ms[i], seqs[i], group, key)
		}
	}
	registry(cx).note(s)
	return nil
}

// ReplayGDeliver applies an XREADGROUP new-message delivery: the
// cursor advances to the last id listed, the entries-read basis moves
// by the count, and each id enters the consumer's PEL at the framed
// time with delivery count one unless the read was NOACK. The `>` path
// only ever delivered ids strictly above the cursor in id order, so a
// framed id at or below it, or out of order, is divergence. The
// consumer is created on first sighting, exactly as the serve path
// lazily does, and its clocks stamp at the framed time.
func ReplayGDeliver(cx *shard.Ctx, key, group, consumer []byte, noAck bool, timeMs int64, ms, seqs []uint64) error {
	s, grp, err := replayGroup(cx, key, group, "gdeliver")
	if err != nil {
		return err
	}
	con := grp.ensureConsumer(consumer, timeMs)
	con.seenTime = timeMs
	prev := grp.lastDeliveredID
	for i := range ms {
		id := streamID{ms: ms[i], seq: seqs[i]}
		if id.cmp(prev) <= 0 {
			return fmt.Errorf("stream replay: gdeliver id %d-%d does not advance the cursor of group %q of %q", ms[i], seqs[i], group, key)
		}
		prev = id
		if noAck {
			continue
		}
		if grp.pel == nil {
			grp.pel = newPEL()
		}
		grp.pel.insert(id, timeMs, con.ord)
		grp.pelCount++
		con.pelCount++
	}
	if !noAck {
		con.activeTime = timeMs
	}
	grp.lastDeliveredID = prev
	grp.entriesRead += uint64(len(ms))
	registry(cx).note(s)
	return nil
}

// ReplayGClaim writes resulting pending-entry state per id: owner,
// delivery time, and delivery count, exactly as XCLAIM, XAUTOCLAIM, or
// XNACK left them. An id not yet pending is the FORCE-created shape
// and gets a fresh slab; an existing slab is rewritten in place, the
// old owner's count dropped. unowned is the XNACK shape where the
// entries belong to no consumer.
func ReplayGClaim(cx *shard.Ctx, key, group, consumer []byte, unowned bool, ms, seqs []uint64, times []int64, counts []uint16) error {
	s, grp, err := replayGroup(cx, key, group, "gclaim")
	if err != nil {
		return err
	}
	var con *streamConsumer
	if !unowned {
		stamp := times[0]
		for _, t := range times[1:] {
			if t > stamp {
				stamp = t
			}
		}
		con = grp.ensureConsumer(consumer, stamp)
		if stamp > con.seenTime {
			con.seenTime = stamp
		}
		if stamp > con.activeTime {
			con.activeTime = stamp
		}
	}
	for i := range ms {
		id := streamID{ms: ms[i], seq: seqs[i]}
		var (
			pe *pelEntry
			ok bool
		)
		if grp.pel != nil {
			pe, ok = grp.pel.find(id)
		}
		if !ok {
			if grp.pel == nil {
				grp.pel = newPEL()
			}
			pe = grp.pel.insertClaimed(id)
			grp.pelCount++
		}
		switch {
		case unowned:
			if pe.consumerOrd != noOwner {
				grp.decOwner(pe.consumerOrd)
				pe.consumerOrd = noOwner
			}
		case pe.consumerOrd == noOwner:
			pe.consumerOrd = con.ord
			con.pelCount++
		case pe.consumerOrd != con.ord:
			grp.decOwner(pe.consumerOrd)
			pe.consumerOrd = con.ord
			con.pelCount++
		}
		pe.deliveryTime = times[i]
		pe.deliveryCount = counts[i]
	}
	registry(cx).note(s)
	return nil
}

// replayGroup resolves the stream and named group, the shared preamble
// every group sub-op needs; a missing stream or group is divergence.
func replayGroup(cx *shard.Ctx, key, group []byte, verb string) (*stream, *streamGroup, error) {
	s := registry(cx).m[string(key)]
	if s == nil {
		return nil, nil, fmt.Errorf("stream replay: %s names key %q but no stream exists", verb, key)
	}
	grp := s.group(group)
	if grp == nil {
		return nil, nil, fmt.Errorf("stream replay: %s names group %q absent from stream %q", verb, group, key)
	}
	return s, grp, nil
}
