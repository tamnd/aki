package command

import (
	"sort"

	"github.com/tamnd/aki/rdb"
)

// This file bridges the in-memory stream model and the neutral rdb.StreamData the
// RDB codec reads and writes, so DUMP, RESTORE, SAVE, DEBUG RELOAD and a full sync
// all carry streams. The internal model keeps one group PEL sorted by entry ID and
// tags each record with its consumer name; the RDB form instead hangs the pending
// IDs off each consumer and keeps the delivery time and count in the group PEL.
// The two converters move between those shapes.

// streamToRDB turns the in-memory stream into the neutral carrier the RDB codec
// serializes. Each group's pending IDs are split out per consumer, while the
// delivery time and count stay in the group PEL.
func streamToRDB(s *stream) rdb.Value {
	sd := &rdb.StreamData{
		LastMS:       s.lastID.ms,
		LastSeq:      s.lastID.seq,
		MaxDelMS:     s.maxDeletedID.ms,
		MaxDelSeq:    s.maxDeletedID.seq,
		EntriesAdded: s.entriesAdded,
	}
	if len(s.entries) > 0 {
		sd.FirstMS = s.entries[0].id.ms
		sd.FirstSeq = s.entries[0].id.seq
	}
	for _, e := range s.entries {
		sd.Entries = append(sd.Entries, rdb.StreamEntry{
			MS:     e.id.ms,
			Seq:    e.id.seq,
			Fields: e.fields,
		})
	}
	for _, g := range s.groups {
		rg := rdb.StreamGroup{
			Name:        []byte(g.name),
			LastMS:      g.lastID.ms,
			LastSeq:     g.lastID.seq,
			EntriesRead: g.entriesRead,
		}
		pending := map[string][]rdb.StreamID{}
		for _, pe := range g.pel {
			rg.PEL = append(rg.PEL, rdb.StreamPEL{
				MS:            pe.id.ms,
				Seq:           pe.id.seq,
				DeliveryTime:  uint64(pe.deliveryTime),
				DeliveryCount: pe.deliveryCount,
			})
			pending[pe.consumer] = append(pending[pe.consumer], rdb.StreamID{MS: pe.id.ms, Seq: pe.id.seq})
		}
		for _, c := range g.consumers {
			rg.Consumers = append(rg.Consumers, rdb.StreamConsumer{
				Name:       []byte(c.name),
				SeenTime:   uint64(c.seenTime),
				ActiveTime: uint64(c.activeTime),
				PendingIDs: pending[c.name],
			})
		}
		sd.Groups = append(sd.Groups, rg)
	}
	return rdb.Value{Kind: rdb.KindStream, Stream: sd}
}

// rdbToStream rebuilds the in-memory stream from the decoded carrier. The group PEL
// is reassembled from the per-consumer pending IDs, with each record's delivery
// time and count looked up from the RDB group PEL, then sorted by entry ID to
// restore the in-memory invariant.
func rdbToStream(sd *rdb.StreamData) *stream {
	if sd == nil {
		return &stream{}
	}
	s := &stream{
		lastID:       streamID{ms: sd.LastMS, seq: sd.LastSeq},
		maxDeletedID: streamID{ms: sd.MaxDelMS, seq: sd.MaxDelSeq},
		entriesAdded: sd.EntriesAdded,
	}
	for _, e := range sd.Entries {
		s.entries = append(s.entries, streamEntry{
			id:     streamID{ms: e.MS, seq: e.Seq},
			fields: e.Fields,
		})
	}
	for _, rg := range sd.Groups {
		g := &group{
			name:        string(rg.Name),
			lastID:      streamID{ms: rg.LastMS, seq: rg.LastSeq},
			entriesRead: rg.EntriesRead,
		}
		meta := make(map[streamID]rdb.StreamPEL, len(rg.PEL))
		for _, p := range rg.PEL {
			meta[streamID{ms: p.MS, seq: p.Seq}] = p
		}
		for _, rc := range rg.Consumers {
			g.consumers = append(g.consumers, &consumer{
				name:       string(rc.Name),
				seenTime:   int64(rc.SeenTime),
				activeTime: int64(rc.ActiveTime),
			})
			for _, id := range rc.PendingIDs {
				sid := streamID{ms: id.MS, seq: id.Seq}
				p := meta[sid]
				g.pel = append(g.pel, pelEntry{
					id:            sid,
					consumer:      string(rc.Name),
					deliveryTime:  int64(p.DeliveryTime),
					deliveryCount: p.DeliveryCount,
				})
			}
		}
		sort.Slice(g.pel, func(i, j int) bool { return g.pel[i].id.less(g.pel[j].id) })
		s.groups = append(s.groups, g)
	}
	return s
}
