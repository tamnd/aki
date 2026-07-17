package store

import "github.com/tamnd/aki/engine/f3/akifile"

// akiRlog is the store-side record log: the first store adapter that persists a
// record row, not just its separated value. Today the store logs value bytes
// (akivlog.go) and cold frames (akicold.go) but never the record itself, so the
// index and the per-shard sequence live only in memory and rebuild from nothing
// on open. This adapter stages record rows through an akifile RecordLogWriter and
// cuts one log segment per flush into the shared .aki, the substrate the durable
// append path publishes against.
//
// It mirrors akivlog's owner-thread contract exactly, because the record log has
// the same deferred-publish shape the value log did. One adapter per shard, plain
// fields, no atomics, because a second toucher does not exist. A staged row reads
// back from the pending buffer the instant stage returns; the absolute address the
// index entry keeps is not known until the group-commit writer assigns the segment
// at flush, so stage returns a batch index and flush resolves the batch to
// addresses the shard publishes at the group boundary. That boundary is where the
// command's ack already waits on the group fsync, so the record log's cut lands on
// the same seam the record's durability does.
//
// This adapter is inert: it is threaded through Open behind the same opt-in handle
// akivlog uses, but nothing routes a command's record through it yet. The two-phase
// publish flip that stages a provisional index entry and patches it to the flush
// address is the next slice, and it is the box-risky one; this proves the record
// log's accounting and read paths in isolation first.
type akiRlog struct {
	w     *akifile.RecordLogWriter
	f     *akifile.File
	shard uint16

	// seq stamps each cut log segment, advanced once per non-empty flush the way
	// the scratch log advanced its flushed tail.
	seq uint64

	// total is every flushed record byte (the framed body plus its length and
	// CRC, the segment payload's own measure); dead is the subset a supersession,
	// a delete, or an expiry unlinked. live = total - dead is what a compaction of
	// the log region keeps, the same accounting the value log exposes, and the
	// dead-byte figure a checkpoint persists so a restart does not zero it (the
	// O10 gap, doc 07 section 6).
	total uint64
	dead  uint64
}

// newAkiRlog builds a record log for shard backed by f's group-commit writer.
func newAkiRlog(f *akifile.File, shard uint16) *akiRlog {
	return &akiRlog{w: akifile.NewRecordLogWriter(f, shard), f: f, shard: shard}
}

// stage frames row into the pending batch and returns its index in stage order,
// readable through readStaged until the next flush. The absolute address for the
// index comes back from flush at the same index.
func (l *akiRlog) stage(row akifile.RecordRow) int { return l.w.Stage(row) }

// staged reports how many records await the next flush.
func (l *akiRlog) staged() int { return l.w.Staged() }

// pendingBytes reports the staged payload size, the signal for when the batch is
// worth a cut.
func (l *akiRlog) pendingBytes() int { return l.w.PendingBytes() }

// readStaged serves a staged record from the pending buffer before its segment is
// cut, the read-before-flush an in-batch resolve of a just-written key takes.
func (l *akiRlog) readStaged(idx int) (akifile.RecordRow, error) { return l.w.ReadStaged(idx) }

// flush cuts one log segment for the staged batch and returns an absolute address
// per staged record in stage order. An empty batch is a no-op that leaves the
// sequence untouched, so shard_seq advances only on a real cut. The flushed record
// bytes join the total the moment they land; the byte figure is the pending
// payload measured before the cut resets it.
func (l *akiRlog) flush() ([]uint64, error) {
	if l.w.Staged() == 0 {
		return nil, nil
	}
	bytes := uint64(l.w.PendingBytes())
	l.seq++
	addrs, err := l.w.Flush(l.seq)
	if err != nil {
		return nil, err
	}
	l.total += bytes
	return addrs, nil
}

// seqHigh reports the highest segment sequence this shard has cut, the cross-check
// a checkpoint header stamps so recovery can confirm the dump reflects every
// durable segment.
func (l *akiRlog) seqHigh() uint64 { return l.seq }

// globalSeq reports the file's highest assigned global sequence, the log position a
// full checkpoint declares itself consistent up to.
func (l *akiRlog) globalSeq() uint64 { return l.f.GlobalSeq() }

// walkShard replays this shard's record log from the start of the append space,
// calling visit for each framed record this shard cut in append order. It skips
// every other shard's segments, so a per-shard recovery reapplies only its own
// records. The row's Key aliases the segment payload for the visit's duration.
func (l *akiRlog) walkShard(visit func(addr uint64, row akifile.RecordRow) error) error {
	return l.walkShardFrom(akifile.PageSize, visit)
}

// walkShardFrom is walkShard bounded to the records this shard cut at or past from,
// the tail a checkpoint-driven recovery replays after loading the settled prefix
// from the dump. from is a byte offset into the append space, the position a
// checkpoint records as its first tail segment so recovery resumes exactly where
// the dump stopped being authoritative.
func (l *akiRlog) walkShardFrom(from uint64, visit func(addr uint64, row akifile.RecordRow) error) error {
	return l.f.WalkShardRecords(l.shard, from, visit)
}

// cursor reports the file's append offset, the position past which every later
// record is cut. Captured the instant a checkpoint is built, it is the tail start
// a checkpoint-driven recovery resumes the log walk from.
func (l *akiRlog) cursor() uint64 { return l.f.Cursor() }

// writeCheckpoint appends payload as this shard's index checkpoint segment and
// returns the absolute offset of the segment header, the IndexCkptOff an SRT row
// records: recovery walks the checkpoint chain by reading the segment header there
// (RebuildShardIndex), so the row names the header start, not the payload. The
// segment is KindIndexCkpt, not KindLog, so a tail replay skips it: it rides the
// append tail only to be pointed at, never walked as a record. It appends but does
// not commit, the meta flip that names the segment live is the coordinator's.
func (l *akiRlog) writeCheckpoint(payload []byte) (uint64, error) {
	offs, err := l.f.AppendGroup([]akifile.Pending{{Shard: l.shard, Kind: akifile.KindIndexCkpt, Payload: payload}})
	if err != nil {
		return 0, err
	}
	return offs[0], nil
}

// readAt decodes a published record from its absolute frame address, the deref the
// index entry and a checkpoint's record_addr take, verifying the frame's own
// trailing CRC so a torn or superseded record fails closed rather than returning
// rot.
func (l *akiRlog) readAt(addr uint64) (akifile.RecordRow, error) { return l.f.ReadRecordAt(addr) }

// unlink records n bytes a supersession, a delete, or an expiry reap no longer
// references, so a later compaction of the log region knows what it can reclaim.
// It must fire at every site that supersedes a logged record, the way the value
// log's dead counter fires on an overwritten value.
func (l *akiRlog) unlink(n uint64) { l.dead += n }

// logBytes reports the total flushed record bytes and the dead subset, the pair a
// compaction of the log region weighs to decide when a rewrite is worth it and the
// pair the seg-stats checkpoint persists across restart.
func (l *akiRlog) logBytes() (total, dead uint64) { return l.total, l.dead }
