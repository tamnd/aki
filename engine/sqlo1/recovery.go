package sqlo1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// WAL payload encodings for the record-bearing ops, doc 03 section 12.2.
// Payloads are physical-logical post-images: a PUT carries the full new
// record, so replay is an idempotent upsert and never re-executes command
// logic. Layouts are little-endian like the frame header. klen is u16 by
// format decision; the command layer owns rejecting keys past 64 KiB
// before they reach a frame.

// PUT rflags bits: which optional fields follow the fixed header.
const (
	putFlagExpire uint8 = 1 << 0
	putFlagGen    uint8 = 1 << 1
)

// appendPutPayload encodes PUT: rtype u8, rflags u8, klen u16, vlen u32,
// [expire_ms u64], [rootgen u32], key, value. rtype 0 is the flat seam
// record; the per-type milestones claim other values with their slices.
func appendPutPayload(buf []byte, rec *Record) []byte {
	var rflags uint8
	if rec.ExpireMs != 0 {
		rflags |= putFlagExpire
	}
	if rec.Gen != 0 {
		rflags |= putFlagGen
	}
	buf = append(buf, 0, rflags)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rec.Key)))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(rec.Value)))
	if rflags&putFlagExpire != 0 {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.ExpireMs))
	}
	if rflags&putFlagGen != 0 {
		buf = binary.LittleEndian.AppendUint32(buf, rec.Gen)
	}
	buf = append(buf, rec.Key...)
	return append(buf, rec.Value...)
}

// parsePutPayload decodes a PUT image into a Record that aliases nothing:
// frame payloads live in the replay buffer and die with the callback, so
// the copies here are the difference between a record and a dangling view.
func parsePutPayload(p []byte) (Record, error) {
	if len(p) < 8 {
		return Record{}, fmt.Errorf("sqlo1: put payload %d bytes, header is 8", len(p))
	}
	rflags := p[1]
	klen := int(binary.LittleEndian.Uint16(p[2:4]))
	vlen := int(binary.LittleEndian.Uint32(p[4:8]))
	rest := p[8:]
	var rec Record
	if rflags&putFlagExpire != 0 {
		if len(rest) < 8 {
			return Record{}, errors.New("sqlo1: put payload truncated at expire_ms")
		}
		rec.ExpireMs = int64(binary.LittleEndian.Uint64(rest[:8]))
		rest = rest[8:]
	}
	if rflags&putFlagGen != 0 {
		if len(rest) < 4 {
			return Record{}, errors.New("sqlo1: put payload truncated at rootgen")
		}
		rec.Gen = binary.LittleEndian.Uint32(rest[:4])
		rest = rest[4:]
	}
	if len(rest) != klen+vlen {
		return Record{}, fmt.Errorf("sqlo1: put payload has %d body bytes, header says %d", len(rest), klen+vlen)
	}
	rec.Key = append([]byte(nil), rest[:klen]...)
	rec.Value = append([]byte(nil), rest[klen:]...)
	return rec, nil
}

// appendDelPayload encodes DEL: klen u16, key.
func appendDelPayload(buf, key []byte) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
	return append(buf, key...)
}

func parseDelPayload(p []byte) ([]byte, error) {
	if len(p) < 2 || len(p)-2 != int(binary.LittleEndian.Uint16(p[:2])) {
		return nil, fmt.Errorf("sqlo1: del payload %d bytes does not match its klen", len(p))
	}
	return append([]byte(nil), p[2:]...), nil
}

// appendPexpirePayload encodes PEXPIRE: klen u16, expire_ms u64, key.
func appendPexpirePayload(buf, key []byte, expireMs int64) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(expireMs))
	return append(buf, key...)
}

func parsePexpirePayload(p []byte) (key []byte, expireMs int64, err error) {
	if len(p) < 10 || len(p)-10 != int(binary.LittleEndian.Uint16(p[:2])) {
		return nil, 0, fmt.Errorf("sqlo1: pexpire payload %d bytes does not match its klen", len(p))
	}
	return append([]byte(nil), p[10:]...), int64(binary.LittleEndian.Uint64(p[2:10])), nil
}

// appendGenbumpPayload encodes GENBUMP: klen u16, newgen u32, key.
func appendGenbumpPayload(buf, key []byte, newgen uint32) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
	buf = binary.LittleEndian.AppendUint32(buf, newgen)
	return append(buf, key...)
}

func parseGenbumpPayload(p []byte) (key []byte, newgen uint32, err error) {
	if len(p) < 6 || len(p)-6 != int(binary.LittleEndian.Uint16(p[:2])) {
		return nil, 0, fmt.Errorf("sqlo1: genbump payload %d bytes does not match its klen", len(p))
	}
	return append([]byte(nil), p[6:]...), binary.LittleEndian.Uint32(p[2:6]), nil
}

// recoverStore is the track-agnostic recovery step (doc 02 section 5,
// doc 03 section 14): the store's high-water mark says which WAL frames
// its checkpointed state already subsumes, and every frame past it
// replays in seq order as drain batches. Exactly-once falls out of the
// two marks meeting: frames at or below the mark are skipped here, and a
// batch at or below the mark is a no-op inside the store, so running
// recovery twice, or crashing inside it and running it again, changes
// nothing.
//
// PEXPIRE and GENBUMP are read-modify ops at the store seam, so the
// pending batch flushes before the read; the read must see every earlier
// frame's effect. SEAL, CKPT, and TRIM frames are no-ops on replay by
// doc 03 section 14, but they still advance the seq the next flush
// stamps, because the mark means "everything at or below is subsumed".
func recoverStore(ctx context.Context, store Store, w *wal, batchCap int) (applied uint64, err error) {
	if batchCap <= 0 {
		batchCap = walBatchCap
	}
	hw := uint64(store.Stats().HighWater)
	var batch DrainBatch
	var lastSeq uint64
	flush := func() error {
		if len(batch.Ops) == 0 {
			return nil
		}
		batch.Seq = int64(lastSeq)
		if err := store.ApplyBatch(ctx, &batch); err != nil {
			return err
		}
		applied += uint64(len(batch.Ops))
		batch.Ops = batch.Ops[:0]
		return nil
	}
	err = w.Replay(func(f walFrame) error {
		if f.Seq <= hw {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		switch f.Op {
		case walOpPut:
			rec, err := parsePutPayload(f.Payload)
			if err != nil {
				return fmt.Errorf("replay seq %d: %w", f.Seq, err)
			}
			batch.Ops = append(batch.Ops, Op{Rec: rec})
		case walOpDel:
			key, err := parseDelPayload(f.Payload)
			if err != nil {
				return fmt.Errorf("replay seq %d: %w", f.Seq, err)
			}
			batch.Ops = append(batch.Ops, Op{Del: true, Rec: Record{Key: key}})
		case walOpPexpire:
			key, exp, err := parsePexpirePayload(f.Payload)
			if err != nil {
				return fmt.Errorf("replay seq %d: %w", f.Seq, err)
			}
			cur, err := flushAndGet(ctx, store, flush, key)
			if err != nil {
				return fmt.Errorf("replay seq %d: %w", f.Seq, err)
			}
			if cur != nil {
				cur.ExpireMs = exp
				batch.Ops = append(batch.Ops, Op{Rec: *cur})
			}
		case walOpGenbump:
			key, newgen, err := parseGenbumpPayload(f.Payload)
			if err != nil {
				return fmt.Errorf("replay seq %d: %w", f.Seq, err)
			}
			cur, err := flushAndGet(ctx, store, flush, key)
			if err != nil {
				return fmt.Errorf("replay seq %d: %w", f.Seq, err)
			}
			if cur != nil {
				cur.Gen = newgen
				batch.Ops = append(batch.Ops, Op{Rec: *cur})
			}
		}
		lastSeq = f.Seq
		if len(batch.Ops) >= batchCap {
			return flush()
		}
		return nil
	})
	if err != nil {
		return applied, err
	}
	return applied, flush()
}

// flushAndGet flushes the pending batch and reads one record for a
// read-modify replay op. A missing key returns nil with no error: an
// expiry or gen bump on a key a later frame never re-created is a no-op,
// same as live.
func flushAndGet(ctx context.Context, store Store, flush func() error, key []byte) (*Record, error) {
	if err := flush(); err != nil {
		return nil, err
	}
	rec, err := store.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}
