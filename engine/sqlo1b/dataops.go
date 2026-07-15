package sqlo1b

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// Data WAL payloads (doc 03 section 12.2, ops 1 and 2). A PUT frame
// carries the physical-logical post-image of one record: the same
// fields as the vlog envelope minus rlen and rcrc, which the frame
// header already covers. The store emits these live and decodes them
// on replay; PEXPIRE and GENBUMP join with their own slices.

const putFixedSize = 8 // rtype u8, rflags u8, klen u16, vlen u32

// EncodePutPayload lays out a PUT frame payload for one record.
func EncodePutPayload(r *Record) ([]byte, error) {
	if err := validateEnvelope(r.RType, r.RFlags, uint64(len(r.Key)), uint64(len(r.Value))); err != nil {
		return nil, err
	}
	b := make([]byte, putFixedSize+optLen(r.RFlags)+len(r.Key)+len(r.Value))
	b[0] = r.RType
	b[1] = r.RFlags
	binary.LittleEndian.PutUint16(b[2:], uint16(len(r.Key)))
	binary.LittleEndian.PutUint32(b[4:], uint32(len(r.Value)))
	off := putFixedSize
	if r.HasExpiry() {
		binary.LittleEndian.PutUint64(b[off:], r.ExpireMS)
		off += 8
	}
	if r.HasRootgen() {
		binary.LittleEndian.PutUint32(b[off:], r.Rootgen)
		off += 4
	}
	off += copy(b[off:], r.Key)
	copy(b[off:], r.Value)
	return b, nil
}

// DecodePutPayload parses a PUT frame payload. Key and Value alias b,
// which on replay is only valid inside the sink call.
func DecodePutPayload(b []byte) (*Record, error) {
	if len(b) < putFixedSize {
		return nil, fmt.Errorf("sqlo1b: PUT payload of %d bytes has no room for the fixed fields", len(b))
	}
	rec := &Record{RType: b[0], RFlags: b[1]}
	klen := uint64(binary.LittleEndian.Uint16(b[2:]))
	vlen := uint64(binary.LittleEndian.Uint32(b[4:]))
	if err := validateEnvelope(rec.RType, rec.RFlags, klen, vlen); err != nil {
		return nil, err
	}
	if want := uint64(putFixedSize+optLen(rec.RFlags)) + klen + vlen; want != uint64(len(b)) {
		return nil, fmt.Errorf("sqlo1b: PUT payload is %d bytes, fields add to %d", len(b), want)
	}
	off := uint64(putFixedSize)
	if rec.HasExpiry() {
		rec.ExpireMS = binary.LittleEndian.Uint64(b[off:])
		off += 8
	}
	if rec.HasRootgen() {
		rec.Rootgen = binary.LittleEndian.Uint32(b[off:])
		off += 4
	}
	rec.Key = b[off : off+klen]
	rec.Value = b[off+klen : off+klen+vlen]
	return rec, nil
}

// EncodeDelPayload lays out a DEL frame payload: klen u16, key.
func EncodeDelPayload(key []byte) ([]byte, error) {
	if len(key) == 0 || len(key) > math.MaxUint16 {
		return nil, fmt.Errorf("sqlo1b: DEL key of %d bytes, want 1..%d", len(key), math.MaxUint16)
	}
	b := make([]byte, 2+len(key))
	binary.LittleEndian.PutUint16(b, uint16(len(key)))
	copy(b[2:], key)
	return b, nil
}

// DecodeDelPayload parses a DEL frame payload. The key aliases b.
func DecodeDelPayload(b []byte) ([]byte, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("sqlo1b: DEL payload of %d bytes", len(b))
	}
	klen := int(binary.LittleEndian.Uint16(b))
	if klen == 0 || 2+klen != len(b) {
		return nil, fmt.Errorf("sqlo1b: DEL payload of %d bytes carries klen %d", len(b), klen)
	}
	return b[2:], nil
}

// The batch high-water mark (doc 03 section 12.2): the last frame of
// every ApplyBatch is a PUT carrying an rtype-meta record with key hw
// and the batch seq as an 8-byte LE value. Replay folds it into the
// store's high-water; it is never indexed and never lands in a vlog
// extent. No user key can collide because records crossing the store
// seam are never rtype meta.
var markKey = []byte("hw")

// EncodeMarkPayload builds the high-water mark frame payload.
func EncodeMarkPayload(seq int64) ([]byte, error) {
	v := make([]byte, 8)
	binary.LittleEndian.PutUint64(v, uint64(seq))
	return EncodePutPayload(&Record{RType: RecMeta, Key: markKey, Value: v})
}

// MarkSeq reports whether a replayed PUT record is the high-water
// mark and returns the batch seq it carries. A meta record with any
// other key is an error: nothing else emits rtype meta yet.
func MarkSeq(rec *Record) (int64, bool, error) {
	if rec.RType != RecMeta {
		return 0, false, nil
	}
	if !bytes.Equal(rec.Key, markKey) {
		return 0, false, fmt.Errorf("sqlo1b: meta record with unknown key %q", rec.Key)
	}
	if len(rec.Value) != 8 {
		return 0, false, fmt.Errorf("sqlo1b: high-water mark value is %d bytes, want 8", len(rec.Value))
	}
	return int64(binary.LittleEndian.Uint64(rec.Value)), true, nil
}
