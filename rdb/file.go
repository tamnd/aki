package rdb

import (
	"fmt"

	"github.com/tamnd/aki/encoding"
)

// RDB file opcodes. They sit between the 9-byte header and the EOF marker and
// frame the database selectors, resize hints, key expiries, and per-key access
// metadata.
const (
	opAux       = 0xFA
	opResizeDB  = 0xFB
	opExpireMS  = 0xFC
	opExpireSec = 0xFD
	opSelectDB  = 0xFE
	opEOF       = 0xFF
	opIdle      = 0xF6
	opFreq      = 0xF5
	opModuleAux = 0xF4
	opFunction2 = 0xF3
	opSlotInfo  = 0xF2
	opFunction  = 0xF1
)

// fileVersion is the RDB file version aki writes, matching Redis 7.2+.
const fileVersion = 12

// Entry is one key in a snapshot: its name, value, optional expiry, and the
// access metadata that seeds the LRU and LFU state on load.
type Entry struct {
	Key      []byte
	Value    Value
	ExpireMS int64 // -1 for no expiry
	Idle     uint32
	HasIdle  bool
	Freq     uint8
	HasFreq  bool
}

// DBData is the contents of one logical database in a snapshot.
type DBData struct {
	Index   int
	Entries []Entry
}

// Snapshot is a whole dataset across its databases, the unit SAVE writes and load
// reads.
type Snapshot struct {
	DBs []DBData
	Aux map[string]string // auxiliary header fields such as redis-ver
}

// MarshalFile serializes a snapshot to a complete RDB file: the REDIS header, the
// aux records, then each non-empty database as a selector, a resize hint, and its
// keys, then the EOF marker and the CRC-64 over everything before the checksum.
func MarshalFile(snap Snapshot) ([]byte, error) {
	out := fmt.Appendf(nil, "REDIS%04d", fileVersion)
	out = appendAux(out, "redis-ver", "7.2.4")
	out = appendAux(out, "redis-bits", "64")
	for k, v := range snap.Aux {
		out = appendAux(out, k, v)
	}
	for _, db := range snap.DBs {
		if len(db.Entries) == 0 {
			continue
		}
		out = append(out, opSelectDB)
		out = appendLength(out, uint64(db.Index))
		out = append(out, opResizeDB)
		out = appendLength(out, uint64(len(db.Entries)))
		out = appendLength(out, uint64(countExpires(db.Entries)))
		for _, e := range db.Entries {
			rec, err := appendEntry(out, e)
			if err != nil {
				return nil, err
			}
			out = rec
		}
	}
	out = append(out, opEOF)
	out = appendCRC64(out, out)
	return out, nil
}

// appendEntry writes one key record: its optional expiry and access opcodes, then
// the type byte, the key, and the value body.
func appendEntry(dst []byte, e Entry) ([]byte, error) {
	if e.ExpireMS >= 0 {
		dst = append(dst, opExpireMS)
		dst = encoding.AppendU64(dst, uint64(e.ExpireMS))
	}
	if e.HasIdle {
		dst = append(dst, opIdle)
		dst = appendLength(dst, uint64(e.Idle))
	}
	if e.HasFreq {
		dst = append(dst, opFreq, e.Freq)
	}
	typeByte, body, err := encodeBody(e.Value)
	if err != nil {
		return nil, err
	}
	dst = append(dst, typeByte)
	dst = appendString(dst, e.Key)
	return append(dst, body...), nil
}

// appendAux writes one auxiliary key-value record.
func appendAux(dst []byte, key, val string) []byte {
	dst = append(dst, opAux)
	dst = appendString(dst, []byte(key))
	return appendString(dst, []byte(val))
}

// countExpires counts the keys in a database that carry an expiry, for the resize
// hint.
func countExpires(entries []Entry) int {
	n := 0
	for _, e := range entries {
		if e.ExpireMS >= 0 {
			n++
		}
	}
	return n
}

// UnmarshalFile parses a complete RDB file into a snapshot. It checks the header
// magic and version, verifies the trailing CRC-64 unless it is zero, and reads the
// opcode stream until EOF.
func UnmarshalFile(data []byte) (Snapshot, error) {
	if len(data) < 9 {
		return Snapshot{}, errTruncated
	}
	if string(data[:5]) != "REDIS" {
		return Snapshot{}, fmt.Errorf("rdb: bad magic %q", data[:5])
	}
	ver, err := parseVersion(data[5:9])
	if err != nil {
		return Snapshot{}, err
	}
	if ver > fileVersion {
		return Snapshot{}, fmt.Errorf("rdb: unsupported version %d", ver)
	}

	r := &reader{buf: data, pos: 9}
	snap := Snapshot{Aux: map[string]string{}}
	curDB := 0
	var pendExpire int64 = -1
	var pendIdle uint32
	var pendHasIdle bool
	var pendFreq uint8
	var pendHasFreq bool

	for {
		op := r.readByte()
		if r.err != nil {
			return Snapshot{}, r.err
		}
		switch op {
		case opEOF:
			// The 8 bytes after EOF are the checksum. Validate unless it is zero.
			if r.pos+8 > len(data) {
				return Snapshot{}, errTruncated
			}
			stored := encoding.U64(data[r.pos : r.pos+8])
			if stored != 0 && crc64(0, data[:r.pos]) != stored {
				return Snapshot{}, errTruncated
			}
			return snap, nil
		case opSelectDB:
			n, _, _ := r.readLength()
			curDB = int(n)
		case opResizeDB:
			r.readLength()
			r.readLength()
		case opAux:
			k := r.readString()
			v := r.readString()
			if r.err != nil {
				return Snapshot{}, r.err
			}
			snap.Aux[string(k)] = string(v)
		case opExpireMS:
			b := r.readBytes(8)
			if r.err != nil {
				return Snapshot{}, r.err
			}
			pendExpire = int64(encoding.U64(b))
		case opExpireSec:
			b := r.readBytes(4)
			if r.err != nil {
				return Snapshot{}, r.err
			}
			pendExpire = int64(encoding.U32BE(b)) * 1000
		case opIdle:
			n, _, _ := r.readLength()
			pendIdle, pendHasIdle = uint32(n), true
		case opFreq:
			pendFreq, pendHasFreq = r.readByte(), true
		case opModuleAux, opFunction, opFunction2, opSlotInfo:
			return Snapshot{}, fmt.Errorf("rdb: opcode 0x%02x not supported", op)
		default:
			// Anything else is a value type byte introducing a key record.
			key := r.readString()
			if r.err != nil {
				return Snapshot{}, r.err
			}
			val, derr := decodeValue(r, op)
			if derr != nil {
				return Snapshot{}, derr
			}
			snap.DBs = appendEntryToDB(snap.DBs, curDB, Entry{
				Key:      key,
				Value:    val,
				ExpireMS: pendExpire,
				Idle:     pendIdle,
				HasIdle:  pendHasIdle,
				Freq:     pendFreq,
				HasFreq:  pendHasFreq,
			})
			pendExpire, pendIdle, pendHasIdle, pendFreq, pendHasFreq = -1, 0, false, 0, false
		}
	}
}

// appendEntryToDB adds an entry under the given database index, creating the
// database bucket the first time it is seen.
func appendEntryToDB(dbs []DBData, index int, e Entry) []DBData {
	for i := range dbs {
		if dbs[i].Index == index {
			dbs[i].Entries = append(dbs[i].Entries, e)
			return dbs
		}
	}
	return append(dbs, DBData{Index: index, Entries: []Entry{e}})
}

// parseVersion reads the 4 ASCII digits of the header version.
func parseVersion(b []byte) (int, error) {
	v := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("rdb: bad version %q", b)
		}
		v = v*10 + int(c-'0')
	}
	return v, nil
}
