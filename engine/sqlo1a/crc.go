package sqlo1a

import (
	"encoding/binary"
	"hash/crc32"
)

// crc is sqlo1's own row checksum (doc 02 section 4): SQLite mainline has
// no page checksums and G-safe demands detection, so every kv row carries
// crc32c over its other five columns and the read path verifies before
// trusting a byte. The encoding below is an on-disk contract: k, then t,
// exp, gen as 8-byte little-endian two's complement, then v. Changing it
// is a schema generation bump, not an edit.

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func rowCRC(k []byte, t, exp, gen int64, v []byte) uint32 {
	var num [24]byte
	binary.LittleEndian.PutUint64(num[0:8], uint64(t))
	binary.LittleEndian.PutUint64(num[8:16], uint64(exp))
	binary.LittleEndian.PutUint64(num[16:24], uint64(gen))
	c := crc32.Update(0, crcTable, k)
	c = crc32.Update(c, crcTable, num[:])
	return crc32.Update(c, crcTable, v)
}
