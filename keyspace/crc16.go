package keyspace

import "bytes"

// crc16tab is the CRC16-CCITT lookup table: polynomial 0x1021, initial value
// 0x0000, no input or output reflection, no final XOR. This is the exact table
// Redis Cluster uses for key slot assignment (crc16.c). Keeping the table here
// means HashSlot matches Redis byte for byte, so CLUSTER KEYSLOT and slot-based
// routing agree with a real Redis node.
var crc16tab = buildCRC16Table()

func buildCRC16Table() [256]uint16 {
	var tab [256]uint16
	for i := range 256 {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		tab[i] = crc
	}
	return tab
}

// crc16 returns the CRC16-CCITT of b.
func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc = (crc << 8) ^ crc16tab[byte(crc>>8)^c]
	}
	return crc
}

// numSlots is the number of hash slots in the Redis Cluster model.
const numSlots = 16384

// HashSlot returns the cluster hash slot for a key. If the key contains a hash
// tag {...} with non-empty content, only the content between the first { and the
// first } after it is hashed, so keys sharing a tag land in the same slot. This
// matches Redis cluster.c keyHashSlot.
func HashSlot(key []byte) uint16 {
	if s := bytes.IndexByte(key, '{'); s >= 0 {
		if e := bytes.IndexByte(key[s+1:], '}'); e > 0 {
			key = key[s+1 : s+1+e]
		}
	}
	return crc16(key) % numSlots
}
