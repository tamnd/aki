package command

import "bytes"

// This file implements the cluster key-to-slot mapping (spec 2064 doc 18
// sections 18.1 through 18.3): the CRC16 used by Redis Cluster, the hash-tag
// rule, and the 16384-slot fold. The mapping is identical to real Redis so
// CLUSTER KEYSLOT returns the same slot for every key and a cluster-aware client
// routes correctly.

// numSlots is the fixed Redis Cluster slot count.
const numSlots = 16384

// crc16Table is the 256-entry lookup table for the CCITT CRC16 variant Redis
// uses: polynomial 0x1021, initial value 0x0000, no reflection, no final XOR. It
// is built once at startup from the polynomial, which yields the exact table the
// Redis Cluster specification lists in its appendix.
var crc16Table [256]uint16

func init() {
	for i := range crc16Table {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		crc16Table[i] = crc
	}
}

// crc16 computes the Redis CRC16 of the given bytes.
func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc = (crc << 8) ^ crc16Table[byte(crc>>8)^b]
	}
	return crc
}

// hashTag returns the substring between the first '{' and the next '}' when that
// substring is non-empty, otherwise the whole key. This is the hash-tag rule
// that lets related keys share a slot.
func hashTag(key []byte) []byte {
	start := bytes.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	end := bytes.IndexByte(key[start+1:], '}')
	if end < 0 {
		return key
	}
	tag := key[start+1 : start+1+end]
	if len(tag) == 0 {
		return key
	}
	return tag
}

// hashSlot maps a key to its cluster slot in 0..16383, applying the hash-tag rule
// first.
func hashSlot(key []byte) int {
	return int(crc16(hashTag(key))) % numSlots
}
