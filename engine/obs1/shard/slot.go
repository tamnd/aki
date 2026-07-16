package shard

// Slot routing, the one adaptation this port carries (spec 2064/obs1
// doc 02 sections 1.2, 1.3, and 2.1, doc 07 section 2): a key routes to
// its CRC16 hash slot with the Redis hash-tag rule applied first, the
// slot to its contiguous slot group, and the group to a shard by group
// id mod shard count. The group is obs1's unit of lease, epoch,
// manifest, fold cursor, and handoff; a group never spans shards, so
// everything below the route decision keeps the f3 single-owner
// contract unchanged.

// totalSlots is the Redis Cluster key space, used unchanged (doc 02
// section 2.1).
const totalSlots = 16384

// DefaultSlotGroups is G, the slot-group count (doc 02 section 1.2): 128
// contiguous slots per group at the default. G is fixed at database
// creation and recorded in the root object; a runtime serving a database
// must be built with that database's G.
const DefaultSlotGroups = 128

// crc16tab is the CRC-16/XMODEM table: polynomial 0x1021, init 0x0000,
// no reflection (doc 02 section 2.1; check value 0x31C3 for
// "123456789").
var crc16tab = func() (tab [256]uint16) {
	for i := range tab {
		crc := uint16(i) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		tab[i] = crc
	}
	return tab
}()

func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc = crc<<8 ^ crc16tab[byte(crc>>8)^c]
	}
	return crc
}

// hashTagged returns the byte range of key that the slot hash covers:
// the whole key, unless it contains a '{' followed by a '}' with at
// least one byte between them, in which case only the bytes between the
// first '{' and the first '}' after it (doc 02 section 2.1, the Redis
// hash-tag rule verbatim).
func hashTagged(key []byte) []byte {
	for i, c := range key {
		if c != '{' {
			continue
		}
		for j := i + 1; j < len(key); j++ {
			if key[j] == '}' {
				if j == i+1 {
					return key // "{}" is empty: hash the whole key
				}
				return key[i+1 : j]
			}
		}
		return key // '{' with no '}' after it: hash the whole key
	}
	return key
}

// HashSlot reports the Redis Cluster hash slot of key.
func HashSlot(key []byte) int {
	return int(crc16(hashTagged(key))) % totalSlots
}

// groupOfSlot maps a slot to its contiguous group among g groups:
// slot / (16384 / g) per doc 02 section 1.2. The formula only tiles
// evenly when g divides 16384 (every sane G is a power of two); for a
// g that does not, the division would name a group past the end for the
// last few slots, so the result is capped and the final group is
// slightly wider. G is validated at database creation; the cap keeps
// the route total here regardless.
func groupOfSlot(slot, g int) int {
	group := slot / (totalSlots / g)
	if group >= g {
		group = g - 1
	}
	return group
}

// GroupOf reports the slot group that owns key. The group is the unit
// every distributed structure keys on (lease, manifest, WAL section,
// fold cursor); dispatch and the cluster render both route through it.
// A runtime built as a bare literal (the f3 tests do this) has groups
// zero and routes at the default, the same zero-value tolerance f3's
// wyhash route had.
func (r *Runtime) GroupOf(key []byte) int {
	g := r.groups
	if g < 1 {
		g = DefaultSlotGroups
	}
	return groupOfSlot(HashSlot(key), g)
}

// Groups reports G, the slot-group count the runtime was built with.
func (r *Runtime) Groups() int { return r.groups }
