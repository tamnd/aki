package derived

import "encoding/binary"

// MurmurHash64A is the Redis variant of Austin Appleby's 64-bit MurmurHash, the
// one HyperLogLog hashes elements with (spec 2064/f3/15 section 7.2). It is
// ported byte-for-byte from Redis's hyperloglog.c so a register set by PFADD in
// aki matches the register a same-version Redis sets: the sketch bytes are a wire
// interop surface, and the hash is where that interop is won or lost.
//
// aki runs on little-endian targets, so the word loads read the input directly
// with binary.LittleEndian, which is what Redis does on the same hardware.
func murmurHash64A(data []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47

	h := seed ^ (uint64(len(data)) * m)

	n := len(data) &^ 7 // whole 8-byte words
	for i := 0; i < n; i += 8 {
		k := binary.LittleEndian.Uint64(data[i:])
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}

	// The trailing 1..7 bytes, folded high to low with fallthrough exactly as the
	// Redis switch does.
	tail := data[n:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// hllPatLen hashes an element and returns the register it lands in and the
// candidate register value: the low HLL_P bits pick the register, and the value
// is the position of the first set bit in the remaining bits, one-based. Redis
// sets bit HLL_Q as a sentinel before the scan so the count never runs past
// Q+1, which is why a 6-bit register is enough. Ported from Redis hllPatLen.
func hllPatLen(ele []byte) (index int, count byte) {
	hash := murmurHash64A(ele, hllHashSeed)
	index = int(hash & hllPMask)
	hash >>= hllP
	hash |= uint64(1) << hllQ // sentinel so the loop always terminates
	var bit uint64 = 1
	count = 1
	for hash&bit == 0 {
		count++
		bit <<= 1
	}
	return index, count
}
