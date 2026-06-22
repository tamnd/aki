package main

import (
	"math/rand"
	"strconv"
)

// mixKind says how a workload chooses between its read and write op.
type mixKind int

const (
	mixBoth  mixKind = iota // pick read or write by the --ratio
	mixRead                 // every op is a read
	mixWrite                // every op is a write
)

// workload describes one synthetic workload: how it mixes reads and writes and
// how it renders a single command into the send buffer.
type workload struct {
	mix   mixKind
	build func(dst []byte, isRead bool, idx int, val []byte, rng *rand.Rand) []byte
}

// keyName renders the key for a given index, shared by every workload.
func keyName(idx int) []byte { return []byte("key:" + strconv.Itoa(idx)) }

// memberName renders a sorted-set or hash member for a given index.
func memberName(idx int) []byte { return []byte("m:" + strconv.Itoa(idx)) }

// workloads is the registry the bench harness dispatches on. The set covers the
// catalog in spec 22 section 6.2: plain key/value, a read-through cache, a
// producer/consumer queue, a leaderboard, a session store, a rate limiter, and
// a stream.
var workloads = map[string]workload{
	"set": {
		mix: mixWrite,
		build: func(dst []byte, _ bool, idx int, val []byte, _ *rand.Rand) []byte {
			return appendArray(dst, [][]byte{[]byte("SET"), keyName(idx), val})
		},
	},
	"get": {
		mix: mixRead,
		build: func(dst []byte, _ bool, idx int, _ []byte, _ *rand.Rand) []byte {
			return appendArray(dst, [][]byte{[]byte("GET"), keyName(idx)})
		},
	},
	"mixed": {
		mix: mixBoth,
		build: func(dst []byte, isRead bool, idx int, val []byte, _ *rand.Rand) []byte {
			if isRead {
				return appendArray(dst, [][]byte{[]byte("GET"), keyName(idx)})
			}
			return appendArray(dst, [][]byte{[]byte("SET"), keyName(idx), val})
		},
	},
	"cache": {
		mix: mixBoth,
		build: func(dst []byte, isRead bool, idx int, val []byte, _ *rand.Rand) []byte {
			if isRead {
				return appendArray(dst, [][]byte{[]byte("GET"), keyName(idx)})
			}
			// Write path fills the cache entry with a TTL, like a read-through fill.
			return appendArray(dst, [][]byte{[]byte("SET"), keyName(idx), val, []byte("EX"), []byte("60")})
		},
	},
	"queue": {
		mix: mixBoth,
		build: func(dst []byte, isRead bool, idx int, val []byte, _ *rand.Rand) []byte {
			if isRead {
				return appendArray(dst, [][]byte{[]byte("LPOP"), keyName(idx)})
			}
			return appendArray(dst, [][]byte{[]byte("RPUSH"), keyName(idx), val})
		},
	},
	"leaderboard": {
		mix: mixBoth,
		build: func(dst []byte, isRead bool, idx int, _ []byte, rng *rand.Rand) []byte {
			if isRead {
				return appendArray(dst, [][]byte{[]byte("ZREVRANK"), keyName(0), memberName(idx)})
			}
			score := strconv.Itoa(rng.Intn(1000000))
			return appendArray(dst, [][]byte{[]byte("ZADD"), keyName(0), []byte(score), memberName(idx)})
		},
	},
	"session": {
		mix: mixBoth,
		build: func(dst []byte, isRead bool, idx int, val []byte, _ *rand.Rand) []byte {
			if isRead {
				return appendArray(dst, [][]byte{[]byte("HGETALL"), keyName(idx)})
			}
			return appendArray(dst, [][]byte{[]byte("HSET"), keyName(idx), []byte("data"), val})
		},
	},
	"ratelimit": {
		mix: mixWrite,
		build: func(dst []byte, _ bool, idx int, _ []byte, _ *rand.Rand) []byte {
			return appendArray(dst, [][]byte{[]byte("INCR"), keyName(idx)})
		},
	},
	"stream": {
		mix: mixBoth,
		build: func(dst []byte, isRead bool, idx int, val []byte, _ *rand.Rand) []byte {
			if isRead {
				return appendArray(dst, [][]byte{[]byte("XLEN"), keyName(idx)})
			}
			return appendArray(dst, [][]byte{[]byte("XADD"), keyName(idx), []byte("*"), []byte("data"), val})
		},
	},
}
