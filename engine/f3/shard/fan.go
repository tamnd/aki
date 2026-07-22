package shard

import (
	"encoding/binary"
	"math/rand/v2"

	"github.com/tamnd/aki/f3srv/resp"
)

// Tier-one multi-key fan-out (spec 2064/f3/03 section 6.2): the reader splits
// a multi-key command into per-shard sub-commands that all carry one reply
// sequence, each owner executes its slice as ordinary single-key work in its
// next batch and answers with a partial, and the partials gather at the
// connection's reply-reordering layer, where the last one to arrive emits the
// single client reply at that sequence. No barrier, no ticket, per-key
// atomicity only, which is what the tier-one commands promise. The scatter
// and gather plumbing here is deliberately command-agnostic (a kind byte and
// a partial encoding), so the tier-two intent commands can reuse the
// coordinator record and the sub-command chunking when they land.

// FanKind selects a fan-out command's partial encoding and final reply shape.
type FanKind uint8

const (
	// FanCount sums 8-byte little-endian partial counts into one integer
	// reply: DEL, UNLINK, EXISTS.
	FanCount FanKind = iota + 1

	// FanOK expects empty partials and replies +OK; a non-empty partial is an
	// error message and the first one becomes the reply: MSET.
	FanOK

	// FanMGet gathers per-key values into a bulk array in argument order. The
	// sub-command carries a trailing positions argument (2 bytes per key,
	// little-endian) the gather side reads back off the returning node.
	FanMGet

	// FanStats sums fixed-width per-shard counter blobs and renders the
	// INFO-style text reply: the evidence surface the LTM harness reads.
	FanStats

	// FanTxn gathers a cross-shard tier-two command's arm partials
	// (txnroute.go) and emits nothing: the reply arrives later on the
	// transaction's loopback node at the same sequence, once the barrier work
	// has run.
	FanTxn

	// FanKeys concatenates each shard's matched keys into one flat bulk array:
	// KEYS. Each partial is a run of length-prefixed keys (the AppendFanValue
	// framing: a 4-byte little-endian length then the bytes), and the gather
	// appends every shard's keys into one array in arrival order, which KEYS
	// leaves unordered like Redis.
	FanKeys

	// FanRandom picks one key uniformly across every shard: RANDOMKEY. Each
	// partial is an 8-byte little-endian key count then that shard's own
	// uniformly-drawn candidate, and the gather reservoir-selects across the
	// shards weighted by count, so every key in the whole keyspace is equally
	// likely. An all-empty keyspace draws a null bulk.
	FanRandom

	// FanScan concatenates each shard's matched keys the same way FanKeys does,
	// but wraps the gathered keys in SCAN's two-element reply: a next-cursor
	// bulk then the key array. The cursor is always "0" because f3's SCAN walks
	// the whole keyspace in one page, the same single-page answer HSCAN gives
	// for its listpack band, so a client's cursor loop makes exactly one pass.
	FanScan

	// FanMemStats sums the same fixed-width per-shard counter blob FanStats
	// gathers, but renders MEMORY STATS' flat field-value array instead of the
	// INFO text: MEMORY STATS. The per-shard op is the INFO blob producer, so
	// only the final reply shape differs.
	FanMemStats

	// FanMemDoctor sums the same counter blob and renders MEMORY DOCTOR's bulk
	// verdict string, a health line folded from the aggregate used-memory figure.
	FanMemDoctor
)

// maxFanKeys caps the keys one sub-command carries, so a sub-command's
// argument run (keys, MSET values, the positions argument) always fits a
// node's span table. A shard with more keys gets several sub-commands, each
// its own partial.
const maxFanKeys = 100

// fanCmd is the coordinator record: created by the reader before any
// sub-command is published, referenced by every sub-command's node slot, and
// mutated only by the connection's writer goroutine as partials arrive, so
// every field past the construction is plain single-consumer state.
type fanCmd struct {
	kind    FanKind
	pending int32
	sum     int64
	vals    [][]byte
	present []bool
	stats   []uint64
	errMsg  []byte
	chosen  []byte // FanRandom's running reservoir winner
	out     []byte

	// txn is the FanTxn coordinator's transaction: the arm builtin reads it
	// on the owner to find the intent its key enqueues. Set before the first
	// enqueue and immutable afterwards, so the owner-side read is ordered by
	// the hop queue's publish.
	txn *Txn
}

// DoFan enqueues one tier-one multi-key command: keys are the routed keys in
// argument order; vals, when non-nil, are the parallel values (MSET). The
// whole command takes one reply sequence, and the reply is emitted only when
// every shard's partial has gathered. Sizes are validated up front so a
// too-big command enqueues nothing and keeps its pipeline slot for the error
// reply.
func (c *Conn) DoFan(op byte, kind FanKind, keys, vals [][]byte) error {
	for i := range keys {
		need := len(keys[i]) + 2
		if vals != nil {
			need += len(vals[i])
		}
		if need > maxCmdBytes {
			return ErrTooBig
		}
	}
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}

	shards := len(c.rt.workers)
	fc := &fanCmd{kind: kind}
	if kind == FanMGet {
		fc.vals = make([][]byte, len(keys))
		fc.present = make([]bool, len(keys))
	}

	// Route every key once into the reader's reused scratch, so a repeated
	// MGET/MSET does not allocate a fresh order slice per command.
	order := c.fanOrder[:0]
	for _, k := range keys {
		order = append(order, c.rt.ShardOf(k))
	}
	c.fanOrder = order

	// The sub-command count must be final before the first enqueue: a partial
	// can come back and merge while the scatter is still publishing, so the
	// coordinator's countdown is set once and touched only by the writer
	// afterwards. Count the sub-commands in a cheap allocation-free pass that
	// mirrors the chunking below (one shard at a time, argument order, chunked
	// under the per-sub caps), then set pending before scattering.
	fc.pending = int32(countFanSubs(keys, vals, order, shards, kind, c.rt.batchDataCap))

	// Scatter: build and enqueue each sub-command out of the reader's reused
	// argv and pos scratch. enqueueFan's b.add copies every argument's bytes
	// into the node's span table, so both buffers are safe to reset and reuse
	// the moment enqueueFan returns.
	argv := c.fanArgv[:0]
	pos := c.fanPos[:0]
	for sh := range shards {
		kn := 0
		bytes := 0
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > c.rt.batchDataCap) {
				// For an MGET fan the position blob rides as the sub's last
				// argument; append it in this scope so a growth of the reused
				// argv backing persists across sub-commands instead of
				// reallocating every flush. enqueueFan copies the whole argv
				// out synchronously, so it is reset for the next sub right after.
				if kind == FanMGet {
					argv = append(argv, pos)
				}
				if err := c.enqueueFan(sh, op, argv, fc); err != nil {
					c.fanArgv, c.fanPos = argv[:0], pos[:0]
					return err
				}
				argv, pos = argv[:0], pos[:0]
				kn, bytes = 0, 0
			}
			argv = append(argv, keys[i])
			bytes += len(keys[i])
			if vals != nil {
				argv = append(argv, vals[i])
				bytes += len(vals[i])
			}
			if kind == FanMGet {
				pos = binary.LittleEndian.AppendUint16(pos, uint16(i))
				bytes += 2
			}
			kn++
		}
		if kn > 0 {
			if kind == FanMGet {
				argv = append(argv, pos)
			}
			if err := c.enqueueFan(sh, op, argv, fc); err != nil {
				c.fanArgv, c.fanPos = argv[:0], pos[:0]
				return err
			}
			argv, pos = argv[:0], pos[:0]
		}
	}
	c.fanArgv, c.fanPos = argv, pos
	c.seq++
	return nil
}

// countFanSubs returns how many sub-commands DoFan's scatter will produce,
// replaying the same shard grouping and per-sub cap chunking without building
// or allocating anything, so the coordinator's pending countdown is final
// before the first enqueue.
func countFanSubs(keys, vals [][]byte, order []int, shards int, kind FanKind, dataCap int) int {
	subs := 0
	for sh := range shards {
		kn := 0
		bytes := 0
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > dataCap) {
				subs++
				kn, bytes = 0, 0
			}
			bytes += len(keys[i])
			if vals != nil {
				bytes += len(vals[i])
			}
			if kind == FanMGet {
				bytes += 2
			}
			kn++
		}
		if kn > 0 {
			subs++
		}
	}
	return subs
}

// DoFanAll enqueues one keyless sub-command per shard, the S-way scatter the
// stats surface rides: every shard answers a partial and the gather renders
// one reply.
func (c *Conn) DoFanAll(op byte, kind FanKind) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	// The countdown is final before the first enqueue; see DoFan.
	fc := &fanCmd{kind: kind, pending: int32(len(c.rt.workers))}
	for sh := range c.rt.workers {
		if err := c.enqueueFan(sh, op, nil, fc); err != nil {
			return err
		}
	}
	c.seq++
	return nil
}

// DoFanAllArgs is DoFanAll with a shared argument list every shard's
// sub-command carries: the keyless scatter for a command that still takes an
// argument, KEYS handing every shard its match pattern. The argv is the same
// for every shard and read-only in the owner (the pattern), so it is enqueued
// verbatim once per shard; enqueueFan copies its bytes into each node's span
// table, so sharing the slice across shards is safe.
func (c *Conn) DoFanAllArgs(op byte, kind FanKind, argv [][]byte) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	// The countdown is final before the first enqueue; see DoFan.
	fc := &fanCmd{kind: kind, pending: int32(len(c.rt.workers))}
	for sh := range c.rt.workers {
		if err := c.enqueueFan(sh, op, argv, fc); err != nil {
			return err
		}
	}
	c.seq++
	return nil
}

// enqueueFan is Do's node handling for one sub-command: same node, same
// spill-to-fresh-node rule, plus the coordinator pointer in the command's
// slot. Every sub-command of one fan shares the connection's current
// sequence; DoFan advances it once at the end.
func (c *Conn) enqueueFan(sh int, op byte, argv [][]byte, fc *fanCmd) error {
	b := c.pending[sh]
	if b == nil {
		b = c.take()
		c.pending[sh] = b
	}
	if !b.add(op, c.seq, true, argv) {
		c.flushShard(sh)
		b = c.take()
		c.pending[sh] = b
		if !b.add(op, c.seq, true, argv) {
			return ErrTooBig
		}
	}
	b.cmds[b.n-1].silent = c.silentNext
	b.setFan(int(b.n)-1, fc)
	return nil
}

// mergeFan folds one arriving partial into its coordinator. It runs on the
// connection's writer goroutine, the single consumer of the outbound queue,
// so the coordinator mutations are plain. When the last partial lands the
// final reply is built and delivered through the reorder ring like any other
// reply, which is what keeps a fan-out ordered against the single-key traffic
// around it.
func (c *Conn) mergeFan(fc *fanCmd, seq uint32, b *hopBatch, i int, emit func([]byte)) int {
	var pos []byte
	if fc.kind == FanMGet {
		cmd := &b.cmds[i]
		pos = b.arg(i, int(cmd.argn)-1)
	}
	fc.fold(b.reply(i), pos)
	fc.pending--
	if fc.pending > 0 {
		return 0
	}
	if fc.kind == FanTxn {
		// The arms have all executed; the reply comes later on the
		// transaction's loopback node, so there is nothing to emit here.
		return 0
	}
	return c.deliver(seq, c.renderFan(fc), emit)
}

// fold accumulates one arriving partial into the coordinator, the per-kind decode
// step mergeFan runs for every sub-command. pos is the FanMGet position argument
// (nil for every other kind and for a keyless gather that carries no positions,
// exec.go's RunFanAllCaptured). Single-consumer state, so the mutations are plain.
func (fc *fanCmd) fold(part, pos []byte) {
	switch fc.kind {
	case FanCount:
		if len(part) == 8 {
			fc.sum += int64(binary.LittleEndian.Uint64(part))
		}
	case FanOK:
		if len(part) > 0 && fc.errMsg == nil {
			fc.errMsg = append([]byte(nil), part...)
		}
	case FanMGet:
		for k := 0; len(part) >= 4; k++ {
			n := binary.LittleEndian.Uint32(part)
			part = part[4:]
			p := int(binary.LittleEndian.Uint16(pos[2*k:]))
			if n == fanNil {
				continue
			}
			fc.present[p] = true
			fc.vals[p] = append(fc.vals[p][:0], part[:n]...)
			part = part[n:]
		}
	case FanStats, FanMemStats, FanMemDoctor:
		// All three gather the same fixed-width counter blob and sum it
		// position-wise; only the final reply shape differs.
		if fc.stats == nil {
			fc.stats = make([]uint64, len(part)/8)
		}
		for k := 0; k+8 <= len(part) && k/8 < len(fc.stats); k += 8 {
			fc.stats[k/8] += binary.LittleEndian.Uint64(part[k:])
		}
	case FanKeys, FanScan:
		// Both gather a run of length-prefixed keys into one key list; only the
		// final reply shape differs (a flat array for KEYS, the cursor envelope
		// for SCAN).
		for len(part) >= 4 {
			n := binary.LittleEndian.Uint32(part)
			part = part[4:]
			if n == fanNil || int(n) > len(part) {
				break
			}
			fc.vals = append(fc.vals, append([]byte(nil), part[:n]...))
			part = part[n:]
		}
	case FanRandom:
		if len(part) >= 8 {
			count := int64(binary.LittleEndian.Uint64(part))
			if count > 0 {
				fc.sum += count
				// Weighted reservoir: the shard's candidate replaces the running
				// winner with probability count/sum, so after every partial the
				// held key is uniform over all keys seen so far.
				if rand.Int64N(fc.sum) < count {
					fc.chosen = append(fc.chosen[:0], part[8:]...)
				}
			}
		}
	}
}

// renderFan builds the coordinator's final RESP reply once every partial has
// folded, the shape step mergeFan and exec.go's RunFanAllCaptured share. The bytes
// live in fc.out, reused across calls.
func (c *Conn) renderFan(fc *fanCmd) []byte {
	fc.out = fc.out[:0]
	switch fc.kind {
	case FanCount:
		fc.out = resp.AppendInt(fc.out, fc.sum)
	case FanOK:
		if fc.errMsg != nil {
			fc.out = resp.AppendErrorBytes(fc.out, fc.errMsg)
		} else {
			fc.out = resp.AppendStatus(fc.out, "OK")
		}
	case FanMGet:
		fc.out = resp.AppendArrayHeader(fc.out, len(fc.vals))
		for p := range fc.vals {
			if fc.present[p] {
				fc.out = resp.AppendBulk(fc.out, fc.vals[p])
			} else {
				fc.out = resp.AppendNull(fc.out)
			}
		}
	case FanStats:
		fc.out = c.rt.renderStats(fc.out, fc.stats)
	case FanMemStats:
		fc.out = c.rt.renderMemStats(fc.out, fc.stats)
	case FanMemDoctor:
		fc.out = c.rt.renderMemDoctor(fc.out, fc.stats)
	case FanKeys:
		fc.out = resp.AppendArrayHeader(fc.out, len(fc.vals))
		for _, v := range fc.vals {
			fc.out = resp.AppendBulk(fc.out, v)
		}
	case FanScan:
		// SCAN's reply is a two-element array: the next cursor then the key page.
		// The whole keyspace fits one page, so the cursor is the terminal "0".
		fc.out = resp.AppendArrayHeader(fc.out, 2)
		fc.out = resp.AppendBulk(fc.out, []byte("0"))
		fc.out = resp.AppendArrayHeader(fc.out, len(fc.vals))
		for _, v := range fc.vals {
			fc.out = resp.AppendBulk(fc.out, v)
		}
	case FanRandom:
		// sum stays zero only when every shard was empty; then RANDOMKEY is
		// the null bulk. Otherwise the reservoir winner is set.
		if fc.sum == 0 {
			fc.out = resp.AppendNull(fc.out)
		} else {
			fc.out = resp.AppendBulk(fc.out, fc.chosen)
		}
	}
	return fc.out
}

// fanNil is the absent-value length marker in an MGET partial.
const fanNil = 0xffffffff

// FanCount writes a partial count for a FanCount fan-out: 8 bytes,
// little-endian.
func (r Reply) FanCount(n int64) {
	off := len(r.b.rep)
	r.b.rep = binary.LittleEndian.AppendUint64(r.b.rep, uint64(n))
	r.span(off)
}

// FanOK writes the empty all-good partial for a FanOK fan-out.
func (r Reply) FanOK() {
	r.span(len(r.b.rep))
}

// FanErrString writes an error partial for a FanOK fan-out; the first error
// any shard reports becomes the command's reply.
func (r Reply) FanErrString(msg string) {
	off := len(r.b.rep)
	r.b.rep = append(r.b.rep, msg...)
	r.span(off)
}

// Raw writes an already encoded partial verbatim: the FanMGet and FanStats
// handlers build their encoding in the shard scratch and hand it over whole.
func (r Reply) Raw(part []byte) {
	off := len(r.b.rep)
	r.b.rep = append(r.b.rep, part...)
	r.span(off)
}

// AppendFanValue appends one MGET partial entry: a 4-byte little-endian
// length then the bytes, or the absent marker.
func AppendFanValue(dst, val []byte, present bool) []byte {
	if !present {
		return binary.LittleEndian.AppendUint32(dst, fanNil)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(val)))
	return append(dst, val...)
}
