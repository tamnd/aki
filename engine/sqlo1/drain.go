package sqlo1

import "context"

// Drain defaults, doc 04 section 7. The threshold is the dirty-bytes
// high-water mark that asks for a drain cycle; the per-cycle op cap
// bounds one cycle's batch so the owner loop never disappears into a
// single giant ApplyBatch. The batch carries the oldest dirty records
// in dirtied order; placement into 4 KiB groups, collection packing,
// and size-class sorting happen below the seam (sqlo1b's ApplyBatch
// does them, Track A has no notion of placement).
const (
	drainThreshold = 8 << 20
	drainMaxOps    = 1024
	// maxVolDefers caps how many queue laps a volatile-near record may
	// sit out before it drains regardless. The dieinram lab put the
	// whole reordering payoff inside two laps: a record still alive
	// after two is one the workload genuinely wants durable.
	maxVolDefers = 2
)

// drainer is the write-behind scheduler: it moves dirty records from a
// HotTable to a Store in first-dirtied-first order, one DrainBatch per
// cycle, and cools the drained headers to resident. Coalescing is the
// queue's doing (one entry per dirty key however often it is rewritten),
// so a key written five times between cycles reaches the store once,
// with its final value.
type drainer struct {
	ht        *HotTable
	store     Store
	seq       int64
	threshold int
	maxOps    int
	// ops, slots, and batch are reused across cycles so a steady-state
	// drain cycle does not allocate.
	ops   []Op
	slots []uint32
	batch DrainBatch
	// pending holds registered generation bumps keyed by the root key
	// whose next op must carry them; bumps and bumpKeys are the reused
	// per-cycle collections. The map is empty except in the window
	// between a collection DEL or type overwrite and the drain cycle
	// that carries the root's post-image out.
	pending  map[string][]Bump
	bumps    []Bump
	bumpKeys []string
	// Die-in-RAM state, doc 11 section 6, shaped by the dieinram lab.
	// deferred parks the cycle's volatile-near slots outside the queue
	// so one cycle can never pop them twice; the cycle's tail re-files
	// them one lap back or force-collects when they were all it had.
	// The cadence pair estimates the queue lap time (dirty bytes at
	// the recent drain pace); both stay zero until the clock moves,
	// which keeps the whole path off for clock-less tests.
	deferred    []uint32
	cancels     int64
	volDefers   int64
	cycleBytes  int64
	lastCycleMs int64
	gapEwmaMs   int64
	bytesEwma   int64
}

func newDrainer(ht *HotTable, store Store) *drainer {
	return &drainer{
		ht:        ht,
		store:     store,
		seq:       store.Stats().HighWater,
		threshold: drainThreshold,
		maxOps:    drainMaxOps,
		ops:       make([]Op, 0, drainMaxOps),
		slots:     make([]uint32, 0, drainMaxOps),
		pending:   map[string][]Bump{},
	}
}

// addBump registers a generation bump to ride the drain batch that
// carries key's next op. Callers register the bump before the mutation
// that retires the generation dirties the root, so the bump can never
// miss the batch carrying the root's post-image; landing the two in
// one batch is what keeps a crash from separating a retired generation
// from the image that retired it.
func (d *drainer) addBump(key []byte, rooth uint64, newgen uint32) {
	d.pending[string(key)] = append(d.pending[string(key)], Bump{Rooth: rooth, NewGen: newgen})
}

// needsDrain reports whether dirty bytes crossed the threshold. The
// other doc 04 triggers, WAL trim debt and shard idleness, belong to the
// WAL slice and the server loop.
func (d *drainer) needsDrain() bool {
	return d.ht.dirtyBytes >= d.threshold
}

// plain reports whether hd is a bare value record with no plane
// machinery attached: no root, fence, or delta role and no generation.
// Only plain records reap-cancel or defer. Collection records answer
// to rule W1 ordering and to genbump lifecycles the queue must not
// perturb, and a dead root needs the genbump only Str.ReapStep and the
// command paths mint, so tombstoning one here would strand its plane.
func plain(hd *hdr) bool {
	return hd.gen == 0 && hd.typeTag&(TagRoot|TagFence|TagDelta) == 0
}

// deferHorizon is how far ahead of its deadline a volatile record is
// still worth holding back: maxVolDefers queue laps, the lap estimated
// as the current dirty backlog at the recent drain pace. Zero until a
// cadence forms, which disables deferral entirely.
func (d *drainer) deferHorizon() int64 {
	if d.gapEwmaMs <= 0 || d.bytesEwma <= 0 {
		return 0
	}
	return maxVolDefers * int64(d.ht.dirtyBytes) * d.gapEwmaMs / d.bytesEwma
}

// collect books slot s into the cycle's batch. An expired plain put
// converts to a tombstone on the way out, the reap-cancel half of doc
// 11 section 6: the value died in RAM, so the store gets a delete that
// clears any stale cold predecessor instead of value bytes the reaper
// would only tombstone later. The header still cools to resident like
// any drained slot; its expired value stays invisible to reads and
// evicts like any clean resident.
func (d *drainer) collect(s uint32) {
	hd := &d.ht.hdrs[s]
	op := Op{Rec: Record{
		Key:   d.ht.keys.data(hd.keyRef),
		Gen:   hd.gen,
		Root:  hd.typeTag&TagRoot != 0,
		Fence: hd.typeTag&TagFence != 0,
	}}
	if hd.valRef == 0 {
		op.Del = true
	} else if exp := expMsOf(hd); exp != 0 && exp <= d.ht.nowMs && plain(hd) {
		op.Del = true
		d.cancels++
	} else {
		// Delta needs both bits: a tombstone revival or a window that
		// mixed in a structural write already lost the flag in PutGen,
		// and a bare TagDelta without TagRoot cannot exist above.
		op.Rec.Delta = hd.typeTag&(TagRoot|TagDelta) == TagRoot|TagDelta
		op.Rec.Value = d.ht.vals.data(hd.valRef)
		// The header carries the exact expire_ms split into the
		// wheel projection and the remainder, so the record gets
		// the millisecond back and the round trip through a drain
		// is exact.
		op.Rec.ExpireMs = exp
	}
	d.ops = append(d.ops, op)
	d.slots = append(d.slots, s)
	d.cycleBytes += int64(len(op.Rec.Key) + len(op.Rec.Value))
	if len(d.pending) != 0 {
		if bs, ok := d.pending[string(op.Rec.Key)]; ok {
			d.bumps = append(d.bumps, bs...)
			d.bumpKeys = append(d.bumpKeys, string(op.Rec.Key))
		}
	}
}

// drain runs one cycle: collect up to maxOps oldest-dirty slots into one
// DrainBatch, apply it, then mark the headers drained. It returns the
// number of records drained. On a store error nothing is marked drained;
// the collected slots re-enter the queue (at the tail, an order
// perturbation only a failing store can see) and the same Seq is reused
// next cycle, which the Store replay contract makes safe.
//
// Volatile-near records drain last, the reordering half of doc 11
// section 6: a plain record within deferHorizon of its deadline parks
// instead of collecting and re-files at the tail, one lap back, up to
// maxVolDefers laps. A lap is about one drain window, so most deferred
// records come back expired and reap-cancel. The loop terminates
// because parked slots leave the queue for the cycle's duration; every
// pop either collects, skips a stale entry, or parks.
func (d *drainer) drain(ctx context.Context) (int, error) {
	d.ops = d.ops[:0]
	d.slots = d.slots[:0]
	d.bumps = d.bumps[:0]
	d.bumpKeys = d.bumpKeys[:0]
	d.deferred = d.deferred[:0]
	d.cycleBytes = 0
	horizon := d.deferHorizon()
	for len(d.ops) < d.maxOps {
		s, ok := d.ht.popDirty()
		if !ok {
			break
		}
		hd := &d.ht.hdrs[s]
		if hd.state != stateDirty {
			continue // stale entry: drained directly or slot reused
		}
		if horizon > 0 && hd.valRef != 0 && hd.queued&queuedVolMask < maxVolDefers*queuedVolStep && plain(hd) {
			if exp := expMsOf(hd); exp != 0 && exp > d.ht.nowMs && exp-d.ht.nowMs <= horizon {
				hd.queued += queuedVolStep
				d.deferred = append(d.deferred, s)
				d.volDefers++
				continue
			}
		}
		d.collect(s)
	}
	// Force-collect when deferral was all the queue held: the ladder's
	// loops read a zero-op cycle as an empty queue, so a cycle must
	// make progress whenever entries exist. Sacrifice from the tail,
	// the youngest parked records with the most life left, the ones a
	// lap is least likely to save.
	if len(d.ops) == 0 {
		for len(d.deferred) > 0 && len(d.ops) < d.maxOps {
			s := d.deferred[len(d.deferred)-1]
			d.deferred = d.deferred[:len(d.deferred)-1]
			d.collect(s)
		}
	}
	// The rest re-file at the tail in pop order, one lap behind
	// everything dirtied before this cycle.
	for _, s := range d.deferred {
		d.ht.hdrs[s].queued |= queuedBit
		d.ht.pushDirty(s)
	}
	if len(d.ops) == 0 {
		return 0, nil
	}

	seq := d.seq + 1
	d.batch.Seq = seq
	d.batch.Ops = d.ops
	d.batch.Bumps = d.bumps
	if err := d.store.ApplyBatch(ctx, &d.batch); err != nil {
		// The pending bumps stay registered: the slots re-enter the
		// queue, so the retry collects the same roots and re-attaches
		// the same bumps under the reused Seq.
		for _, s := range d.slots {
			d.ht.enqueueDirty(s)
		}
		return 0, err
	}
	d.seq = seq
	for _, k := range d.bumpKeys {
		delete(d.pending, k)
	}
	for _, s := range d.slots {
		// vptr is the batch Seq: disk positions never cross the seam
		// (Track A has none to give), and the state machine only needs
		// nonzero-when-clean; cold reads go back through BatchGet.
		if !d.ht.drained(s, uint64(seq)) {
			panic("sqlo1: collected slot left dirty state mid-cycle")
		}
	}
	// Cadence for the defer horizon: EWMA the wall gap between
	// successful cycles and the bytes a cycle moves. Same-millisecond
	// cycles are skipped rather than averaged in, so a burst of
	// sub-tick cycles cannot collapse the estimate to zero.
	if now := d.ht.nowMs; d.lastCycleMs != 0 && now > d.lastCycleMs {
		gap := now - d.lastCycleMs
		if d.gapEwmaMs == 0 {
			d.gapEwmaMs, d.bytesEwma = gap, d.cycleBytes
		} else {
			d.gapEwmaMs = (3*d.gapEwmaMs + gap) / 4
			d.bytesEwma = (3*d.bytesEwma + d.cycleBytes) / 4
		}
		d.lastCycleMs = now
	} else if d.lastCycleMs == 0 {
		d.lastCycleMs = d.ht.nowMs
	}
	return len(d.slots), nil
}
