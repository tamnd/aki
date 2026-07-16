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

// drain runs one cycle: collect up to maxOps oldest-dirty slots into one
// DrainBatch, apply it, then mark the headers drained. It returns the
// number of records drained. On a store error nothing is marked drained;
// the collected slots re-enter the queue (at the tail, an order
// perturbation only a failing store can see) and the same Seq is reused
// next cycle, which the Store replay contract makes safe.
func (d *drainer) drain(ctx context.Context) (int, error) {
	d.ops = d.ops[:0]
	d.slots = d.slots[:0]
	d.bumps = d.bumps[:0]
	d.bumpKeys = d.bumpKeys[:0]
	for len(d.ops) < d.maxOps {
		s, ok := d.ht.popDirty()
		if !ok {
			break
		}
		hd := &d.ht.hdrs[s]
		if hd.state != stateDirty {
			continue // stale entry: drained directly or slot reused
		}
		op := Op{Rec: Record{Key: d.ht.keys.data(hd.keyRef), Gen: hd.gen, Root: hd.typeTag&TagRoot != 0}}
		if hd.valRef == 0 {
			op.Del = true
		} else {
			op.Rec.Value = d.ht.vals.data(hd.valRef)
			if hd.expireLo != 0 {
				// The header holds the coarse projection only; the record
				// gets it back at 1024 ms grain. Stamps are made by ceil
				// division, so the reconstruction never expires early and
				// the round trip through a drain is a fixed point. Exact
				// PEXPIRE plumbing rides the WAL's pexpire frames when doc
				// 11 wires the wheel into this runtime.
				op.Rec.ExpireMs = int64(hd.expireLo) << 10
			}
		}
		d.ops = append(d.ops, op)
		d.slots = append(d.slots, s)
		if len(d.pending) != 0 {
			if bs, ok := d.pending[string(op.Rec.Key)]; ok {
				d.bumps = append(d.bumps, bs...)
				d.bumpKeys = append(d.bumpKeys, string(op.Rec.Key))
			}
		}
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
	return len(d.slots), nil
}
