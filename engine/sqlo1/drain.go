package sqlo1

import "context"

// Drain defaults, doc 04 section 7. The threshold is the dirty-bytes
// high-water mark that asks for a drain cycle; the per-cycle op cap
// bounds one cycle's batch so the owner loop never disappears into a
// single giant ApplyBatch. Real 4 KiB group buffers with size-class
// sorting arrive with the Track B format; against the placeholder Store
// the batch is just the oldest dirty records in order.
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
	}
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
	for len(d.ops) < d.maxOps {
		s, ok := d.ht.popDirty()
		if !ok {
			break
		}
		hd := &d.ht.hdrs[s]
		if hd.state != stateDirty {
			continue // stale entry: drained directly or slot reused
		}
		op := Op{Rec: Record{Key: d.ht.keys.data(hd.keyRef), Gen: hd.gen}}
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
	}
	if len(d.ops) == 0 {
		return 0, nil
	}

	seq := d.seq + 1
	d.batch.Seq = seq
	d.batch.Ops = d.ops
	if err := d.store.ApplyBatch(ctx, &d.batch); err != nil {
		for _, s := range d.slots {
			d.ht.enqueueDirty(s)
		}
		return 0, err
	}
	d.seq = seq
	for _, s := range d.slots {
		// vptr is the batch Seq for now: the placeholder Store has no
		// disk positions, and the state machine only needs "assigned".
		// Track B's group buffers hand out real positions here.
		if !d.ht.drained(s, uint64(seq)) {
			panic("sqlo1: collected slot left dirty state mid-cycle")
		}
	}
	return len(d.slots), nil
}
