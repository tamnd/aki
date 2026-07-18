package shard

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/store"
)

// akiWriterRing is the per-shard in-flight bound on the shared file's group-commit
// writer. It is sized generously so a submit an owner makes rarely finds the ring
// full: the design note (M8-group-commit-writer.md) keeps the first slices wide and
// exposes writer saturation as owner backpressure for the lab to find the knee,
// rather than tuning the ring down before a number demands it.
const akiWriterRing = 64

// akiSepThreshold is the separation threshold stamped into a freshly created .aki
// prefix. It is metadata a reader reports; the store's own band decision runs off
// ResidentCapBytes, so this only records the geometry the file was made with.
const akiSepThreshold = 64

// Runtime is the shard topology: S workers, each a single goroutine owning
// one store (and optionally locked to an OS thread), fixed at startup. Shards never split, merge, or rebalance
// at runtime; resizing S means restarting the process (doc 03 section 2.2).
type Runtime struct {
	workers []*worker
	started bool

	// aki and gw back a durable runtime with the one shared .aki file and its one
	// group-commit writer (Config.AkiPath), the M8 durable arc's runtime seam. Every
	// worker's store borrows the file and routes its record-log cuts through the one
	// writer, so no two shards race the single-writer append cursor. Both are nil on
	// the default scratch-vlog path, which owns per-shard files and needs neither.
	// The runtime owns their lifetime: Stop joins the writer and closes the file
	// after the owners are gone.
	aki *akifile.File
	gw  *akifile.GroupWriter

	// txnTicket is the process-global tier-two ticket source (doc 03 section
	// 6.1, intent.go): one atomic touched only by Begin, off the single-key path
	// entirely. The total order it hands out is what makes the intent schedule
	// deadlock-free.
	txnTicket atomic.Uint64

	// netInfo, when set, appends the transport's "# Net" lines to the INFO
	// stats text (doc 08 section 9.5). The server layer owns the transport
	// counters and registers the renderer through SetNetInfo before Start;
	// connection writer goroutines read the field with plain loads during an
	// INFO gather, which the fixed-before-Start rule makes safe.
	netInfo func([]byte) []byte

	// live counts connections currently being served across every driver. The
	// connection writers read it in idleOnce to decide whether to spin before
	// they park (see connSpinHighWater): past the high-water the box is
	// saturated, so a writer parks at once and leaves its core to the shard
	// workers. A driver bumps it as it admits and drops a connection (through
	// ConnOpened and ConnClosed, which its register and unregister already call
	// under their registry lock); NewConn does not touch it, so a test that
	// builds a bare Conn never perturbs the spin decision.
	live atomic.Int64

	// The per-connection hop-transport sizes, resolved once at construction from
	// the tuning.go defaults or the Config overrides and read by NewConn and the
	// batch pool. These are the M0 memory-bar lever (labs/f3/m0/24_conn_buffers
	// located it, labs/f3/m0/25_conn_caps sweeps it): at high fan-out the pooled
	// hopBatch data/reply buffers and the reply reorder ring dominate resident
	// footprint, and they hold ~640B of each 8KiB+ buffer at the 64B gate cell.
	// batchDataCap and repCap start each node's data and reply buffers; a bigger
	// command grows them on demand (batch.go), so a smaller start only trims the
	// steady 64B path. replyRing is the reply reorder window and freeListCap the
	// per-connection node free list.
	batchDataCap int
	repCap       int
	replyRing    int
	freeListCap  int
}

// resolveConnCaps fills the per-connection hop-transport sizes from the Config
// overrides, taking the tuning.go default for every field left non-positive.
// repCap tracks batchDataCap by the same +64*batchCap headroom the const
// carries, so a swept data cap keeps its matched reply headroom.
func (r *Runtime) resolveConnCaps(c Config) {
	r.batchDataCap = batchDataCap
	if c.BatchDataCap > 0 {
		r.batchDataCap = c.BatchDataCap
	}
	r.repCap = r.batchDataCap + 64*batchCap
	if c.RepCap > 0 {
		r.repCap = c.RepCap
	}
	r.replyRing = replyRing
	if c.ReplyRing > 0 {
		r.replyRing = c.ReplyRing
	}
	r.freeListCap = freeListCap
	if c.FreeListCap > 0 {
		r.freeListCap = c.FreeListCap
	}
}

// ConnOpened records that a driver has begun serving a connection, and
// ConnClosed pairs with it when the connection is torn down. They maintain the
// live count that drives the connSpinHighWater park-immediately switch; every
// driver routes through register and unregister, so calling these there keeps
// the count correct across the goroutine, reactor, and uring transports.
func (r *Runtime) ConnOpened() { r.live.Add(1) }

// ConnClosed records that a served connection has been torn down; see
// ConnOpened.
func (r *Runtime) ConnClosed() { r.live.Add(-1) }

// New builds a runtime of shards workers, each with its own store of
// arenaBytes tiled into segments of segBytes (non-positive segBytes takes the
// store default). Nothing runs until Start.
func New(shards, arenaBytes, segBytes int) *Runtime {
	if shards < 1 {
		shards = 1
	}
	r := &Runtime{workers: make([]*worker, shards)}
	r.resolveConnCaps(Config{})
	for i := range r.workers {
		r.workers[i] = newWorker(i, store.New(arenaBytes, segBytes))
		r.workers[i].rt = r
	}
	return r
}

// Config is the runtime topology plus the larger-than-memory knobs of doc 09
// section 8: a value-log directory and a resident byte budget, both per
// shard. Sharding is fixed at startup like everything else here.
type Config struct {
	Shards     int
	ArenaBytes int
	SegBytes   int

	// VlogDir, when set, gives every shard its own value log under this
	// directory (one file per shard, fresh-start semantics). Without it the
	// stores are memory-only and ResidentCapBytes is ignored.
	VlogDir string

	// AkiPath, when set, backs the whole runtime with the one shared .aki durable
	// file at this path instead of per-shard scratch logs (the M8 durable arc). One
	// file and one group-commit writer serve every shard: a shard stages its record
	// rows and separated values into the shared file and cuts them through the one
	// writer that owns the append cursor. An existing file is recovered on open, so
	// a restart rebuilds every shard's index from the durable log; a missing file is
	// created fresh. It is mutually exclusive with VlogDir (AkiPath wins). Opt-in:
	// without it the runtime keeps the scratch-vlog path unchanged.
	AkiPath string

	// ResidentCapBytes is each shard's resident byte budget; past it a
	// separated or chunked value's bytes spill to the shard's log. 0 means
	// uncapped.
	ResidentCapBytes uint64

	// PinWorkers locks each worker goroutine to an OS thread. Off by default:
	// the single-owner invariant is goroutine affinity and needs no thread,
	// and the labs/f3/m0/11_transport sweep measured the lock as a net loss
	// through the locked-M park/unpark handoff. The knob stays for boxes
	// where thread residency measurably pays.
	PinWorkers bool

	// BatchDataCap, RepCap, ReplyRing, and FreeListCap override the
	// per-connection hop-transport sizes (tuning.go batchDataCap, its reply
	// headroom, replyRing, freeListCap); each non-positive field takes its
	// tuning.go default. They are the M0 memory-bar lever swept by
	// labs/f3/m0/25_conn_caps and 27_rep_headroom: at high fan-out the pooled
	// hopBatch buffers and the reorder ring dominate resident footprint.
	// BatchDataCap starts a node's data buffer, and it grows on demand for a
	// larger command, so a smaller start only trims the steady small-value path.
	// RepCap starts a node's reply buffer independently; it also grows on
	// demand, so a write-heavy load (SET replies are +OK) never pays the
	// batchDataCap+64*batchCap default headroom, and a reply-heavy node grows
	// once and keeps the buffer (up to keepNodeBytes) with no steady cost.
	BatchDataCap int
	RepCap       int
	ReplyRing    int
	FreeListCap  int
}

// Open is New with the value-log configuration: each shard gets its own log
// file so the single-owner contract extends to the disk tier. With Config.AkiPath
// set the whole runtime shares one durable .aki file instead (openAkiStores).
func Open(cfg Config) (*Runtime, error) {
	if cfg.Shards < 1 {
		cfg.Shards = 1
	}
	r := &Runtime{workers: make([]*worker, cfg.Shards)}
	r.resolveConnCaps(cfg)
	if cfg.AkiPath != "" {
		if err := r.openAkiStores(cfg); err != nil {
			return nil, err
		}
		return r, nil
	}
	for i := range r.workers {
		o := store.Options{ArenaBytes: cfg.ArenaBytes, SegBytes: cfg.SegBytes}
		if cfg.VlogDir != "" {
			o.VlogPath = filepath.Join(cfg.VlogDir, fmt.Sprintf("shard-%03d.vlog", i))
			o.ResidentCapBytes = cfg.ResidentCapBytes
		}
		st, err := store.Open(o)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = r.workers[j].st.Close()
			}
			return nil, err
		}
		r.workers[i] = newWorker(i, st)
		r.workers[i].rt = r
		r.workers[i].pin = cfg.PinWorkers
	}
	return r, nil
}

// openAkiStores builds the durable runtime: it opens or creates the one shared .aki
// file, stands up the one group-commit writer over it, and opens every shard's store
// borrowing that file and that writer. An existing file is recovered before any store
// serves a command, so a restart rebuilds each shard's index from the durable record
// log; a freshly created file has nothing to recover. On any failure it unwinds what
// it built (stores, then the writer, then the file) so a half-open runtime never
// leaks the file handle or the writer goroutine.
func (r *Runtime) openAkiStores(cfg Config) error {
	f, existed, err := openOrCreateAki(cfg.AkiPath, len(r.workers))
	if err != nil {
		return err
	}
	r.aki = f
	r.gw = akifile.NewGroupWriter(f, len(r.workers), akiWriterRing)

	var rec *akifile.Recovery
	if existed {
		if rec, err = f.Recover(); err != nil {
			r.gw.Stop()
			_ = f.Close()
			return fmt.Errorf("shard: recover %s: %w", cfg.AkiPath, err)
		}
	}
	now := time.Now().UnixMilli()
	for i := range r.workers {
		st, err := store.Open(store.Options{
			ArenaBytes:       cfg.ArenaBytes,
			SegBytes:         cfg.SegBytes,
			AkiValueLog:      f,
			Shard:            uint16(i),
			AkiGroupWriter:   r.gw,
			ResidentCapBytes: cfg.ResidentCapBytes,
		})
		if err == nil && rec != nil {
			err = st.RecoverIndex(rec, now)
		}
		if err != nil {
			if st != nil {
				_ = st.Close()
			}
			for j := 0; j < i; j++ {
				_ = r.workers[j].st.Close()
			}
			r.gw.Stop()
			_ = f.Close()
			return fmt.Errorf("shard: open shard %d over %s: %w", i, cfg.AkiPath, err)
		}
		r.workers[i] = newWorker(i, st)
		r.workers[i].rt = r
		r.workers[i].pin = cfg.PinWorkers
	}
	return nil
}

// openOrCreateAki opens the shared .aki at path, or creates it fresh when it does
// not exist yet. It returns the file, whether it already existed (so the caller
// knows to recover), and any error. A path that exists but does not open (a torn or
// wrong-format file) is a hard error, not a silent re-create, so a damaged file is
// never overwritten.
func openOrCreateAki(path string, shards int) (*akifile.File, bool, error) {
	f, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncEverySec})
	if err == nil {
		return f, true, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, false, fmt.Errorf("shard: open %s: %w", path, err)
	}
	f, err = akifile.Create(path, akifile.CreateOptions{
		ShardCount:   uint32(shards),
		SepThreshold: akiSepThreshold,
		Sync:         akifile.SyncEverySec,
	})
	if err != nil {
		return nil, false, fmt.Errorf("shard: create %s: %w", path, err)
	}
	return f, false, nil
}

// Use registers the op-indexed handler table on every worker: the handler for
// op b sits at index b. Index 0 and OpError are reserved. Use must run before
// Start; the table is fixed for the runtime's life so the owner loop reads it
// with plain loads.
func (r *Runtime) Use(handlers []Handler) {
	if r.started {
		panic("shard: Use after Start")
	}
	for _, w := range r.workers {
		w.handlers = handlers
	}
}

// UseDemoter registers the collection-demotion hook every worker's demote loop
// calls at a boundary under memory pressure (spec 2064/f3/06 section 6). The
// server layer, which imports the collection packages the shard cannot, passes
// the type's demote entry (set.DemoteQuantum); a string-only runtime leaves the
// hook nil and the loop skips it. Fixed before Start like Use, so the worker reads
// it with no synchronization on the hot path.
func (r *Runtime) UseDemoter(fn func(*Ctx) int) {
	if r.started {
		panic("shard: UseDemoter after Start")
	}
	for _, w := range r.workers {
		w.demoteColl = fn
	}
}

// Shards reports the shard count.
func (r *Runtime) Shards() int { return len(r.workers) }

// ShardOf routes a key to its owner: wyhash mod S, the hash computed once and
// shared with the owner's index probe. The CRC16 slot table with hash-tag
// semantics (doc 03 section 2.1) replaces this route when the multi-key
// slices need slot-honest co-location; nothing below the route decision sees
// the difference.
func (r *Runtime) ShardOf(key []byte) int {
	return int(store.Hash(key) % uint64(len(r.workers)))
}

// Start launches every worker goroutine.
func (r *Runtime) Start() {
	if r.started {
		return
	}
	r.started = true
	for _, w := range r.workers {
		go w.run()
	}
}

// Stop halts every worker after it drains what its queue already holds, waits for
// the goroutines to exit, and releases the durable resources Open acquired. The
// worker shutdown runs only for a started runtime, but the release always runs:
// Open stands up the shared writer goroutine and the file eagerly, so an
// Open-without-Start still has to hand them back or they leak.
func (r *Runtime) Stop() {
	if r.started {
		r.started = false
		for _, w := range r.workers {
			w.stop.Store(true)
		}
		for _, w := range r.workers {
			w.wk.wake()
		}
		for _, w := range r.workers {
			<-w.done
		}
		// The owners are gone, so no more drains can be submitted: shut each shard's
		// I/O worker down (a no-op on a shard that never drained). This joins before
		// the store close so an in-flight pwrite finishes against a live file.
		for _, w := range r.workers {
			w.io.stop()
		}
	}
	// On the shared-.aki path the runtime owns the one writer and the one file. Join
	// the writer first, before any store closes: the owners have quiesced so no Submit
	// can race the drain, and the join commits whatever was queued one last time. Once
	// it returns this goroutine alone touches the file's single-writer append cursor,
	// which the clean-shutdown checkpoint below appends against directly.
	if r.gw != nil {
		r.gw.Stop()
	}
	// With the writer joined and the stores still live, take a clean-shutdown
	// checkpoint so the next open recovers through the bounded checkpoint-plus-tail
	// path instead of walking every record ever logged. Best-effort: every record is
	// already durable, so a checkpoint error just sends the next open down the full
	// walk, which rebuilds the same index.
	if r.aki != nil {
		_ = r.checkpointOnStop(uint64(time.Now().Unix()))
	}
	// Release each store's own value log. On the shared path this closes only the
	// store's scratch, not the borrowed file, which the runtime closes last.
	for _, w := range r.workers {
		if w != nil {
			_ = w.st.Close()
		}
	}
	if r.aki != nil {
		_ = r.aki.Close()
	}
}

// checkpointOnStop writes a clean-shutdown index checkpoint for every shard and
// commits them as the file's live root in one meta flip. It runs only on the
// shared-.aki path and only after the group writer has joined, so its direct
// checkpoint appends never race the writer for the file's single-writer append
// cursor. Each shard's row names the dump the recovery fast path reads and the tail
// it replays after; the aggregated stats seed a reopen's compaction with the record
// region's live and dead bytes without a rescan. A returned error is not fatal to the
// shutdown: the records are durable regardless, so a missing checkpoint only sends the
// next open down the full-log walk.
func (r *Runtime) checkpointOnStop(nowUnix uint64) error {
	rows := make([]akifile.SRTRow, len(r.workers))
	var stats akifile.CheckpointStats
	for i, w := range r.workers {
		row, err := w.st.WriteIndexCheckpoint()
		if err != nil {
			return err
		}
		rows[i] = row
		total, dead := w.st.RecordLogBytes()
		stats.LiveBytes += total - dead
		stats.DeadBytes += dead
		stats.RecordCount += row.LiveRecords
	}
	stats.LastCkptUnix = nowUnix
	stats.Clean = true
	return store.CommitCheckpoint(r.aki, rows, stats)
}
