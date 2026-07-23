// The fold pass (spec 2064/obs1 doc 06 section 1): packing cooled data out
// of RAM into immutable bucket segments. The Folder never re-reads WAL
// objects; its input is the store's staged cold drains, delivered through
// the fold tap on the shard owner's goroutine, which is the #1111 finding
// made structural: selection and pacing are the stage machinery's SIEVE
// hand and budgets, and the folder adds one frame-serialization copy on
// the owner plus block building and PUTs off it.
//
// Eligibility (doc 06 section 1.2) needs no per-record seq: the tap fires
// on the goroutine that emits the group's WAL frames, so the group's last
// emitted seq at tap time covers every mutation the staged frames reflect.
// The segment PUTs eagerly and publishes to the ledger only once the
// committed watermark reaches that seq; a record mutated after staging has
// a later seq, so replay above the fold cursor shadows the stale copy the
// segment holds.
package obs1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/obs1/store"
)

const (
	// foldSegTarget is the level-0 segment cut threshold (doc 06 section
	// 1.4): an accumulator past it closes its segment on the next tap.
	foldSegTarget = 64 << 20

	// foldChunkTarget is the run-chunk payload target, the middle of the
	// doc 08 section 1 4-32KiB band.
	foldChunkTarget = 16 << 10

	// foldAgeDefault is the age trigger's bound (doc 06 section 1.4): an
	// accumulator holding bytes older than this cuts on the next cadence
	// tick even when it never reaches the segment target.
	foldAgeDefault = 15 * time.Second

	// The PUT retry backoff, the flusher's shape on the same store.
	foldRetryBase = 20 * time.Millisecond
	foldRetryCap  = time.Second
)

// FoldConfig configures one node's Folder.
type FoldConfig struct {
	// Store, Prefix, and Node are the PUT target and identity, the same
	// values the node's flusher carries.
	Store  Store
	Prefix string
	Node   uint64

	// MapKey maps a key to its hash slot and group, the dispatcher's route.
	MapKey func(key []byte) (slot uint16, group uint16)

	// Mark is the eligibility snapshot, WriteLog.GroupMark: the group's
	// lease epoch and last emitted seq, read on the owner goroutine that
	// is feeding the tap.
	Mark func(group uint16) (epoch uint32, last uint64)

	// Marks is the committed-watermark surface the publish gate parks on,
	// WriteLog.Marks().
	Marks *Watermarks

	// OnPublish, when set, hears every segment as it publishes, after the
	// watermark covers it. Called off the owner; the manifest slice's seam.
	OnPublish func(FoldedSegment)

	// Keymap, when set, resolves a group's resident cold-key index. The
	// folder maintains it under its own mutex: publish applies each
	// surviving record placement, Delete drops the key at apply time, and
	// serializing both under one lock is what closes the delete-versus-
	// publish race on a key whose stale copy sits in a cut segment. Nil
	// keeps the pre-keymap behavior: no index, unfiltered tombstones.
	Keymap func(group uint16) *Keymap

	// SegTargetBytes and ChunkTargetBytes override the cut thresholds,
	// zero for the defaults. Tests shrink them.
	SegTargetBytes   int
	ChunkTargetBytes int

	// FoldAge overrides the age trigger's bound, zero for the 15s doc 06
	// default; negative disables the cadence for tests that cut explicitly.
	FoldAge time.Duration

	// Seed carries each group's winning manifest from boot recovery, so
	// SegSeq continues past every published slot instead of restarting at
	// one and colliding with live rows. At most one manifest per group.
	Seed []Manifest
}

// FoldedSegment is one ledger row: a segment durably in the bucket whose
// covered seq the committed watermark has reached. FooterOff and
// FooterLen are the #1102 manifest fields: a cold open ranged-GETs the
// footer directly instead of walking from the tail.
type FoldedSegment struct {
	Group      uint16
	Epoch      uint32
	SegSeq     uint64
	Key        string
	Size       int64
	FooterOff  uint64
	FooterLen  uint32
	NRecords   uint64
	RawBytes   uint64
	CoveredSeq uint64
	// Places lists every whole-record frame's chunk placement, the
	// keymap's feed, minus placements killed by a delete that landed
	// between the cut and the publish.
	Places []KeyPlace
}

// KeyPlace is one record's landing spot inside its published segment:
// the keymap maintenance unit (doc 05 section 2.1).
type KeyPlace struct {
	Key       []byte
	Fp        uint64
	Chunk     uint32
	Kind      byte
	Tombstone bool
}

// FolderStats counts the folder's work for tests and the INFO surface.
type FolderStats struct {
	Records        uint64 // whole-record frames accumulated
	Chunks         uint64 // collection chunk frames accumulated
	Tombstones     uint64 // delete claims accumulated
	Replaced       uint64 // accumulated entries displaced by a newer state for the same key
	PointerSkipped uint64 // pointer-band frames left to the separated-band route
	NoEpoch        uint64 // frames dropped for want of a group epoch
	SegmentsCut    uint64
	SegmentsPut    uint64
	Published      uint64
	PutRetries     uint64
	WalkErrs       uint64
	BuildErrs      uint64

	TombstonesSkipped uint64 // deletes of keys with no cold copy anywhere: no tombstone emitted
	PlacesApplied     uint64 // record placements applied to the keymap at publish
	PlacesKilled      uint64 // placements dropped because a delete landed after the cut
	PlaceErrs         uint64 // placements the keymap refused, each one a loud bug
}

// foldRec is one accumulated whole-record frame, copied out of the tap
// buffer. fp is the key fingerprint (the bloom's first hash), the run
// order doc 08 section 2 packs by.
type foldRec struct {
	kind  byte
	fp    uint64
	key   []byte
	frame []byte
}

// foldGroup is one group's accumulator. recs and chunks are deduped by
// identity so a record staged twice (a failed pwrite re-stages it later)
// folds once, at its newest state.
type foldGroup struct {
	epoch   uint32
	seq     uint64 // next SegSeq
	covered uint64 // max eligibility mark over contributors
	bytes   int
	since   time.Time // when the accumulator went non-empty, zero while empty
	recs    []foldRec
	recIdx  map[string]int
	chunks  []SegmentChunk
	chIdx   map[string]int
}

// segJob is one cut segment on its way to the bucket. keys is the
// accumulator's retired index, kept as a membership set so a delete
// landing after the cut can find the job; kills collects those deletes,
// and the publish filters places by them under the folder's mutex.
// places is written by buildSegment on the putter goroutine and read at
// publish; kills is the only field two goroutines share.
type segJob struct {
	group   uint16
	epoch   uint32
	seq     uint64
	covered uint64
	recs    []foldRec
	chunks  []SegmentChunk
	keys    map[string]int
	kills   map[string]struct{}
	places  []KeyPlace
}

// Folder packs staged cold frames into segments and PUTs them. Add runs
// on shard owner goroutines under one mutex (drains are boundary-paced,
// so contention is a non-event); building and PUTs run on the folder's own
// goroutine.
type Folder struct {
	cfg       FoldConfig
	segTarget int
	chTarget  int

	mu      sync.Mutex
	cond    *sync.Cond
	groups  map[uint16]*foldGroup
	queue   []*segJob
	pending []*segJob // cut but not yet published or abandoned: Delete's kill targets
	ledger  []FoldedSegment
	stats   FolderStats

	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	cadDone chan struct{} // nil when the age cadence is disabled
}

// NewFolder builds and starts a Folder.
func NewFolder(cfg FoldConfig) (*Folder, error) {
	if cfg.Store == nil || cfg.MapKey == nil || cfg.Mark == nil || cfg.Marks == nil {
		return nil, fmt.Errorf("obs1: FoldConfig needs Store, MapKey, Mark, and Marks")
	}
	f := &Folder{
		cfg:       cfg,
		segTarget: cfg.SegTargetBytes,
		chTarget:  cfg.ChunkTargetBytes,
		groups:    make(map[uint16]*foldGroup),
	}
	if f.segTarget <= 0 {
		f.segTarget = foldSegTarget
	}
	if f.chTarget <= 0 {
		f.chTarget = foldChunkTarget
	}
	for _, m := range cfg.Seed {
		if _, ok := f.groups[m.Group]; ok {
			return nil, fmt.Errorf("obs1: two seed manifests for group %d", m.Group)
		}
		seq := uint64(1)
		for _, s := range m.Segs {
			if s.SegSeq >= seq {
				seq = s.SegSeq + 1
			}
		}
		f.groups[m.Group] = &foldGroup{
			epoch: m.Epoch, seq: seq,
			recIdx: make(map[string]int), chIdx: make(map[string]int),
		}
	}
	f.cond = sync.NewCond(&f.mu)
	f.ctx, f.cancel = context.WithCancel(context.Background())
	f.done = make(chan struct{})
	go f.run()
	if age := cfg.FoldAge; age >= 0 {
		if age == 0 {
			age = foldAgeDefault
		}
		f.cadDone = make(chan struct{})
		go f.cadence(age)
	}
	return f, nil
}

// cadence is the age trigger (doc 06 section 1.4): every quarter of the age
// bound it cuts any accumulator whose oldest bytes have waited the full
// bound, so a quiet group's cooled data reaches the bucket without needing
// the size trigger or an explicit Flush.
func (f *Folder) cadence(age time.Duration) {
	defer close(f.cadDone)
	tick := max(age/4, time.Millisecond)
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		f.mu.Lock()
		for group, g := range f.groups {
			if !g.since.IsZero() && now.Sub(g.since) >= age {
				f.cutLocked(group, g)
			}
		}
		f.mu.Unlock()
	}
}

// Add ingests one staged drain buffer, the fold tap's target: it walks the
// frames, routes each to its group's accumulator with the eligibility mark
// taken now, and cuts any accumulator past the segment target. Pointer-band
// frames are skipped and counted; their bytes live in the local value log
// and fold by their own route in a later slice (#1111). The buffer is the
// store's and recycles after the drain, so everything kept is copied here.
func (f *Folder) Add(frames []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	touched := make(map[uint16]*foldGroup)
	err := store.WalkStagedFrames(frames, func(fr store.FoldFrame) error {
		if fr.Pointer {
			f.stats.PointerSkipped++
			return nil
		}
		_, group := f.cfg.MapKey(fr.Key)
		epoch, last := f.cfg.Mark(group)
		if epoch == 0 {
			f.stats.NoEpoch++
			return nil
		}
		g := f.groupFor(group, epoch, last)
		touched[group] = g
		if fr.Chunk {
			id := string([]byte{fr.Kind}) + string(fr.Key) + "\x00" + string(fr.Disc)
			data := append([]byte(nil), fr.Frame...)
			c := SegmentChunk{
				Key: append([]byte(nil), fr.Key...), Kind: fr.Kind,
				FirstDisc: disc64(fr.Disc), Count: fr.Count, LiveHint: fr.Count,
				Data: data,
			}
			if i, ok := g.chIdx[id]; ok {
				g.bytes += len(data) - len(g.chunks[i].Data)
				g.chunks[i] = c
				f.stats.Replaced++
			} else {
				g.chIdx[id] = len(g.chunks)
				g.chunks = append(g.chunks, c)
				g.bytes += len(data)
				f.stats.Chunks++
			}
			return nil
		}
		key := append([]byte(nil), fr.Key...)
		h1, _ := bloomHash(key)
		r := foldRec{kind: fr.Kind, fp: h1, key: key, frame: append([]byte(nil), fr.Frame...)}
		if f.putRec(g, r) {
			f.stats.Records++
		}
		return nil
	})
	if err != nil {
		f.stats.WalkErrs++
	}
	for group, g := range touched {
		if g.bytes >= f.segTarget {
			f.cutLocked(group, g)
		} else if g.bytes > 0 && g.since.IsZero() {
			f.age(g)
		}
	}
}

// age arms the group's age trigger: the oldest pending bytes' arrival time,
// kept until a cut empties the accumulator.
func (f *Folder) age(g *foldGroup) {
	g.since = time.Now()
}

// Delete handles a committed delete of key (doc 06 section 1.3). Call it
// on the owner goroutine right after the WriteLog delete emission, the
// same contract as the tap: the mark taken here then covers the delete's
// seq, and a segment carrying the tombstone publishes only once the
// watermark commits it.
//
// With a keymap configured, deletion is index-authoritative (doc 05
// section 8): the entry drops here, at apply time, and a tombstone is
// emitted only when a cold copy exists to shadow, meaning the key was in
// the keymap or rides a cut segment still in flight; those in-flight
// placements are killed so the publish cannot resurrect the key. A key
// whose only copy is still in the live accumulator just drops out of it,
// and a key with no cold presence at all skips the tombstone entirely,
// the filter this method long promised. Within the accumulating segment
// a tombstone displaces any pending copy of the key and a later re-set
// displaces the tombstone; across segments a higher SegSeq claim shadows
// every lower one. Without a keymap, emission stays unfiltered.
func (f *Folder) Delete(key []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, group := f.cfg.MapKey(key)
	epoch, last := f.cfg.Mark(group)
	if epoch == 0 {
		f.stats.NoEpoch++
		return
	}
	g := f.groupFor(group, epoch, last)
	k := append([]byte(nil), key...)
	h1, _ := bloomHash(k)

	cold := true
	if f.cfg.Keymap != nil {
		cold = false
		if km := f.cfg.Keymap(group); km != nil && km.Delete(h1) {
			cold = true
		}
		for _, job := range f.pending {
			if job.group != group {
				continue
			}
			if _, ok := job.keys[string(k)]; ok {
				job.kills[string(k)] = struct{}{}
				cold = true
			}
		}
	}
	if !cold {
		if i, ok := g.recIdx[string(k)]; ok && g.recs[i].kind != store.KindTombstone {
			f.dropRecLocked(g, i)
		}
		f.stats.TombstonesSkipped++
		return
	}

	r := foldRec{
		kind: store.KindTombstone, fp: h1, key: k,
		frame: store.AppendTombstoneFrame(nil, k),
	}
	f.putRec(g, r)
	f.stats.Tombstones++
	if g.bytes >= f.segTarget {
		f.cutLocked(group, g)
	} else if g.since.IsZero() {
		f.age(g)
	}
}

// dropRecLocked removes the accumulator's record at index i, the delete
// path for a key that never reached the bucket: nothing cold exists, so
// nothing needs shadowing and the staged copy simply leaves.
func (f *Folder) dropRecLocked(g *foldGroup, i int) {
	g.bytes -= len(g.recs[i].frame)
	delete(g.recIdx, string(g.recs[i].key))
	last := len(g.recs) - 1
	if i != last {
		g.recs[i] = g.recs[last]
		g.recIdx[string(g.recs[i].key)] = i
	}
	g.recs = g.recs[:last]
	if len(g.recs) == 0 && len(g.chunks) == 0 {
		g.since = time.Time{}
	}
}

// groupFor returns the group's accumulator, created on first touch, with
// the eligibility snapshot folded in.
func (f *Folder) groupFor(group uint16, epoch uint32, last uint64) *foldGroup {
	g := f.groups[group]
	if g == nil {
		g = &foldGroup{seq: 1, recIdx: make(map[string]int), chIdx: make(map[string]int)}
		f.groups[group] = g
	}
	g.epoch = epoch
	if last > g.covered {
		g.covered = last
	}
	return g
}

// putRec inserts one record or tombstone into the accumulator, newest state
// per key winning. It reports whether the key was fresh; a displacement
// counts as Replaced instead.
func (f *Folder) putRec(g *foldGroup, r foldRec) bool {
	if i, ok := g.recIdx[string(r.key)]; ok {
		g.bytes += len(r.frame) - len(g.recs[i].frame)
		g.recs[i] = r
		f.stats.Replaced++
		return false
	}
	g.recIdx[string(r.key)] = len(g.recs)
	g.recs = append(g.recs, r)
	g.bytes += len(r.frame)
	return true
}

// Flush cuts every non-empty accumulator now: the shutdown entry point and
// the pressure trigger's target (Runtime.SetFoldKick wires it, doc 06
// section 1.4); the age trigger runs on the folder's own cadence goroutine.
func (f *Folder) Flush() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for group, g := range f.groups {
		f.cutLocked(group, g)
	}
}

// cutLocked closes the group's accumulator into a PUT job and resets it.
func (f *Folder) cutLocked(group uint16, g *foldGroup) {
	if len(g.recs) == 0 && len(g.chunks) == 0 {
		return
	}
	job := &segJob{
		group: group, epoch: g.epoch, covered: g.covered,
		recs: g.recs, chunks: g.chunks,
		keys: g.recIdx, kills: make(map[string]struct{}),
	}
	f.queue = append(f.queue, job)
	f.pending = append(f.pending, job)
	g.covered = 0
	g.bytes = 0
	g.since = time.Time{}
	g.recs, g.chunks = nil, nil
	g.recIdx, g.chIdx = make(map[string]int), make(map[string]int)
	f.stats.SegmentsCut++
	f.cond.Signal()
}

// Ledger returns the published segments in publish order, a copy.
func (f *Folder) Ledger() []FoldedSegment {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FoldedSegment(nil), f.ledger...)
}

// Stats returns a snapshot of the folder's counters.
func (f *Folder) Stats() FolderStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

// Close stops the folder. Queued segments not yet PUT are abandoned: the
// WAL still holds their data above the fold cursor, so nothing is lost,
// and the next incarnation folds it again.
func (f *Folder) Close() {
	f.cancel()
	f.mu.Lock()
	f.cond.Broadcast()
	f.mu.Unlock()
	<-f.done
	if f.cadDone != nil {
		<-f.cadDone
	}
}

// run is the folder's off-owner half: it builds and PUTs cut segments one
// at a time, in cut order.
func (f *Folder) run() {
	defer close(f.done)
	for {
		f.mu.Lock()
		for len(f.queue) == 0 && f.ctx.Err() == nil {
			f.cond.Wait()
		}
		if f.ctx.Err() != nil {
			f.mu.Unlock()
			return
		}
		job := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()
		f.putSegment(job)
	}
}

// buildSegment packs the job into a Segment: collection chunk frames pass
// through verbatim, whole-record frames sort by (kind, fingerprint) and
// pack into homogeneous run chunks (doc 08 section 2) at the chunk target,
// and the blocks take the #1085 default size with comp 0 until the codec
// milestone (#1097).
func (f *Folder) buildSegment(job *segJob) (*Segment, error) {
	sort.Slice(job.recs, func(i, j int) bool {
		a, b := &job.recs[i], &job.recs[j]
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if a.fp != b.fp {
			return a.fp < b.fp
		}
		return string(a.key) < string(b.key)
	})
	chunks := make([]SegmentChunk, 0, len(job.chunks)+1)
	memberKeys := make([][]byte, 0, len(job.recs)+len(job.chunks))
	// The PUT retry loop rebuilds; placements must not accumulate across
	// attempts.
	job.places = job.places[:0]
	var payload []byte
	var run []foldRec
	cut := func() {
		if len(run) == 0 {
			return
		}
		first := run[0]
		var disc [8]byte
		binary.LittleEndian.PutUint64(disc[:], first.fp)
		data := store.AppendRunChunk(nil, first.kind|store.ChunkKindBit, 0,
			uint16(len(run)), first.key, disc[:], payload)
		for _, r := range run {
			job.places = append(job.places, KeyPlace{
				Key: r.key, Fp: r.fp, Chunk: uint32(len(chunks)),
				Kind: r.kind, Tombstone: r.kind == store.KindTombstone,
			})
		}
		chunks = append(chunks, SegmentChunk{
			Key: first.key, Kind: first.kind | store.ChunkKindBit,
			FirstDisc: first.fp, Count: uint16(len(run)), LiveHint: uint16(len(run)),
			Data: data,
		})
		payload, run = nil, nil
	}
	for i := range job.recs {
		r := &job.recs[i]
		if len(run) > 0 && (run[0].kind != r.kind || len(payload) >= f.chTarget || len(run) == 0xFFFF) {
			cut()
		}
		run = append(run, *r)
		payload = append(payload, r.frame...)
		memberKeys = append(memberKeys, r.key)
	}
	cut()
	for i := range job.chunks {
		memberKeys = append(memberKeys, job.chunks[i].Key)
	}
	chunks = append(chunks, job.chunks...)
	footer := SegmentFooter{
		Group: job.group, Epoch: job.epoch, SegSeq: job.seq, Level: 0,
		// Cold frames carry no expiry word, so level-0 segments are TTL
		// class 0 until the fold route carries deadlines (doc 03 section
		// 5.1, recorded gap).
		TTLClass: 0,
	}
	return BuildSegment(footer, chunks, memberKeys, 0)
}

// putSegment encodes and PUTs one job, the flusher's retry shape on a
// CAS-create key. RecheckOther on seg/<group>/<seq> is not fencing failure
// here: a prior incarnation of this group's folder can have landed that
// seq, so the folder advances to the next slot; the boot recovery slice
// seeds the counter from the manifest instead. The seq is drawn from the
// group counter here, not at cut time, and written back only on success:
// jobs run serially on the one putter goroutine, so no two segments can
// alias a slot, which the ambiguity recheck relies on (a stable per-slot
// batch tag must never name two different segments).
func (f *Folder) putSegment(job *segJob) {
	f.mu.Lock()
	job.seq = f.groups[job.group].seq
	f.mu.Unlock()
	backoff := foldRetryBase
	for {
		seg, err := f.buildSegment(job)
		if err == nil && len(seg.Footer.Chunks) == 0 {
			err = fmt.Errorf("obs1: fold cut an empty segment")
		}
		if err != nil {
			f.mu.Lock()
			f.stats.BuildErrs++
			f.dropPendingLocked(job)
			f.mu.Unlock()
			return
		}
		obj, err := AppendSegment(nil, f.cfg.Node, seg)
		if err != nil {
			f.mu.Lock()
			f.stats.BuildErrs++
			f.dropPendingLocked(job)
			f.mu.Unlock()
			return
		}
		key := segKey(f.cfg.Prefix, job.group, job.seq)
		tag := WriteTag{
			Writer: fmt.Sprintf("%016x", f.cfg.Node),
			Batch:  fmt.Sprintf("seg-%03d-%s", job.group, seq16(job.seq)),
		}
		_, perr := f.cfg.Store.PutIfAbsent(f.ctx, key, obj, tag)
		if perr == nil {
			f.finishPut(job, obj, key)
			return
		}
		if f.ctx.Err() != nil {
			return
		}
		if isCASRace(perr) {
			out, _, _, rerr := f.cfg.Store.Recheck(f.ctx, key, tag)
			switch {
			case rerr != nil:
				if f.ctx.Err() != nil {
					return
				}
			case out == RecheckOurs:
				f.finishPut(job, obj, key)
				return
			case out == RecheckOther:
				job.seq++
				continue
			}
			// RecheckAbsent: the PUT never landed, same bytes go again.
		}
		f.mu.Lock()
		f.stats.PutRetries++
		f.mu.Unlock()
		sleep := backoff/2 + rand.N(backoff/2+1)
		select {
		case <-f.ctx.Done():
			return
		case <-time.After(sleep):
		}
		if backoff *= 2; backoff > foldRetryCap {
			backoff = foldRetryCap
		}
	}
}

// finishPut books a durable segment and arms its publish gate: the ledger
// row appears once the committed watermark covers the job's mark.
func (f *Folder) finishPut(job *segJob, obj []byte, key string) {
	footerOff, footerLen, err := ParseTail(obj[len(obj)-TailSize:])
	var footer SegmentFooter
	if err == nil {
		footer, err = ParseSegmentFooter(obj[footerOff : uint64(len(obj))-TailSize])
	}
	if err != nil {
		// Unreachable for bytes AppendSegment just built; count and drop.
		f.mu.Lock()
		f.stats.BuildErrs++
		f.dropPendingLocked(job)
		f.mu.Unlock()
		return
	}
	entry := FoldedSegment{
		Group: job.group, Epoch: job.epoch, SegSeq: job.seq, Key: key,
		Size: int64(len(obj)), FooterOff: footerOff, FooterLen: footerLen,
		NRecords: footer.NRecords, RawBytes: footer.RawBytes, CoveredSeq: job.covered,
	}
	f.mu.Lock()
	f.groups[job.group].seq = job.seq + 1
	f.stats.SegmentsPut++
	f.mu.Unlock()
	f.cfg.Marks.Notify(job.group, job.covered, func() {
		f.mu.Lock()
		entry.Places = f.applyPlacesLocked(job)
		f.dropPendingLocked(job)
		f.ledger = append(f.ledger, entry)
		f.stats.Published++
		cb := f.cfg.OnPublish
		f.mu.Unlock()
		if cb != nil {
			cb(entry)
		}
	})
}

// applyPlacesLocked filters the job's placements by its kill set and
// applies the survivors to the group's keymap, all under the folder's
// mutex so a racing Delete either killed the placement here or removes
// the entry it just produced; there is no in-between. Tombstone
// placements survive into the ledger row for the rebuild story but
// never touch the live keymap: the apply-time delete already did.
func (f *Folder) applyPlacesLocked(job *segJob) []KeyPlace {
	if len(job.kills) == 0 && f.cfg.Keymap == nil {
		return job.places
	}
	var km *Keymap
	if f.cfg.Keymap != nil {
		km = f.cfg.Keymap(job.group)
	}
	kept := job.places[:0]
	for _, p := range job.places {
		if _, dead := job.kills[string(p.Key)]; dead {
			f.stats.PlacesKilled++
			continue
		}
		kept = append(kept, p)
		if km == nil || p.Tombstone {
			continue
		}
		if job.seq > 1<<32-1 {
			f.stats.PlaceErrs++
			continue
		}
		l := KeyLoc{Seg: uint32(job.seq), Chunk: p.Chunk}
		if err := km.Shadow(p.Fp, l, false); err != nil {
			f.stats.PlaceErrs++
			continue
		}
		f.stats.PlacesApplied++
	}
	return kept
}

// dropPendingLocked retires a job from the kill-target list, at publish
// or on a build failure.
func (f *Folder) dropPendingLocked(job *segJob) {
	for i, j := range f.pending {
		if j == job {
			f.pending = append(f.pending[:i], f.pending[i+1:]...)
			return
		}
	}
}

// disc64 lifts a chunk discriminator's leading bytes into the footer's
// u64 FirstDisc, little-endian, zero-padded when shorter.
func disc64(disc []byte) uint64 {
	var b [8]byte
	copy(b[:], disc)
	return binary.LittleEndian.Uint64(b[:])
}

// segKey names a segment object (doc 03 section 1): per-group prefixes so
// cold-read throughput scales with group count.
func segKey(prefix string, group uint16, seq uint64) string {
	return dbKey(prefix, fmt.Sprintf("seg/g%03d/%s", group, seq16(seq)))
}

// isCASRace reports the PUT outcomes that warrant a recheck rather than a
// blind retry, the append.go taxonomy.
func isCASRace(err error) bool {
	return errors.Is(err, ErrPrecondition) || errors.Is(err, ErrConflict) || errors.Is(err, ErrAmbiguous)
}
