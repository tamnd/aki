// The manifest publisher (spec 2064/obs1 doc 03 section 6, doc 06 section
// 1.5): after every fold it states the group's complete truth as the next
// manifest in the dense CAS sequence. The fold cursor it writes has two
// parts: FoldSeq is the highest frame seq the segments cover, taken from
// the fold's eligibility mark, and FoldPos is the chain position of the
// first commit verdict whose section reaches that seq, learned by
// listening to the lease fold's verdict stream. The verdict feed must run
// before Watermarks.ApplyVerdict so the covering position is on file
// before the watermark releases the fold's publish gate.
package obs1

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// ManPubConfig configures one node's ManifestPublisher.
type ManPubConfig struct {
	// Store, Prefix, and Node are the PUT target and identity, the same
	// values the node's folder carries.
	Store  Store
	Prefix string
	Node   uint64

	// OnManifest, when set, hears every manifest as its PUT lands. Called
	// off the verdict and fold goroutines.
	OnManifest func(Manifest)

	// Seed carries each group's winning manifest from boot recovery, so
	// publishing continues the dense sequence and keeps every live row. At
	// most one manifest per group.
	Seed []Manifest
}

// ManPubStats counts the publisher's work for tests and the INFO surface.
type ManPubStats struct {
	Published  uint64
	PutRetries uint64
	SlotSkips  uint64 // man slots held by another writer, advanced past
	CoverMiss  uint64 // folds ingested before any verdict covered their seq
	RowErrs    uint64 // folded segments dropped for a non-increasing SegSeq
	BuildErrs  uint64
}

// covEntry is one live commit section's coverage fact: frames through last
// were committed by the verdict at pos.
type covEntry struct {
	last uint64
	pos  ChainPos
}

// manGroup is one group's publisher state. cover is the verdict trail in
// chain order, pruned below the fold cursor as it advances; segs is the
// live-row list the next manifest will carry.
type manGroup struct {
	epoch   uint32
	manSeq  uint64 // next slot in the dense sequence
	foldSeq uint64
	foldPos ChainPos
	segs    []ManifestSeg
	cover   []covEntry
	dirty   bool // a manifest PUT is owed and queued
}

// ManifestPublisher turns folded segments into manifests. OnVerdict runs
// on the chain fold goroutine and OnFolded on the folder's publish path,
// both under one mutex; encoding and PUTs run on the publisher's own
// goroutine, one manifest at a time, so a burst of folds that lands while
// a PUT is in flight coalesces into the next manifest.
type ManifestPublisher struct {
	cfg ManPubConfig

	mu     sync.Mutex
	cond   *sync.Cond
	groups map[uint16]*manGroup
	queue  []uint16
	stats  ManPubStats

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// NewManifestPublisher builds and starts a ManifestPublisher.
func NewManifestPublisher(cfg ManPubConfig) (*ManifestPublisher, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("obs1: ManPubConfig needs a Store")
	}
	p := &ManifestPublisher{cfg: cfg, groups: make(map[uint16]*manGroup)}
	for _, m := range cfg.Seed {
		if _, ok := p.groups[m.Group]; ok {
			return nil, fmt.Errorf("obs1: two seed manifests for group %d", m.Group)
		}
		p.groups[m.Group] = &manGroup{
			epoch: m.Epoch, manSeq: m.ManSeq + 1,
			foldSeq: m.FoldSeq, foldPos: m.FoldPos,
			segs: append([]ManifestSeg(nil), m.Segs...),
		}
	}
	p.cond = sync.NewCond(&p.mu)
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.done = make(chan struct{})
	go p.run()
	return p, nil
}

// OnVerdict records each live section's coverage fact. Wire it into the
// lease fold's OnCommit in front of Watermarks.ApplyVerdict; the error is
// always nil and the signature matches so the two calls chain.
func (p *ManifestPublisher) OnVerdict(v CommitVerdict) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, s := range v.Commit.Sections {
		if !v.Live[i] {
			continue
		}
		g := p.groupFor(s.Group)
		if s.LastSeq < g.foldSeq {
			continue // a future fold cursor can only be higher
		}
		if n := len(g.cover); n > 0 && s.LastSeq <= g.cover[n-1].last {
			continue
		}
		g.cover = append(g.cover, covEntry{last: s.LastSeq, pos: v.Pos})
	}
	return nil
}

// OnFolded ingests one published segment, the Folder's OnPublish target:
// it appends the ledger row, advances the fold cursor to the segment's
// covered seq, resolves the covering chain position from the verdict
// trail, and queues the group for its next manifest.
func (p *ManifestPublisher) OnFolded(seg FoldedSegment) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g := p.groupFor(seg.Group)
	if n := len(g.segs); n > 0 && seg.SegSeq <= g.segs[n-1].SegSeq {
		p.stats.RowErrs++
		return
	}
	g.epoch = seg.Epoch
	g.segs = append(g.segs, ManifestSeg{
		SegSeq: seg.SegSeq, Level: 0,
		// Cold frames carry no expiry word yet, so level-0 rows are TTL
		// class 0 with zero bounds (doc 03 section 5.1, recorded gap).
		TTLClass: 0,
		Size:     uint64(seg.Size), NRecords: seg.NRecords, RawBytes: seg.RawBytes,
		FooterOff: seg.FooterOff, FooterLen: seg.FooterLen,
	})
	if seg.CoveredSeq > g.foldSeq {
		g.foldSeq = seg.CoveredSeq
	}
	for len(g.cover) > 0 && g.cover[0].last < g.foldSeq {
		g.cover = g.cover[1:]
	}
	if len(g.cover) > 0 {
		g.foldPos = g.cover[0].pos
	} else if g.foldSeq > 0 {
		// The verdict feed is wired in front of ApplyVerdict, so the
		// covering verdict is on file before the publish gate opens; a miss
		// means a wiring bug, kept visible here, and the stale FoldPos is
		// still safe because FoldSeq alone bounds what replay applies.
		p.stats.CoverMiss++
	}
	if !g.dirty {
		g.dirty = true
		p.queue = append(p.queue, seg.Group)
		p.cond.Signal()
	}
}

// groupFor returns the group's state, created on first touch.
func (p *ManifestPublisher) groupFor(group uint16) *manGroup {
	g := p.groups[group]
	if g == nil {
		g = &manGroup{}
		p.groups[group] = g
	}
	return g
}

// Stats returns a snapshot of the publisher's counters.
func (p *ManifestPublisher) Stats() ManPubStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// Close stops the publisher. A queued manifest not yet PUT is abandoned:
// its segments are durable and the WAL retains everything above the last
// published cursor, so the next incarnation folds and states it again.
func (p *ManifestPublisher) Close() {
	p.cancel()
	p.mu.Lock()
	p.cond.Broadcast()
	p.mu.Unlock()
	<-p.done
}

// run is the publisher's off-owner half: one manifest PUT at a time, in
// queue order.
func (p *ManifestPublisher) run() {
	defer close(p.done)
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && p.ctx.Err() == nil {
			p.cond.Wait()
		}
		if p.ctx.Err() != nil {
			p.mu.Unlock()
			return
		}
		group := p.queue[0]
		p.queue = p.queue[1:]
		g := p.groups[group]
		g.dirty = false
		m := Manifest{
			Group: group, Epoch: g.epoch, ManSeq: g.manSeq,
			FoldPos: g.foldPos, FoldSeq: g.foldSeq,
			Segs: append([]ManifestSeg(nil), g.segs...),
		}
		p.mu.Unlock()
		p.putManifest(m)
	}
}

// putManifest encodes and PUTs one manifest, the folder's retry shape on
// the dense-sequence key. RecheckOther on a man slot means a prior
// incarnation of this group's folder landed that seq, so the publisher
// restates the same truth at the next slot; the boot recovery slice seeds
// the counter from the winning manifest instead. The slot is written back
// only on success, and PUTs run serially on the one goroutine, so no two
// manifests can alias a slot.
func (p *ManifestPublisher) putManifest(m Manifest) {
	backoff := foldRetryBase
	for {
		obj, err := AppendManifest(nil, p.cfg.Node, m)
		if err != nil {
			p.mu.Lock()
			p.stats.BuildErrs++
			p.mu.Unlock()
			return
		}
		key := manifestKey(p.cfg.Prefix, m.Group, m.ManSeq)
		tag := WriteTag{Writer: fmt.Sprintf("%016x", p.cfg.Node), Batch: seq16(m.ManSeq)}
		_, perr := p.cfg.Store.PutIfAbsent(p.ctx, key, obj, tag)
		if perr == nil {
			p.finishPut(m)
			return
		}
		if p.ctx.Err() != nil {
			return
		}
		if isCASRace(perr) {
			out, _, _, rerr := p.cfg.Store.Recheck(p.ctx, key, tag)
			switch {
			case rerr != nil:
				if p.ctx.Err() != nil {
					return
				}
			case out == RecheckOurs:
				p.finishPut(m)
				return
			case out == RecheckOther:
				m.ManSeq++
				p.mu.Lock()
				p.stats.SlotSkips++
				p.mu.Unlock()
				continue
			}
			// RecheckAbsent: the PUT never landed, same bytes go again.
		}
		p.mu.Lock()
		p.stats.PutRetries++
		p.mu.Unlock()
		sleep := backoff/2 + rand.N(backoff/2+1)
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(sleep):
		}
		if backoff *= 2; backoff > foldRetryCap {
			backoff = foldRetryCap
		}
	}
}

// finishPut books a landed manifest and advances the group's slot.
func (p *ManifestPublisher) finishPut(m Manifest) {
	p.mu.Lock()
	p.groups[m.Group].manSeq = m.ManSeq + 1
	p.stats.Published++
	cb := p.cfg.OnManifest
	p.mu.Unlock()
	if cb != nil {
		cb(m)
	}
}
