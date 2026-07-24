// Graceful handoff (spec 2064/obs1 doc 02 section 4.3): moving a group
// from holder to taker without moving a byte of key data. The holder's
// half is LeaseManager.Handoff, steps 2 through 4: stop serving, final
// flush, and the release record in the same chain batch as the flush's
// commit. The taker's half is TakeGroup, step 6: rebuild the resident
// structures from the winning manifest (behind a bounded prefetch fan,
// the handoff-time lab's baked constant) and replay the WAL frames the
// fold has not folded yet, located by the commit records the taker's own
// chain follow already retained in a TailWindow.
//
// The TailWindow is why step 6 needs no chain re-walk: every node follows
// the chain anyway (doc 02 section 4.2), so the live commit sections
// between the last checkpoint and the tail are a byproduct of reading,
// records only and never WAL bytes, the same window the chain trimmer
// keeps. A checkpoint record folding through trims the retained window
// the way it trims the chain.
package obs1

import (
	"context"
	"fmt"
	"sync"
)

// DefaultHandoffFan is the taker's segment prefetch width, the
// handoff-time lab's verdict (#1349): serial RebuildResident pays 30ms
// per segment GET, a fan of 8 restores a 256-segment rebuild to about
// one second with identical rebuilt stats.
const DefaultHandoffFan = 8

// Handoff sheds a group gracefully, the holder's steps 2 through 4. The
// gate stops serving the group first, so new writes park or redirect
// while in-flight ones finish; flush then runs the final flush under our
// still-valid epoch and returns the commit records to publish; the
// release lands in the same chain batch as those commits, ordered after
// them, one chain object for step 4. A nil flush means the group had
// nothing buffered. If the append fails the group stays ours on the
// chain but suspended at the gate, the safe side, and the caller
// retries.
func (m *LeaseManager) Handoff(ctx context.Context, group uint16, flush func(ctx context.Context) ([]ChainRecord, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	epoch, ok := m.held[group]
	if !ok {
		return nil
	}
	m.gate.Release(group)
	var recs []ChainRecord
	if flush != nil {
		rs, err := flush(ctx)
		if err != nil {
			return err
		}
		recs = rs
	}
	recs = append(recs, ReleaseRecord{Group: group, Epoch: epoch})
	t := m.now()
	if _, err := m.ap.Append(ctx, recs); err != nil {
		return err
	}
	delete(m.held, group)
	m.lastAppend = t
	m.reconcileLocked(t)
	return nil
}

// TailSection is one live commit section the window retained, with the
// WAL object identity that plans its ranged GET.
type TailSection struct {
	Pos     ChainPos
	WALNode uint64
	WALSeq  uint64
	Sec     CommitSection
}

// TailWindow retains the live commit sections a node's chain follow
// passes, the taker-side replay source. It composes twice, matching how
// the verdicts flow: as a ChainApplier wrapper it sees checkpoint
// records fold through and trims retention to the checkpoint-to-tail
// window, and its OnCommit hears the lease fold's verdicts, forwarding
// each to Next so it chains in front of Watermarks.ApplyVerdict in the
// full stack. Only sections with a live verdict are ever retained, so
// epoch-stale commits never enter a replay.
type TailWindow struct {
	inner ChainApplier

	// Next, when set, hears every verdict after retention, the
	// downstream consumer's slot in the OnCommit chain.
	Next func(CommitVerdict) error

	mu   sync.Mutex
	tail []TailSection
}

// NewTailWindow wraps inner and installs the window's intake as the
// fold's OnCommit, chaining whatever hook the fold already had.
func NewTailWindow(inner ChainApplier, fold *LeaseFold) (*TailWindow, error) {
	if inner == nil || fold == nil {
		return nil, fmt.Errorf("obs1: tail window needs an inner applier and a fold")
	}
	w := &TailWindow{inner: inner, Next: fold.OnCommit}
	fold.OnCommit = w.onCommit
	return w, nil
}

func (w *TailWindow) onCommit(v CommitVerdict) error {
	w.mu.Lock()
	for i, live := range v.Live {
		if !live {
			continue
		}
		w.tail = append(w.tail, TailSection{
			Pos: v.Pos, WALNode: v.Commit.WALNode, WALSeq: v.Commit.WALSeq,
			Sec: v.Commit.Sections[i],
		})
	}
	w.mu.Unlock()
	if w.Next != nil {
		return w.Next(v)
	}
	return nil
}

// ApplyChain applies the batch through the inner applier first, which is
// when the fold's verdicts land in the window, then trims retention at
// any checkpoint record the batch carried: sections at or below the
// checkpoint's position are folded history the segments cover, exactly
// what the chain trimmer is about to delete.
func (w *TailWindow) ApplyChain(pos ChainPos, h Header, batch ChainBatch) error {
	if err := w.inner.ApplyChain(pos, h, batch); err != nil {
		return err
	}
	for _, r := range batch.Records {
		ckpt, ok := r.(CheckpointRecord)
		if !ok {
			continue
		}
		w.mu.Lock()
		kept := w.tail[:0]
		for _, s := range w.tail {
			if ckpt.Pos.Before(s.Pos) {
				kept = append(kept, s)
			}
		}
		w.tail = kept
		w.mu.Unlock()
	}
	return nil
}

// Sections lists the retained live sections for group, in chain order.
func (w *TailWindow) Sections(group uint16) []TailSection {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []TailSection
	for _, s := range w.tail {
		if s.Sec.Group == group {
			out = append(out, s)
		}
	}
	return out
}

// Retained reports the window's size across all groups, for tests and
// the INFO surface.
func (w *TailWindow) Retained() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.tail)
}

// prefetchedStore serves Get from fan-fetched bodies and delegates
// everything else, so RebuildResident runs unchanged over the prefetch.
type prefetchedStore struct {
	Store
	bodies map[string][]byte
}

func (p prefetchedStore) Get(ctx context.Context, key string) ([]byte, ObjectInfo, error) {
	if b, ok := p.bodies[key]; ok {
		return b, ObjectInfo{Size: int64(len(b))}, nil
	}
	return p.Store.Get(ctx, key)
}

// PrewarmGroup rebuilds a group's directory and keymap from its winning
// manifest with the segment GETs fanned fan wide (zero takes the lab's
// DefaultHandoffFan, and the fan never exceeds the segment count). The
// mesh-ask path calls this before the release even lands, which is what
// keeps the handoff window flat in data size; the balancer-take path
// calls it through TakeGroup after winning the grant.
func PrewarmGroup(ctx context.Context, s Store, prefix string, m Manifest, dir *Directory, km *Keymap, fan int) (ResidentStats, error) {
	if fan <= 0 {
		fan = DefaultHandoffFan
	}
	if fan > len(m.Segs) {
		fan = len(m.Segs)
	}
	bodies := make(map[string][]byte, len(m.Segs))
	if fan > 1 {
		keys := make([]string, len(m.Segs))
		for i, ms := range m.Segs {
			keys[i] = segKey(prefix, m.Group, ms.SegSeq)
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		errs := make([]error, fan)
		next := make(chan string)
		for i := 0; i < fan; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for key := range next {
					b, _, err := s.Get(ctx, key)
					if err != nil {
						errs[i] = err
						continue
					}
					mu.Lock()
					bodies[key] = b
					mu.Unlock()
				}
			}(i)
		}
		for _, k := range keys {
			next <- k
		}
		close(next)
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return ResidentStats{}, err
			}
		}
	}
	return RebuildResident(ctx, prefetchedStore{Store: s, bodies: bodies}, prefix, m, dir, km)
}

// TakeConfig configures the taker's step 6 for one group.
type TakeConfig struct {
	Store  Store
	Prefix string
	Group  uint16

	// Manifest is the group's winning manifest; HasManifest false means
	// the group has never folded and there is nothing to rebuild.
	Manifest    Manifest
	HasManifest bool
	// Warm skips the rebuild: PrewarmGroup already ran on Dir and Km,
	// the mesh-ask path.
	Warm bool

	// Window is the taker's retained replay source.
	Window *TailWindow
	Dir    *Directory
	Km     *Keymap
	// Apply hears every replayed frame in seq order, the replay
	// applier's seam; nil counts without delivering.
	Apply func(group uint16, f WALFrame) error
	Fan   int
}

// TakeStats reports what a take did.
type TakeStats struct {
	Resident        ResidentStats
	WALGets         uint64
	SectionsSkipped uint64
	FramesSkipped   uint64
	FramesApplied   uint64
	// Applied is the group's frame cursor after replay: SetGroup's next
	// seq is Applied+1.
	Applied uint64
}

// TakeGroup runs the taker's step 6 after its grant won: rebuild the
// resident structures unless already warm, then replay the retained live
// sections above the manifest's fold cursor through the tail. The seq
// discipline is boot recovery's exactly: frames still present below the
// cursor replay anyway, a missing prefix at or below the cursor is
// trimmed history the segments cover, and a gap above it is an error
// because the committed stream never has holes.
func TakeGroup(ctx context.Context, cfg TakeConfig) (TakeStats, error) {
	var st TakeStats
	if cfg.Store == nil || cfg.Window == nil {
		return st, fmt.Errorf("obs1: TakeGroup needs a store and a tail window")
	}
	var floor uint64
	if cfg.HasManifest {
		if cfg.Manifest.Group != cfg.Group {
			return st, fmt.Errorf("obs1: take of group %d handed group %d's manifest", cfg.Group, cfg.Manifest.Group)
		}
		floor = cfg.Manifest.FoldSeq
		if !cfg.Warm {
			rs, err := PrewarmGroup(ctx, cfg.Store, cfg.Prefix, cfg.Manifest, cfg.Dir, cfg.Km, cfg.Fan)
			if err != nil {
				return st, err
			}
			st.Resident = rs
		}
	}
	var cur uint64
	for _, ts := range cfg.Window.Sections(cfg.Group) {
		cs := ts.Sec
		if cs.LastSeq <= cur {
			st.SectionsSkipped++
			continue
		}
		if cs.FirstSeq > cur+1 {
			if cs.FirstSeq-1 > floor {
				return st, fmt.Errorf("obs1: group %d section %d-%d after applied %d: the committed stream has a gap", cfg.Group, cs.FirstSeq, cs.LastSeq, cur)
			}
			cur = cs.FirstSeq - 1
		}
		e := WALIndexEntry{
			Group: cs.Group, Epoch: cs.Epoch, Offset: cs.Offset,
			StoredLen: cs.StoredLen, RawLen: cs.StoredLen, NFrames: cs.NFrames,
			FirstSeq: cs.FirstSeq, LastSeq: cs.LastSeq,
		}
		off, n := e.SectionSpan()
		key := walObjectKey(cfg.Prefix, ts.WALNode, ts.WALSeq)
		b, _, err := cfg.Store.GetRange(ctx, key, off, n)
		if err != nil {
			return st, fmt.Errorf("obs1: take section GET %s: %w", key, err)
		}
		st.WALGets++
		sec, err := ParseWALSection(b, e)
		if err != nil {
			return st, err
		}
		for _, f := range sec.Frames {
			if f.Seq <= cur {
				st.FramesSkipped++
				continue
			}
			if f.Seq != cur+1 {
				return st, fmt.Errorf("obs1: group %d frame seq %d after applied %d: the committed stream has a gap", cfg.Group, f.Seq, cur)
			}
			if cfg.Apply != nil {
				if err := cfg.Apply(cfg.Group, f); err != nil {
					return st, err
				}
			}
			cur = f.Seq
			st.FramesApplied++
		}
	}
	if cur < floor {
		cur = floor
	}
	st.Applied = cur
	return st, nil
}
