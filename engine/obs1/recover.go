// Boot recovery (spec 2064/obs1 doc 02 section 2.5, doc 04 section 2,
// doc 06 section 1.5): the one path that ever reads WAL objects back.
// A booting node reads the root, primes a lease fold from the checkpoint
// the root points at, replays the chain from there to the tail, selects
// each group's winning manifest against the replayed lease history, and
// then replays the WAL tail exactly once above each group's fold cursor.
// Everything below the cursor is in segments the manifest states; the
// frames above it are the only data the chain still owes the store.
//
// The chain walk buffers commit verdicts instead of GETting WAL sections
// as they appear, because manifest selection needs the lease history the
// walk is still building, and the fold cursor that gates the WAL reads
// comes from the selected manifest. The buffer holds the checkpoint-to-
// tail window's commit records only, the same window the chain trimmer
// keeps, and no WAL bytes.
package obs1

import (
	"context"
	"errors"
	"fmt"
)

// walObjectKey names one WAL object. The node id owns the wal/<node16>/
// namespace, so a key is planned from any commit record's WALNode and
// WALSeq alone.
func walObjectKey(prefix string, node, seq uint64) string {
	return dbKey(prefix, fmt.Sprintf("wal/%016x/%s", node, seq16(seq)))
}

// RecoverConfig configures one node's boot recovery.
type RecoverConfig struct {
	// Store and Prefix locate the database; Fallback is LoadRoot's
	// verified-copy fallback switch.
	Store    Store
	Prefix   string
	Fallback bool

	// DD is the log domain to boot, Node and Incarnation the identity the
	// returned appender will write under.
	DD          uint8
	Node        uint64
	Incarnation uint32

	// Apply hears every accepted frame in seq order per group, the store
	// applier's seam. Nil counts frames without delivering them, the
	// audit-walk shape. Frames within one commit section arrive
	// contiguously, so a txn body is delivered unbroken; atomic txn
	// application is the applier's contract (doc 04 section 2), not the
	// walk's.
	Apply func(group uint16, f WALFrame) error
}

// RecoverStats counts the walk for tests and the INFO surface.
type RecoverStats struct {
	Verdicts        uint64 // live commit verdicts buffered from the chain
	WALGets         uint64 // ranged section GETs actually issued
	SectionsSkipped uint64 // live sections wholly at or below the fold cursor
	FramesSkipped   uint64 // frames below a cursor inside a straddling section
	FramesApplied   uint64
}

// Recovery is the boot's yield: everything the server needs to compose
// the write path and the cold pipeline on top of recovered state.
type Recovery struct {
	Root Root
	Ckpt Checkpoint

	// Chain is primed at the tail and ready to append; hand it to
	// WriteLogConfig.Chain. Fold is the lease fold the chain replay
	// built; hand it to WriteLogConfig.Fold, whose committer takes over
	// its OnCommit.
	Chain *ChainAppender
	Fold  *LeaseFold

	// Winning holds each group's selected manifest, absent when the group
	// has never folded; seed the ManifestPublisher and the Folder from
	// it. Applied is each group's frame cursor after replay, so
	// WriteLog.SetGroup's next seq is Applied[g]+1; a group absent from
	// both maps has never seen a frame.
	Winning map[uint16]Manifest
	Applied map[uint16]uint64

	// NextWALSeq is this node's open sequence: one past the last WAL
	// object under our node id, committed or orphaned. A restarted
	// flusher must start here (WriteLogConfig.StartSeq) because reusing
	// an occupied seq hits our own tag on the previous incarnation's
	// object and the PUT recheck silently adopts the old content.
	NextWALSeq uint64

	Stats RecoverStats
}

// Recover runs the full boot sequence. It issues only GETs: recovery
// mutates nothing in the bucket, so a crashed boot re-runs from scratch.
func Recover(ctx context.Context, cfg RecoverConfig) (*Recovery, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("obs1: RecoverConfig needs a Store")
	}
	root, err := LoadRoot(ctx, cfg.Store, cfg.Prefix, cfg.Fallback)
	if err != nil {
		return nil, err
	}
	fold := NewLeaseFold()
	var ckpt Checkpoint
	var start ChainPos
	if root.CkptSeq != 0 {
		if root.CkptDD != uint16(cfg.DD) {
			return nil, fmt.Errorf("obs1: root checkpoint is in domain %d, booting domain %d", root.CkptDD, cfg.DD)
		}
		ckpt, _, err = LoadCheckpoint(ctx, cfg.Store, cfg.Prefix, cfg.DD, root.CkptSeq)
		if err != nil {
			return nil, err
		}
		if err := fold.Prime(ckpt); err != nil {
			return nil, err
		}
		start = ckpt.Through
	}

	r := &Recovery{
		Root: root, Ckpt: ckpt, Fold: fold,
		Winning: make(map[uint16]Manifest),
		Applied: make(map[uint16]uint64),
	}
	var verdicts []CommitVerdict
	fold.OnCommit = func(v CommitVerdict) error {
		for _, live := range v.Live {
			if live {
				verdicts = append(verdicts, v)
				r.Stats.Verdicts++
				break
			}
		}
		return nil
	}
	a, err := NewChainAppender(cfg.Store, cfg.Prefix, cfg.DD, cfg.Node, cfg.Incarnation, start, fold)
	if err != nil {
		return nil, err
	}
	if err := a.Follow(ctx); err != nil {
		return nil, err
	}
	fold.OnCommit = nil
	r.Chain = a

	// Manifest selection, against the lease history the walk just built.
	// The checkpoint's group cursor floors the ManSeq walk; a group
	// beyond the checkpoint's table starts at zero.
	for g := range int(root.G) {
		group := uint16(g)
		var from uint64
		if g < len(ckpt.Groups) {
			from = ckpt.Groups[g].ManSeq
		}
		ms, err := LoadManifests(ctx, cfg.Store, cfg.Prefix, group, from)
		if err != nil {
			return nil, err
		}
		if m, ok := SelectManifest(group, ms, fold); ok {
			r.Winning[group] = m
			r.Applied[group] = m.FoldSeq
		}
	}

	// The WAL tail, exactly once above each fold cursor. The gate is the
	// doc 04 section 2 idempotence rule: at or below the cursor skips, a
	// jump past cursor plus one is a gap in the committed stream, which
	// means the trimmer violated the replay floor and the state is not
	// recoverable from here.
	var maxOurs uint64
	for _, v := range verdicts {
		if v.Commit.WALNode == cfg.Node && v.Commit.WALSeq > maxOurs {
			maxOurs = v.Commit.WALSeq
		}
		for i, cs := range v.Commit.Sections {
			if !v.Live[i] {
				continue
			}
			if err := r.section(ctx, cfg, v.Commit, cs); err != nil {
				return nil, err
			}
		}
	}

	// The open sequence probe: an orphan object from a crashed flush sits
	// past the last committed seq without a commit record, so probe
	// forward until the namespace is empty.
	next := maxOurs + 1
	for {
		_, _, err := cfg.Store.GetTail(ctx, walObjectKey(cfg.Prefix, cfg.Node, next), 1)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		next++
	}
	r.NextWALSeq = next
	return r, nil
}

// section replays one live commit section through the gate. The commit
// record repeats the WAL footer's index, so the ranged GET is planned
// from the chain alone; RawLen equals StoredLen because the WAL writes
// comp 0 (#1097's verdict), the only compression this build reads.
func (r *Recovery) section(ctx context.Context, cfg RecoverConfig, rec CommitRecord, cs CommitSection) error {
	cur := r.Applied[cs.Group]
	if cs.LastSeq <= cur {
		r.Stats.SectionsSkipped++
		return nil
	}
	if cs.FirstSeq > cur+1 {
		return fmt.Errorf("obs1: group %d section %d-%d after applied %d: the committed stream has a gap", cs.Group, cs.FirstSeq, cs.LastSeq, cur)
	}
	e := WALIndexEntry{
		Group: cs.Group, Epoch: cs.Epoch, Offset: cs.Offset,
		StoredLen: cs.StoredLen, RawLen: cs.StoredLen, NFrames: cs.NFrames,
		FirstSeq: cs.FirstSeq, LastSeq: cs.LastSeq,
	}
	off, n := e.SectionSpan()
	key := walObjectKey(cfg.Prefix, rec.WALNode, rec.WALSeq)
	b, _, err := cfg.Store.GetRange(ctx, key, off, n)
	if err != nil {
		return fmt.Errorf("obs1: section GET %s: %w", key, err)
	}
	r.Stats.WALGets++
	sec, err := ParseWALSection(b, e)
	if err != nil {
		return err
	}
	for _, f := range sec.Frames {
		if f.Seq <= cur {
			r.Stats.FramesSkipped++
			continue
		}
		if f.Seq != cur+1 {
			return fmt.Errorf("obs1: group %d frame seq %d after applied %d: the committed stream has a gap", cs.Group, f.Seq, cur)
		}
		if cfg.Apply != nil {
			if err := cfg.Apply(cs.Group, f); err != nil {
				return err
			}
		}
		cur = f.Seq
		r.Stats.FramesApplied++
	}
	r.Applied[cs.Group] = cur
	return nil
}
