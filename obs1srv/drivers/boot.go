package drivers

// Boot composition (spec 2064/obs1 doc 02 section 2.5, doc 04 section
// 6): everything a serving node does between owning a bucket and
// accepting its first connection. BootDurability runs inside the
// Options.Boot seam, after the runtime is built and before any worker
// starts: it creates the root on a fresh bucket, recovers the chain,
// manifests, and WAL tail, replays the tail into the shard stores
// through the string-plane applier, self-grants every ungranted group
// (the O1c single-node shape; the lease manager takes this over when
// multi-node lands), and wires the write log, folder, and publisher
// onto the runtime, seeded so seqs continue where the last incarnation
// stopped.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/replay"
	"github.com/tamnd/aki/engine/obs1/shard"
)

// ClusterMapKey is the production key route: cluster hash slot to slot
// group, the same mapping dispatch and Runtime.ShardOf use, so a frame's
// group and the shard that owns its key can never disagree.
func ClusterMapKey(key []byte) (uint16, uint16) {
	slot := shard.HashSlot(key)
	return uint16(slot), uint16(shard.GroupOfSlot(slot, shard.DefaultSlotGroups))
}

// BootConfig parameterizes a node's boot against its bucket.
type BootConfig struct {
	// Store is the bucket client; Prefix namespaces the database inside
	// it and Fallback selects the conditional-write dialect, both exactly
	// as everywhere else in the engine.
	Store    obs1.Store
	Prefix   string
	Fallback bool
	// DD is the node's chain domain, Node its stable id, and Incarnation
	// its boot count, which must increase on every boot of the same node
	// id (the open-sequence probe and the #1074 tag hazard both key on
	// it).
	DD          uint8
	Node        uint64
	Incarnation uint32
	// FlushAge and FoldAge pass through to the write log and folder;
	// zero takes each one's default cadence.
	FlushAge time.Duration
	FoldAge  time.Duration
}

// Booted is the running durability pipeline BootDurability composed.
// Close stops it in dependency order; call it after the server's Close
// has drained the connections that feed it.
type Booted struct {
	WL     *obs1.WriteLog
	Folder *obs1.Folder
	Pub    *obs1.ManifestPublisher
	Rec    *obs1.Recovery
	Replay replay.Stats
	// Keymaps is the per-group regime A cold-key index the folder
	// maintains; the cold read path consumes it. Rebuilding it from
	// segments at takeover lands with the directory slice, so after a
	// restart it covers keys folded since boot.
	Keymaps []*obs1.Keymap
}

// Close drains and stops the pipeline: write log first so its final
// flush commits, then the folder, then the publisher.
func (b *Booted) Close() error {
	err := b.WL.Close()
	b.Folder.Close()
	b.Pub.Close()
	return err
}

// BootDurability is the Options.Boot body: recover, replay, self-grant,
// compose, wire. The runtime it receives is built but not started, so
// the shard stores have no owner yet and replay writes them directly.
func BootDurability(ctx context.Context, cfg BootConfig, rt *shard.Runtime) (*Booted, error) {
	if _, err := obs1.LoadRoot(ctx, cfg.Store, cfg.Prefix, cfg.Fallback); err != nil {
		if !errors.Is(err, obs1.ErrNotFound) {
			return nil, err
		}
		root := obs1.Root{
			CreatedMS: uint64(time.Now().UnixMilli()),
			G:         shard.DefaultSlotGroups,
			D:         1,
		}
		if err := obs1.CreateRoot(ctx, cfg.Store, cfg.Prefix, cfg.Fallback, root); err != nil {
			return nil, err
		}
	}

	ap := replay.New(replay.Config{Ctx: func(key []byte) *shard.Ctx {
		return rt.BootCtx(rt.ShardOf(key))
	}})
	r, err := obs1.Recover(ctx, obs1.RecoverConfig{
		Store: cfg.Store, Prefix: cfg.Prefix, Fallback: cfg.Fallback,
		DD: cfg.DD, Node: cfg.Node, Incarnation: cfg.Incarnation,
		Apply: ap.Apply,
	})
	if err != nil {
		return nil, err
	}
	if err := ap.Finish(); err != nil {
		return nil, err
	}
	if r.Root.G != shard.DefaultSlotGroups {
		return nil, fmt.Errorf("drivers: root has %d groups, this build maps %d", r.Root.G, shard.DefaultSlotGroups)
	}

	// Self-grant whatever no one holds. A group already ours continues at
	// its epoch; a group held by another node is a cluster this
	// single-node boot cannot join yet.
	var grants []obs1.ChainRecord
	for g := range uint16(shard.DefaultSlotGroups) {
		node, _, ok := r.Fold.Holder(g)
		if !ok {
			grants = append(grants, obs1.GrantRecord{Group: g, Node: cfg.Node, Epoch: 1})
			continue
		}
		if node != cfg.Node {
			return nil, fmt.Errorf("drivers: group %d is held by node %x, single-node boot cannot take it", g, node)
		}
	}
	if len(grants) > 0 {
		if _, err := r.Chain.Append(ctx, grants); err != nil {
			return nil, err
		}
	}

	seeds := make([]obs1.Manifest, 0, len(r.Winning))
	for _, m := range r.Winning {
		seeds = append(seeds, m)
	}
	pub, err := obs1.NewManifestPublisher(obs1.ManPubConfig{
		Store: cfg.Store, Prefix: cfg.Prefix, Node: cfg.Node, Seed: seeds,
	})
	if err != nil {
		return nil, err
	}
	wl, err := obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: cfg.Store, Prefix: cfg.Prefix, Node: cfg.Node,
		Chain: r.Chain, Fold: r.Fold,
		Groups: shard.DefaultSlotGroups, MapKey: ClusterMapKey,
		FlushAge: cfg.FlushAge, StartSeq: r.NextWALSeq,
		OnVerdict: pub.OnVerdict,
	})
	if err != nil {
		pub.Close()
		return nil, err
	}
	kms := make([]*obs1.Keymap, shard.DefaultSlotGroups)
	for g := range kms {
		kms[g] = obs1.NewKeymap()
	}
	folder, err := obs1.NewFolder(obs1.FoldConfig{
		Store: cfg.Store, Prefix: cfg.Prefix, Node: cfg.Node,
		MapKey: ClusterMapKey, Mark: wl.GroupMark, Marks: wl.Marks(),
		OnPublish: pub.OnFolded, FoldAge: cfg.FoldAge, Seed: seeds,
		Keymap: func(group uint16) *obs1.Keymap { return kms[group] },
	})
	if err != nil {
		_ = wl.Close()
		pub.Close()
		return nil, err
	}
	wl.SetKeyDelFeed(folder.Delete)
	for g := range uint16(shard.DefaultSlotGroups) {
		_, epoch, ok := r.Fold.Holder(g)
		if !ok {
			_ = wl.Close()
			folder.Close()
			pub.Close()
			return nil, fmt.Errorf("drivers: group %d has no holder after the grant round", g)
		}
		wl.SetGroup(g, epoch, r.Applied[g]+1)
	}

	rt.SetWriteLog(wl)
	rt.SetWALInfo(wl.AppendInfo)
	rt.SetFoldTap(folder.Add)
	rt.SetFoldProgress(func() uint64 { return pub.Stats().Published })
	rt.SetFoldKick(folder.Flush)

	return &Booted{WL: wl, Folder: folder, Pub: pub, Rec: r, Replay: ap.Stats(), Keymaps: kms}, nil
}
