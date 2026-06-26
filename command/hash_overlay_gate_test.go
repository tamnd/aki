package command

import (
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// The hash overlay absorbs element writes in memory and folds them into the
// sub-tree in batches, so an absorbed write has no durable record until the next
// fold. That is incompatible with commitAlways without the AOF, where the
// durability contract is a per-write pager checkpoint before the reply. The gate in
// applyHashOverlay forces the overlay off in exactly that case. This test pins the
// gate across the four reachable durability states, including a runtime AOF toggle.
func TestHashOverlayGateTracksDurability(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "gate.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	d := New(Config{Databases: 16, Engine: NewEngine(ks)})

	// Default policy is everysec, so the directive alone engages the overlay.
	if err := d.SetConfig("aki-hash-overlay", "yes"); err != nil {
		t.Fatalf("set aki-hash-overlay yes: %v", err)
	}
	if !ks.HashOverlayEnabled() {
		t.Fatalf("overlay not engaged under everysec with directive on")
	}

	// appendfsync always without the AOF is commitAlways: the gate forces the overlay
	// off even though the directive stays yes.
	if err := d.SetConfig("appendfsync", "always"); err != nil {
		t.Fatalf("set appendfsync always: %v", err)
	}
	if ks.HashOverlayEnabled() {
		t.Fatalf("overlay engaged under commitAlways without the AOF")
	}

	// Turning the AOF on restores a durable record for every acked write, so the
	// policy leaves commitAlways and the overlay re-engages. This also exercises the
	// runtime appendonly toggle recomputing the gate.
	if err := d.SetConfig("appendonly", "yes"); err != nil {
		t.Fatalf("set appendonly yes: %v", err)
	}
	if !ks.HashOverlayEnabled() {
		t.Fatalf("overlay not restored with AOF on under appendfsync always")
	}

	// Turning the directive off disengages the overlay regardless of policy.
	if err := d.SetConfig("aki-hash-overlay", "no"); err != nil {
		t.Fatalf("set aki-hash-overlay no: %v", err)
	}
	if ks.HashOverlayEnabled() {
		t.Fatalf("overlay engaged with directive off")
	}
}
