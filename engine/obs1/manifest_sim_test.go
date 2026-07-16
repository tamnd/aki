package obs1_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

func TestManifestOnSim(t *testing.T) {
	s := sim.New(sim.Config{Seed: 4})
	ctx := t.Context()

	m0 := obs1.Manifest{Group: 2, Epoch: 7, ManSeq: 0, FoldPos: obs1.ChainPos{Seq: 10}}
	m1 := obs1.Manifest{Group: 2, Epoch: 7, ManSeq: 1, FoldPos: obs1.ChainPos{DD: 1, Seq: 4}, Segs: []obs1.ManifestSeg{
		{SegSeq: 1, Size: 100, NRecords: 2, RawBytes: 64},
	}}
	if err := obs1.PutManifest(ctx, s, "db/a", 9, m0); err != nil {
		t.Fatal(err)
	}
	if err := obs1.PutManifest(ctx, s, "db/a", 9, m1); err != nil {
		t.Fatal(err)
	}
	// A second folder racing for the same dense slot loses the CAS-create.
	loser := m1
	loser.Epoch = 8
	if err := obs1.PutManifest(ctx, s, "db/a", 10, loser); !errors.Is(err, obs1.ErrPrecondition) {
		t.Fatalf("racing put: %v", err)
	}

	got, err := obs1.LoadManifests(ctx, s, "db/a", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []obs1.Manifest{m0, m1}) {
		t.Fatalf("walk from 0: %+v", got)
	}
	got, err = obs1.LoadManifests(ctx, s, "db/a", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []obs1.Manifest{m1}) {
		t.Fatalf("walk from hint 1: %+v", got)
	}
	for _, tail := range []struct {
		group uint16
		from  uint64
	}{{2, 2}, {3, 0}} {
		got, err = obs1.LoadManifests(ctx, s, "db/a", tail.group, tail.from)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("group %d from %d: %+v", tail.group, tail.from, got)
		}
	}
}
