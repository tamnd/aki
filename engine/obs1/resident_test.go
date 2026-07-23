package obs1_test

import (
	"context"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
)

// residentManifest lifts the fixture folder's ledger into the manifest
// shape RebuildResident consumes, the same rows the publisher writes.
func residentManifest(led []obs1.FoldedSegment) obs1.Manifest {
	m := obs1.Manifest{Group: 3, Epoch: 7}
	for _, e := range led {
		m.Segs = append(m.Segs, obs1.ManifestSeg{
			SegSeq: e.SegSeq, Size: uint64(e.Size),
			NRecords: e.NRecords, RawBytes: e.RawBytes,
			FooterOff: e.FooterOff, FooterLen: e.FooterLen,
		})
	}
	return m
}

// TestRebuildResident folds three segments through the live pipeline,
// then rebuilds a fresh keymap and directory from the manifest the way a
// takeover would, and checks the rebuilt state agrees with the live one:
// a re-folded key points at its newest segment, a tombstoned key is
// definitively absent with its shadow claim swept, and every live
// locator resolves to the segment the ledger names.
func TestRebuildResident(t *testing.T) {
	fx, km, _ := newFoldDirFixture(t)
	ctx := context.Background()

	// Segment 1: three values. Segment 2: tombstones for k1 and k3.
	// Segment 3: k1 folded again, so its tombstone is not the last word.
	fx.folder.Add(frames("k1", "v1", "k2", "v2", "k3", "v3"))
	fx.folder.Flush()
	waitFor(t, "segment 1", func() bool { return len(fx.folder.Ledger()) == 1 })
	fx.folder.Delete([]byte("k1"))
	fx.folder.Delete([]byte("k3"))
	fx.folder.Flush()
	waitFor(t, "segment 2", func() bool { return len(fx.folder.Ledger()) == 2 })
	fx.folder.Add(frames("k1", "v1b"))
	fx.folder.Flush()
	waitFor(t, "segment 3", func() bool { return len(fx.folder.Ledger()) == 3 })
	led := fx.folder.Ledger()

	km2, dir2 := obs1.NewKeymap(), obs1.NewDirectory()
	st, err := obs1.RebuildResident(ctx, fx.sim, "db/t", residentManifest(led), dir2, km2)
	if err != nil {
		t.Fatal(err)
	}
	if st.Segments != 3 || st.Records != 6 || st.Tombstones != 2 || st.Swept != 1 {
		t.Fatalf("rebuild stats %+v", st)
	}
	if dir2.Segments() != 3 {
		t.Fatalf("directory holds %d segments", dir2.Segments())
	}

	for _, key := range []string{"k1", "k2"} {
		fp := obs1.Fingerprint([]byte(key))
		live, ok := km.Lookup(fp)
		if !ok {
			t.Fatalf("%s missing from the live keymap", key)
		}
		got, ok := km2.Lookup(fp)
		if !ok || got != live {
			t.Fatalf("%s rebuilt %+v ok=%v, live %+v", key, got, ok, live)
		}
		ref, ok := dir2.Resolve(got)
		if !ok {
			t.Fatalf("%s locator %+v does not resolve after rebuild", key, got)
		}
		if want := led[got.Seg-1].Key; ref.ObjKey != want {
			t.Fatalf("%s resolves to %q, ledger says %q", key, ref.ObjKey, want)
		}
	}
	if got, ok := km2.Lookup(obs1.Fingerprint([]byte("k1"))); !ok || got.Seg != uint32(led[2].SegSeq) {
		t.Fatalf("k1 %+v ok=%v, want its re-fold segment %d", got, ok, led[2].SegSeq)
	}
	if _, ok := km2.Lookup(obs1.Fingerprint([]byte("k3"))); ok {
		t.Fatal("tombstoned k3 present after rebuild")
	}
	if km2.Len() != km.Len() {
		t.Fatalf("rebuilt keymap holds %d keys, live holds %d", km2.Len(), km.Len())
	}

	// A manifest naming a segment the bucket does not hold fails loudly.
	bad := residentManifest(led)
	bad.Segs[1].SegSeq = 99
	_, err = obs1.RebuildResident(ctx, fx.sim, "db/t", bad, obs1.NewDirectory(), obs1.NewKeymap())
	if err == nil || !strings.Contains(err.Error(), "segment 99") {
		t.Fatalf("missing segment err %v", err)
	}
}

// TestRebuildResidentDirectoryOnly checks the nil-keymap shape the
// directory-only caller uses: footers book, no claims feed.
func TestRebuildResidentDirectoryOnly(t *testing.T) {
	fx, _, _ := newFoldDirFixture(t)
	fx.folder.Add(frames("k1", "v1"))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	dir2 := obs1.NewDirectory()
	st, err := obs1.RebuildResident(context.Background(), fx.sim, "db/t", residentManifest(fx.folder.Ledger()), dir2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Segments != 1 || st.Records != 0 || dir2.Segments() != 1 {
		t.Fatalf("stats %+v segments %d", st, dir2.Segments())
	}
}
