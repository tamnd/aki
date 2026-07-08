package f1raw

import (
	"path/filepath"
	"testing"
)

// BenchmarkInsertFloodP1 is the engine-level stand-in for SADDNEW at P1: a single writer streams
// distinct new records into a segmented arena with the migrator engaged, so the arena stays near
// full and the background migrator drains segments cold at roughly the writer's arrival rate. One
// writer means no pipeline overlap, so ns/op is the pure per-insert write-path latency: arena
// allocate, record encode, index insert, plus whatever CPU the concurrent migrator steals. This is
// the quantity the SADDNEW P1 2x gate turns on, and it profiles cleanly with -cpuprofile.
func BenchmarkInsertFloodP1(b *testing.B) {
	const (
		valLen = 256
		nSeg   = 12
	)
	segSize := int(align8(maxRecordBytes))
	ov := 1 << 20
	arena := 8 + ov + (nSeg+1)*segSize
	// The resident index keeps an entry for every distinct key ever written (cold ones too), so a
	// long benchtime drives tens of millions of inserts: size the primary bucket count generously.
	s := NewSegmented(1<<25, arena, segSize, ov)
	if !s.segmented {
		b.Fatal("NewSegmented did not enable the segmented arena")
	}
	if err := s.EnableColdRecords(filepath.Join(b.TempDir(), "recs.log")); err != nil {
		b.Fatalf("EnableColdRecords: %v", err)
	}
	defer s.Close()
	s.EnableMigrator()

	val := make([]byte, valLen)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	var kb [16]byte
	i := 0
	for b.Loop() {
		for j := range 16 {
			kb[j] = byte(i>>(uint(j)*4)) | 0x40
		}
		if err := s.Set(kb[:], val); err != nil {
			b.Fatalf("Set blocked past the backpressure budget at i=%d: %v", i, err)
		}
		i++
	}
}
