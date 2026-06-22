package bench_test

import (
	"testing"

	"github.com/tamnd/aki/vfs"
	"github.com/tamnd/aki/wal"
)

const benchPageSize = 4096

// newBenchWAL creates an in-memory write-ahead log for append benchmarks.
func newBenchWAL(b *testing.B) *wal.WAL {
	b.Helper()
	fs := vfs.NewMem()
	w, err := wal.Create(fs, "bench.aki-wal", benchPageSize, wal.Options{Salt1: 1, Salt2: 2})
	if err != nil {
		b.Fatalf("create wal: %v", err)
	}
	b.Cleanup(func() { _ = w.Close() })
	return w
}

// benchPage returns a page-sized buffer filled with a byte.
func benchPage(fill byte) []byte {
	p := make([]byte, benchPageSize)
	for i := range p {
		p[i] = fill
	}
	return p
}

// BenchmarkWALAppendOne commits a single-frame transaction, the WAL hot path for
// a one-key write. fsync cost depends on the backing file, which is in memory
// here, so this isolates the framing and checksum work.
func BenchmarkWALAppendOne(b *testing.B) {
	w := newBenchWAL(b)
	data := benchPage(0xAB)
	b.ReportAllocs()
	pageNo := uint32(2)
	for b.Loop() {
		if err := w.CommitTxn([]wal.Frame{{PageNo: pageNo, Data: data}}, pageNo+1); err != nil {
			b.Fatalf("commit: %v", err)
		}
		pageNo++
	}
}

// BenchmarkWALAppendBatch10 commits a ten-frame transaction, standing in for a
// multi-key write or a group commit batched into one transaction.
func BenchmarkWALAppendBatch10(b *testing.B) {
	w := newBenchWAL(b)
	frames := make([]wal.Frame, 10)
	for i := range frames {
		frames[i] = wal.Frame{PageNo: uint32(i + 2), Data: benchPage(byte(i))}
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := w.CommitTxn(frames, 12); err != nil {
			b.Fatalf("commit batch: %v", err)
		}
	}
}
