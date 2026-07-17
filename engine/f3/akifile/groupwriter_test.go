package akifile

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// oneRecord frames row into its own payload and returns the payload plus the
// single-entry frame table a Submit needs, the shape akiRlog.flush will hand the
// writer once one record is sealed per batch.
func oneRecord(row RecordRow) ([]byte, []RecordFrame) {
	var payload []byte
	var fr RecordFrame
	payload, fr = AppendRecordFrame(payload, row)
	return payload, []RecordFrame{fr}
}

// TestGroupWriterSerializesConcurrentShards is the slice-1 race test: many shard
// goroutines submit record batches at once, and the one writer must serialize every
// append onto the single-writer file without racing the cursor. Each record carries
// a distinguishable key, so a swapped, dropped, or torn append shows up as a bad
// round trip. It asserts every record lands, every returned address is distinct and
// decodes back to its own row, and each shard's own walk sees exactly its records,
// which is the per-shard segregation the shard filter promises. Run under -race this
// is the proof that routing S producers through the one writer removes the direct
// AppendGroup race.
func TestGroupWriterSerializesConcurrentShards(t *testing.T) {
	const shards, per = 4, 250
	dev := &memDevice{}
	f, err := CreateOnDevice(dev, CreateOptions{ShardCount: shards, Sync: SyncNo})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// A ring smaller than the per-shard batch count forces the backpressure path
	// (a false Submit the producer retries), so this exercises a full ring too.
	gw := NewGroupWriter(f, shards, 32)

	type got struct {
		key  string
		addr uint64
	}
	var mu sync.Mutex
	results := make([]got, 0, shards*per)
	var wg sync.WaitGroup
	wg.Add(shards * per)
	var failed atomic.Bool

	var start sync.WaitGroup
	start.Add(1)
	for s := 0; s < shards; s++ {
		go func(shard int) {
			start.Wait()
			for i := 0; i < per; i++ {
				key := fmt.Sprintf("s%02d-k%04d", shard, i)
				payload, frames := oneRecord(sampleRow(key, uint64(shard)<<32|uint64(i)))
				k := key
				done := func(addrs []uint64, err error) {
					if err != nil || len(addrs) != 1 {
						failed.Store(true)
						wg.Done()
						return
					}
					mu.Lock()
					results = append(results, got{k, addrs[0]})
					mu.Unlock()
					wg.Done()
				}
				for !gw.Submit(uint16(shard), uint64(i+1), payload, frames, done) {
					runtime.Gosched()
				}
			}
		}(s)
	}
	start.Done()
	wg.Wait()
	gw.Stop()

	if failed.Load() {
		t.Fatal("a completion reported an error or a wrong address count")
	}
	if len(results) != shards*per {
		t.Fatalf("completions: got %d, want %d", len(results), shards*per)
	}
	seen := make(map[uint64]bool, len(results))
	for _, g := range results {
		if seen[g.addr] {
			t.Fatalf("duplicate address %#x for %s", g.addr, g.key)
		}
		seen[g.addr] = true
		row, err := f.ReadRecordAt(g.addr)
		if err != nil {
			t.Fatalf("read %s at %#x: %v", g.key, g.addr, err)
		}
		if string(row.Key) != g.key {
			t.Fatalf("address %#x decoded key %q, want %q", g.addr, row.Key, g.key)
		}
	}

	// Each shard's own walk must see exactly its own records and no other shard's,
	// the segment filter that keeps a per-shard recovery from replaying a neighbor.
	for s := 0; s < shards; s++ {
		n := 0
		err := f.WalkShardRecords(uint16(s), PageSize, func(_ uint64, row RecordRow) error {
			if want := fmt.Sprintf("s%02d-", s); string(row.Key[:4]) != want {
				t.Errorf("shard %d walk saw foreign key %q", s, row.Key)
			}
			n++
			return nil
		})
		if err != nil {
			t.Fatalf("walk shard %d: %v", s, err)
		}
		if n != per {
			t.Fatalf("shard %d walk: got %d records, want %d", s, n, per)
		}
	}
}

// TestGroupWriterPublishesAfterDurable pins the load-bearing edge: an address must
// not reach a completion until the group's fsync has run, so no reader ever observes
// an index entry pointing at bytes a crash could lose (doc 07 section 8, step 6
// before step 7). Under SyncAlways the memDevice counts a sync per group, so the
// completion asserts the sync already happened by the time it holds the address.
func TestGroupWriterPublishesAfterDurable(t *testing.T) {
	dev := &memDevice{}
	f, err := CreateOnDevice(dev, CreateOptions{ShardCount: 1, Sync: SyncAlways})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	gw := NewGroupWriter(f, 1, 4)

	var syncedBefore int
	var addr uint64
	done := make(chan struct{})
	payload, frames := oneRecord(sampleRow("durable", 1<<63|9))
	if !gw.Submit(0, 1, payload, frames, func(addrs []uint64, err error) {
		if err != nil {
			t.Errorf("completion error: %v", err)
		} else {
			syncedBefore = dev.syncs
			addr = addrs[0]
		}
		close(done)
	}) {
		t.Fatal("submit rejected on an empty ring")
	}
	<-done
	gw.Stop()

	if syncedBefore < 1 {
		t.Fatalf("address published before any fsync (syncs=%d)", syncedBefore)
	}
	row, err := f.ReadRecordAt(addr)
	if err != nil {
		t.Fatalf("read durable record: %v", err)
	}
	if string(row.Key) != "durable" {
		t.Fatalf("durable record key %q, want durable", row.Key)
	}
}

// gateDevice is an in-memory device whose Sync blocks until the test releases it,
// so a test can hold the writer inside a group's fsync while it queues more records
// behind it. It counts fsyncs and announces each one on entered before blocking on
// release, which lets a test drive the exact interleaving the coalescing claim needs.
type gateDevice struct {
	buf     []byte
	syncs   int32
	armed   atomic.Bool
	entered chan int32
	release chan struct{}
}

func (d *gateDevice) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(d.buf)) {
		return 0, io.EOF
	}
	n := copy(p, d.buf[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (d *gateDevice) WriteAt(p []byte, off int64) (int, error) {
	if end := off + int64(len(p)); end > int64(len(d.buf)) {
		grown := make([]byte, end)
		copy(grown, d.buf)
		d.buf = grown
	}
	copy(d.buf[off:], p)
	return len(p), nil
}

// Sync gates only once armed, so the fsyncs CreateOnDevice issues while setting up
// the file pass straight through (the test arms the gate after creation). Once
// armed, each fsync announces its number on entered and blocks on release, and the
// count reflects only the armed fsyncs, so the first gated group is fsync 1.
func (d *gateDevice) Sync() error {
	if !d.armed.Load() {
		return nil
	}
	n := atomic.AddInt32(&d.syncs, 1)
	d.entered <- n
	<-d.release
	return nil
}

func (d *gateDevice) Truncate(size int64) error {
	grown := make([]byte, size)
	copy(grown, d.buf)
	d.buf = grown
	return nil
}

func (d *gateDevice) Size() (int64, error) { return int64(len(d.buf)), nil }
func (d *gateDevice) Close() error         { return nil }

// TestGroupWriterFsyncIsTheWindow is the engine proof behind the design decision to
// ship no explicit group timer: the fsync itself is the coalescing window. It holds
// the writer inside the first group's fsync, submits three more records behind it,
// and asserts all three ride the single next group, so four records cost two fsyncs
// rather than four. Each completion records the fsync count it observed, which pins
// the grouping deterministically with no wall-clock: the first record rode fsync 1,
// the three queued behind it all rode fsync 2. If a later change split the queued
// records across groups this test fails, which is the guard the no-timer decision
// leans on.
func TestGroupWriterFsyncIsTheWindow(t *testing.T) {
	dev := &gateDevice{entered: make(chan int32), release: make(chan struct{})}
	f, err := CreateOnDevice(dev, CreateOptions{ShardCount: 1, Sync: SyncAlways})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	gw := NewGroupWriter(f, 1, 16)
	dev.armed.Store(true) // creation fsyncs are done; gate every fsync from here

	type obs struct {
		key    string
		atSync int32
	}
	seen := make(chan obs, 4)
	submit := func(seq uint64, key string) {
		payload, frames := oneRecord(sampleRow(key, seq))
		k := key
		if !gw.Submit(0, seq, payload, frames, func(_ []uint64, err error) {
			if err != nil {
				t.Errorf("completion %s: %v", k, err)
			}
			seen <- obs{k, atomic.LoadInt32(&dev.syncs)}
		}) {
			t.Errorf("submit %s rejected", k)
		}
	}

	// First record: the writer drains it, enters fsync 1, and blocks there.
	submit(1, "first")
	if n := <-dev.entered; n != 1 {
		t.Fatalf("first fsync numbered %d, want 1", n)
	}
	// While the writer is stuck in fsync 1, queue three more. They cannot be cut
	// until the writer returns from the fsync and drains again.
	submit(2, "a")
	submit(3, "b")
	submit(4, "c")
	// Let fsync 1 finish: the first record's completion fires at sync count 1.
	dev.release <- struct{}{}
	// The writer now drains all three queued records in one pass and enters fsync 2.
	if n := <-dev.entered; n != 2 {
		t.Fatalf("second fsync numbered %d, want 2", n)
	}
	dev.release <- struct{}{}
	gw.Stop()

	got := make(map[string]int32, 4)
	for i := 0; i < 4; i++ {
		o := <-seen
		got[o.key] = o.atSync
	}
	if got["first"] != 1 {
		t.Fatalf("first rode fsync %d, want 1", got["first"])
	}
	for _, k := range []string{"a", "b", "c"} {
		if got[k] != 2 {
			t.Fatalf("%s rode fsync %d, want 2 (the one group the queued records coalesced into)", k, got[k])
		}
	}
	if total := atomic.LoadInt32(&dev.syncs); total != 2 {
		t.Fatalf("four records cost %d fsyncs, want 2", total)
	}
}

// TestGroupWriterStopDrainsQueued checks that Stop lands what is already queued: a
// batch submitted just before Stop still commits and its completion still fires, so
// a shutdown never silently drops a record the shard already handed off. Stop waits
// on the final drain, so the completion has run by the time Stop returns.
func TestGroupWriterStopDrainsQueued(t *testing.T) {
	dev := &memDevice{}
	f, err := CreateOnDevice(dev, CreateOptions{ShardCount: 1, Sync: SyncNo})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	gw := NewGroupWriter(f, 1, 4)

	var landed atomic.Bool
	var addr atomic.Uint64
	payload, frames := oneRecord(sampleRow("queued", 42))
	if !gw.Submit(0, 1, payload, frames, func(addrs []uint64, err error) {
		if err == nil && len(addrs) == 1 {
			addr.Store(addrs[0])
			landed.Store(true)
		}
	}) {
		t.Fatal("submit rejected on an empty ring")
	}
	gw.Stop()

	if !landed.Load() {
		t.Fatal("Stop returned before the queued batch committed")
	}
	row, err := f.ReadRecordAt(addr.Load())
	if err != nil {
		t.Fatalf("read queued record: %v", err)
	}
	if string(row.Key) != "queued" {
		t.Fatalf("queued record key %q, want queued", row.Key)
	}
}
