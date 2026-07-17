package akifile

import (
	"fmt"
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
