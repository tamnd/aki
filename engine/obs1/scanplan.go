// Scan coalescing and readahead (spec 2064/obs1 doc 05 section 3): a
// collection scan plans its chunk sequence from the directory, coalesces
// adjacent blocks into large ranges at the AWS-guidance GET size, fans
// out up to scan-fan parallel GETs, and streams the blocks to the reply
// in order. When consumption passes the midpoint of a fetched range the
// next range launches speculatively, so a sequential cold scan overlaps
// transfer with reply writing.
//
// Scan and readahead GETs are admission-exempt by construction: they go
// straight through Store.GetRange and never touch the point-read flight
// table, so when the section 4 block cache lands under ColdReader this
// fetcher is already the exempt lane the scan-plan lab (#1324) priced,
// and the class split shows in the stats (RangeGETs and ReadaheadGETs
// here, BlockGETs on the reader).
package obs1

import (
	"context"
	"fmt"
	"sync"
)

// ScanRangeTargetDefault is the coalescing target per GET, the top of
// the 8 to 16 MiB AWS guidance band the scan-plan lab confirmed.
const ScanRangeTargetDefault = 16 << 20

// ScanFanDefault is the parallel-GET ceiling per scan, the last
// near-linear doubling under the doc 01 lane fit (#1324).
const ScanFanDefault = 8

// ScanRange is one coalesced GET: a contiguous byte span of one segment
// object covering whole blocks in plan order.
type ScanRange struct {
	Obj    string
	Off    int64
	N      int64
	Blocks []SegmentBlockEntry
}

// ScanRanges coalesces a plan's refs into GET ranges. Refs stay in plan
// order; consecutive refs sharing a block fold into it, zero-count refs
// (a trim's manifest drops) plan nothing, and a range breaks on an
// object change, a gap or backward step between blocks, or the size
// target. target zero takes the default.
func ScanRanges(refs []DirRef, target int64) []ScanRange {
	if target <= 0 {
		target = ScanRangeTargetDefault
	}
	var out []ScanRange
	var cur *ScanRange
	for _, r := range refs {
		if r.Count == 0 {
			continue
		}
		off, n := r.Block.BlockSpan()
		if cur != nil && r.ObjKey == cur.Obj {
			last := cur.Blocks[len(cur.Blocks)-1]
			if r.Block.Offset == last.Offset {
				continue // another chunk of the block already planned
			}
			lastOff, lastN := last.BlockSpan()
			if off == lastOff+lastN && cur.N+n <= target {
				cur.N += n
				cur.Blocks = append(cur.Blocks, r.Block)
				continue
			}
		}
		out = append(out, ScanRange{Obj: r.ObjKey, Off: off, N: n, Blocks: []SegmentBlockEntry{r.Block}})
		cur = &out[len(out)-1]
	}
	return out
}

// ScanFetchStats counts a fetcher's GET classes and served blocks.
type ScanFetchStats struct {
	RangeGETs     uint64
	ReadaheadGETs uint64
	Blocks        uint64
}

// scanRangeState tracks one range's fetch: launched under the fetcher's
// lock, filled by its goroutine, released once consumption passes it.
type scanRangeState struct {
	launched bool
	done     chan struct{}
	data     []byte
	err      error
}

// ScanFetcher streams a plan's blocks out of coalesced range GETs. Its
// Fetch and Prefetch methods match the run iterators' seams, so any
// type's iterator scans through it unchanged: Fetch serves a block from
// its range (launching the GET on first touch, waiting if it flies),
// then past the range's midpoint launches the next range as readahead;
// Prefetch launches a block's range without waiting, the iterators'
// next-distinct-block announcement. Consumption is assumed to move
// forward, the plan order every iterator holds, and a range behind the
// newest one touched frees its buffer, so a scan retains at most the
// fan's worth of ranges. One consumer goroutine; launches are internal.
type ScanFetcher struct {
	ctx    context.Context
	store  Store
	ranges []ScanRange
	byOff  map[string]map[uint64]int

	mu     sync.Mutex
	states []scanRangeState
	newest int
	ahead  int
	stats  ScanFetchStats

	sem chan struct{}
}

// NewScanFetcher builds a fetcher over the plan's ranges. fan caps the
// concurrent range GETs, zero for the default; ctx bounds every GET, so
// an abandoned scan unwinds when its command context does.
func NewScanFetcher(ctx context.Context, st Store, ranges []ScanRange, fan int) *ScanFetcher {
	if fan <= 0 {
		fan = ScanFanDefault
	}
	f := &ScanFetcher{
		ctx: ctx, store: st, ranges: ranges,
		byOff:  make(map[string]map[uint64]int),
		states: make([]scanRangeState, len(ranges)),
		newest: -1,
		ahead:  1,
		sem:    make(chan struct{}, fan),
	}
	for i, r := range ranges {
		m := f.byOff[r.Obj]
		if m == nil {
			m = make(map[uint64]int)
			f.byOff[r.Obj] = m
		}
		for _, b := range r.Blocks {
			m[b.Offset] = i
		}
	}
	return f
}

// launch starts range i's GET if it has not started, counting it in the
// given class. Callers hold no lock.
func (f *ScanFetcher) launch(i int, readahead bool) {
	f.mu.Lock()
	st := &f.states[i]
	if st.launched {
		f.mu.Unlock()
		return
	}
	st.launched = true
	st.done = make(chan struct{})
	if readahead {
		f.stats.ReadaheadGETs++
	} else {
		f.stats.RangeGETs++
	}
	f.mu.Unlock()
	go func() {
		defer close(st.done)
		select {
		case f.sem <- struct{}{}:
		case <-f.ctx.Done():
			st.err = f.ctx.Err()
			return
		}
		defer func() { <-f.sem }()
		r := f.ranges[i]
		data, _, err := f.store.GetRange(f.ctx, r.Obj, r.Off, r.N)
		if err == nil && int64(len(data)) != r.N {
			err = fmt.Errorf("obs1: scan range GET returned %d bytes, want %d", len(data), r.N)
		}
		st.data, st.err = data, err
	}()
}

// Fetch serves ref's block, matching the run iterators' fetch seam.
func (f *ScanFetcher) Fetch(ref DirRef) ([]byte, error) {
	i, ok := f.byOff[ref.ObjKey][ref.Block.Offset]
	if !ok {
		return nil, fmt.Errorf("obs1: block %s@%d is not in the scan plan", ref.ObjKey, ref.Block.Offset)
	}
	f.launch(i, false)
	st := &f.states[i]
	<-st.done
	if st.err != nil {
		return nil, st.err
	}
	r := f.ranges[i]
	off, n := ref.Block.BlockSpan()
	data, err := ParseSegmentBlock(st.data[off-r.Off:off-r.Off+n], ref.Block)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.stats.Blocks++
	if i > f.newest {
		// Consumption moved forward: everything behind the new front is
		// done with, free the buffers.
		for j := max(f.newest, 0); j < i; j++ {
			if f.states[j].launched {
				f.states[j].data = nil
			}
		}
		f.newest = i
	}
	f.mu.Unlock()
	// The midpoint rule: once the served block's end crosses half the
	// range, the window ahead flies while the rest of this one streams.
	// The window is 1 deep by default and Prime widens it, so a primed
	// full scan keeps its fan busy end to end.
	if (off+n-r.Off)*2 >= r.N {
		f.mu.Lock()
		ahead := f.ahead
		f.mu.Unlock()
		for j := i + 1; j <= i+ahead && j < len(f.ranges); j++ {
			f.launch(j, true)
		}
	}
	return data, nil
}

// Prime launches the plan's first n ranges as scan GETs and keeps the
// launch window that deep for the rest of the scan, the full-collection
// mode: an HGETALL primes the fan and streams, while an incremental
// catch-up skips priming and rides the one-deep midpoint readahead. n
// clamps to the fan.
func (f *ScanFetcher) Prime(n int) {
	if n > cap(f.sem) {
		n = cap(f.sem)
	}
	if n < 1 {
		return
	}
	f.mu.Lock()
	f.ahead = n
	f.mu.Unlock()
	for i := 0; i < n && i < len(f.ranges); i++ {
		f.launch(i, false)
	}
}

// Prefetch launches the range holding ref without waiting, the hook the
// run iterators call as they announce the next distinct block.
func (f *ScanFetcher) Prefetch(ref DirRef) {
	if i, ok := f.byOff[ref.ObjKey][ref.Block.Offset]; ok {
		f.launch(i, true)
	}
}

// Stats returns the fetcher's counters.
func (f *ScanFetcher) Stats() ScanFetchStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}
