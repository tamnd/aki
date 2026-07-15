package sqlo1b

import (
	"fmt"
	"io"
	"sync"
)

// The IO layer (doc 04 section 12): one interface, batched
// submission, completions posted to a mailbox, fsync never on the
// submission path. iopool is the portable backend, always available
// and the only one on macOS; the Linux ring backend lands in B5
// behind the same shape. Per R-I4, no code above this layer calls
// pread or pwrite directly.

// IO ops.
const (
	OpRead uint8 = iota + 1
	OpWrite
)

// IO priorities: fg is command-path work, bg is drain, compaction,
// and scrub. Workers serve fg first but never starve bg.
const (
	PrioFG uint8 = iota
	PrioBG
)

// IOReq is one group-aligned read or write against the data file.
// Off is extent-relative; a request never crosses its extent.
type IOReq struct {
	Op   uint8
	Prio uint8
	Ext  uint64
	Off  uint32
	Buf  []byte
	Tag  uint64 // owner continuation slot, echoed on completion
}

// IOResult is posted to the completion mailbox, one per request and
// one per Sync, in no promised order; the owner sequences through
// tags.
type IOResult struct {
	Tag uint64
	Err error
}

// Backend is the ioBackend contract from doc 04 section 12.
type Backend interface {
	Submit(reqs []IOReq)
	Sync(tag uint64)
	Close()
}

// FileIO is what a backend needs from the data file.
type FileIO interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
}

// coalesceMax caps a coalesced run at 16 groups, the measured
// syscall amortization point the ring backend also targets.
const coalesceMax = 16 * GroupSize

// IOPool is the portable backend: a fixed pool of worker goroutines
// issuing pread and pwrite, a submission path that coalesces
// file-adjacent same-op requests into single calls, and a dedicated
// sync goroutine so fsync never blocks a worker or a submitter.
type IOPool struct {
	f          FileIO
	extentSize uint32
	comp       chan<- IOResult
	fg, bg     chan []IOReq
	syncq      chan uint64
	wg         sync.WaitGroup
}

// NewIOPool starts workers plus one sync goroutine. Completions post
// to comp; the caller sizes it to its in-flight window so workers do
// not stall on the mailbox. Close the pool before dropping comp.
func NewIOPool(f FileIO, extentSize uint32, workers int, comp chan<- IOResult) *IOPool {
	p := &IOPool{
		f:          f,
		extentSize: extentSize,
		comp:       comp,
		fg:         make(chan []IOReq, 64),
		bg:         make(chan []IOReq, 64),
		syncq:      make(chan uint64, 16),
	}
	for range workers {
		p.wg.Go(p.worker)
	}
	p.wg.Go(p.syncer)
	return p
}

// abs is a request's absolute file offset.
func (p *IOPool) abs(r *IOReq) int64 {
	return int64(r.Ext)*int64(p.extentSize) + int64(r.Off)
}

// Submit enqueues a batch. Requests that are file-adjacent, same op,
// and same priority coalesce into one run and one syscall; the run
// caps at coalesceMax bytes. Order between runs is not promised; the
// owner orders through tags and Sync. Not safe for concurrent use
// with Close (single-writer discipline).
func (p *IOPool) Submit(reqs []IOReq) {
	for start := 0; start < len(reqs); {
		run := reqs[start : start+1 : start+1]
		end := p.abs(&reqs[start]) + int64(len(reqs[start].Buf))
		bytes := len(reqs[start].Buf)
		for next := start + 1; next < len(reqs); next++ {
			r := &reqs[next]
			if r.Op != run[0].Op || r.Prio != run[0].Prio || p.abs(r) != end ||
				bytes+len(r.Buf) > coalesceMax {
				break
			}
			run = append(run, *r)
			end += int64(len(r.Buf))
			bytes += len(r.Buf)
		}
		if run[0].Prio == PrioFG {
			p.fg <- run
		} else {
			p.bg <- run
		}
		start += len(run)
	}
}

// Sync hands the fsync to the dedicated sync goroutine and returns
// immediately; the completion carries the tag. Writes the sync must
// cover are the ones whose completions the owner already saw.
func (p *IOPool) Sync(tag uint64) { p.syncq <- tag }

// Close stops the pool after draining queued work. The owner must
// not Submit or Sync after calling it.
func (p *IOPool) Close() {
	close(p.fg)
	close(p.bg)
	close(p.syncq)
	p.wg.Wait()
}

// worker serves fg runs before bg without starving either; both
// lanes hit the same file so the split is scheduling, not placement.
func (p *IOPool) worker() {
	for {
		var run []IOReq
		var ok bool
		select {
		case run, ok = <-p.fg:
		default:
			select {
			case run, ok = <-p.fg:
			case run, ok = <-p.bg:
			}
		}
		if !ok {
			// One lane closed; drain the other to nil delivery.
			for run = range p.fg {
				p.serve(run)
			}
			for run = range p.bg {
				p.serve(run)
			}
			return
		}
		if run != nil {
			p.serve(run)
		}
	}
}

// serve issues one coalesced run as a single pread or pwrite,
// scattering or gathering through a scratch buffer when the run has
// more than one request. A 4 KiB memcpy is far cheaper than the
// syscall it saves.
func (p *IOPool) serve(run []IOReq) {
	var err error
	if bad := p.validate(run); bad != nil {
		err = bad
	} else if len(run) == 1 {
		r := &run[0]
		if r.Op == OpRead {
			_, err = p.f.ReadAt(r.Buf, p.abs(r))
		} else {
			_, err = p.f.WriteAt(r.Buf, p.abs(r))
		}
	} else {
		total := 0
		for i := range run {
			total += len(run[i].Buf)
		}
		scratch := make([]byte, total)
		if run[0].Op == OpWrite {
			off := 0
			for i := range run {
				off += copy(scratch[off:], run[i].Buf)
			}
			_, err = p.f.WriteAt(scratch, p.abs(&run[0]))
		} else {
			_, err = p.f.ReadAt(scratch, p.abs(&run[0]))
			if err == nil {
				off := 0
				for i := range run {
					off += copy(run[i].Buf, scratch[off:])
				}
			}
		}
	}
	for i := range run {
		p.comp <- IOResult{Tag: run[i].Tag, Err: err}
	}
}

// validate rejects malformed requests before they touch the file.
func (p *IOPool) validate(run []IOReq) error {
	for i := range run {
		r := &run[i]
		if r.Op != OpRead && r.Op != OpWrite {
			return fmt.Errorf("sqlo1b: io op %d", r.Op)
		}
		if len(r.Buf) == 0 {
			return fmt.Errorf("sqlo1b: empty io buffer at ext %d off %d", r.Ext, r.Off)
		}
		if uint64(r.Off)+uint64(len(r.Buf)) > uint64(p.extentSize) {
			return fmt.Errorf("sqlo1b: io at ext %d off %d len %d crosses the extent", r.Ext, r.Off, len(r.Buf))
		}
	}
	return nil
}

// syncer is the dedicated fsync goroutine: syncs are serialized here
// and never occupy a worker, so writes keep flowing while the device
// flushes.
func (p *IOPool) syncer() {
	for tag := range p.syncq {
		p.comp <- IOResult{Tag: tag, Err: p.f.Sync()}
	}
}
