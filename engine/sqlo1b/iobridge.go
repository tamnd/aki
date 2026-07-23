package sqlo1b

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// The store's seam onto the IO backend (doc 04 section 12, R-I4):
// live group reads go through Backend.Submit instead of pread, so the
// ring backend slots in under the store without the read path knowing
// which backend runs. Recovery-time readers (loadStoreDirectory,
// readSnapshot) stay on fileGroups because they run before the store,
// and therefore the backend, exists.

// storeIOWorkers sizes the iopool under the store. Provisional until
// the ringpool sweep on the gate box prices the pool.
const storeIOWorkers = 4

// storeIOComp sizes the completion mailbox; NewIOPool asks the caller
// to cover its in-flight window so workers never stall posting.
const storeIOComp = 64

// storeRingDepth is the ring's SQ depth under the store. Provisional
// like the worker count; the ringpool sweep prices it.
const storeRingDepth = 64

// ringSelfTestTimeout bounds the startup self-test read. A ring that
// sets up but never completes is broken in a way teardown could hang
// on, so the deadline abandons it instead of joining it.
const ringSelfTestTimeout = 2 * time.Second

// ForceIOPool pins the store to the iopool backend regardless of ring
// support, the gate's arm switch for the B5 on/off delta note. Set it
// before opening a store; sqlo1srv wires it to -io-backend.
var ForceIOPool bool

// IOBridge turns the asynchronous Backend contract into the
// synchronous reads the store's lookup path wants: each Read takes a
// fresh tag and a one-slot reply channel and blocks until the router
// goroutine hands it its completion.
type IOBridge struct {
	b         Backend
	comp      chan IOResult
	seq       atomic.Uint64
	done      chan struct{}
	closeOnce sync.Once

	mu   sync.Mutex
	wait map[uint64]chan IOResult
}

// NewIOBridge owns comp from here on: the backend posts to it, the
// router drains it, and Close tears both down in that order.
func NewIOBridge(b Backend, comp chan IOResult) *IOBridge {
	br := &IOBridge{
		b:    b,
		comp: comp,
		done: make(chan struct{}),
		wait: map[uint64]chan IOResult{},
	}
	go br.route()
	return br
}

// route delivers completions to their waiting readers. A tag with no
// waiter is dropped; that only happens for completions nobody blocked
// on, which the bridge never issues today.
func (br *IOBridge) route() {
	defer close(br.done)
	for res := range br.comp {
		br.mu.Lock()
		ch := br.wait[res.Tag]
		delete(br.wait, res.Tag)
		br.mu.Unlock()
		if ch != nil {
			ch <- res
		}
	}
}

// Read issues one extent-relative read at prio and blocks for its
// completion.
func (br *IOBridge) Read(prio uint8, ext uint64, off uint32, buf []byte) error {
	tag := br.seq.Add(1)
	ch := make(chan IOResult, 1)
	br.mu.Lock()
	br.wait[tag] = ch
	br.mu.Unlock()
	br.b.Submit([]IOReq{{Op: OpRead, Prio: prio, Ext: ext, Off: off, Buf: buf, Tag: tag}})
	return (<-ch).Err
}

// Close stops the backend first, which drains its queues and posts
// every remaining completion, then closes the mailbox so the router
// exits. No Read may be in flight or follow. Idempotent, because
// Store.Close runs twice under test rigs that also close on cleanup.
func (br *IOBridge) Close() {
	br.closeOnce.Do(func() {
		br.b.Close()
		close(br.comp)
	})
	<-br.done
}

// backendGroups is fileGroups through the bridge: identical group
// geometry, but the offset is extent-relative per the IOReq contract
// and the read rides the backend at the caller's priority.
type backendGroups struct {
	br   *IOBridge
	prio uint8
}

func (g backendGroups) ReadGroup(ext uint64, grp uint16) ([]byte, error) {
	off := uint32(grp) * GroupSize
	n := GroupSize
	if grp == 0 {
		off += ExtentHeaderSize
		n = Group0Payload
	}
	b := make([]byte, n)
	if err := g.br.Read(g.prio, ext, off, b); err != nil {
		return nil, fmt.Errorf("sqlo1b: group %d/%d: %w", ext, grp, err)
	}
	return b, nil
}
