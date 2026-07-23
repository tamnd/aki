// The async cold-read fetcher (spec 2064/obs1 doc 05 section 5): the f3
// async cold-pread shape retargeted from pread to GET. A caller with a
// keymap locator hands Fetch the key, the locator, and a completion; the
// reader resolves the locator through the group's directory, coalesces
// concurrent intents on the same (segment, block) into one ranged GET
// (single-flight, per node), fetches under a node-wide in-flight cap,
// decodes the block, and extracts each waiter's record from its chunk.
//
// Completions run on the reader's fetch goroutines. The shard seam, not
// this file, owns re-serialization onto the owner (postCompletion) and
// epoch retirement: the reader is epoch-blind by design, because a
// completion for a group the node no longer owns must be dropped where
// the ownership fact lives.
//
// The pool shape follows the #1113 cold-latency lab: the in-flight cap
// taxes the median first, not the p99, so health watching (p50 drift and
// wait quantiles) rides the O4a hedging slice; this slice carries plain
// counters.
package obs1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/tamnd/aki/engine/obs1/store"
)

// ErrColdUnresolved marks a locator the directory cannot resolve: a
// refresh race, never key-absent. The caller retries after the directory
// catches up (or fails over on a lost lease); it must not serve a miss.
var ErrColdUnresolved = errors.New("obs1: cold locator does not resolve")

// ColdRecord is one extracted record. Found false with a nil error means
// the resolved chunk does not hold the key, which serves as a definitive
// miss: the keymap's u64 fingerprint made the locator, so a vanishing
// pair is a fingerprint collision, negligible by the doc 05 arithmetic
// but never allowed to serve a wrong value. Tombstone true reads as
// absent too; the record rode a tombstone run.
type ColdRecord struct {
	Found     bool
	Tombstone bool
	Kind      byte
	Flags     byte
	Value     []byte // copied out of the fetch buffer
}

// ColdReadConfig parameterizes the node's reader.
type ColdReadConfig struct {
	Store Store
	// Dir resolves a group's directory, the boot wiring's accessor.
	Dir func(group uint16) *Directory
	// MaxInFlight caps concurrent GETs node-wide, zero for the default.
	MaxInFlight int
}

// DefaultColdInFlight is the doc 05 elastic ceiling: 64 in-flight GETs
// carry about 1850 cold reads per second per node (#1113).
const DefaultColdInFlight = 64

// ColdReadStats counts the reader's work.
type ColdReadStats struct {
	Fetches    uint64 // intents accepted
	BlockGETs  uint64 // ranged GETs issued
	Attached   uint64 // intents that rode another intent's GET
	Unresolved uint64 // locators the directory could not resolve
	Misses     uint64 // resolved chunks that did not hold the key
	Errs       uint64 // GETs or decodes that failed
}

// coldFlightKey identifies one block fetch: the object and the block's
// offset inside it.
type coldFlightKey struct {
	obj string
	off uint64
}

// coldIntent is one waiter on a flight.
type coldIntent struct {
	key  []byte
	ref  DirRef
	done func(ColdRecord, error)
}

// coldFlight is one in-flight block GET and everyone waiting on it.
type coldFlight struct {
	ref     DirRef
	waiters []coldIntent
}

// ColdReader is the node-wide fetcher. One per node, like the
// single-flight table it carries.
type ColdReader struct {
	cfg ColdReadConfig
	ctx context.Context
	stp context.CancelFunc
	sem chan struct{}

	mu      sync.Mutex
	flights map[coldFlightKey]*coldFlight
	stats   ColdReadStats
	wg      sync.WaitGroup
	closed  bool
}

// NewColdReader validates the config and returns a running reader.
func NewColdReader(cfg ColdReadConfig) (*ColdReader, error) {
	if cfg.Store == nil || cfg.Dir == nil {
		return nil, fmt.Errorf("obs1: cold reader needs a store and a directory accessor")
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = DefaultColdInFlight
	}
	if cfg.MaxInFlight < 1 {
		return nil, fmt.Errorf("obs1: cold reader in-flight cap %d", cfg.MaxInFlight)
	}
	ctx, stp := context.WithCancel(context.Background())
	return &ColdReader{
		cfg: cfg, ctx: ctx, stp: stp,
		sem:     make(chan struct{}, cfg.MaxInFlight),
		flights: make(map[coldFlightKey]*coldFlight),
	}, nil
}

// Fetch registers one read intent and returns immediately; done runs
// exactly once, on a fetch goroutine, with the extracted record or the
// error. Safe from any goroutine.
func (c *ColdReader) Fetch(group uint16, key []byte, loc KeyLoc, done func(ColdRecord, error)) {
	ref, ok := c.cfg.Dir(group).Resolve(loc)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		done(ColdRecord{}, fmt.Errorf("obs1: cold reader is closed"))
		return
	}
	c.stats.Fetches++
	if !ok {
		c.stats.Unresolved++
		c.mu.Unlock()
		done(ColdRecord{}, ErrColdUnresolved)
		return
	}
	fk := coldFlightKey{obj: ref.ObjKey, off: ref.Block.Offset}
	in := coldIntent{key: append([]byte(nil), key...), ref: ref, done: done}
	if fl, live := c.flights[fk]; live {
		fl.waiters = append(fl.waiters, in)
		c.stats.Attached++
		c.mu.Unlock()
		return
	}
	fl := &coldFlight{ref: ref, waiters: []coldIntent{in}}
	c.flights[fk] = fl
	c.stats.BlockGETs++
	c.wg.Add(1)
	c.mu.Unlock()
	go c.fly(fk, fl)
}

// fly executes one flight: cap, GET, decode, settle every waiter.
func (c *ColdReader) fly(fk coldFlightKey, fl *coldFlight) {
	defer c.wg.Done()
	select {
	case c.sem <- struct{}{}:
	case <-c.ctx.Done():
		c.settle(fk, nil, c.ctx.Err())
		return
	}
	defer func() { <-c.sem }()
	off, n := fl.ref.Block.BlockSpan()
	raw, _, err := c.cfg.Store.GetRange(c.ctx, fk.obj, off, n)
	var data []byte
	if err == nil {
		data, err = ParseSegmentBlock(raw, fl.ref.Block)
	}
	c.settle(fk, data, err)
}

// settle removes the flight and completes its waiters, extracting each
// one's record from the decoded block. New intents on the same block
// after this point start a fresh flight, which is what lets a caller
// retry an error without a poisoned table entry.
func (c *ColdReader) settle(fk coldFlightKey, data []byte, err error) {
	c.mu.Lock()
	fl := c.flights[fk]
	delete(c.flights, fk)
	if err != nil {
		c.stats.Errs++
	}
	c.mu.Unlock()
	for _, in := range fl.waiters {
		if err != nil {
			in.done(ColdRecord{}, err)
			continue
		}
		rec, xerr := extractColdRecord(data, in.ref.OffInBlock, in.key)
		if xerr != nil {
			c.count(func(s *ColdReadStats) { s.Errs++ })
		} else if !rec.Found {
			c.count(func(s *ColdReadStats) { s.Misses++ })
		}
		in.done(rec, xerr)
	}
}

func (c *ColdReader) count(f func(*ColdReadStats)) {
	c.mu.Lock()
	f(&c.stats)
	c.mu.Unlock()
}

// Stats snapshots the counters.
func (c *ColdReader) Stats() ColdReadStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Close cancels the context and waits for in-flight completions; every
// waiter hears its done exactly once.
func (c *ColdReader) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.stp()
	c.wg.Wait()
}

// extractColdRecord walks the chunk frame at off inside a decoded block
// and finds key's record. A chunk without the run flag cannot hold
// whole-record keys, so it reads as a decode error here: the keymap only
// ever points at runs.
func extractColdRecord(data []byte, off uint32, key []byte) (ColdRecord, error) {
	if int(off)+4 > len(data) {
		return ColdRecord{}, fmt.Errorf("obs1: chunk offset %d past the block", off)
	}
	total := binary.LittleEndian.Uint32(data[off:])
	if total < 4 || int(off)+int(total) > len(data) {
		return ColdRecord{}, fmt.Errorf("obs1: chunk frame total %d runs past the block", total)
	}
	var outer store.FoldFrame
	if err := store.WalkStagedFrames(data[off:off+total], func(f store.FoldFrame) error {
		outer = f
		return nil
	}); err != nil {
		return ColdRecord{}, err
	}
	if !outer.Chunk || outer.Flags&store.ChunkFlagRun == 0 {
		return ColdRecord{}, fmt.Errorf("obs1: locator points at a non-run chunk (kind 0x%02x)", outer.Kind)
	}
	var rec ColdRecord
	err := store.WalkStagedFrames(outer.Payload, func(r store.FoldFrame) error {
		if !rec.Found && string(r.Key) == string(key) {
			rec = ColdRecord{
				Found: true, Tombstone: r.Tombstone,
				Kind: r.Kind, Flags: r.Flags,
				Value: append([]byte(nil), r.Payload...),
			}
		}
		return nil
	})
	if err != nil {
		return ColdRecord{}, err
	}
	return rec, nil
}
