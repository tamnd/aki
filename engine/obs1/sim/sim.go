// Package sim is E-sim (spec 2064/obs1 doc 10): a deterministic in-process
// object store implementing obs1.Store with byte-accurate conditional-write
// semantics, a pluggable latency model fitted to the doc 01 section 2.2
// distributions, the doc 09 price table as data, and fault injection.
//
// Determinism: all randomness comes from one seeded generator and draws
// happen in call order, so a single-goroutine script replays exactly under
// the same seed. Concurrent callers are safe (one mutex) but their
// interleaving is the scheduler's, same as a real bucket.
//
// The sim sits at the Store surface, above the wire client's retry loop:
// an injected fault is the outcome the caller sees after retries, and
// usage counts Store operations, not wire attempts. The doc 10 honesty
// gate owns closing that gap if it ever matters to a bill.
package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/obs1"
)

// Config wires a Sim. The zero value is a fast, faultless, empty bucket.
type Config struct {
	Seed    uint64
	Latency LatencyModel
	Fault   FaultFn
}

type object struct {
	body  []byte
	token string
	tag   obs1.WriteTag
}

func (o *object) info() obs1.ObjectInfo {
	return obs1.ObjectInfo{ETag: o.token, Size: int64(len(o.body)), Tag: o.tag}
}

type part struct {
	body []byte
	etag string
}

type upload struct {
	key   string
	tag   obs1.WriteTag
	parts map[int]part
}

// Sim is one in-memory bucket. Safe for concurrent use.
type Sim struct {
	mu    sync.Mutex
	rng   *rand.Rand
	cfg   Config
	obj   map[string]*object
	up    map[string]*upload
	seq   int64
	usage Usage
	sleep func(context.Context, time.Duration) error // test hook
}

var _ obs1.Store = (*Sim)(nil)

// New builds a Sim from cfg.
func New(cfg Config) *Sim {
	return &Sim{
		rng:   rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		cfg:   cfg,
		obj:   map[string]*object{},
		up:    map[string]*upload{},
		sleep: realSleep,
	}
}

// Usage returns a snapshot of the run's counters.
func (s *Sim) Usage() Usage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usage
}

func realSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// begin draws the op's latency and fault under the lock, then sleeps
// outside it, so concurrent ops overlap the way real requests do. A
// cancelled sleep returns ctx.Err with nothing applied, matching the wire
// client, whose attempt loop hands back ctx.Err when the caller gave up.
func (s *Sim) begin(ctx context.Context, op Op, key string, read bool) (*Fault, error) {
	s.mu.Lock()
	dist := s.cfg.Latency.Put
	if read {
		dist = s.cfg.Latency.Get
	}
	d := dist.draw(s.rng.NormFloat64())
	var f *Fault
	if s.cfg.Fault != nil {
		f = s.cfg.Fault(op, key)
	}
	if f != nil {
		d += f.Extra
	}
	s.mu.Unlock()
	if err := s.sleep(ctx, d); err != nil {
		return nil, err
	}
	return f, nil
}

// applyPut commits a whole object, moving the CAS token.
func (s *Sim) applyPut(key string, body []byte, tag obs1.WriteTag) obs1.ObjectInfo {
	if old, ok := s.obj[key]; ok {
		s.usage.BytesStored -= int64(len(old.body))
	}
	s.seq++
	o := &object{
		body:  append([]byte(nil), body...),
		token: fmt.Sprintf(`"sim-%d"`, s.seq),
		tag:   tag,
	}
	s.obj[key] = o
	s.usage.BytesStored += int64(len(body))
	s.usage.BytesStoredPeak = max(s.usage.BytesStoredPeak, s.usage.BytesStored)
	return o.info()
}

// Get fetches a whole object. A missing key is obs1.ErrNotFound.
func (s *Sim) Get(ctx context.Context, key string) ([]byte, obs1.ObjectInfo, error) {
	f, err := s.begin(ctx, OpGet, key, true)
	if err != nil {
		return nil, obs1.ObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.GetRequests++
	if f != nil && f.Err != nil {
		return nil, obs1.ObjectInfo{}, f.Err
	}
	o, ok := s.obj[key]
	if !ok {
		return nil, obs1.ObjectInfo{}, fmt.Errorf("sim: GET %s: %w", key, obs1.ErrNotFound)
	}
	s.usage.BytesDown += int64(len(o.body))
	return append([]byte(nil), o.body...), o.info(), nil
}

// GetRange fetches n bytes at off. Past-end ranges truncate like S3; a
// range starting at or beyond the end, or any range of an empty object,
// is obs1.ErrRange.
func (s *Sim) GetRange(ctx context.Context, key string, off, n int64) ([]byte, obs1.ObjectInfo, error) {
	if off < 0 || n <= 0 {
		return nil, obs1.ObjectInfo{}, fmt.Errorf("sim: bad range %d+%d for %s", off, n, key)
	}
	return s.getRange(ctx, key, func(size int64) (int64, int64, bool) {
		if off >= size {
			return 0, 0, false
		}
		return off, min(off+n, size), true
	})
}

// GetTail fetches the last n bytes, the footer read a taking-over node
// does before it has a manifest.
func (s *Sim) GetTail(ctx context.Context, key string, n int64) ([]byte, obs1.ObjectInfo, error) {
	if n <= 0 {
		return nil, obs1.ObjectInfo{}, fmt.Errorf("sim: bad tail %d for %s", n, key)
	}
	return s.getRange(ctx, key, func(size int64) (int64, int64, bool) {
		if size == 0 {
			return 0, 0, false
		}
		return max(0, size-n), size, true
	})
}

func (s *Sim) getRange(ctx context.Context, key string, plan func(size int64) (int64, int64, bool)) ([]byte, obs1.ObjectInfo, error) {
	f, err := s.begin(ctx, OpGet, key, true)
	if err != nil {
		return nil, obs1.ObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.GetRequests++
	if f != nil && f.Err != nil {
		return nil, obs1.ObjectInfo{}, f.Err
	}
	o, ok := s.obj[key]
	if !ok {
		return nil, obs1.ObjectInfo{}, fmt.Errorf("sim: GET %s: %w", key, obs1.ErrNotFound)
	}
	lo, hi, ok := plan(int64(len(o.body)))
	if !ok {
		return nil, obs1.ObjectInfo{}, fmt.Errorf("sim: GET %s: %w", key, obs1.ErrRange)
	}
	b := append([]byte(nil), o.body[lo:hi]...)
	s.usage.BytesDown += int64(len(b))
	info := o.info()
	info.Size = int64(len(b))
	return b, info, nil
}

// Put writes a whole object unconditionally, last writer wins.
func (s *Sim) Put(ctx context.Context, key string, body []byte) (obs1.ObjectInfo, error) {
	return s.write(ctx, OpPut, key, body, obs1.WriteTag{}, func() error { return nil })
}

// PutIfAbsent creates key only if nothing is there. An object already
// present is obs1.ErrPrecondition.
func (s *Sim) PutIfAbsent(ctx context.Context, key string, body []byte, tag obs1.WriteTag) (obs1.ObjectInfo, error) {
	return s.write(ctx, OpPutIfAbsent, key, body, tag, func() error {
		if _, ok := s.obj[key]; ok {
			return fmt.Errorf("sim: PUT %s: %w", key, obs1.ErrPrecondition)
		}
		return nil
	})
}

// PutIfMatch replaces key only while its token still matches. A missing
// key is obs1.ErrNotFound (what AWS and MinIO answer, verified live); a
// stale token is obs1.ErrPrecondition.
func (s *Sim) PutIfMatch(ctx context.Context, key string, body []byte, token string, tag obs1.WriteTag) (obs1.ObjectInfo, error) {
	return s.write(ctx, OpPutIfMatch, key, body, tag, func() error {
		o, ok := s.obj[key]
		if !ok {
			return fmt.Errorf("sim: PUT %s: %w", key, obs1.ErrNotFound)
		}
		if o.token != token {
			return fmt.Errorf("sim: PUT %s: %w", key, obs1.ErrPrecondition)
		}
		return nil
	})
}

// write is the shared conditional-put path. The condition is evaluated at
// commit time, after the latency sleep, the way a real bucket evaluates it
// at the moment the request lands. An injected fault with Applied commits
// the write (condition permitting) and then loses the response, which is
// exactly the shape Recheck exists for.
func (s *Sim) write(ctx context.Context, op Op, key string, body []byte, tag obs1.WriteTag, cond func() error) (obs1.ObjectInfo, error) {
	f, err := s.begin(ctx, op, key, false)
	if err != nil {
		return obs1.ObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.PutRequests++
	s.usage.BytesUp += int64(len(body))
	if f != nil && f.Err != nil {
		if f.Applied && cond() == nil {
			s.applyPut(key, body, tag)
		}
		return obs1.ObjectInfo{}, f.Err
	}
	if err := cond(); err != nil {
		return obs1.ObjectInfo{}, err
	}
	return s.applyPut(key, body, tag), nil
}

// Recheck resolves an ambiguous write by re-reading and comparing tags,
// the same doc 02 section 2.4 pass the wire client implements.
func (s *Sim) Recheck(ctx context.Context, key string, tag obs1.WriteTag) (obs1.RecheckOutcome, []byte, obs1.ObjectInfo, error) {
	if tag == (obs1.WriteTag{}) {
		return 0, nil, obs1.ObjectInfo{}, fmt.Errorf("sim: Recheck needs a non-zero tag")
	}
	body, info, err := s.Get(ctx, key)
	switch {
	case errors.Is(err, obs1.ErrNotFound):
		return obs1.RecheckAbsent, nil, obs1.ObjectInfo{}, nil
	case err != nil:
		return 0, nil, obs1.ObjectInfo{}, err
	case info.Tag == tag:
		return obs1.RecheckOurs, body, info, nil
	}
	return obs1.RecheckOther, body, info, nil
}

// Delete removes a key; deleting a missing key succeeds, matching S3.
func (s *Sim) Delete(ctx context.Context, key string) error {
	f, err := s.begin(ctx, OpDelete, key, false)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.FreeRequests++
	if f != nil && f.Err != nil {
		if f.Applied {
			s.remove(key)
		}
		return f.Err
	}
	s.remove(key)
	return nil
}

func (s *Sim) remove(key string) {
	if o, ok := s.obj[key]; ok {
		s.usage.BytesStored -= int64(len(o.body))
		delete(s.obj, key)
	}
}

// DeleteObjects removes up to 1000 keys in one request, mirroring the
// client's surface: empty is a no-op, oversize is the caller's bug.
func (s *Sim) DeleteObjects(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) > 1000 {
		return fmt.Errorf("sim: DeleteObjects: %d keys, the API caps a batch at 1000", len(keys))
	}
	f, err := s.begin(ctx, OpDeleteObjects, keys[0], false)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.FreeRequests++
	if f != nil && f.Err != nil {
		if f.Applied {
			for _, k := range keys {
				s.remove(k)
			}
		}
		return f.Err
	}
	for _, k := range keys {
		s.remove(k)
	}
	return nil
}

// CreateMultipart starts an upload; the tag lands on the final object.
func (s *Sim) CreateMultipart(ctx context.Context, key string, tag obs1.WriteTag) (string, error) {
	f, err := s.begin(ctx, OpCreateMultipart, key, false)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.PutRequests++
	start := func() string {
		s.seq++
		id := fmt.Sprintf("sim-up-%d", s.seq)
		s.up[id] = &upload{key: key, tag: tag, parts: map[int]part{}}
		return id
	}
	if f != nil && f.Err != nil {
		if f.Applied {
			start() // an orphan upload, the doc 06 sweeper's problem
		}
		return "", f.Err
	}
	return start(), nil
}

// UploadPart stores one part; unknown upload ids are obs1.ErrNotFound.
func (s *Sim) UploadPart(ctx context.Context, key, uploadID string, n int, body []byte) (string, error) {
	f, err := s.begin(ctx, OpUploadPart, key, false)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.PutRequests++
	s.usage.BytesUp += int64(len(body))
	if f != nil && f.Err != nil {
		return "", f.Err // parts replay, so a lost response never surfaces
	}
	u, ok := s.up[uploadID]
	if !ok || u.key != key {
		return "", fmt.Errorf("sim: UploadPart %s %s: %w", key, uploadID, obs1.ErrNotFound)
	}
	s.seq++
	etag := fmt.Sprintf(`"sim-part-%d"`, s.seq)
	u.parts[n] = part{body: append([]byte(nil), body...), etag: etag}
	return etag, nil
}

// CompleteMultipart stitches the named parts, in ascending part order like
// S3 requires, and commits the object with the create-time tag. Completing
// a finished or unknown upload is obs1.ErrNotFound.
func (s *Sim) CompleteMultipart(ctx context.Context, key, uploadID string, parts []obs1.Part) (obs1.ObjectInfo, error) {
	f, err := s.begin(ctx, OpCompleteMultipart, key, false)
	if err != nil {
		return obs1.ObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.PutRequests++
	commit := func() (obs1.ObjectInfo, error) {
		u, ok := s.up[uploadID]
		if !ok || u.key != key {
			return obs1.ObjectInfo{}, fmt.Errorf("sim: CompleteMultipart %s %s: %w", key, uploadID, obs1.ErrNotFound)
		}
		var body []byte
		last := 0
		for _, p := range parts {
			if p.N <= last {
				return obs1.ObjectInfo{}, fmt.Errorf("sim: CompleteMultipart %s: InvalidPartOrder at part %d", key, p.N)
			}
			last = p.N
			got, ok := u.parts[p.N]
			if !ok || got.etag != p.ETag {
				return obs1.ObjectInfo{}, fmt.Errorf("sim: CompleteMultipart %s: InvalidPart %d", key, p.N)
			}
			body = append(body, got.body...)
		}
		delete(s.up, uploadID)
		return s.applyPut(key, body, u.tag), nil
	}
	if f != nil && f.Err != nil {
		if f.Applied {
			if _, err := commit(); err != nil {
				return obs1.ObjectInfo{}, fmt.Errorf("sim: %w: and the lost complete had failed: %w", f.Err, err)
			}
		}
		return obs1.ObjectInfo{}, f.Err
	}
	return commit()
}

// AbortMultipart drops an upload; unknown ids are obs1.ErrNotFound, which
// the sweeper treats as done.
func (s *Sim) AbortMultipart(ctx context.Context, key, uploadID string) error {
	f, err := s.begin(ctx, OpAbortMultipart, key, false)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage.FreeRequests++
	if f != nil && f.Err != nil {
		return f.Err
	}
	u, ok := s.up[uploadID]
	if !ok || u.key != key {
		return fmt.Errorf("sim: AbortMultipart %s %s: %w", key, uploadID, obs1.ErrNotFound)
	}
	delete(s.up, uploadID)
	return nil
}
