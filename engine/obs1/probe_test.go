package obs1

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// probeStore fakes just the Store surface ProbeCAS touches; the embedded
// nil Store panics if the probe ever grows a call this fake does not model.
// Behavior knobs reproduce the provider shapes the matrix cares about.
type probeStore struct {
	Store
	create404 bool // MinIO pre-2025-09-07: conditional create on a missing key 404s
	dupWins   bool // duplicate create overwrites silently
	noReplace bool // If-Match ignored: stale swaps land
	ambiguous bool // every conditional write dies on the wire

	obj  []byte
	seq  int
	live string
}

func (f *probeStore) Delete(context.Context, string) error {
	f.obj, f.live = nil, ""
	return nil
}

func (f *probeStore) Put(_ context.Context, _ string, body []byte) (ObjectInfo, error) {
	f.seq++
	f.obj, f.live = body, fmt.Sprintf("e%d", f.seq)
	return ObjectInfo{ETag: f.live, Size: int64(len(body))}, nil
}

func (f *probeStore) PutIfAbsent(ctx context.Context, key string, body []byte, _ WriteTag) (ObjectInfo, error) {
	if f.ambiguous {
		return ObjectInfo{}, fmt.Errorf("%w: PUT %s: wire cut", ErrAmbiguous, key)
	}
	if f.create404 {
		return ObjectInfo{}, fmt.Errorf("%w: PUT %s: NoSuchKey", ErrNotFound, key)
	}
	if f.obj != nil && !f.dupWins {
		return ObjectInfo{}, fmt.Errorf("%w: PUT %s", ErrPrecondition, key)
	}
	return f.Put(ctx, key, body)
}

func (f *probeStore) PutIfMatch(ctx context.Context, key string, body []byte, token string, _ WriteTag) (ObjectInfo, error) {
	if f.ambiguous {
		return ObjectInfo{}, fmt.Errorf("%w: PUT %s: wire cut", ErrAmbiguous, key)
	}
	if !f.noReplace && token != f.live {
		return ObjectInfo{}, fmt.Errorf("%w: PUT %s", ErrPrecondition, key)
	}
	return f.Put(ctx, key, body)
}

func TestProbeFullSupport(t *testing.T) {
	caps, err := ProbeCAS(context.Background(), &probeStore{}, "scratch")
	if err != nil {
		t.Fatal(err)
	}
	if !caps.CASCreate || !caps.CASReplace || caps.CreateNote != "" || caps.ReplaceNote != "" {
		t.Fatalf("caps: %+v", caps)
	}
	if !caps.CanHost() || caps.RootVFallback() {
		t.Fatalf("verdict: %+v", caps)
	}
}

// TestProbeCreate404 is the MinIO 2025-09-06 shape: the create primitive
// is broken but replace still works, and the probe must say exactly that.
func TestProbeCreate404(t *testing.T) {
	caps, err := ProbeCAS(context.Background(), &probeStore{create404: true}, "scratch")
	if err != nil {
		t.Fatal(err)
	}
	if caps.CASCreate || !strings.Contains(caps.CreateNote, "missing key failed") {
		t.Fatalf("create verdict: %+v", caps)
	}
	if !caps.CASReplace {
		t.Fatalf("replace should probe independently: %+v", caps)
	}
	if caps.CanHost() || caps.RootVFallback() {
		t.Fatalf("verdict: %+v", caps)
	}
}

func TestProbeDupWins(t *testing.T) {
	caps, err := ProbeCAS(context.Background(), &probeStore{dupWins: true}, "scratch")
	if err != nil {
		t.Fatal(err)
	}
	if caps.CASCreate || caps.CreateNote != "duplicate create overwrote silently" {
		t.Fatalf("caps: %+v", caps)
	}
}

func TestProbeNoReplace(t *testing.T) {
	caps, err := ProbeCAS(context.Background(), &probeStore{noReplace: true}, "scratch")
	if err != nil {
		t.Fatal(err)
	}
	if !caps.CASCreate || caps.CASReplace || caps.ReplaceNote != "swap with a stale token succeeded" {
		t.Fatalf("caps: %+v", caps)
	}
	if !caps.CanHost() || !caps.RootVFallback() {
		t.Fatalf("verdict: %+v", caps)
	}
}

// TestProbeAmbiguous: a dead network proves nothing about the provider, so
// the probe reports an error instead of a verdict.
func TestProbeAmbiguous(t *testing.T) {
	if _, err := ProbeCAS(context.Background(), &probeStore{ambiguous: true}, "scratch"); err == nil {
		t.Fatal("want an inconclusive error")
	}
}
