package obs1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// condStore is a tiny in-memory bucket honoring If-None-Match: * and
// If-Match, echoing write tags back as metadata, the way S3 does.
type condStore struct {
	mu   sync.Mutex
	obj  map[string][]byte
	etag map[string]string
	meta map[string]http.Header
	seq  int
}

func newCondStore() *condStore {
	return &condStore{obj: map[string][]byte{}, etag: map[string]string{}, meta: map[string]http.Header{}}
}

func (s *condStore) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/b/")
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") == "*" {
				if _, ok := s.obj[key]; ok {
					w.WriteHeader(412)
					return
				}
			}
			if m := r.Header.Get("If-Match"); m != "" {
				if s.etag[key] != m {
					w.WriteHeader(412)
					return
				}
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			s.seq++
			s.obj[key] = b
			s.etag[key] = fmt.Sprintf(`"e%d"`, s.seq)
			meta := http.Header{}
			for _, h := range []string{writerHeader, batchHeader} {
				if v := r.Header.Get(h); v != "" {
					meta.Set(h, v)
				}
			}
			s.meta[key] = meta
			w.Header().Set("ETag", s.etag[key])
		case http.MethodGet:
			b, ok := s.obj[key]
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("ETag", s.etag[key])
			maps.Copy(w.Header(), s.meta[key])
			if _, err := w.Write(b); err != nil {
				t.Error(err)
			}
		case http.MethodDelete:
			delete(s.obj, key)
			delete(s.etag, key)
			delete(s.meta, key)
			w.WriteHeader(204)
		}
	}
}

func TestPutIfAbsent(t *testing.T) {
	store := newCondStore()
	srv := httptest.NewServer(store.handler(t))
	defer srv.Close()
	c := testClient(t, srv)
	ctx := context.Background()
	tag := WriteTag{Writer: "w1", Batch: "7"}

	info, err := c.PutIfAbsent(ctx, "chain/00/7", []byte("batch"), tag)
	if err != nil || info.ETag == "" {
		t.Fatalf("create: %+v %v", info, err)
	}
	if _, err := c.PutIfAbsent(ctx, "chain/00/7", []byte("rival"), WriteTag{Writer: "w2", Batch: "7"}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second create: want ErrPrecondition, got %v", err)
	}
	body, got, err := c.Get(ctx, "chain/00/7")
	if err != nil || string(body) != "batch" {
		t.Fatalf("get: %q %v", body, err)
	}
	if got.Tag != tag {
		t.Fatalf("tag round trip: got %+v, want %+v", got.Tag, tag)
	}
}

func TestPutIfMatch(t *testing.T) {
	store := newCondStore()
	srv := httptest.NewServer(store.handler(t))
	defer srv.Close()
	c := testClient(t, srv)
	ctx := context.Background()

	info, err := c.Put(ctx, "manifest", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	info2, err := c.PutIfMatch(ctx, "manifest", []byte("v2"), info.ETag, WriteTag{Writer: "w1", Batch: "2"})
	if err != nil {
		t.Fatalf("swap with fresh etag: %v", err)
	}
	// The first ETag is now stale; the swap must lose.
	if _, err := c.PutIfMatch(ctx, "manifest", []byte("v3"), info.ETag, WriteTag{Writer: "w1", Batch: "3"}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale swap: want ErrPrecondition, got %v", err)
	}
	body, cur, err := c.Get(ctx, "manifest")
	if err != nil || string(body) != "v2" || cur.ETag != info2.ETag {
		t.Fatalf("winner: %q %+v %v", body, cur, err)
	}
}

// The write tag must be part of the signature, not decoration: if the meta
// headers were unsigned, a provider would reject or strip them.
func TestTagHeadersAreSigned(t *testing.T) {
	var auth atomic.Value
	store := newCondStore()
	h := store.handler(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			auth.Store(r.Header.Get("Authorization"))
		}
		h(w, r)
	}))
	defer srv.Close()
	c := testClient(t, srv)

	if _, err := c.PutIfAbsent(context.Background(), "k", []byte("x"), WriteTag{Writer: "w1", Batch: "1"}); err != nil {
		t.Fatal(err)
	}
	a, _ := auth.Load().(string)
	for _, h := range []string{"if-none-match", writerHeader, batchHeader} {
		if !strings.Contains(a, h) {
			t.Errorf("SignedHeaders is missing %s: %s", h, a)
		}
	}
}

// A 409 (concurrent conditional write settling) surfaces as ErrConflict on
// the first attempt; the append protocol re-reads, the client never replays.
func TestConflictNotReplayed(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(409)
		if _, err := w.Write([]byte(`<Error><Code>ConditionalRequestConflict</Code></Error>`)); err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()
	c := testClient(t, srv)

	_, err := c.PutIfAbsent(context.Background(), "k", []byte("x"), WriteTag{Writer: "w1", Batch: "1"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("conflict must not be replayed, calls = %d", calls.Load())
	}
}

func TestRecheck(t *testing.T) {
	store := newCondStore()
	srv := httptest.NewServer(store.handler(t))
	defer srv.Close()
	c := testClient(t, srv)
	ctx := context.Background()
	ours := WriteTag{Writer: "w1", Batch: "7"}

	// Absent: nothing landed, the PUT is safe to retry.
	out, _, _, err := c.Recheck(ctx, "chain/00/7", ours)
	if err != nil || out != RecheckAbsent {
		t.Fatalf("absent: %v %v", out, err)
	}

	// Ours: the ambiguous PUT actually landed.
	if _, err := c.PutIfAbsent(ctx, "chain/00/7", []byte("batch"), ours); err != nil {
		t.Fatal(err)
	}
	out, body, info, err := c.Recheck(ctx, "chain/00/7", ours)
	if err != nil || out != RecheckOurs || string(body) != "batch" || info.Tag != ours {
		t.Fatalf("ours: %v %q %+v %v", out, body, info, err)
	}

	// Other: a rival's object is there; the body comes back so the caller
	// can apply it before retrying at the next seq.
	out, body, info, err = c.Recheck(ctx, "chain/00/7", WriteTag{Writer: "w2", Batch: "7"})
	if err != nil || out != RecheckOther || string(body) != "batch" || info.Tag != ours {
		t.Fatalf("other: %v %q %+v %v", out, body, info, err)
	}

	// A zero tag can never self-recognize; refuse it loudly.
	if _, _, _, err := c.Recheck(ctx, "chain/00/7", WriteTag{}); err == nil {
		t.Fatal("zero tag must error")
	}
}

// End to end through the ambiguity rule: the PUT lands server-side but the
// response never arrives, the client reports ErrAmbiguous without a replay,
// and Recheck recognizes the write as ours.
func TestAmbiguousCreateThenRecheck(t *testing.T) {
	store := newCondStore()
	h := store.handler(t)
	var puts atomic.Int32
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && puts.Add(1) == 1 {
			h(httptest.NewRecorder(), r) // the store takes the write
			<-block                      // the response never makes it back
			return
		}
		h(w, r)
	}))
	// Unblock the handler before srv.Close waits on it (defers run LIFO).
	defer srv.Close()
	defer close(block)
	c := testClient(t, srv)
	c.attemptTimeout = 50 * time.Millisecond
	ctx := context.Background()
	ours := WriteTag{Writer: "w1", Batch: "7"}

	_, err := c.PutIfAbsent(ctx, "chain/00/7", []byte("batch"), ours)
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("want ErrAmbiguous, got %v", err)
	}
	out, body, _, err := c.Recheck(ctx, "chain/00/7", ours)
	if err != nil || out != RecheckOurs || string(body) != "batch" {
		t.Fatalf("recheck: %v %q %v", out, body, err)
	}
	if puts.Load() != 1 {
		t.Fatalf("the ambiguous PUT must not be replayed, puts = %d", puts.Load())
	}
}
