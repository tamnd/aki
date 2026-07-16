package obs1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDialectDefaultsToS3(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	c := testClient(t, srv)
	if c.dialect.Name != "s3" {
		t.Fatalf("zero-value dialect: got %q, want s3", c.dialect.Name)
	}
}

// gcsStore is a tiny in-memory bucket speaking the GCS XML API's condition
// grammar: x-goog-if-generation-match with 0 meaning create, and the
// generation coming back in x-goog-generation instead of a usable ETag.
type gcsStore struct {
	mu  sync.Mutex
	obj map[string][]byte
	gen map[string]int64
	seq int64
}

func (s *gcsStore) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/b/")
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Match") != "" {
				t.Errorf("S3 condition headers on a GCS wire: %v", r.Header)
			}
			if m := r.Header.Get("x-goog-if-generation-match"); m != "" {
				if !signedHeader(r, "x-goog-if-generation-match") {
					t.Error("condition header not signed")
				}
				want := fmt.Sprint(s.gen[key]) // 0 for a missing key, matching the dialect's create
				if m != want {
					w.WriteHeader(412)
					return
				}
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			s.seq++
			s.obj[key], s.gen[key] = b, s.seq
			w.Header().Set("x-goog-generation", fmt.Sprint(s.seq))
			w.Header().Set("ETag", `"md5-of-body"`) // present but never the CAS token
		case http.MethodGet:
			if s.gen[key] == 0 {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("x-goog-generation", fmt.Sprint(s.gen[key]))
			if _, err := w.Write(s.obj[key]); err != nil {
				t.Error(err)
			}
		case http.MethodDelete:
			delete(s.obj, key)
			delete(s.gen, key)
		}
	}
}

func signedHeader(r *http.Request, name string) bool {
	_, list, ok := strings.Cut(r.Header.Get("Authorization"), "SignedHeaders=")
	if !ok {
		return false
	}
	list, _, _ = strings.Cut(list, ",")
	return slices.Contains(strings.Split(list, ";"), name)
}

// TestDialectGCSWire proves the dialect swap is complete: the GCS client
// sends only x-goog condition headers, signs them, and hands the
// generation back through Info.ETag so callers stay token-agnostic.
func TestDialectGCSWire(t *testing.T) {
	store := &gcsStore{obj: map[string][]byte{}, gen: map[string]int64{}}
	srv := httptest.NewServer(store.handler(t))
	defer srv.Close()
	c, err := NewClient(ClientConfig{
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		Bucket:    "b",
		AccessKey: "AK",
		SecretKey: "SK",
		PathStyle: true,
		Dialect:   DialectGCS,
		Retry:     RetryPolicy{Attempts: 3, Base: time.Millisecond, SlowBase: time.Millisecond, Cap: 5 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	info, err := c.PutIfAbsent(ctx, "chain/00/1", []byte("v1"), WriteTag{Writer: "w1", Batch: "1"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if info.ETag != "1" {
		t.Fatalf("token: got %q, want the generation", info.ETag)
	}
	if _, err := c.PutIfAbsent(ctx, "chain/00/1", []byte("rival"), WriteTag{Writer: "w2", Batch: "1"}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second create: want ErrPrecondition, got %v", err)
	}

	info2, err := c.PutIfMatch(ctx, "chain/00/1", []byte("v2"), info.ETag, WriteTag{Writer: "w1", Batch: "2"})
	if err != nil {
		t.Fatalf("swap: %v", err)
	}
	if info2.ETag == info.ETag || info2.ETag == "" {
		t.Fatalf("generation did not move: %q -> %q", info.ETag, info2.ETag)
	}
	if _, err := c.PutIfMatch(ctx, "chain/00/1", []byte("v3"), info.ETag, WriteTag{Writer: "w1", Batch: "3"}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale swap: want ErrPrecondition, got %v", err)
	}
}

// TestProbeThroughClient runs the probe over the real wire client against
// the S3-shaped fake, the same composition the MinIO and R2 probes use.
func TestProbeThroughClient(t *testing.T) {
	store := newCondStore()
	srv := httptest.NewServer(store.handler(t))
	defer srv.Close()
	c := testClient(t, srv)

	caps, err := ProbeCAS(context.Background(), c, "probe-scratch")
	if err != nil {
		t.Fatal(err)
	}
	if !caps.CASCreate || !caps.CASReplace {
		t.Fatalf("caps: %+v", caps)
	}
	if _, ok := store.obj["probe-scratch/casprobe"]; ok {
		t.Fatal("probe left its scratch object behind")
	}
}
