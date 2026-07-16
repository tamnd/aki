package obs1

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := NewClient(ClientConfig{
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		Bucket:    "b",
		AccessKey: "AK",
		SecretKey: "SK",
		PathStyle: true,
		Retry:     RetryPolicy{Attempts: 3, Base: time.Millisecond, SlowBase: time.Millisecond, Cap: 5 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGetPutDelete(t *testing.T) {
	store := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/b/")
		switch r.Method {
		case http.MethodPut:
			b := make([]byte, r.ContentLength)
			if _, err := r.Body.Read(b); err != nil && err.Error() != "EOF" {
				t.Error(err)
			}
			store[key] = b
			w.Header().Set("ETag", `"e1"`)
		case http.MethodGet:
			b, ok := store[key]
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("ETag", `"e1"`)
			if _, err := w.Write(b); err != nil {
				t.Error(err)
			}
		case http.MethodDelete:
			delete(store, key)
			w.WriteHeader(204)
		}
	}))
	defer srv.Close()
	c := testClient(t, srv)
	ctx := context.Background()

	if _, err := c.Put(ctx, "chain/00/1", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	body, info, err := c.Get(ctx, "chain/00/1")
	if err != nil || string(body) != "hello" || info.ETag != `"e1"` {
		t.Fatalf("get: %q %+v %v", body, info, err)
	}
	if err := c.Delete(ctx, "chain/00/1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Get(ctx, "chain/00/1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	// Deleting a missing key succeeds, matching S3.
	if err := c.Delete(ctx, "never-existed"); err != nil {
		t.Fatal(err)
	}
}

// A 500 answer is replayed and the request succeeds on the next attempt;
// the loop gives up once Attempts is spent.
func TestRetryOn5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(500)
			return
		}
	}))
	defer srv.Close()
	c := testClient(t, srv)

	if _, _, err := c.Get(context.Background(), "k"); err != nil {
		t.Fatalf("second attempt should have won: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestAttemptsExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		if _, err := w.Write([]byte(`<Error><Code>SlowDown</Code><Message>slow</Message></Error>`)); err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()
	c := testClient(t, srv)

	_, _, err := c.Get(context.Background(), "k")
	if !errors.Is(err, ErrSlowDown) {
		t.Fatalf("want ErrSlowDown, got %v", err)
	}
}

// The CAS-visible conditions surface immediately, no retries burned.
func TestNoRetryOnPrecondition(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(412)
	}))
	defer srv.Close()
	c := testClient(t, srv)

	_, err := c.Put(context.Background(), "k", []byte("x"))
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("want ErrPrecondition, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

// A PUT whose attempt dies on the wire is ambiguous: surfaced, not replayed.
// A GET in the same spot is replayed. The server hangs past the attempt
// timeout to simulate the cut.
func TestAmbiguousPut(t *testing.T) {
	var calls atomic.Int32
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 && r.Method == http.MethodPut {
			<-block
			return
		}
	}))
	// Unblock the handler before srv.Close waits on it (defers run LIFO).
	defer srv.Close()
	defer close(block)
	c := testClient(t, srv)
	c.attemptTimeout = 50 * time.Millisecond

	_, err := c.Put(context.Background(), "k", []byte("x"))
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("want ErrAmbiguous, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("an ambiguous PUT must not be replayed, calls = %d", calls.Load())
	}
}

func TestErrorTaxonomy(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   error
		retry  bool
	}{
		{404, "", ErrNotFound, false},
		{412, "", ErrPrecondition, false},
		{409, `<Error><Code>ConditionalRequestConflict</Code></Error>`, ErrConflict, false},
		{503, `<Error><Code>SlowDown</Code></Error>`, ErrSlowDown, true},
		{500, `<Error><Code>InternalError</Code></Error>`, nil, true},
		{400, `<Error><Code>RequestTimeout</Code></Error>`, nil, true},
		{403, `<Error><Code>AccessDenied</Code></Error>`, nil, false},
	}
	for _, tc := range cases {
		err := storeErr(tc.status, []byte(tc.body), "rid")
		if tc.want != nil && !errors.Is(err, tc.want) {
			t.Errorf("status %d: want %v, got %v", tc.status, tc.want, err)
		}
		if got := retryable(err); got != tc.retry {
			t.Errorf("status %d %s: retryable = %v, want %v", tc.status, tc.body, got, tc.retry)
		}
	}
}

func TestBackoffWindows(t *testing.T) {
	p := RetryPolicy{Attempts: 5, Base: 10 * time.Millisecond, SlowBase: 100 * time.Millisecond, Cap: 40 * time.Millisecond}
	for range 200 {
		if d := p.backoff(1, false); d > 10*time.Millisecond {
			t.Fatalf("first retry backoff %v above base window", d)
		}
		if d := p.backoff(4, false); d > 40*time.Millisecond {
			t.Fatalf("late backoff %v above cap", d)
		}
		if d := p.backoff(1, true); d > 40*time.Millisecond {
			t.Fatalf("slow backoff %v above cap", d)
		}
	}
}
