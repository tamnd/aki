package obs1

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mpStore fakes the multipart lifecycle: initiate, parts, complete, abort,
// with the create-time metadata landing on the final object.
type mpStore struct {
	mu      sync.Mutex
	nextID  int
	uploads map[string]map[int][]byte // uploadID -> part -> bytes
	meta    map[string]http.Header    // uploadID -> metadata from initiate
	objects map[string][]byte
	objMeta map[string]http.Header
}

func newMPStore() *mpStore {
	return &mpStore{
		uploads: map[string]map[int][]byte{},
		meta:    map[string]http.Header{},
		objects: map[string][]byte{},
		objMeta: map[string]http.Header{},
	}
}

func (s *mpStore) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			s.nextID++
			id := fmt.Sprintf("up-%d", s.nextID)
			s.uploads[id] = map[int][]byte{}
			meta := http.Header{}
			for _, h := range []string{writerHeader, batchHeader} {
				if v := r.Header.Get(h); v != "" {
					meta.Set(h, v)
				}
			}
			s.meta[id] = meta
			fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
		case r.Method == http.MethodPut && q.Has("uploadId"):
			id := q.Get("uploadId")
			up, ok := s.uploads[id]
			if !ok {
				w.WriteHeader(404)
				return
			}
			n, _ := strconv.Atoi(q.Get("partNumber"))
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			up[n] = b
			w.Header().Set("ETag", fmt.Sprintf(`"part-%s-%d"`, id, n))
		case r.Method == http.MethodPost && q.Has("uploadId"):
			id := q.Get("uploadId")
			up, ok := s.uploads[id]
			if !ok {
				w.WriteHeader(404)
				fmt.Fprint(w, `<Error><Code>NoSuchUpload</Code></Error>`)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			var cr completeReq
			if err := xml.Unmarshal(body, &cr); err != nil {
				t.Error(err)
			}
			var all []byte
			for _, p := range cr.Parts {
				if want := fmt.Sprintf(`"part-%s-%d"`, id, p.PartNumber); p.ETag != want {
					fmt.Fprint(w, `<Error><Code>InvalidPart</Code></Error>`)
					return
				}
				all = append(all, up[p.PartNumber]...)
			}
			key := r.URL.Path[len("/b/"):]
			s.objects[key] = all
			s.objMeta[key] = s.meta[id]
			delete(s.uploads, id)
			fmt.Fprintf(w, `<CompleteMultipartUploadResult><ETag>"mp-%s"</ETag></CompleteMultipartUploadResult>`, id)
		case r.Method == http.MethodDelete && q.Has("uploadId"):
			id := q.Get("uploadId")
			if _, ok := s.uploads[id]; !ok {
				w.WriteHeader(404)
				return
			}
			delete(s.uploads, id)
			w.WriteHeader(204)
		case r.Method == http.MethodGet:
			key := r.URL.Path[len("/b/"):]
			b, ok := s.objects[key]
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("ETag", `"whole"`)
			maps.Copy(w.Header(), s.objMeta[key])
			if _, err := w.Write(b); err != nil {
				t.Error(err)
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(400)
		}
	}
}

func TestMultipartRoundTrip(t *testing.T) {
	store := newMPStore()
	srv := httptest.NewServer(store.handler(t))
	defer srv.Close()
	c := testClient(t, srv)
	ctx := context.Background()
	tag := WriteTag{Writer: "folder-1", Batch: "seg-9"}

	id, err := c.CreateMultipart(ctx, "seg/00/9", tag)
	if err != nil || id == "" {
		t.Fatalf("create: %q %v", id, err)
	}
	var parts []Part
	for n, chunk := range []string{"first half ", "second half"} {
		etag, err := c.UploadPart(ctx, "seg/00/9", id, n+1, []byte(chunk))
		if err != nil || etag == "" {
			t.Fatalf("part %d: %q %v", n+1, etag, err)
		}
		parts = append(parts, Part{N: n + 1, ETag: etag})
	}
	info, err := c.CompleteMultipart(ctx, "seg/00/9", id, parts)
	if err != nil || info.ETag == "" {
		t.Fatalf("complete: %+v %v", info, err)
	}

	body, got, err := c.Get(ctx, "seg/00/9")
	if err != nil || string(body) != "first half second half" {
		t.Fatalf("stitched object: %q %v", body, err)
	}
	// The create-time tag landed on the final object, so Recheck works
	// after an ambiguous complete.
	if got.Tag != tag {
		t.Fatalf("tag on final object: %+v", got.Tag)
	}

	// Completing again is NoSuchUpload: the caller's Recheck path, not a
	// silent success.
	if _, err := c.CompleteMultipart(ctx, "seg/00/9", id, parts); !errors.Is(err, ErrNotFound) {
		t.Fatalf("re-complete: want ErrNotFound, got %v", err)
	}
}

func TestMultipartAbort(t *testing.T) {
	store := newMPStore()
	srv := httptest.NewServer(store.handler(t))
	defer srv.Close()
	c := testClient(t, srv)
	ctx := context.Background()

	id, err := c.CreateMultipart(ctx, "seg/00/10", WriteTag{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadPart(ctx, "seg/00/10", id, 1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := c.AbortMultipart(ctx, "seg/00/10", id); err != nil {
		t.Fatal(err)
	}
	// Aborting an unknown id is ErrNotFound; the sweeper treats it as done.
	if err := c.AbortMultipart(ctx, "seg/00/10", id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("re-abort: want ErrNotFound, got %v", err)
	}
}

// S3 can answer a complete with 200 and an error document in the body. An
// InternalError there retries like a 500 and the next attempt wins.
func TestCompleteErrorInOKBody(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>try again</Message></Error>`)
			return
		}
		fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>"done"</ETag></CompleteMultipartUploadResult>`)
	}))
	defer srv.Close()
	c := testClient(t, srv)

	info, err := c.CompleteMultipart(context.Background(), "k", "up-1", []Part{{N: 1, ETag: `"e"`}})
	if err != nil || info.ETag != `"done"` {
		t.Fatalf("complete: %+v %v", info, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want a retry then a win", calls.Load())
	}
}

// A cut wire on complete is ambiguous, same rule as a PUT: surfaced, not
// replayed.
func TestCompleteAmbiguous(t *testing.T) {
	var calls atomic.Int32
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-block
	}))
	// Unblock the handler before srv.Close waits on it (defers run LIFO).
	defer srv.Close()
	defer close(block)
	c := testClient(t, srv)
	c.attemptTimeout = 50 * time.Millisecond

	_, err := c.CompleteMultipart(context.Background(), "k", "up-1", []Part{{N: 1, ETag: `"e"`}})
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("want ErrAmbiguous, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("an ambiguous complete must not be replayed, calls = %d", calls.Load())
	}
}
