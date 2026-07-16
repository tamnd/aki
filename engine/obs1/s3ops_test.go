package obs1

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// rangeHandler serves body honoring the two Range forms the client sends.
func rangeHandler(t *testing.T, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if rng == "" {
			if _, err := w.Write(body); err != nil {
				t.Error(err)
			}
			return
		}
		spec := strings.TrimPrefix(rng, "bytes=")
		var lo, hi int64
		if after, ok := strings.CutPrefix(spec, "-"); ok {
			n, _ := strconv.ParseInt(after, 10, 64)
			lo, hi = max(int64(len(body))-n, 0), int64(len(body))-1
		} else {
			a, b, _ := strings.Cut(spec, "-")
			lo, _ = strconv.ParseInt(a, 10, 64)
			hi, _ = strconv.ParseInt(b, 10, 64)
			if lo >= int64(len(body)) {
				w.WriteHeader(416)
				return
			}
			hi = min(hi, int64(len(body))-1)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi, len(body)))
		w.WriteHeader(206)
		if _, err := w.Write(body[lo : hi+1]); err != nil {
			t.Error(err)
		}
	}
}

func TestGetRange(t *testing.T) {
	srv := httptest.NewServer(rangeHandler(t, []byte("0123456789")))
	defer srv.Close()
	c := testClient(t, srv)
	ctx := context.Background()

	got, info, err := c.GetRange(ctx, "seg", 2, 4)
	if err != nil || string(got) != "2345" || info.Size != 4 {
		t.Fatalf("mid range: %q %+v %v", got, info, err)
	}
	// A range running past the end returns the bytes that exist.
	got, _, err = c.GetRange(ctx, "seg", 8, 100)
	if err != nil || string(got) != "89" {
		t.Fatalf("overlong range: %q %v", got, err)
	}
	// A range starting past the end is ErrRange, never retried.
	if _, _, err := c.GetRange(ctx, "seg", 10, 1); !errors.Is(err, ErrRange) {
		t.Fatalf("past-end range: want ErrRange, got %v", err)
	}
	got, _, err = c.GetTail(ctx, "seg", 3)
	if err != nil || string(got) != "789" {
		t.Fatalf("tail: %q %v", got, err)
	}
	if _, _, err := c.GetRange(ctx, "seg", -1, 4); err == nil {
		t.Fatal("negative offset must error client-side")
	}
}

func TestDeleteObjects(t *testing.T) {
	store := map[string]bool{"a": true, "b": true, "keys with/odd chars+~": true}
	var gotMD5 atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("delete") {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(400)
			return
		}
		gotMD5.Store(r.Header.Get("Content-MD5"))
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var dr deleteReq
		if err := xml.Unmarshal(body, &dr); err != nil {
			t.Error(err)
		}
		for _, o := range dr.Objects {
			delete(store, o.Key) // deleting a missing key succeeds
		}
		if _, err := w.Write([]byte(`<DeleteResult/>`)); err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()
	c := testClient(t, srv)

	if err := c.DeleteObjects(context.Background(), []string{"a", "b", "keys with/odd chars+~", "never-existed"}); err != nil {
		t.Fatal(err)
	}
	if len(store) != 0 {
		t.Fatalf("keys survived: %v", store)
	}
	if md5h, _ := gotMD5.Load().(string); md5h == "" {
		t.Fatal("Content-MD5 missing; the API requires it")
	}

	// Client-side guards: the S3 ceiling and the empty batch.
	if err := c.DeleteObjects(context.Background(), make([]string, deleteBatchMax+1)); err == nil {
		t.Fatal("oversized batch must error before the wire")
	}
	if err := c.DeleteObjects(context.Background(), nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
}

func TestDeleteObjectsPartialFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte(`<DeleteResult>
			<Error><Key>locked</Key><Code>AccessDenied</Code><Message>no</Message></Error>
		</DeleteResult>`)); err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()
	c := testClient(t, srv)

	err := c.DeleteObjects(context.Background(), []string{"a", "locked"})
	if err == nil || !strings.Contains(err.Error(), "locked (AccessDenied)") {
		t.Fatalf("want the surviving key named, got %v", err)
	}
}
