package obs1

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

// TestMinIORoundTrip runs the client against a real MinIO when one is
// offered via the environment, the first rung of the differential ladder
// (milestone O0a slice 7 makes this a scripted CI suite):
//
//	MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server /tmp/d &
//	AKI_OBS1_S3=http://127.0.0.1:9000 go test -run TestMinIO ./engine/obs1
//
// The httptest suites prove the loop against a scripted server; this one
// proves the signature and encoding against an implementation we did not
// write. Skipped when AKI_OBS1_S3 is unset.
func TestMinIORoundTrip(t *testing.T) {
	endpoint := os.Getenv("AKI_OBS1_S3")
	if endpoint == "" {
		t.Skip("AKI_OBS1_S3 not set")
	}
	user := envOr("AKI_OBS1_S3_USER", "minioadmin")
	pass := envOr("AKI_OBS1_S3_PASS", "minioadmin")
	bucket := fmt.Sprintf("obs1-test-%d", time.Now().UnixNano())

	createBucket(t, endpoint, bucket, user, pass)

	c, err := NewClient(ClientConfig{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Bucket:    bucket,
		AccessKey: user,
		SecretKey: pass,
		PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Keys with the shapes the bucket layout actually uses (doc 03), plus
	// one with characters that force the encoding paths to agree.
	keys := []string{
		"chain/00/000000000042",
		"seg/00/000000000042-0001",
		"odd keys/a+b~c (d)",
	}
	for _, key := range keys {
		body := []byte("payload for " + key)
		if _, err := c.Put(ctx, key, body); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
		got, info, err := c.Get(ctx, key)
		if err != nil || string(got) != string(body) {
			t.Fatalf("get %q: %q %v", key, got, err)
		}
		if info.ETag == "" {
			t.Errorf("get %q: no etag", key)
		}
		if err := c.Delete(ctx, key); err != nil {
			t.Fatalf("delete %q: %v", key, err)
		}
		if _, _, err := c.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("get after delete %q: want ErrNotFound, got %v", key, err)
		}
	}
}

// TestMinIOConditional proves the CAS surface against a real server: the
// If-None-Match create, the If-Match swap, and the metadata round trip the
// self-recognition pass depends on. This is doc 01 section 12 item 2's
// first data point; the provider-seam slice turns it into a matrix probe.
func TestMinIOConditional(t *testing.T) {
	endpoint := os.Getenv("AKI_OBS1_S3")
	if endpoint == "" {
		t.Skip("AKI_OBS1_S3 not set")
	}
	user := envOr("AKI_OBS1_S3_USER", "minioadmin")
	pass := envOr("AKI_OBS1_S3_PASS", "minioadmin")
	bucket := fmt.Sprintf("obs1-test-%d", time.Now().UnixNano())

	createBucket(t, endpoint, bucket, user, pass)

	c, err := NewClient(ClientConfig{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Bucket:    bucket,
		AccessKey: user,
		SecretKey: pass,
		PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ours := WriteTag{Writer: "node-a", Batch: "42"}

	info, err := c.PutIfAbsent(ctx, "chain/00/42", []byte("batch"), ours)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.PutIfAbsent(ctx, "chain/00/42", []byte("rival"), WriteTag{Writer: "node-b", Batch: "42"}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second create: want ErrPrecondition, got %v", err)
	}

	// The self-recognition pass sees our tag on a real server.
	out, body, _, err := c.Recheck(ctx, "chain/00/42", ours)
	if err != nil || out != RecheckOurs || string(body) != "batch" {
		t.Fatalf("recheck ours: %v %q %v", out, body, err)
	}
	out, _, _, err = c.Recheck(ctx, "chain/00/42", WriteTag{Writer: "node-b", Batch: "42"})
	if err != nil || out != RecheckOther {
		t.Fatalf("recheck other: %v %v", out, err)
	}

	// Swap with the live ETag wins; the now-stale one loses.
	info2, err := c.PutIfMatch(ctx, "chain/00/42", []byte("v2"), info.ETag, ours)
	if err != nil {
		t.Fatalf("swap: %v", err)
	}
	if _, err := c.PutIfMatch(ctx, "chain/00/42", []byte("v3"), info.ETag, ours); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale swap: want ErrPrecondition, got %v", err)
	}
	got, cur, err := c.Get(ctx, "chain/00/42")
	if err != nil || string(got) != "v2" || cur.ETag != info2.ETag {
		t.Fatalf("winner: %q %+v %v", got, cur, err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// createBucket signs a raw bucket-level PUT, the one request shape the
// Client itself never makes (obs1 nodes are handed an existing bucket).
func createBucket(t *testing.T, endpoint, bucket, user, pass string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, endpoint+"/"+bucket, nil)
	if err != nil {
		t.Fatal(err)
	}
	signV4(req, credentials{accessKey: user, secretKey: pass}, "us-east-1", "s3", emptySHA256, time.Now())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create bucket: http %d", resp.StatusCode)
	}
}
