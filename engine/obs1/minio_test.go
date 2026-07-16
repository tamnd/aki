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

// TestMinIOBatchAndRange exercises slice 4 against a real server: ranged
// and tail reads, the 416 mapping, a DeleteObjects batch, and a two-part
// multipart upload whose create-time tag survives onto the final object.
func TestMinIOBatchAndRange(t *testing.T) {
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

	if _, err := c.Put(ctx, "seg/00/1", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	got, _, err := c.GetRange(ctx, "seg/00/1", 2, 4)
	if err != nil || string(got) != "2345" {
		t.Fatalf("range: %q %v", got, err)
	}
	got, _, err = c.GetTail(ctx, "seg/00/1", 3)
	if err != nil || string(got) != "789" {
		t.Fatalf("tail: %q %v", got, err)
	}
	if _, _, err := c.GetRange(ctx, "seg/00/1", 10, 1); !errors.Is(err, ErrRange) {
		t.Fatalf("past-end range: want ErrRange, got %v", err)
	}

	keys := []string{"tomb/a", "tomb/b", "tomb/odd +~()", "tomb/missing"}
	for _, k := range keys[:3] {
		if _, err := c.Put(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.DeleteObjects(ctx, keys); err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	for _, k := range keys[:3] {
		if _, _, err := c.Get(ctx, k); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s survived the batch: %v", k, err)
		}
	}

	// Multipart with two minimum-size parts; the create-time tag must land
	// on the final object so Recheck works after an ambiguous complete.
	tag := WriteTag{Writer: "folder-1", Batch: "seg-2"}
	id, err := c.CreateMultipart(ctx, "seg/00/2", tag)
	if err != nil {
		t.Fatal(err)
	}
	part := make([]byte, MinPartSize)
	for i := range part {
		part[i] = byte('a' + i%26)
	}
	var parts []Part
	for n := 1; n <= 2; n++ {
		etag, err := c.UploadPart(ctx, "seg/00/2", id, n, part)
		if err != nil {
			t.Fatalf("part %d: %v", n, err)
		}
		parts = append(parts, Part{N: n, ETag: etag})
	}
	if _, err := c.CompleteMultipart(ctx, "seg/00/2", id, parts); err != nil {
		t.Fatalf("complete: %v", err)
	}
	out, tinfo, err := c.GetTail(ctx, "seg/00/2", 4)
	if err != nil || len(out) != 4 {
		t.Fatalf("tail of stitched object: %q %v", out, err)
	}
	if tinfo.Tag != tag {
		t.Fatalf("tag on multipart object: %+v", tinfo.Tag)
	}
	if err := c.DeleteObjects(ctx, []string{"seg/00/1", "seg/00/2"}); err != nil {
		t.Fatal(err)
	}
}

// TestMinIOProbe is doc 01 section 12 item 2, the matrix probe form: the
// scripted CAS sequence against a real MinIO must come back fully capable.
// MinIO before RELEASE.2025-09-07 fails the create step (conditional PUT
// on a missing key answered 404), which is exactly what this catches; the
// floor is pinned in CI.
func TestMinIOProbe(t *testing.T) {
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
	caps, err := ProbeCAS(context.Background(), c, "probe")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("minio caps: %+v", caps)
	if !caps.CASCreate || !caps.CASReplace {
		t.Fatalf("MinIO below the 2025-09-07 floor or CAS regressed: %+v", caps)
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
