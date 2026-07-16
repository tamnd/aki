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
