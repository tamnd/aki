package obs1

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestR2Probe is doc 01 section 12 item 1: R2's conditional-write support
// is verified against a live account, never assumed from docs. Needs a
// bucket dedicated to probes, since it writes and deletes scratch keys:
//
//	AKI_OBS1_R2_ENDPOINT=https://<account>.r2.cloudflarestorage.com \
//	AKI_OBS1_R2_KEY=... AKI_OBS1_R2_SECRET=... AKI_OBS1_R2_BUCKET=... \
//	go test -run TestR2Probe ./engine/obs1
//
// CI runs it when the repo has the secrets; forks and local runs skip.
func TestR2Probe(t *testing.T) {
	endpoint := os.Getenv("AKI_OBS1_R2_ENDPOINT")
	if endpoint == "" {
		t.Skip("AKI_OBS1_R2_ENDPOINT not set")
	}
	c, err := NewClient(ClientConfig{
		Endpoint:  endpoint,
		Region:    "auto",
		Bucket:    os.Getenv("AKI_OBS1_R2_BUCKET"),
		AccessKey: os.Getenv("AKI_OBS1_R2_KEY"),
		SecretKey: os.Getenv("AKI_OBS1_R2_SECRET"),
		PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("probe/%d", time.Now().UnixNano())
	caps, err := ProbeCAS(context.Background(), c, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("r2 caps: %+v", caps)
	if !caps.CASCreate || !caps.CASReplace {
		t.Fatalf("R2 CAS support changed; update the doc 03 matrix: %+v", caps)
	}
}
