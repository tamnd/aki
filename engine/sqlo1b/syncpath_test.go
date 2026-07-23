package sqlo1b

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The doc 04 durability rule: fsync never enters ring submission.
// Both backends expose Sync as its own method riding a dedicated
// goroutine, and the submission contract has no sync op at all, so
// these tests pin the contract half; the Linux half asserting a live
// ring's submit counters stay flat across a sync burst sits in
// ioring_linux_test.go.

// TestIOReqNoSyncOp pins the request contract: the only ops a backend
// accepts are read and write, so a sync can never be smuggled into a
// submission batch as an IOReq.
func TestIOReqNoSyncOp(t *testing.T) {
	for _, op := range []uint8{0, OpWrite + 1, 7} {
		err := validateIOReqs(1<<16, []IOReq{{Op: op, Buf: make([]byte, 8), Tag: 1}})
		if err == nil || !strings.Contains(err.Error(), "io op") {
			t.Fatalf("op %d: err = %v, want the io op rejection", op, err)
		}
	}
}

// TestPoolRejectsSyncShapedReq drives the rejection through a live
// pool: a request with an op outside read/write fails to its owner as
// an error completion, it never reaches the file.
func TestPoolRejectsSyncShapedReq(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "pool.dat"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(1 << 16); err != nil {
		t.Fatal(err)
	}
	comp := make(chan IOResult, 4)
	p := NewIOPool(f, 1<<16, 2, comp)
	defer p.Close()
	p.Submit([]IOReq{{Op: OpWrite + 1, Ext: 0, Off: 0, Buf: make([]byte, 8), Tag: 9}})
	res := <-comp
	if res.Tag != 9 || res.Err == nil || !strings.Contains(res.Err.Error(), "io op") {
		t.Fatalf("completion = %+v, want tag 9 with the io op rejection", res)
	}
}
