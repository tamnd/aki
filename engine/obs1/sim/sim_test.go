package sim

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
)

// instant replaces the sleeper and records every drawn duration.
func instant(s *Sim) *[]time.Duration {
	var draws []time.Duration
	s.sleep = func(ctx context.Context, d time.Duration) error {
		draws = append(draws, d)
		return ctx.Err()
	}
	return &draws
}

func TestSimConditional(t *testing.T) {
	s := New(Config{})
	ctx := context.Background()
	ours := obs1.WriteTag{Writer: "node-a", Batch: "42"}

	info, err := s.PutIfAbsent(ctx, "chain/00/42", []byte("batch"), ours)
	if err != nil || info.ETag == "" {
		t.Fatalf("create: %+v %v", info, err)
	}
	if _, err := s.PutIfAbsent(ctx, "chain/00/42", []byte("rival"), obs1.WriteTag{Writer: "node-b", Batch: "42"}); !errors.Is(err, obs1.ErrPrecondition) {
		t.Fatalf("second create: want ErrPrecondition, got %v", err)
	}

	out, body, _, err := s.Recheck(ctx, "chain/00/42", ours)
	if err != nil || out != obs1.RecheckOurs || string(body) != "batch" {
		t.Fatalf("recheck ours: %v %q %v", out, body, err)
	}
	out, _, _, err = s.Recheck(ctx, "chain/00/42", obs1.WriteTag{Writer: "node-b", Batch: "42"})
	if err != nil || out != obs1.RecheckOther {
		t.Fatalf("recheck other: %v %v", out, err)
	}
	out, _, _, err = s.Recheck(ctx, "chain/00/43", ours)
	if err != nil || out != obs1.RecheckAbsent {
		t.Fatalf("recheck absent: %v %v", out, err)
	}
	if _, _, _, err := s.Recheck(ctx, "chain/00/42", obs1.WriteTag{}); err == nil {
		t.Fatal("zero-tag recheck must refuse")
	}

	info2, err := s.PutIfMatch(ctx, "chain/00/42", []byte("v2"), info.ETag, ours)
	if err != nil {
		t.Fatalf("swap: %v", err)
	}
	if _, err := s.PutIfMatch(ctx, "chain/00/42", []byte("v3"), info.ETag, ours); !errors.Is(err, obs1.ErrPrecondition) {
		t.Fatalf("stale swap: want ErrPrecondition, got %v", err)
	}
	// What AWS and MinIO answer for If-Match on a missing key, verified
	// live against MinIO 2025-09-07.
	if _, err := s.PutIfMatch(ctx, "chain/00/missing", []byte("x"), info2.ETag, ours); !errors.Is(err, obs1.ErrNotFound) {
		t.Fatalf("if-match on missing key: want ErrNotFound, got %v", err)
	}
	got, cur, err := s.Get(ctx, "chain/00/42")
	if err != nil || string(got) != "v2" || cur.ETag != info2.ETag {
		t.Fatalf("winner: %q %+v %v", got, cur, err)
	}
}

func TestSimRangeAndBatch(t *testing.T) {
	s := New(Config{})
	ctx := context.Background()

	if _, err := s.Put(ctx, "seg/00/1", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	got, info, err := s.GetRange(ctx, "seg/00/1", 2, 4)
	if err != nil || string(got) != "2345" || info.Size != 4 {
		t.Fatalf("range: %q %+v %v", got, info, err)
	}
	got, _, err = s.GetRange(ctx, "seg/00/1", 8, 100)
	if err != nil || string(got) != "89" {
		t.Fatalf("past-end truncation: %q %v", got, err)
	}
	if _, _, err := s.GetRange(ctx, "seg/00/1", 10, 1); !errors.Is(err, obs1.ErrRange) {
		t.Fatalf("past-end start: want ErrRange, got %v", err)
	}
	if _, _, err := s.GetRange(ctx, "seg/00/1", -1, 4); err == nil {
		t.Fatal("negative offset must refuse")
	}
	got, _, err = s.GetTail(ctx, "seg/00/1", 3)
	if err != nil || string(got) != "789" {
		t.Fatalf("tail: %q %v", got, err)
	}
	got, _, err = s.GetTail(ctx, "seg/00/1", 100)
	if err != nil || string(got) != "0123456789" {
		t.Fatalf("overlong tail: %q %v", got, err)
	}
	if _, err := s.Put(ctx, "seg/00/empty", nil); err != nil {
		t.Fatal(err)
	}
	// The tail of an empty object is zero bytes, the client's normalized
	// contract (AWS 416s the suffix range there, MinIO 200s it; the wire
	// client folds both to an empty read).
	if b, _, err := s.GetTail(ctx, "seg/00/empty", 4); err != nil || len(b) != 0 {
		t.Fatalf("tail of empty object: want empty read, got %q %v", b, err)
	}
	if _, _, err := s.GetRange(ctx, "seg/00/missing", 0, 4); !errors.Is(err, obs1.ErrNotFound) {
		t.Fatalf("range on missing key: want ErrNotFound, got %v", err)
	}

	keys := []string{"tomb/a", "tomb/b", "tomb/missing"}
	for _, k := range keys[:2] {
		if _, err := s.Put(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteObjects(ctx, keys); err != nil {
		t.Fatalf("batch: %v", err)
	}
	for _, k := range keys[:2] {
		if _, _, err := s.Get(ctx, k); !errors.Is(err, obs1.ErrNotFound) {
			t.Fatalf("%s survived: %v", k, err)
		}
	}
	if err := s.DeleteObjects(ctx, nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if err := s.DeleteObjects(ctx, make([]string, 1001)); err == nil {
		t.Fatal("oversize batch must refuse")
	}
	if err := s.Delete(ctx, "tomb/missing"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
}

func TestSimMultipart(t *testing.T) {
	s := New(Config{})
	ctx := context.Background()
	tag := obs1.WriteTag{Writer: "folder-1", Batch: "seg-2"}

	id, err := s.CreateMultipart(ctx, "seg/00/2", tag)
	if err != nil || id == "" {
		t.Fatalf("create: %q %v", id, err)
	}
	var parts []obs1.Part
	for n := 1; n <= 2; n++ {
		etag, err := s.UploadPart(ctx, "seg/00/2", id, n, fmt.Appendf(nil, "part%d|", n))
		if err != nil {
			t.Fatalf("part %d: %v", n, err)
		}
		parts = append(parts, obs1.Part{N: n, ETag: etag})
	}
	if _, err := s.UploadPart(ctx, "seg/00/2", "sim-up-nope", 1, []byte("x")); !errors.Is(err, obs1.ErrNotFound) {
		t.Fatalf("part on unknown id: want ErrNotFound, got %v", err)
	}
	if _, err := s.CompleteMultipart(ctx, "seg/00/2", id, []obs1.Part{parts[1], parts[0]}); err == nil {
		t.Fatal("descending part order must refuse")
	}
	if _, err := s.CompleteMultipart(ctx, "seg/00/2", id, []obs1.Part{{N: 1, ETag: `"wrong"`}, parts[1]}); err == nil {
		t.Fatal("wrong part etag must refuse")
	}
	info, err := s.CompleteMultipart(ctx, "seg/00/2", id, parts)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	body, got, err := s.Get(ctx, "seg/00/2")
	if err != nil || string(body) != "part1|part2|" || got.ETag != info.ETag {
		t.Fatalf("stitched: %q %+v %v", body, got, err)
	}
	if got.Tag != tag {
		t.Fatalf("tag on stitched object: %+v", got.Tag)
	}
	if _, err := s.CompleteMultipart(ctx, "seg/00/2", id, parts); !errors.Is(err, obs1.ErrNotFound) {
		t.Fatalf("re-complete: want ErrNotFound, got %v", err)
	}

	id2, err := s.CreateMultipart(ctx, "seg/00/3", tag)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AbortMultipart(ctx, "seg/00/3", id2); err != nil {
		t.Fatalf("abort: %v", err)
	}
	// Abort means ensure-gone: re-aborting succeeds (MinIO 204s it, the
	// wire client folds AWS's 404 NoSuchUpload to nil).
	if err := s.AbortMultipart(ctx, "seg/00/3", id2); err != nil {
		t.Fatalf("re-abort: %v", err)
	}
}

// TestSimDeterminism: the same seed replays the same latency draws, a
// different seed does not, and the drawn values sit in the modeled ballpark.
func TestSimDeterminism(t *testing.T) {
	script := func(seed uint64) []time.Duration {
		s := New(Config{Seed: seed, Latency: S3Standard})
		draws := instant(s)
		ctx := context.Background()
		for i := range 64 {
			key := fmt.Sprintf("k/%d", i)
			if _, err := s.Put(ctx, key, []byte("v")); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.Get(ctx, key); err != nil {
				t.Fatal(err)
			}
		}
		return *draws
	}
	a, b, c := script(1), script(1), script(2)
	if len(a) != 128 {
		t.Fatalf("draws: %d", len(a))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at %d: %v vs %v", i, a[i], b[i])
		}
	}
	same := 0
	for i := range a {
		if a[i] == c[i] {
			same++
		}
	}
	if same == len(a) {
		t.Fatal("different seeds produced identical draws")
	}
	for _, d := range a {
		if d <= 0 || d > 5*time.Second {
			t.Fatalf("draw out of any plausible envelope: %v", d)
		}
	}
}

func TestSimFaults(t *testing.T) {
	ctx := context.Background()
	ours := obs1.WriteTag{Writer: "node-a", Batch: "9"}

	// An ambiguous create that landed: ErrAmbiguous surfaces, Recheck says ours.
	s := New(Config{Fault: func(op Op, key string) *Fault {
		if op == OpPutIfAbsent {
			return &Fault{Err: fmt.Errorf("sim: PUT %s: %w", key, obs1.ErrAmbiguous), Applied: true}
		}
		return nil
	}})
	if _, err := s.PutIfAbsent(ctx, "chain/00/9", []byte("batch"), ours); !errors.Is(err, obs1.ErrAmbiguous) {
		t.Fatalf("want ErrAmbiguous, got %v", err)
	}
	out, body, _, err := s.Recheck(ctx, "chain/00/9", ours)
	if err != nil || out != obs1.RecheckOurs || string(body) != "batch" {
		t.Fatalf("recheck after applied ambiguity: %v %q %v", out, body, err)
	}

	// The same fault without Applied: nothing landed, Recheck says absent.
	s = New(Config{Fault: func(op Op, key string) *Fault {
		if op == OpPutIfAbsent {
			return &Fault{Err: fmt.Errorf("sim: PUT %s: %w", key, obs1.ErrAmbiguous)}
		}
		return nil
	}})
	if _, err := s.PutIfAbsent(ctx, "chain/00/9", []byte("batch"), ours); !errors.Is(err, obs1.ErrAmbiguous) {
		t.Fatalf("want ErrAmbiguous, got %v", err)
	}
	if out, _, _, err := s.Recheck(ctx, "chain/00/9", ours); err != nil || out != obs1.RecheckAbsent {
		t.Fatalf("recheck after lost write: %v %v", out, err)
	}

	// Applied cannot override the condition: the key exists, so the lost
	// create never landed and the rival's object survives.
	s = New(Config{})
	if _, err := s.PutIfAbsent(ctx, "chain/00/10", []byte("rival"), obs1.WriteTag{Writer: "node-b", Batch: "10"}); err != nil {
		t.Fatal(err)
	}
	s.cfg.Fault = func(op Op, key string) *Fault {
		if op == OpPutIfAbsent {
			return &Fault{Err: fmt.Errorf("sim: PUT %s: %w", key, obs1.ErrAmbiguous), Applied: true}
		}
		return nil
	}
	if _, err := s.PutIfAbsent(ctx, "chain/00/10", []byte("ours"), ours); !errors.Is(err, obs1.ErrAmbiguous) {
		t.Fatalf("want ErrAmbiguous, got %v", err)
	}
	if out, body, _, _ := s.Recheck(ctx, "chain/00/10", ours); out != obs1.RecheckOther || string(body) != "rival" {
		t.Fatalf("condition must hold under Applied: %v %q", out, body)
	}

	// SlowDown surfaces as itself, and a storm's Extra reaches the sleeper.
	s = New(Config{Fault: func(op Op, key string) *Fault {
		switch op {
		case OpGet:
			return &Fault{Err: fmt.Errorf("sim: GET %s: %w", key, obs1.ErrSlowDown)}
		case OpPut:
			return &Fault{Extra: 400 * time.Millisecond}
		}
		return nil
	}})
	draws := instant(s)
	if _, _, err := s.Get(ctx, "k"); !errors.Is(err, obs1.ErrSlowDown) {
		t.Fatalf("want ErrSlowDown, got %v", err)
	}
	if _, err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("storm put: %v", err)
	}
	if (*draws)[1] != 400*time.Millisecond {
		t.Fatalf("storm extra missing from sleep: %v", *draws)
	}
}

func TestSimCancelledContext(t *testing.T) {
	s := New(Config{Latency: S3Standard})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Put(ctx, "k", []byte("v")); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if _, _, err := s.Get(context.Background(), "k"); !errors.Is(err, obs1.ErrNotFound) {
		t.Fatalf("a cancelled write must not land: %v", err)
	}
}

// TestSimBill scripts a run and asserts the counters and the dollars, the
// doc 09 section 9 contract that labs assert bills, not just latencies.
func TestSimBill(t *testing.T) {
	s := New(Config{})
	ctx := context.Background()
	body := make([]byte, 1<<20)

	for i := range 3 {
		if _, err := s.Put(ctx, fmt.Sprintf("seg/%d", i), body); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.Get(ctx, "seg/0"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetRange(ctx, "seg/1", 0, 512); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "seg/2"); err != nil {
		t.Fatal(err)
	}

	u := s.Usage()
	want := Usage{
		PutRequests:     3,
		GetRequests:     2,
		FreeRequests:    1,
		BytesUp:         3 << 20,
		BytesDown:       1<<20 + 512,
		BytesStored:     2 << 20,
		BytesStoredPeak: 3 << 20,
	}
	if u != want {
		t.Fatalf("usage:\n got %+v\nwant %+v", u, want)
	}

	b := S3StandardPrices.Bill(u, 730*time.Hour)
	wantStorage := 3.0 / 1024 * 0.023
	wantPuts := 3.0 / 1e6 * 5.00
	wantGets := 2.0 / 1e6 * 0.40
	if !within(b.Storage, wantStorage) || !within(b.Puts, wantPuts) || !within(b.Gets, wantGets) || !within(b.Total, wantStorage+wantPuts+wantGets) {
		t.Fatalf("bill: %+v", b)
	}
}

func within(a, b float64) bool {
	d := a - b
	return d < 1e-12 && d > -1e-12
}

// TestSimProbe: the CAS probe is provider-agnostic, and the sim must come
// back fully capable, same as the floor MinIO.
func TestSimProbe(t *testing.T) {
	caps, err := obs1.ProbeCAS(context.Background(), New(Config{}), "probe")
	if err != nil {
		t.Fatal(err)
	}
	if !caps.CASCreate || !caps.CASReplace {
		t.Fatalf("caps: %+v", caps)
	}
}
