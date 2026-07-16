// The differential suite (spec 2064/obs1 doc 11, O0a slice 7): the same
// operation script drives the simulator and a real MinIO through the same
// Store surface, and every client-visible outcome must match. This is what
// lets every later number gate on E-sim: the sim's semantics are pinned to
// an implementation we did not write, on every PR. Lives in the external
// test package so it can import both the client and the simulator.
package obs1_test

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// env is one run's memory: tokens and upload ids recorded by earlier steps
// so later steps can replay them. Tokens differ between stores, so scripts
// reference them by name, never by value.
type env struct {
	token  map[string]string
	upload map[string]string
	hist   []string // every token ever returned, for stale picks
}

// stepFn runs one operation and returns its normalized outcome.
type stepFn func(ctx context.Context, s obs1.Store, e *env) string

// outcome collapses an error to the class a caller can act on. Anything
// outside the typed taxonomy is a reject: both sides must refuse, the
// exact wording is theirs.
func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, obs1.ErrNotFound):
		return "notfound"
	case errors.Is(err, obs1.ErrPrecondition):
		return "precondition"
	case errors.Is(err, obs1.ErrConflict):
		return "conflict"
	case errors.Is(err, obs1.ErrRange):
		return "range"
	case errors.Is(err, obs1.ErrAmbiguous):
		return "ambiguous"
	case errors.Is(err, obs1.ErrSlowDown):
		return "slowdown"
	}
	return "reject"
}

func digest(b []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return fmt.Sprintf("%d:%x", len(b), h.Sum64())
}

func record(e *env, name string, info obs1.ObjectInfo, err error) {
	if err != nil {
		return
	}
	if name != "" {
		e.token[name] = info.ETag
	}
	e.hist = append(e.hist, info.ETag)
}

// runScript drives one store and returns the trace.
func runScript(t *testing.T, s obs1.Store, script []stepFn) []string {
	t.Helper()
	ctx := context.Background()
	e := &env{token: map[string]string{}, upload: map[string]string{}}
	trace := make([]string, len(script))
	for i, fn := range script {
		trace[i] = fn(ctx, s, e)
	}
	return trace
}

// diff runs the script against the sim and, when AKI_OBS1_S3 offers one, a
// real MinIO, then compares the traces step by step.
func diff(t *testing.T, script []stepFn) {
	t.Helper()
	endpoint := os.Getenv("AKI_OBS1_S3")
	if endpoint == "" {
		t.Skip("AKI_OBS1_S3 not set; the differential needs a real server")
	}
	user := envOrDiff("AKI_OBS1_S3_USER", "minioadmin")
	pass := envOrDiff("AKI_OBS1_S3_PASS", "minioadmin")
	bucket := fmt.Sprintf("obs1-diff-%d", time.Now().UnixNano())
	obs1.CreateTestBucket(t, endpoint, bucket, user, pass)
	c, err := obs1.NewClient(obs1.ClientConfig{
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

	simTrace := runScript(t, sim.New(sim.Config{}), script)
	realTrace := runScript(t, c, script)
	for i := range script {
		if simTrace[i] != realTrace[i] {
			t.Errorf("step %d diverged:\n  sim   %s\n  minio %s", i, simTrace[i], realTrace[i])
		}
	}
}

func envOrDiff(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Step builders. Each returns "op key -> outcome[:detail]".

func opGet(key string) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		b, info, err := s.Get(ctx, key)
		record(e, "", info, err)
		out := outcome(err)
		if err == nil {
			out += ":" + digest(b) + ":" + info.Tag.Writer + "/" + info.Tag.Batch
		}
		return fmt.Sprintf("get %s -> %s", key, out)
	}
}

func opGetRange(key string, off, n int64) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		b, _, err := s.GetRange(ctx, key, off, n)
		out := outcome(err)
		if err == nil {
			out += ":" + digest(b)
		}
		return fmt.Sprintf("range %s %d+%d -> %s", key, off, n, out)
	}
}

func opGetTail(key string, n int64) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		b, _, err := s.GetTail(ctx, key, n)
		out := outcome(err)
		if err == nil {
			out += ":" + digest(b)
		}
		return fmt.Sprintf("tail %s %d -> %s", key, n, out)
	}
}

func opPut(key, body string) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		info, err := s.Put(ctx, key, []byte(body))
		record(e, key, info, err)
		return fmt.Sprintf("put %s -> %s", key, outcome(err))
	}
}

func opCreate(key, body string, tag obs1.WriteTag) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		info, err := s.PutIfAbsent(ctx, key, []byte(body), tag)
		record(e, key, info, err)
		return fmt.Sprintf("create %s -> %s", key, outcome(err))
	}
}

// opSwap replaces key using the token an earlier step recorded under from.
func opSwap(key, body, from string, tag obs1.WriteTag) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		info, err := s.PutIfMatch(ctx, key, []byte(body), e.token[from], tag)
		record(e, key, info, err)
		return fmt.Sprintf("swap %s from %s -> %s", key, from, outcome(err))
	}
}

func opRecheck(key string, tag obs1.WriteTag) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		out, body, _, err := s.Recheck(ctx, key, tag)
		res := outcome(err)
		if err == nil {
			res = fmt.Sprintf("%d:%s", out, digest(body))
		}
		return fmt.Sprintf("recheck %s %s/%s -> %s", key, tag.Writer, tag.Batch, res)
	}
}

func opDelete(key string) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		return fmt.Sprintf("delete %s -> %s", key, outcome(s.Delete(ctx, key)))
	}
}

func opDeleteBatch(keys ...string) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		return fmt.Sprintf("batch %v -> %s", keys, outcome(s.DeleteObjects(ctx, keys)))
	}
}

// multipart steps share the upload id through the env under name.

func opMpCreate(name, key string, tag obs1.WriteTag) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		id, err := s.CreateMultipart(ctx, key, tag)
		if err == nil {
			e.upload[name] = id
		}
		return fmt.Sprintf("mpcreate %s -> %s", key, outcome(err))
	}
}

func opMpPart(name, key string, n int, body []byte) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		etag, err := s.UploadPart(ctx, key, e.upload[name], n, body)
		if err == nil {
			e.token[fmt.Sprintf("%s.%d", name, n)] = etag
		}
		return fmt.Sprintf("mppart %s %d -> %s", key, n, outcome(err))
	}
}

// opMpComplete stitches parts named by their numbers, in the order given.
func opMpComplete(name, key string, ns ...int) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		parts := make([]obs1.Part, len(ns))
		for i, n := range ns {
			parts[i] = obs1.Part{N: n, ETag: e.token[fmt.Sprintf("%s.%d", name, n)]}
		}
		info, err := s.CompleteMultipart(ctx, key, e.upload[name], parts)
		record(e, key, info, err)
		return fmt.Sprintf("mpcomplete %s %v -> %s", key, ns, outcome(err))
	}
}

func opMpCompleteBadETag(name, key string) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		_, err := s.CompleteMultipart(ctx, key, e.upload[name], []obs1.Part{{N: 1, ETag: `"0000-bogus"`}})
		return fmt.Sprintf("mpcomplete-bad %s -> %s", key, outcome(err))
	}
}

func opMpAbort(name, key string) stepFn {
	return func(ctx context.Context, s obs1.Store, e *env) string {
		return fmt.Sprintf("mpabort %s -> %s", key, outcome(s.AbortMultipart(ctx, key, e.upload[name])))
	}
}

// TestDifferentialScenario is the hand-written script: every documented
// semantic the client suites pin, replayed against both stores.
func TestDifferentialScenario(t *testing.T) {
	wa := obs1.WriteTag{Writer: "node-a", Batch: "1"}
	wb := obs1.WriteTag{Writer: "node-b", Batch: "1"}
	part := make([]byte, obs1.MinPartSize)
	for i := range part {
		part[i] = byte('a' + i%26)
	}

	script := []stepFn{
		opGet("chain/00/1"),
		opCreate("chain/00/1", "batch-a", wa),
		opCreate("chain/00/1", "batch-b", wb),
		opGet("chain/00/1"),
		opRecheck("chain/00/1", wa),
		opRecheck("chain/00/1", wb),
		opSwap("chain/00/1", "v2", "chain/00/1", wa), // live token
		opGet("chain/00/1"),
		opCreate("odd keys/a+b~c (d)", "odd", wa),
		opGet("odd keys/a+b~c (d)"),

		opPut("seg/00/1", "0123456789"),
		opGetRange("seg/00/1", 2, 4),
		opGetRange("seg/00/1", 8, 100),
		opGetRange("seg/00/1", 10, 1),
		opGetTail("seg/00/1", 3),
		opGetTail("seg/00/1", 100),
		opPut("seg/00/empty", ""),
		opGetTail("seg/00/empty", 4),
		opGetRange("seg/00/missing", 0, 4),

		opPut("tomb/a", "x"),
		opPut("tomb/b", "y"),
		opDeleteBatch("tomb/a", "tomb/b", "tomb/missing"),
		opGet("tomb/a"),
		opDelete("tomb/missing"),

		opMpCreate("u1", "seg/00/2", wa),
		opMpPart("u1", "seg/00/2", 1, part),
		opMpPart("u1", "seg/00/2", 2, part),
		opMpComplete("u1", "seg/00/2", 2, 1), // descending order refused
		opMpCompleteBadETag("u1", "seg/00/2"),
		opMpComplete("u1", "seg/00/2", 1, 2),
		opGet("seg/00/2"),
		opGetTail("seg/00/2", 4),
		opMpComplete("u1", "seg/00/2", 1, 2), // finished upload is gone
		opMpCreate("u2", "seg/00/3", wa),
		opMpAbort("u2", "seg/00/3"),
		opMpAbort("u2", "seg/00/3"),

		opDeleteBatch("chain/00/1", "odd keys/a+b~c (d)", "seg/00/1", "seg/00/empty", "seg/00/2"),
	}
	diff(t, script)
}

// stale swaps need a token that once was live; the scenario covers the
// named cases, this covers the space between them. The script is generated
// from a fixed seed, so both stores see the same operations and CI replays
// the same run every time.
func TestDifferentialRandom(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			diff(t, randomScript(seed, 250))
		})
	}
}

func randomScript(seed uint64, n int) []stepFn {
	rng := rand.New(rand.NewPCG(seed, seed^0xdeadbeef))
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = fmt.Sprintf("rnd/%02d/key", i)
	}
	tags := []obs1.WriteTag{
		{Writer: "node-a", Batch: "r1"},
		{Writer: "node-b", Batch: "r2"},
	}
	script := make([]stepFn, 0, n+1)
	for range n {
		key := keys[rng.IntN(len(keys))]
		tag := tags[rng.IntN(len(tags))]
		body := fmt.Sprintf("body-%d", rng.Uint64())
		switch rng.IntN(10) {
		case 0, 1:
			script = append(script, opPut(key, body))
		case 2, 3:
			script = append(script, opCreate(key, body, tag))
		case 4:
			// A token some step once recorded: sometimes live, mostly stale.
			pick := rng.Uint64()
			script = append(script, func(ctx context.Context, s obs1.Store, e *env) string {
				token := `"never-live"`
				if len(e.hist) > 0 {
					token = e.hist[pick%uint64(len(e.hist))]
				}
				info, err := s.PutIfMatch(ctx, key, []byte(body), token, tag)
				record(e, key, info, err)
				return fmt.Sprintf("swap %s hist -> %s", key, outcome(err))
			})
		case 5, 6:
			script = append(script, opGet(key))
		case 7:
			script = append(script, opGetRange(key, int64(rng.IntN(24)), int64(1+rng.IntN(16))))
		case 8:
			script = append(script, opRecheck(key, tag))
		case 9:
			script = append(script, opDelete(key))
		}
	}
	script = append(script, opDeleteBatch(keys...))
	return script
}
