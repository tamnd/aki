package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// addQuick lands one explicit-ID XADD in the model without the full
// per-add audit, so the trim tests can build long streams cheaply; the
// callers run rig.check once after the build.
func (r *streamRig) addQuick(key string, ms uint64, fv ...string) {
	r.t.Helper()
	bs := make([][]byte, len(fv))
	for i, f := range fv {
		bs[i] = []byte(f)
	}
	id := streamID{ms: ms, seq: 1}
	_, ok, err := r.x.Add(context.Background(), []byte(key), xidExplicit, id, r.nowMs, false, bs)
	if err != nil || !ok {
		r.t.Fatalf("Add(%q, %d-1): ok=%v err=%v", key, ms, ok, err)
	}
	r.model = append(r.model, streamModelEnt{id: id, fv: bs})
}

// trim runs one Trim, shifts the model by the removed count (a trim
// always removes a prefix of the live entries in ID order), and runs
// the full audit.
func (r *streamRig) trim(key string, byID bool, maxlen int64, minid streamID, approx bool, limit int64) int64 {
	r.t.Helper()
	removed, err := r.x.Trim(context.Background(), []byte(key), byID, maxlen, minid, approx, limit)
	if err != nil {
		r.t.Fatalf("Trim(%q): %v", key, err)
	}
	if removed < 0 || removed > int64(len(r.model)) {
		r.t.Fatalf("Trim(%q) removed %d of %d modeled entries", key, removed, len(r.model))
	}
	r.model = r.model[removed:]
	r.check(key)
	return removed
}

// streamRootOf decodes key's persisted root for the field assertions.
func (r *streamRig) streamRootOf(key string) streamRoot {
	r.t.Helper()
	v, _, _, ok, err := r.tr.LookupEntry(context.Background(), []byte(key))
	if err != nil || !ok {
		r.t.Fatalf("LookupEntry(%q): ok=%v err=%v", key, ok, err)
	}
	sr, err := decodeStreamRoot(v, nil, nil)
	if err != nil {
		r.t.Fatalf("decode root %q: %v", key, err)
	}
	return sr
}

// TestStreamTrimOracle drives the flat-fence trim shapes against the
// model: exact MAXLEN boundary rewrites, whole-run drops, approximate
// run-boundary cuts, MINID in both forms, the run-granular limit, and
// the X-I2 root fields surviving a trim to empty with the key alive.
func TestStreamTrimOracle(t *testing.T) {
	rig := newStreamRig(t)
	ctx := context.Background()
	for ms := uint64(1); ms <= 300; ms++ {
		rig.addQuick("k", ms, "f", fmt.Sprintf("v%03d", ms))
	}
	rig.check("k")
	if runs := len(rig.streamRootOf("k").fence); runs < 3 {
		t.Fatalf("only %d runs; the test wants a multi-run fence", runs)
	}
	added := rig.streamRootOf("k").added
	last := rig.streamRootOf("k").last

	// Exact MAXLEN inside the first run: a boundary rewrite only.
	if got := rig.trim("k", false, 250, streamID{}, false, 0); got != 50 {
		t.Fatalf("MAXLEN 250 removed %d, want 50", got)
	}
	// Exact MAXLEN across a run: one whole-run drop plus the rewrite.
	if got := rig.trim("k", false, 128, streamID{}, false, 0); got != 122 {
		t.Fatalf("MAXLEN 128 removed %d, want 122", got)
	}
	// A no-op trim: already at or under the target.
	if got := rig.trim("k", false, 128, streamID{}, false, 0); got != 0 {
		t.Fatalf("MAXLEN 128 rerun removed %d, want 0", got)
	}
	// The approximate form cannot cut inside a run: with the excess
	// smaller than the head run, nothing moves.
	if got := rig.trim("k", false, 100, streamID{}, true, 100*streamRunMaxEntries); got != 0 {
		t.Fatalf("MAXLEN ~100 removed %d, want 0", got)
	}

	// Exact MINID: everything below the threshold goes, the threshold
	// entry itself survives.
	minid := rig.model[60].id
	if got := rig.trim("k", true, 0, minid, false, 0); got != 60 {
		t.Fatalf("MINID removed %d, want 60", got)
	}
	if rig.model[0].id != minid {
		t.Fatalf("head after MINID = %v, want %v", rig.model[0].id, minid)
	}
	// MINID above the last ID empties the stream, and X-I2 holds: the
	// key lives with last and added intact and max-deleted-ID still
	// zero, so the next append keeps generating upward.
	if got := rig.trim("k", true, 0, streamID{ms: last.ms + 1}, false, 0); got != 68 {
		t.Fatalf("MINID past last removed %d, want 68", got)
	}
	sr := rig.streamRootOf("k")
	if sr.count != 0 || sr.last != last || sr.added != added || sr.maxDel != (streamID{}) {
		t.Fatalf("emptied root = count %d last %v added %d maxDel %v, want 0 %v %d 0-0", sr.count, sr.last, sr.added, sr.maxDel, last, added)
	}
	rig.addQuick("k", last.ms+5, "f", "fresh")
	rig.check("k")

	// The run-granular limit: rebuild, then an unbounded approximate
	// trim stops at the first run the limit cannot swallow whole.
	for ms := last.ms + 6; ms < last.ms+300; ms++ {
		rig.addQuick("k", ms, "f", "x")
	}
	rig.check("k")
	if got := rig.trim("k", false, 0, streamID{}, true, 130); got != 128 {
		t.Fatalf("MAXLEN ~0 LIMIT 130 removed %d, want the head run's 128", got)
	}
	// Limit 0 is unlimited: the rest of the stream drops at run
	// boundaries and the key survives empty.
	rest := int64(len(rig.model))
	if got := rig.trim("k", false, 0, streamID{}, true, 0); got != rest {
		t.Fatalf("MAXLEN ~0 removed %d, want %d", got, rest)
	}
	if n, err := rig.x.Len(ctx, []byte("k")); err != nil || n != 0 {
		t.Fatalf("Len after full trim = %d, %v", n, err)
	}

	// A missing key trims nothing and a string refuses.
	if got, err := rig.x.Trim(ctx, []byte("nosuch"), false, 0, streamID{}, false, 0); err != nil || got != 0 {
		t.Fatalf("Trim(missing) = %d, %v", got, err)
	}
	str, err := NewStr(rig.tr, StrConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := str.Set(ctx, []byte("plain"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.x.Trim(ctx, []byte("plain"), false, 0, streamID{}, false, 0); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Trim on a string = %v", err)
	}
}

// TestStreamTrimEdgeIO is the operator table's cost shape and X-I4 in
// one bill: an approximate trim never writes a run image, only deletes
// and the one root, and an exact trim rewrites at most the single
// boundary run however many runs fall.
func TestStreamTrimEdgeIO(t *testing.T) {
	rig := newStreamRig(t)
	ctx := context.Background()
	for ms := uint64(1); ms <= 600; ms++ {
		rig.addQuick("k", ms, "f", "vvvv")
	}
	rig.check("k")
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	batchBill := func() (puts, dels int) {
		t.Helper()
		mark := len(rig.rs.batches)
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		for _, b := range rig.rs.batches[mark:] {
			for _, op := range b.Ops {
				if op.Del {
					dels++
				} else {
					puts++
				}
			}
		}
		return puts, dels
	}

	// Approximate: three whole runs fall as deletes, one root put, and
	// no run image is ever rewritten (X-I4).
	if got := rig.trim("k", false, 200, streamID{}, true, 100*streamRunMaxEntries); got != 384 {
		t.Fatalf("MAXLEN ~200 removed %d, want 384", got)
	}
	puts, dels := batchBill()
	if puts != 1 || dels != 3 {
		t.Fatalf("approx trim billed %d puts, %d dels, want 1 and 3", puts, dels)
	}

	// Exact inside the head run: one run rewrite plus the root.
	if got := rig.trim("k", false, 100, streamID{}, false, 0); got != 116 {
		t.Fatalf("MAXLEN 100 removed %d, want 116", got)
	}
	puts, dels = batchBill()
	if puts != 2 || dels != 0 {
		t.Fatalf("exact boundary trim billed %d puts, %d dels, want 2 and 0", puts, dels)
	}

	// Exact to empty: every surviving run leaves as a delete, the root
	// rewrites once, and no run image is touched.
	if got := rig.trim("k", false, 0, streamID{}, false, 0); got != 100 {
		t.Fatalf("MAXLEN 0 removed %d, want 100", got)
	}
	puts, dels = batchBill()
	if puts != 1 || dels != 2 {
		t.Fatalf("full trim billed %d puts, %d dels, want 1 and 2", puts, dels)
	}
}

// TestStreamTrimPaged drives the same shapes over a paged fence at
// dialed caps: whole pages drop with their runs and their records
// delete after the root, the boundary page rewrites in place, and a
// stream emptied through the paged rung appends again onto a fresh
// page.
func TestStreamTrimPaged(t *testing.T) {
	defer SetStreamFenceCapsForTest(3, 2, 8)()
	rig := newStreamRig(t)
	ctx := context.Background()

	// ~1500 byte values put two entries in a run: 28 entries make 14
	// runs on 7 pages.
	val := strings.Repeat("v", 1500)
	for ms := uint64(1); ms <= 28; ms++ {
		rig.addQuick("k", ms, "f", val)
	}
	rig.check("k")
	sr := rig.streamRootOf("k")
	if !sr.paged || len(sr.pidx) < 5 {
		t.Fatalf("paged=%v pages=%d; the test wants a deep page index", sr.paged, len(sr.pidx))
	}
	headPage := sr.pidx[0].segid

	// Approximate MAXLEN across pages: whole runs only, and the
	// emptied head pages' records are gone afterwards.
	if got := rig.trim("k", false, 17, streamID{}, true, 100*streamRunMaxEntries); got != 10 {
		t.Fatalf("MAXLEN ~17 removed %d, want 10", got)
	}
	var pkbuf [SubkeySize]byte
	putHashFenceKey(pkbuf[:], sr.rooth, headPage)
	if _, ok, err := rig.tr.Get(ctx, pkbuf[:]); err != nil || ok {
		t.Fatalf("dropped page record still readable: ok=%v err=%v", ok, err)
	}

	// Exact MINID inside a run: the boundary run rewrites within its
	// page and everything below it drops, pages included.
	minid := rig.model[7].id
	if got := rig.trim("k", true, 0, minid, false, 0); got != 7 {
		t.Fatalf("paged MINID removed %d, want 7", got)
	}
	if rig.model[0].id != minid {
		t.Fatalf("head after paged MINID = %v, want %v", rig.model[0].id, minid)
	}

	// Exact to empty through the paged rung: key alive, IDs intact,
	// and the next append starts a fresh page.
	last := rig.streamRootOf("k").last
	rest := int64(len(rig.model))
	if got := rig.trim("k", false, 0, streamID{}, false, 0); got != rest {
		t.Fatalf("paged MAXLEN 0 removed %d, want %d", got, rest)
	}
	sr = rig.streamRootOf("k")
	if sr.count != 0 || len(sr.pidx) != 0 || !sr.paged || sr.last != last {
		t.Fatalf("emptied paged root = count %d pages %d paged %v last %v", sr.count, len(sr.pidx), sr.paged, sr.last)
	}
	rig.addQuick("k", last.ms+1, "f", val)
	rig.addQuick("k", last.ms+2, "f", val)
	rig.check("k")
}

// TestXtrimWire pins the XTRIM and XADD trim option grammar against
// Redis 8.8's observed replies: the error table with its exact texts,
// parse failures beating both the missing key and WRONGTYPE, and the
// trim-after-append order where XADD MAXLEN 0 lands the entry and then
// empties the stream without deleting the key.
func TestXtrimWire(t *testing.T) {
	do, clock := dispatchServer(t)

	for i := range 5 {
		*clock = int64(1_000_000 + i)
		if got := do("XADD", "s", "*", "f", "v"); !strings.HasPrefix(got, "$") {
			t.Fatal(got)
		}
	}

	// The error table, on live keys, missing keys, and a string key
	// alike: the parse always wins.
	do("SET", "plain", "v")
	table := []struct {
		args []string
		want string
	}{
		{[]string{"XTRIM", "s", "MAXLEN"}, "-ERR wrong number of arguments for 'xtrim' command\r\n"},
		{[]string{"XTRIM", "s", "FOO", "5"}, "-ERR syntax error\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "-1"}, "-ERR The MAXLEN argument must be >= 0.\r\n"},
		{[]string{"XTRIM", "nosuch", "MAXLEN", "-1"}, "-ERR The MAXLEN argument must be >= 0.\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "abc"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"XTRIM", "plain", "MAXLEN", "abc"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "~"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"XTRIM", "s", "MINID", "abc"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XTRIM", "s", "MINID", "-"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XTRIM", "s", "MINID", "+"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XTRIM", "s", "MINID", "*"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XTRIM", "s", "MINID", "5-*"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "5", "LIMIT", "2"}, "-ERR syntax error, LIMIT cannot be used without the special ~ option\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "=", "5", "LIMIT", "0"}, "-ERR syntax error, LIMIT cannot be used without the special ~ option\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "~", "5", "LIMIT", "abc"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "~", "5", "LIMIT", "-1"}, "-ERR The LIMIT argument must be >= 0.\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "5", "LIMIT"}, "-ERR syntax error\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "~", "5", "LIMIT"}, "-ERR syntax error\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "5", "extra"}, "-ERR syntax error\r\n"},
		{[]string{"XTRIM", "s", "MAXLEN", "5", "MAXLEN", "6"}, "-ERR syntax error, MAXLEN and MINID options at the same time are not compatible\r\n"},
		{[]string{"XTRIM", "plain", "MAXLEN", "5"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XADD", "s", "MAXLEN", "5", "LIMIT", "2", "*", "f", "v"}, "-ERR syntax error, LIMIT cannot be used without the special ~ option\r\n"},
		{[]string{"XADD", "s", "MAXLEN", "5", "MINID", "5", "*", "f", "v"}, "-ERR syntax error, MAXLEN and MINID options at the same time are not compatible\r\n"},
	}
	for _, row := range table {
		if got := do(row.args...); got != row.want {
			t.Fatalf("%v = %q, want %q", row.args, got, row.want)
		}
	}

	// The working forms: exact MAXLEN, a rerun's zero, the missing
	// key's zero, and MINID with both the bare-ms and full forms.
	if got := do("XTRIM", "s", "MAXLEN", "2"); got != ":3\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "s"); got != ":2\r\n" {
		t.Fatal(got)
	}
	if got := do("XTRIM", "s", "MAXLEN", "=", "2"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("XTRIM", "nosuch", "MAXLEN", "5"); got != ":0\r\n" {
		t.Fatal(got)
	}
	do("XADD", "m", "1-1", "f", "v")
	do("XADD", "m", "2-1", "f", "v")
	do("XADD", "m", "3-1", "f", "v")
	if got := do("XTRIM", "m", "MINID", "2"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("XTRIM", "m", "MINID", "2-2"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "m"); got != ":1\r\n" {
		t.Fatal(got)
	}

	// XADD with a trim clause runs the trim after the append, and the
	// key survives a trim to zero for NOMKSTREAM to find, with the
	// options legal in either order.
	*clock = 2_000_000
	if got := do("XADD", "s", "MAXLEN", "1", "*", "f", "v"); got != "$9\r\n2000000-0\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "s"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s", "MAXLEN", "0", "*", "f", "v"); got != "$9\r\n2000000-1\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "s"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("TYPE", "s"); got != "+stream\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s", "NOMKSTREAM", "MAXLEN", "~", "5", "*", "f", "v"); got != "$9\r\n2000000-2\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s", "MAXLEN", "~", "5", "NOMKSTREAM", "*", "f", "v"); got != "$9\r\n2000000-3\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "s"); got != ":2\r\n" {
		t.Fatal(got)
	}
}
