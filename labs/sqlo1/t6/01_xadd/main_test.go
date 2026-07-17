package main

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestStreamModelOracle drives the model against a reference slice
// through appends and both trim forms and holds every doc 10 shape
// claim: ID order, run caps, X-I4's whole-run rule for approximate
// trim and one-edge-rewrite rule for exact trim, and count agreement
// at every step.
func TestStreamModelOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	bill := newBilling()
	s := newStream(0, 512, 16, 3, bill)
	g := &gen{rng: rng, nfields: 3, elen: 60, burst: 5}
	var ref []*entry

	check := func(step string) {
		t.Helper()
		if s.count != len(ref) {
			t.Fatalf("%s: count %d, reference holds %d", step, s.count, len(ref))
		}
		var walked []*entry
		s.walk(func(e *entry) { walked = append(walked, e) })
		if len(walked) != len(ref) {
			t.Fatalf("%s: walked %d, reference holds %d", step, len(walked), len(ref))
		}
		for i, e := range walked {
			if e.ms != ref[i].ms || e.seq != ref[i].seq {
				t.Fatalf("%s: entry %d is %d-%d, reference holds %d-%d", step, i, e.ms, e.seq, ref[i].ms, ref[i].seq)
			}
		}
		prevMs, prevSeq := uint64(0), uint64(0)
		for ri, r := range s.runs {
			if len(r.entries) == 0 {
				t.Fatalf("%s: run %d empty", step, ri)
			}
			if len(r.entries) > s.ecap {
				t.Fatalf("%s: run %d holds %d entries past the cap", step, ri, len(r.entries))
			}
			for _, e := range r.entries {
				if e.ms < prevMs || (e.ms == prevMs && e.seq <= prevSeq && !(prevMs == 0 && prevSeq == 0)) {
					t.Fatalf("%s: ID order broken at %d-%d after %d-%d", step, e.ms, e.seq, prevMs, prevSeq)
				}
				prevMs, prevSeq = e.ms, e.seq
			}
		}
	}

	for i := range 3000 {
		e := g.next()
		s.xadd(e)
		ref = append(ref, e)
		if i%97 == 96 {
			before := len(s.runs)
			edge := bill.edgeRewr
			s.trimApprox(2000)
			if bill.edgeRewr != edge {
				t.Fatal("approximate trim rewrote a run")
			}
			// Approximate trim only cuts whole runs: the survivors'
			// head run is intact, so the reference drops exactly the
			// entries the dropped runs held.
			dropped := before - len(s.runs)
			if dropped > 0 {
				total := len(ref) - s.count
				ref = ref[total:]
			}
			if s.count < 2000 && dropped > 0 {
				t.Fatalf("approximate trim went under the cap: %d", s.count)
			}
			check("trim~")
		}
	}
	check("appended")

	edge := bill.edgeRewr
	s.trimExact(1500)
	if s.count != 1500 {
		t.Fatalf("exact trim left %d", s.count)
	}
	if bill.edgeRewr-edge > 1 {
		t.Fatalf("exact trim rewrote %d runs, at most one edge allowed", bill.edgeRewr-edge)
	}
	ref = ref[len(ref)-1500:]
	check("trim exact")

	// A cap the stream is already under is writeless.
	frames := bill.walFrames
	s.trimExact(5000)
	if bill.walFrames != frames {
		t.Fatal("no-op trim billed frames")
	}
	check("trim noop")
}

// TestRunCodecRoundtrip pins encodeRun against decodeRun across entry
// shapes: bursty and sparse IDs, single and many fields, empty and
// fat values.
func TestRunCodecRoundtrip(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for _, shape := range []struct {
		nfields, elen, burst, n int
	}{
		{1, 8, 1, 1},
		{1, 0, 1, 7},
		{4, 200, 10, 128},
		{16, 64, 3, 50},
		{2, 2000, 100, 12},
	} {
		bill := newBilling()
		s := newStream(0, 1<<20, shape.n, shape.nfields, bill)
		g := &gen{rng: rng, nfields: shape.nfields, elen: shape.elen, burst: shape.burst}
		for range shape.n {
			s.xadd(g.next())
		}
		if len(s.runs) != 1 {
			t.Fatalf("shape %+v cut %d runs, want the single-run form", shape, len(s.runs))
		}
		r := s.runs[0]
		buf := encodeRun(nil, s.names, r)
		names, entries, err := decodeRun(buf)
		if err != nil {
			t.Fatalf("shape %+v: decode: %v", shape, err)
		}
		if len(names) != len(s.names) {
			t.Fatalf("shape %+v: %d names back, want %d", shape, len(names), len(s.names))
		}
		for i, nm := range names {
			if !bytes.Equal(nm, s.names[i]) {
				t.Fatalf("shape %+v: name %d is %q, want %q", shape, i, nm, s.names[i])
			}
		}
		if len(entries) != len(r.entries) {
			t.Fatalf("shape %+v: %d entries back, want %d", shape, len(entries), len(r.entries))
		}
		for i, e := range entries {
			want := r.entries[i]
			if e.ms != want.ms || e.seq != want.seq {
				t.Fatalf("shape %+v: entry %d ID %d-%d, want %d-%d", shape, i, e.ms, e.seq, want.ms, want.seq)
			}
			if len(e.vals) != len(want.vals) {
				t.Fatalf("shape %+v: entry %d has %d values, want %d", shape, i, len(e.vals), len(want.vals))
			}
			for j, v := range e.vals {
				if !bytes.Equal(v, want.vals[j]) {
					t.Fatalf("shape %+v: entry %d value %d differs", shape, i, j)
				}
			}
		}
	}
}

// TestRunSizeArithmetic holds the incremental byte accounting the WAL
// bill leans on to the real codec's output.
func TestRunSizeArithmetic(t *testing.T) {
	bill := newBilling()
	s := newStream(0, 4032, 128, 4, bill)
	g := &gen{rng: rand.New(rand.NewSource(3)), nfields: 4, elen: 120, burst: 7}
	for range 1000 {
		s.xadd(g.next())
	}
	var buf []byte
	for ri, r := range s.runs {
		buf = encodeRun(buf, s.names, r)
		if got := runEnvBytes + len(buf); got != r.bytes {
			t.Fatalf("run %d: tracked %d bytes, real encoding is %d", ri, r.bytes, got)
		}
	}
}
