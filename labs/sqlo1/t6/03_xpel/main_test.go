package main

import (
	"math/rand"
	"testing"
)

// refEntry mirrors pelEntry for the reference model.
type refEntry struct {
	ms, seq uint64
	cidx    byte
	dcount  uint32
	dtime   uint64
}

// TestPelOracle drives the segment model and a flat reference slice
// through random deliver, ack, and claim interleavings and demands
// identical pending sets, identical ownership and delivery fields,
// canonical segment sizes, and untouched entry runs.
func TestPelOracle(t *testing.T) {
	for _, tc := range []struct{ segMax, pcap int }{
		{256, 1024}, {1024, 1024}, {4096, 16}, {4096, 1024},
	} {
		rng := rand.New(rand.NewSource(int64(tc.segMax*31 + tc.pcap)))
		bill := newBilling()
		p := newPel(tc.segMax, tc.pcap, bill)
		g := &gen{}
		var ref []refEntry
		now := uint64(100)
		for step := range 400 {
			now++
			switch op := rng.Intn(3); {
			case op == 0 || len(ref) == 0:
				n := 1 + rng.Intn(20)
				batch := make([]pelEntry, 0, n)
				for range n {
					e := g.next(now)
					batch = append(batch, e)
					ref = append(ref, refEntry{ms: e.ms, seq: e.seq, cidx: e.cidx, dcount: e.dcount, dtime: e.dtime})
				}
				p.deliver(batch)
			case op == 1:
				n := 1 + rng.Intn(min(20, len(ref)))
				ids := make([][2]uint64, 0, n)
				seen := map[int]bool{}
				for len(ids) < n {
					i := rng.Intn(len(ref))
					if seen[i] {
						continue
					}
					seen[i] = true
					ids = append(ids, [2]uint64{ref[i].ms, ref[i].seq})
				}
				acked := p.ack(ids)
				if acked != n {
					t.Fatalf("step %d: acked %d of %d", step, acked, n)
				}
				kept := ref[:0]
				for _, e := range ref {
					if !seen2(ids, e.ms, e.seq) {
						kept = append(kept, e)
					}
				}
				ref = kept
			default:
				i := rng.Intn(len(ref))
				curMs, curSeq := ref[i].ms, ref[i].seq
				if curSeq > 0 {
					curSeq--
				} else {
					curMs--
					curSeq = 1 << 40
				}
				count := 1 + rng.Intn(10)
				nc := byte(rng.Intn(2))
				claimed, _ := p.claim(curMs, curSeq, count, nc, now)
				want := 0
				for j := range ref {
					e := &ref[j]
					if e.ms < curMs || (e.ms == curMs && e.seq < curSeq) {
						continue
					}
					if want >= count {
						break
					}
					e.cidx = nc
					e.dcount++
					e.dtime = now
					want++
				}
				if claimed != want {
					t.Fatalf("step %d: claimed %d want %d", step, claimed, want)
				}
			}
			checkPel(t, p, ref)
		}
		if bill.runRewr != 0 {
			t.Fatalf("entry runs touched %d times", bill.runRewr)
		}
	}
}

func seen2(ids [][2]uint64, ms, seq uint64) bool {
	for _, id := range ids {
		if id[0] == ms && id[1] == seq {
			return true
		}
	}
	return false
}

// checkPel audits the model against the reference: same entries in
// ID order, fence bases and pending count exact, every segment
// within caps and byte-canonical, and the codec roundtrips.
func checkPel(t *testing.T, p *pel, ref []refEntry) {
	t.Helper()
	var got []refEntry
	p.walk(func(e *pelEntry) {
		got = append(got, refEntry{ms: e.ms, seq: e.seq, cidx: e.cidx, dcount: e.dcount, dtime: e.dtime})
	})
	if len(got) != len(ref) || p.pending != len(ref) {
		t.Fatalf("pending %d walk %d want %d", p.pending, len(got), len(ref))
	}
	for i := range ref {
		if got[i] != ref[i] {
			t.Fatalf("entry %d: got %+v want %+v", i, got[i], ref[i])
		}
	}
	for i := 1; i < len(got); i++ {
		if !(got[i-1].ms < got[i].ms || (got[i-1].ms == got[i].ms && got[i-1].seq < got[i].seq)) {
			t.Fatalf("walk out of order at %d", i)
		}
	}
	var buf []byte
	for _, s := range p.segs {
		if len(s.ents) == 0 {
			t.Fatal("empty segment retained")
		}
		if len(s.ents) > p.pcap {
			t.Fatalf("segment over entry cap: %d", len(s.ents))
		}
		want := *s
		want.recount()
		if s.bytes != want.bytes {
			t.Fatalf("segment %d bytes %d want canonical %d", s.id, s.bytes, want.bytes)
		}
		buf = encodeSeg(buf, s)
		if len(buf) != s.bytes-segEnvBytes {
			t.Fatalf("segment %d encoded %d want %d", s.id, len(buf), s.bytes-segEnvBytes)
		}
		dec, err := decodeSeg(buf)
		if err != nil {
			t.Fatalf("segment %d decode: %v", s.id, err)
		}
		if len(dec) != len(s.ents) {
			t.Fatalf("segment %d roundtrip length %d want %d", s.id, len(dec), len(s.ents))
		}
		for j := range dec {
			if dec[j] != s.ents[j] {
				t.Fatalf("segment %d entry %d roundtrip mismatch", s.id, j)
			}
		}
	}
	if p.groupBill() != groupHdrBytes+groupNameB+consBytes+len(p.segs)*fenceEntBytes {
		t.Fatal("group bill drifted from the fence")
	}
}
