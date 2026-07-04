package f1raw

import (
	"bytes"
	"fmt"
	"testing"
)

// The partition data structure tests target the three ways slice 2 can be wrong before any
// command routes through it: the partition-aware key encode and decode disagree, the P=1 layout
// drifts from the unpartitioned bytes an existing store already holds, or a per-partition rebuild
// pulls in a sibling partition's members. They exercise the pure key helpers directly and build a
// partitioned set through the same engine calls a served set will use, so a test partition is
// byte-identical to a served one.

// TestPartKeyRoundTrip checks that appendPartKey and splitPartKey are inverses across every legal
// partition count, and that the partition a key lands in is exactly partOf(member, P). A key built
// for a member must decode back to the same set key, the same member, and the partition the member
// routes to, or a routed lookup in slice 3 would miss the row it just wrote.
func TestPartKeyRoundTrip(t *testing.T) {
	skeys := [][]byte{[]byte("s"), []byte("myset"), []byte(""), bytes.Repeat([]byte("k"), 300)}
	members := [][]byte{[]byte("m0"), []byte("member-one"), []byte(""), []byte("\x00\x01\xff"), bytes.Repeat([]byte("x"), 200)}
	for _, p := range []int{1, 2, 4, 8, 16, 256} {
		for _, sk := range skeys {
			for _, m := range members {
				key := appendPartKey(nil, sk, m, p)
				gotSkey, gotPart, gotMember, ok := splitPartKey(key, p)
				if !ok {
					t.Fatalf("p=%d skey=%q member=%q: splitPartKey reported not ok", p, sk, m)
				}
				if !bytes.Equal(gotSkey, sk) {
					t.Fatalf("p=%d: skey round-trip got %q want %q", p, gotSkey, sk)
				}
				if !bytes.Equal(gotMember, m) {
					t.Fatalf("p=%d: member round-trip got %q want %q", p, gotMember, m)
				}
				wantPart := partOf(m, p)
				if gotPart != wantPart {
					t.Fatalf("p=%d skey=%q member=%q: decoded part %d, partOf says %d", p, sk, m, gotPart, wantPart)
				}
				if p == 1 && gotPart != 0 {
					t.Fatalf("p=1: part must be 0, got %d", gotPart)
				}
				if wantPart < 0 || wantPart >= p {
					t.Fatalf("p=%d: partOf out of range: %d", p, wantPart)
				}
			}
		}
	}
}

// TestPartKeyP1Identical pins the compatibility promise: a P=1 partition key and prefix are the
// exact bytes the unpartitioned member key and set prefix already produce, so a set that never
// engages partitioning stores byte-identical rows and an existing store reads back unchanged. If
// this drifts, turning the feature on would orphan every set written before it.
func TestPartKeyP1Identical(t *testing.T) {
	for _, sk := range []string{"s", "myset", "", "another-set-key"} {
		for _, m := range []string{"m0", "member", "", "\x00\xff"} {
			gotKey := appendPartKey(nil, []byte(sk), []byte(m), 1)
			wantKey := memberKeyBytes(sk, m)
			if !bytes.Equal(gotKey, wantKey) {
				t.Fatalf("P=1 member key skey=%q member=%q: got %x want %x", sk, m, gotKey, wantKey)
			}
			gotPrefix := appendPartPrefix(nil, []byte(sk), 0, 1)
			wantPrefix := setPrefixBytes(sk)
			if !bytes.Equal(gotPrefix, wantPrefix) {
				t.Fatalf("P=1 prefix skey=%q: got %x want %x", sk, gotPrefix, wantPrefix)
			}
		}
	}
}

// TestPartPrefixBoundsOnePartition checks that a partition prefix bounds precisely its own
// partition: every key built for members that route to a partition starts with that partition's
// prefix, and no key from a sibling partition does. This is what lets derivePartVec scan one
// partition's prefix range and capture exactly that partition's members.
func TestPartPrefixBoundsOnePartition(t *testing.T) {
	const p = 8
	skey := []byte("hot")
	// Bucket 4000 members by their routed partition and confirm each key sits under its own
	// partition prefix and under no other.
	byPart := map[int][][]byte{}
	for i := 0; i < 4000; i++ {
		m := []byte(fmt.Sprintf("m%d", i))
		byPart[partOf(m, p)] = append(byPart[partOf(m, p)], appendPartKey(nil, skey, m, p))
	}
	for part := 0; part < p; part++ {
		prefix := appendPartPrefix(nil, skey, part, p)
		for _, key := range byPart[part] {
			if !bytes.HasPrefix(key, prefix) {
				t.Fatalf("part %d key %x lacks its own prefix %x", part, key, prefix)
			}
		}
		for other := 0; other < p; other++ {
			if other == part {
				continue
			}
			otherPrefix := appendPartPrefix(nil, skey, other, p)
			for _, key := range byPart[part] {
				if bytes.HasPrefix(key, otherPrefix) {
					t.Fatalf("part %d key %x wrongly matches part %d prefix %x", part, key, other, otherPrefix)
				}
			}
		}
	}
}

// TestPartOfDistribution checks the router spreads members close to evenly across partitions, the
// property section 2.2 rests on: an uneven split would leave one partition hot and defeat the point
// of partitioning. It is a coarse check, not a chi-squared test: with 16k members over 16 partitions
// the expected count is 1000 each and every bucket should land inside a generous band.
func TestPartOfDistribution(t *testing.T) {
	const (
		p = 16
		n = 16000
	)
	counts := make([]int, p)
	for i := 0; i < n; i++ {
		counts[partOf([]byte(fmt.Sprintf("member:%d", i)), p)]++
	}
	expect := n / p
	for part, c := range counts {
		if c < expect*3/4 || c > expect*5/4 {
			t.Fatalf("partition %d holds %d members, expected near %d (skewed router)", part, c, expect)
		}
	}
}

// TestDerivePartVec builds a partitioned set through the real add path (a member row plus its
// ordered-index node under the partition-prefixed key) and checks that derivePartVec rebuilds each
// partition's vector holding exactly that partition's members and nothing from a sibling. This is
// the per-partition analogue of the whole-set deriveOnFirstDraw, and it is the operation slice 4's
// first draw against a partition will run.
func TestDerivePartVec(t *testing.T) {
	const (
		p = 4
		n = 2000
	)
	s := New(1<<20, 1<<30)
	s.SetTopKindFunc(func(byte) bool { return false })
	skey := []byte("bigset")

	// Insert every member under its routed partition key, mirroring what a routed SADD will do.
	want := make([]map[string]bool, p)
	for i := range want {
		want[i] = map[string]bool{}
	}
	for i := 0; i < n; i++ {
		m := []byte(fmt.Sprintf("m%d", i))
		part := partOf(m, p)
		mk := appendPartMemberKey(nil, skey, m, part, p)
		created, err := s.PutKind(mk, nil, tKindSetMember)
		if err != nil {
			t.Fatalf("PutKind(%q): %v", m, err)
		}
		if !created {
			t.Fatalf("member %q reported not created", m)
		}
		s.CollInsert(mk, tKindSetMember)
		want[part][string(m)] = true
	}

	ps := newPartSet(p)
	var rebuilt int
	for part := 0; part < p; part++ {
		v := s.derivePartVec(skey, part, p)
		ps.setPartVec(part, v)
		ps.count[part].Store(int64(len(v.slots)))
		rebuilt += len(v.slots)

		got := map[string]bool{}
		for _, off := range v.slots {
			key := s.keyAt(off)
			gotSkey, gotPart, gotMember, ok := splitPartKey(key, p)
			if !ok {
				t.Fatalf("part %d: keyAt(%x) did not split", part, key)
			}
			if !bytes.Equal(gotSkey, skey) {
				t.Fatalf("part %d: rebuilt member under wrong set key %q", part, gotSkey)
			}
			if gotPart != part {
				t.Fatalf("part %d: rebuilt vector holds a member routed to partition %d", part, gotPart)
			}
			got[string(gotMember)] = true
		}
		if len(got) != len(want[part]) {
			t.Fatalf("part %d: rebuilt %d members, want %d", part, len(got), len(want[part]))
		}
		for m := range want[part] {
			if !got[m] {
				t.Fatalf("part %d: rebuilt vector missing member %q", part, m)
			}
		}
	}
	if rebuilt != n {
		t.Fatalf("rebuilt %d members across all partitions, want %d", rebuilt, n)
	}
	if ps.total() != int64(n) {
		t.Fatalf("partSet.total() = %d, want %d", ps.total(), n)
	}
}

// TestValidPartCount checks the power-of-two-in-range guard, the invariant partOf's mask routing
// depends on.
func TestValidPartCount(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256} {
		if !validPartCount(p) {
			t.Fatalf("validPartCount(%d) = false, want true", p)
		}
	}
	for _, p := range []int{0, 3, 5, 6, 7, 100, 257, 512, -1, -2} {
		if validPartCount(p) {
			t.Fatalf("validPartCount(%d) = true, want false", p)
		}
	}
}
