package keyspace

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// scoreBitsDiag mirrors command.zScoreBits: a float64 score to a uint64 whose
// big-endian byte order matches numeric order. The diagnostic needs the same
// score-row layout the zset coll form uses so Rank lands on a real row.
func scoreBitsDiag(f float64) uint64 {
	b := math.Float64bits(f)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | (1 << 63)
}

func memberRowDiag(member []byte) []byte {
	k := make([]byte, 1+len(member))
	k[0] = 'm'
	copy(k[1:], member)
	return k
}

func scoreRowDiag(score float64, member []byte) []byte {
	k := make([]byte, 0, 1+8+len(member))
	k = append(k, 's')
	var u [8]byte
	binary.BigEndian.PutUint64(u[:], scoreBitsDiag(score))
	k = append(k, u[:]...)
	return append(k, member...)
}

func scoreValueDiag(score float64) []byte {
	var u [8]byte
	binary.BigEndian.PutUint64(u[:], scoreBitsDiag(score))
	return u[:]
}

// TestLTMZRankOrderStatSurvivesReopen is the diagnostic for the ZRANK LTM gap.
// It builds a zset-shaped order-statistic sub-tree the way a coll-form ZADD does
// (member rows then score rows, TypeZSet so collCreateTree picks the order-stat
// tree), commits, closes the file, and reopens it. The question it settles is
// binary: after a reload from the .aki file, does CollRead report OrderStat, so
// ZRANK takes the O(log n) Rank descent, or does it fall back to the O(rank)
// count walk that would explain a 10x slowdown under a memory cap.
func TestLTMZRankOrderStatSurvivesReopen(t *testing.T) {
	const n = 20000
	key := []byte("z:0")
	pad := make([]byte, 16)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte {
		m := make([]byte, 0, 12+len(pad))
		m = append(m, []byte{
			byte('0' + i/100000000000%10), byte('0' + i/10000000000%10),
			byte('0' + i/1000000000%10), byte('0' + i/100000000%10),
			byte('0' + i/10000000%10), byte('0' + i/1000000%10),
			byte('0' + i/100000%10), byte('0' + i/10000%10),
			byte('0' + i/1000%10), byte('0' + i/100%10),
			byte('0' + i/10%10), byte('0' + i%10),
		}...)
		return append(m, pad...)
	}

	fs := vfs.NewMem()
	p, err := pager.Create(fs, "z.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatal(err)
	}
	ks, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	db := ks.dbs[0]
	if err := db.CollUpdate(key, TypeZSet, EncSkiplist, func(w *CollWriter) error {
		for i := range n {
			m := member(i)
			if _, e := w.Put(scoreRowDiag(float64(i), m), nil); e != nil {
				return e
			}
			if _, e := w.Put(memberRowDiag(m), scoreValueDiag(float64(i))); e != nil {
				return e
			}
		}
		w.SetCount(uint64(n))
		return nil
	}); err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen from the file, exactly as the LTM benchmark restarts aki under the cap.
	p2, err := pager.Open(fs, "z.aki", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := Open(p2)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}
	db2 := ks2.dbs[0]

	var sawOrderStat bool
	var rankOf5000 uint64
	ok, err := db2.CollRead(key, func(r *CollReader) error {
		sawOrderStat = r.OrderStat()
		if r.Count() != n {
			t.Fatalf("count after reopen = %d, want %d", r.Count(), n)
		}
		if sawOrderStat {
			// ZRANK's second descent: rank of the score row for member 5000.
			rk, _, e := r.Rank(scoreRowDiag(5000, member(5000)))
			if e != nil {
				return e
			}
			rankOf5000 = rk
		}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("collread: ok=%v err=%v", ok, err)
	}
	if !sawOrderStat {
		t.Fatalf("FALLBACK CONFIRMED: reopened zset sub-tree reports OrderStat=false, "+
			"so coll-form ZRANK takes the O(rank) count walk, not the O(log n) Rank descent")
	}
	// Score rows sort after the n member rows, so the absolute rank of member 5000's
	// score row is n + 5000.
	if want := uint64(n + 5000); rankOf5000 != want {
		t.Fatalf("Rank after reopen = %d, want %d (order-stat counts wrong after reload)", rankOf5000, want)
	}
}
