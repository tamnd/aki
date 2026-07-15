package sqlo1a

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

func TestScrubRollingSweep(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	db.now = func() int64 { return 1000 }

	// The zero-length key leads, and gated rows (expired, foreign tag)
	// are mixed in: a scrub counts and checks everything in the file.
	rawPut(t, db, []byte(""), recordTag, 0, 0, []byte("empty"), false)
	total := 1
	for i := range 9 {
		exp, tag := int64(0), int64(recordTag)
		if i%3 == 1 {
			exp = 500
		}
		if i%3 == 2 {
			tag = 5
		}
		rawPut(t, db, fmt.Appendf(nil, "k%d", i), tag, exp, 0, []byte("v"), false)
		total++
	}

	var scanned int
	var passes []int
	var cur sqlo1.Cursor
	for {
		next, res, err := db.Scrub(ctx, cur, 3)
		if err != nil {
			t.Fatalf("pass %d: %v", len(passes), err)
		}
		if len(res.Corrupt) != 0 {
			t.Fatalf("pass %d reported rot in a clean store: %v", len(passes), res.Corrupt)
		}
		scanned += res.Scanned
		passes = append(passes, res.Scanned)
		if next == nil {
			break
		}
		cur = next
	}
	if scanned != total {
		t.Fatalf("sweep scanned %d rows over passes %v, want %d", scanned, passes, total)
	}
	if want := []int{3, 3, 3, 1}; len(passes) != len(want) {
		t.Fatalf("sweep took passes %v, want %v", passes, want)
	}
}

func TestScrubReportsEveryRottenRow(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	db.now = func() int64 { return 1000 }

	rot := map[string]bool{"bad-a": true, "bad-m": true, "bad-z": true}
	for i := range 10 {
		rawPut(t, db, fmt.Appendf(nil, "good%d", i), recordTag, 0, 0, []byte("v"), false)
	}
	for k := range rot {
		rawPut(t, db, []byte(k), recordTag, 0, 0, []byte("v"), true)
	}
	// Rot inside a gated row must not hide behind the expiry.
	rawPut(t, db, []byte("bad-expired"), recordTag, 500, 0, []byte("v"), true)
	rot["bad-expired"] = true

	next, res, err := db.Scrub(ctx, nil, 0)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if next != nil {
		t.Fatalf("default limit did not finish a 14-row sweep")
	}
	if res.Scanned != 14 {
		t.Fatalf("scanned %d rows, want 14: rot must not stop the sweep", res.Scanned)
	}
	if len(res.Corrupt) != len(rot) {
		t.Fatalf("reported %d rotten rows, want %d: %v", len(res.Corrupt), len(rot), res.Corrupt)
	}
	seen := map[string]bool{}
	for _, cerr := range res.Corrupt {
		if !errors.Is(cerr, ErrCorrupt) {
			t.Fatalf("finding does not wrap ErrCorrupt: %v", cerr)
		}
		for k := range rot {
			if strings.Contains(cerr.Error(), fmt.Sprintf("%x", []byte(k))) {
				seen[k] = true
			}
		}
	}
	if len(seen) != len(rot) {
		t.Fatalf("findings name %d of %d rotten keys: %v", len(seen), len(rot), res.Corrupt)
	}

	// Detection only: the damaged rows are still there for forensics and
	// the good ones still read.
	if n := countWhereKey(t, db, `SELECT count(*) FROM kv WHERE k = ?1`, []byte("bad-a")); n != 1 {
		t.Fatalf("scrub deleted a damaged row")
	}
	if _, err := db.Get(ctx, []byte("good0")); err != nil {
		t.Fatalf("good row after scrub: %v", err)
	}
}

func TestScrubRejectsUnknownCursor(t *testing.T) {
	db := openTest(t)
	if _, _, err := db.Scrub(context.Background(), sqlo1.Cursor{0xff, 'k'}, 10); err == nil {
		t.Fatal("unknown cursor tag accepted")
	}
}
