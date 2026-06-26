package command

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
)

// listBlobPush splices the pushed run into the raw body, so its result must match
// the simple decode, mutate, re-encode reference for both ends and for a fresh
// list.
func TestListBlobPushMatchesReference(t *testing.T) {
	cases := []struct {
		name  string
		start [][]byte
		vals  [][]byte
		head  bool
	}{
		{"rpush-fresh", nil, [][]byte{[]byte("a"), []byte("b")}, false},
		{"lpush-fresh", nil, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, true},
		{"rpush-onto", [][]byte{[]byte("x"), []byte("y")}, [][]byte{[]byte("z")}, false},
		{"lpush-onto", [][]byte{[]byte("x"), []byte("y")}, [][]byte{[]byte("p"), []byte("q")}, true},
		{"rpush-empty-elem", [][]byte{[]byte("x")}, [][]byte{[]byte("")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := listEncode(tc.start)
			got, gotN, err := listBlobPush(body, tc.vals, tc.head)
			if err != nil {
				t.Fatalf("listBlobPush: %v", err)
			}

			ref := append([][]byte(nil), tc.start...)
			if tc.head {
				next := make([][]byte, 0, len(tc.vals)+len(ref))
				for i := len(tc.vals) - 1; i >= 0; i-- {
					next = append(next, tc.vals[i])
				}
				ref = append(next, ref...)
			} else {
				ref = append(ref, tc.vals...)
			}
			if gotN != len(ref) {
				t.Fatalf("count = %d want %d", gotN, len(ref))
			}
			if !bytes.Equal(got, listEncode(ref)) {
				t.Fatalf("body mismatch:\n got %x\nwant %x", got, listEncode(ref))
			}
			decoded, err := listDecode(got)
			if err != nil {
				t.Fatalf("listDecode spliced body: %v", err)
			}
			if !equalElems(decoded, ref) {
				t.Fatalf("decoded = %v want %v", decoded, ref)
			}
		})
	}
}

// listBlobReportedEnc reads the body directly and must agree with the canonical
// listEncoding over the decoded element set, under the default -2 byte budget and
// under a positive entry-count fill.
func TestListBlobReportedEncMatchesListEncoding(t *testing.T) {
	rep := func(n int, s string) [][]byte {
		out := make([][]byte, n)
		for i := range out {
			out[i] = []byte(s)
		}
		return out
	}

	cases := []struct {
		name string
		lim  encLimits
		all  [][]byte
	}{
		// Default -2: a short-element list stays listpack well past 128 entries.
		{"default-200-short", defaultEncLimits(), rep(200, "v")},
		// A single long element stays listpack: there is no per-element cap, only
		// the 8KB byte budget.
		{"default-long-element", defaultEncLimits(), [][]byte{[]byte(strings.Repeat("a", 200))}},
		// Enough bytes to cross the 8KB tier flips to quicklist.
		{"default-over-8kb", defaultEncLimits(), rep(1000, "0123456789")},
		// Positive fill caps the entry count: 129 short elements exceed 128.
		{"fill128-over-cap", encLimits{listSize: 128}, rep(129, "v")},
		{"fill128-at-cap", encLimits{listSize: 128}, rep(128, "v")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := listEncode(tc.all)
			want := listEncoding(tc.lim, tc.all, keyspace.EncListpack)
			got, err := listBlobReportedEnc(tc.lim, keyspace.EncListpack, body)
			if err != nil {
				t.Fatalf("listBlobReportedEnc: %v", err)
			}
			if got != want {
				t.Fatalf("enc = %d want %d", got, want)
			}
		})
	}

	// A quicklist never demotes regardless of the new size.
	got, err := listBlobReportedEnc(defaultEncLimits(), keyspace.EncQuicklist, listEncode(rep(1, "v")))
	if err != nil {
		t.Fatalf("listBlobReportedEnc sticky: %v", err)
	}
	if got != keyspace.EncQuicklist {
		t.Fatalf("sticky enc = %d want quicklist", got)
	}
}

func equalElems(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
