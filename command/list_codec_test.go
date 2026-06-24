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

// listBlobReportedEnc must report the same encoding as the canonical listEncoding
// over the whole element set, both below and across the listpack threshold.
func TestListBlobReportedEncMatchesListEncoding(t *testing.T) {
	lim := defaultEncLimits()
	maxEntries, _ := lim.listLimits()

	small := make([][]byte, maxEntries) // exactly the entry cap stays listpack
	for i := range small {
		small[i] = []byte("v")
	}
	bigElem := [][]byte{[]byte(strings.Repeat("a", listMaxListpackElemBytes+1))}

	cases := []struct {
		name string
		all  [][]byte
		push [][]byte
	}{
		{"at-cap", small, [][]byte{[]byte("v")}},
		{"over-cap", append(append([][]byte(nil), small...), []byte("v")), [][]byte{[]byte("v")}},
		{"big-element", bigElem, bigElem},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := listEncoding(lim, tc.all, keyspace.EncListpack)
			got, err := listBlobReportedEnc(lim, keyspace.EncListpack, len(tc.all), tc.push, func() ([][]byte, error) {
				return tc.all, nil
			})
			if err != nil {
				t.Fatalf("listBlobReportedEnc: %v", err)
			}
			if got != want {
				t.Fatalf("enc = %d want %d", got, want)
			}
		})
	}

	// A quicklist never demotes regardless of the new size.
	got, err := listBlobReportedEnc(lim, keyspace.EncQuicklist, 1, [][]byte{[]byte("v")}, func() ([][]byte, error) {
		t.Fatal("decode must not be called for a sticky quicklist")
		return nil, nil
	})
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
