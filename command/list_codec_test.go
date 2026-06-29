package command

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
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

// BenchmarkLRange100 contrasts the two ways to answer LRANGE 0 99 on a hot
// blob-form list: the old path decodes the whole body into a [][]byte, slices the
// window, and writes it; the streaming path walks the encoded body in place and
// writes only the window. The redis-benchmark lrange_100 shape is a list grown
// far past 100 elements read with a 100-element window, so the decode allocates
// for the whole list while the stream allocates nothing.
func BenchmarkLRange100(b *testing.B) {
	const n = 1000
	elems := make([][]byte, n)
	for i := range elems {
		elems[i] = []byte("element-value-0123456789")
	}
	body := listEncode(elems)

	b.Run("decode-slice-write", func(b *testing.B) {
		var buf bytes.Buffer
		b.ReportAllocs()
		for range b.N {
			buf.Reset()
			enc := resp.NewEncoder(&buf, 2)
			decoded, err := listDecode(body)
			if err != nil {
				b.Fatal(err)
			}
			out := listSlice(decoded, 0, 99)
			enc.WriteArrayLen(len(out))
			for _, e := range out {
				enc.WriteBulkString(e)
			}
		}
	})

	b.Run("stream-window", func(b *testing.B) {
		var buf bytes.Buffer
		b.ReportAllocs()
		for range b.N {
			buf.Reset()
			enc := resp.NewEncoder(&buf, 2)
			if !listBlobRangeReply(enc, body, 0, 99) {
				b.Fatal("reported corrupt")
			}
		}
	})
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

// listBlobRangeReply streams the requested window straight off the encoded body.
// Its bytes must match the reference of decoding, slicing with listSlice, and
// writing an array of bulk strings, across every index form. A corrupt body must
// return false with nothing written so the caller can fall back.
func TestListBlobRangeReplyMatchesReference(t *testing.T) {
	elems := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte(""), []byte("eeeee")}
	body := listEncode(elems)

	ranges := []struct{ start, stop int64 }{
		{0, -1}, {0, 0}, {1, 3}, {2, 100}, {-2, -1}, {-100, 1},
		{3, 2}, {5, 10}, {-1, -1}, {0, 4}, {2, 2},
	}
	for _, r := range ranges {
		var ref bytes.Buffer
		re := resp.NewEncoder(&ref, 2)
		out := listSlice(elems, r.start, r.stop)
		re.WriteArrayLen(len(out))
		for _, e := range out {
			re.WriteBulkString(e)
		}

		var got bytes.Buffer
		if !listBlobRangeReply(resp.NewEncoder(&got, 2), body, r.start, r.stop) {
			t.Fatalf("range [%d,%d]: reported corrupt on a good body", r.start, r.stop)
		}
		if !bytes.Equal(got.Bytes(), ref.Bytes()) {
			t.Fatalf("range [%d,%d]: got %q want %q", r.start, r.stop, got.Bytes(), ref.Bytes())
		}
	}

	// An empty body is a valid empty list.
	var empty bytes.Buffer
	if !listBlobRangeReply(resp.NewEncoder(&empty, 2), nil, 0, -1) {
		t.Fatal("empty body reported corrupt")
	}
	if empty.String() != "*0\r\n" {
		t.Fatalf("empty body: got %q want %q", empty.String(), "*0\r\n")
	}

	// A body whose element length runs past the end is corrupt: nothing written,
	// false returned, so the caller takes the cold path.
	corrupt := []byte{0x01, 0x7f} // count 1, element length 127, no bytes follow
	var cb bytes.Buffer
	if listBlobRangeReply(resp.NewEncoder(&cb, 2), corrupt, 0, -1) {
		t.Fatal("corrupt body reported ok")
	}
	if cb.Len() != 0 {
		t.Fatalf("corrupt body wrote %q, want nothing", cb.Bytes())
	}
}
