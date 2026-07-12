package hash

import (
	"strconv"
	"testing"
)

// The band microbenchmarks (spec 2064/f3/10 sections 3.5 and 4.3). HGET is the
// probe floor on each band; HSET is the insert path amortized over a full build,
// folding in blob rewrites (inline) or table and slab growth (native); the
// conversion benchmark prices the one-way inline-to-native replay. These are Go
// microbenchmarks, not the GamingPC gate; they order the mechanism and quote a
// floor, and the gate rows key on the server numbers.

// pairs returns n field-value pairs, all short enough to stay in the inline band
// up to the 128-field cap, so the benchmarks never allocate keys inside the timed
// loop.
func pairs(n int) [][2][]byte {
	out := make([][2][]byte, n)
	for i := range out {
		out[i] = [2][]byte{[]byte("f" + strconv.Itoa(i)), []byte("v" + strconv.Itoa(i))}
	}
	return out
}

// BenchmarkHSetInline builds a fresh inline hash of 64 fields each iteration, so
// the per-field cost folds in the blob rewrites the inline band pays per HSET.
func BenchmarkHSetInline(b *testing.B) {
	ps := pairs(64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := newHash()
		for _, p := range ps {
			h.set(p[0], p[1])
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(ps)), "ns/field")
}

// BenchmarkHGetInline is the inline probe floor: a bounded forward scan of a
// 64-field blob.
func BenchmarkHGetInline(b *testing.B) {
	ps := pairs(64)
	h := newHash()
	for _, p := range ps {
		h.set(p[0], p[1])
	}
	if h.enc != encListpack {
		b.Fatalf("enc = %s, want listpack", h.enc)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkVal, sinkOk = h.get(ps[i%len(ps)][0])
	}
}

// BenchmarkHGetNative is the native probe floor: the SWAR field-table lookup at a
// 10k-field cell.
func BenchmarkHGetNative(b *testing.B) {
	const n = 10_000
	ps := pairs(n)
	h := forceNative(newHash())
	for _, p := range ps {
		h.set(p[0], p[1])
	}
	if h.enc != encHashtable {
		b.Fatalf("enc = %s, want hashtable", h.enc)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkVal, sinkOk = h.get(ps[i%n][0])
	}
}

// BenchmarkConvertInlineToNative prices the one-way promotion replay at the 128
// entry cap: allocate the table, replay every blob entry, drop the blob.
func BenchmarkConvertInlineToNative(b *testing.B) {
	ps := pairs(maxListpackEntries)
	// Build one inline template, then clone its blob per iteration so the timed
	// region is only the conversion, not the build.
	tmpl := newHash()
	for _, p := range ps {
		tmpl.set(p[0], p[1])
	}
	if tmpl.enc != encListpack {
		b.Fatalf("template enc = %s, want listpack", tmpl.enc)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		h := &hash{enc: encListpack, blob: append([]byte(nil), tmpl.blob...)}
		b.StartTimer()
		h.inlineToNative()
	}
}
