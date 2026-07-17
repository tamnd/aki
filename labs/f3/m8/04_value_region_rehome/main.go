// Command 04_value_region_rehome models the on-disk cost of re-homing the
// store's per-shard scratch value log into the single .aki value region: the
// space amplification the change trades for one consolidated, group-committed
// file, and the batch size where that amplification amortizes back to near the
// raw bytes.
//
// The scratch log (store/vlog.go) appends raw value bytes into a per-shard file:
// no per-value framing, no segment alignment, so V values of S bytes cost exactly
// V*S on disk. The .aki value region frames every value (a uvarint length, the
// bytes, a trailing CRC32C) and cuts each batch as a 4KiB-aligned value_log
// segment with a 64-byte header (akifile format.go). So the re-home pays two
// overheads the scratch log did not: a fixed per-value frame tax, and a per-batch
// segment header plus the padding that rounds the batch up to the next 4KiB.
//
// The frame tax is small and unavoidable (it is the torn-blob guard the record's
// bare pointer leans on). The segment padding is the one that bites, and it is
// exactly what the value-log writer's batching (the ValueLogWriter accumulator)
// exists to amortize: a batch of B values shares one header and one rounding, so
// the padding cost per value falls as 1/B. At B=1 a tiny value pays a whole 4KiB
// segment, 256x amplification for a 16-byte value; batch it and the amplification
// collapses toward 1 plus the frame tax. This lab sweeps value size against batch
// size and reports where the amplification crosses back under a chosen bar, the
// flush threshold the store's spill path should hold.
//
// The model imports akifile and frames with the real codec, so the byte counts
// are the format's byte counts, not a restatement of them: it builds one batch's
// payload with AppendValueFrame and sizes the segment with SegmentSpan.
package main

import (
	"flag"
	"fmt"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// row is one (value size, batch size) point: the on-disk bytes a value costs in
// the .aki value region against the raw bytes it costs in the scratch log.
type row struct {
	valSize   int     // S, value bytes
	batch     int     // B, values per value_log segment
	rawPerVal float64 // scratch log bytes per value = S
	akiPerVal float64 // .aki value region bytes per value, framing plus amortized segment
	amplify   float64 // akiPerVal / rawPerVal
	frameTax  float64 // per-value frame overhead (varint + crc), the unavoidable part
	padPerVal float64 // per-value share of the segment header plus 4KiB padding
	segBytes  uint64  // the whole segment span for the batch
}

// measure frames B values of S bytes into one batch and sizes its segment, the
// real codec doing the arithmetic.
func measure(valSize, batch int) row {
	val := make([]byte, valSize)
	var payload []byte
	for i := 0; i < batch; i++ {
		payload, _ = akifile.AppendValueFrame(payload, val)
	}
	seg := akifile.SegmentSpan(uint64(len(payload)))
	akiPerVal := float64(seg) / float64(batch)
	raw := float64(valSize)
	frameTax := float64(len(payload))/float64(batch) - raw
	return row{
		valSize:   valSize,
		batch:     batch,
		rawPerVal: raw,
		akiPerVal: akiPerVal,
		amplify:   akiPerVal / raw,
		frameTax:  frameTax,
		padPerVal: akiPerVal - float64(len(payload))/float64(batch),
		segBytes:  seg,
	}
}

func main() {
	bar := flag.Float64("bar", 1.10, "amplification bar the batch threshold must bring the value region under")
	quick := flag.Bool("quick", false, "run a short sweep")
	flag.Parse()

	sizes := []int{16, 64, 256, 1024, 4096, 16384, 65536}
	batches := []int{1, 8, 64, 512, 4096}
	if *quick {
		sizes = []int{16, 256, 4096, 65536}
		batches = []int{1, 64, 4096}
	}

	fmt.Printf("value region re-home: on-disk bytes per value, scratch raw vs .aki framed+aligned (bar %.2fx)\n\n", *bar)

	for _, s := range sizes {
		fmt.Printf("value %d bytes (scratch raw = %d B/value):\n", s, s)
		const hdr = "  %-8s %-12s %-11s %-10s %-10s\n"
		fmt.Printf(hdr, "batch B", "aki B/value", "amplify", "frame tax", "pad/value")
		fmt.Printf(hdr, "-------", "-----------", "-------", "---------", "---------")
		var threshold int
		for _, b := range batches {
			r := measure(s, b)
			fmt.Printf("  %-8d %-12.1f %-11s %-10.1f %-10.1f\n",
				r.batch, r.akiPerVal, fmt.Sprintf("%.2fx", r.amplify), r.frameTax, r.padPerVal)
			if threshold == 0 && r.amplify <= *bar {
				threshold = b
			}
		}
		if threshold == 0 {
			fmt.Printf("  no batch in the sweep brings %d-byte values under %.2fx; the frame tax alone exceeds it\n\n", s, *bar)
		} else {
			fmt.Printf("  batch >= %d holds %d-byte values under %.2fx\n\n", threshold, s, *bar)
		}
	}

	// The headline the batching writer exists for: a tiny value unbatched pays a
	// whole segment, and batching collapses it.
	tiny := measure(16, 1)
	tinyBatched := measure(16, 4096)
	fmt.Printf("Verdict:\n")
	fmt.Printf("  a 16-byte value in its own segment costs %.0f B on disk, %.0fx amplification: per-value segments are unusable, which is why the spill path stages and cuts one segment per group, not per value\n",
		tiny.akiPerVal, tiny.amplify)
	fmt.Printf("  batched 4096 to a segment the same value costs %.1f B, %.2fx: the 4KiB padding amortizes to %.3f B/value and only the %.0f B frame tax (varint + crc) remains\n",
		tinyBatched.akiPerVal, tinyBatched.amplify, tinyBatched.padPerVal, tinyBatched.frameTax)
	fmt.Printf("  large values are insensitive: a 64KiB value amplifies %.3fx even unbatched, so the spill path pays the padding only where values are small and a batch is cheap to fill\n",
		measure(65536, 1).amplify)
}
