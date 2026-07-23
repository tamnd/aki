package sqlo1b

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Scheme 5, plain zstd, per the cascade (#1295) and zdict (#1296) lab
// verdicts: zstd is the string workhorse for payloads the scalar
// cascade cannot represent cheaply, and the trained-dictionary
// variant (scheme 6) stays in the boxed stretch because it only pays
// on groups far smaller than the 4 KiB stride. The frame compresses
// whole, stems included: unlike the cascade schemes there is no
// value split, the encoder sees the raw payload bytes and the exact
// original comes back out.

var (
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
)

func init() {
	// SpeedDefault matches the lab arms. EncodeAll and DecodeAll are
	// safe for concurrent use; the concurrency options only size the
	// streaming goroutine pools this path never touches. The decoder
	// memory cap bounds what a corrupt frame header can make DecodeAll
	// allocate, since scrub feeds it whatever is on disk.
	zstdEnc, _ = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1))
	zstdDec, _ = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(cframeMaxUlen+1))
}

// zstdEncode compresses one frame payload whole.
func zstdEncode(payload []byte) []byte {
	return zstdEnc.EncodeAll(payload, make([]byte, 0, len(payload)))
}

// zstdDecode reconstructs the exact frame payload or fails loudly.
func zstdDecode(comp []byte, ulen int) ([]byte, error) {
	if ulen < 0 || ulen > cframeMaxUlen {
		return nil, fmt.Errorf("sqlo1b: zstd ulen %d out of range", ulen)
	}
	out, err := zstdDec.DecodeAll(comp, make([]byte, 0, ulen))
	if err != nil {
		return nil, fmt.Errorf("sqlo1b: zstd frame: %w", err)
	}
	if len(out) != ulen {
		return nil, fmt.Errorf("sqlo1b: zstd frame decoded to %d bytes, ulen %d", len(out), ulen)
	}
	return out, nil
}
