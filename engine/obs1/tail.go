// The 16-byte tail every multi-part obs1 object ends with (doc 03
// section 4): WAL objects, segments, and checkpoints all close the same
// way, so recovery can find any object's footer without knowing what the
// object is.
package obs1

import (
	"encoding/binary"
	"fmt"
)

// TailSize is the fixed tail length.
const TailSize = 16

func appendTail(b []byte, footerOff uint64, footerLen uint32) []byte {
	at := len(b)
	b = binary.LittleEndian.AppendUint64(b, footerOff)
	b = binary.LittleEndian.AppendUint32(b, footerLen)
	return binary.LittleEndian.AppendUint32(b, crc32c(b[at:]))
}

// ParseTail reads a tail and returns where the footer lives.
func ParseTail(tail []byte) (footerOff uint64, footerLen uint32, err error) {
	if len(tail) != TailSize {
		return 0, 0, fmt.Errorf("obs1: tail is %d bytes, want %d", len(tail), TailSize)
	}
	if got, want := crc32c(tail[:12]), binary.LittleEndian.Uint32(tail[12:]); got != want {
		return 0, 0, fmt.Errorf("obs1: tail crc 0x%08x, computed 0x%08x", want, got)
	}
	return binary.LittleEndian.Uint64(tail[0:8]), binary.LittleEndian.Uint32(tail[8:12]), nil
}
