// Command 04_cold_chunk_packing models the set cold chunk packing factor: how many
// members a 4 KiB payload holds, what the frame plus directory overhead comes to
// per member, and how much resident memory a demote frees against the two rivals
// the spec sets a bar against (element-per-row cold storage, which f1 shipped, and
// the fully-resident native slab).
//
// It is the per-perf-change lab the set demotion pack owes (PR D2 and PR E were
// the first slices to pack real members). The model mirrors the shipped encoding
// exactly, so its numbers are the code's numbers, not a separate estimate:
//
//   - a member packs as an unsigned-varint length then its bytes (set/cold.go
//     appendEntry), so a w-byte member costs uvarintLen(w)+w payload bytes;
//   - a chunk fills until the payload reaches the 4 KiB byte target and can overrun
//     it by one member, because the flush check runs after the append (set/cold.go
//     htable.demote), so a chunk holds the first count whose running payload is at
//     or past the target;
//   - the frame adds a fixed 16-byte header plus the collection key and the 8-byte
//     first discriminator (store/chunkframe.go), all on disk, not resident;
//   - resident cold metadata per chunk is one 48-byte directory descriptor plus an
//     8-byte offset-table slot (set/cold.go coldChunks.residentBytes,
//     tier/directory.go descBytes);
//   - the demoted record itself stays resident under the retier-free design (record
//     12 bytes plus a 4-byte draw-vector slot), and only the member's slab bytes
//     leave for the chunk on disk.
//
// The spec's target is section 6.2's table (a 20-byte member packs ~170 per 4 KiB
// chunk, ~1% frame overhead) and section 6.3's ~13 bytes-per-element element-per-row
// cost. The verdict reports where the shipped encoding lands against both.
//
// The lab imports no engine package: the constants below are copied from the code
// they mirror, and a drift between them and the source is a real regression the
// test pins.
package main

import (
	"flag"
	"fmt"
)

// Encoding constants, copied from the packages they mirror. A change to any of the
// source constants that this lab does not track is a packing-factor regression the
// verdict would miss, so the test asserts the derived numbers against the spec's
// table rather than trusting these blind.
const (
	chunkByteTarget = 4096 // set/cold.go: pack until the payload reaches this
	chunkHdr        = 16   // store/chunkframe.go: u32 total + kind + flags + u16 klen + u32 payloadLen + u16 count + u16 dlen
	discBytes       = 8    // set/cold.go discOf: an 8-byte big-endian member hash
	descBytes       = 48   // tier/directory.go: one resident chunk descriptor cell
	offsBytes       = 8    // set/cold.go coldChunks.offs: one uint64 offset slot per chunk
	recordBytes     = 12   // set/member.go record: loc+vslot+mlen+band, padded
	vecBytes        = 4    // set/member.go vec: one uint32 draw-vector slot per member

	// elementPerRowResident is what f1's shipped cold layout paid per cold element:
	// about 13 bytes of resident cold index per element (spec 06 section 6.1), the
	// cost the chunk form exists to amortize away.
	elementPerRowResident = 13.0
)

// uvarintLen is the byte width binary.AppendUvarint writes for n, the length prefix
// each packed member carries.
func uvarintLen(n int) int {
	l := 1
	for n >= 0x80 {
		n >>= 7
		l++
	}
	return l
}

// pack models one chunk's fill for a fixed member width. It returns the members
// that land in the chunk and the payload bytes they occupy, following the code's
// fill-then-check flush so the chunk overruns the target by at most one member.
func pack(memberWidth int) (members, payloadBytes int) {
	entry := uvarintLen(memberWidth) + memberWidth
	for payloadBytes < chunkByteTarget {
		payloadBytes += entry
		members++
	}
	return members, payloadBytes
}

// row is one member-width point in the sweep, every figure derived from the shipped
// encoding.
type row struct {
	width     int
	members   int     // members packed into one 4 KiB chunk
	payload   int     // payload bytes those members occupy
	frame     int     // whole on-disk frame bytes (header + key + disc + payload)
	overhead  float64 // frame overhead as a fraction of the frame
	metaPerEl float64 // resident cold metadata bytes per cold member
	amortize  float64 // element-per-row resident cost over the chunk's, the packing win
	hotPerEl  float64 // fully-resident bytes per member (record + vec + slab)
	coldPerEl float64 // retier-free resident bytes per member after a demote
	freedFrac float64 // resident fraction a demote frees to disk
}

func measure(width, keyLen int) row {
	members, payload := pack(width)
	frame := chunkHdr + keyLen + discBytes + payload
	metaPerEl := float64(descBytes+offsBytes) / float64(members)
	hotPerEl := float64(recordBytes + vecBytes + width)
	coldPerEl := float64(recordBytes+vecBytes) + metaPerEl
	return row{
		width:     width,
		members:   members,
		payload:   payload,
		frame:     frame,
		overhead:  float64(frame-payload) / float64(frame),
		metaPerEl: metaPerEl,
		amortize:  elementPerRowResident / metaPerEl,
		hotPerEl:  hotPerEl,
		coldPerEl: coldPerEl,
		freedFrac: (hotPerEl - coldPerEl) / hotPerEl,
	}
}

func main() {
	quick := flag.Bool("quick", false, "run the reduced width sweep")
	keyLen := flag.Int("keylen", 16, "collection key byte length (on-disk frame only)")
	flag.Parse()

	widths := []int{8, 16, 20, 32, 64, 128, 256}
	if *quick {
		widths = []int{8, 20, 64}
	}

	fmt.Printf("set cold chunk packing, %d-byte payload target, %d-byte key\n\n", chunkByteTarget, *keyLen)
	const hdr = "%-8s %-9s %-8s %-7s %-9s %-14s %-11s %-9s %-8s\n"
	fmt.Printf(hdr, "member", "members/", "payload", "frame", "frame", "resident meta", "vs element", "resident", "freed to")
	fmt.Printf(hdr, "bytes", "chunk", "bytes", "bytes", "overhead", "B/member", "per-row", "B/member", "disk")
	fmt.Printf(hdr, "------", "--------", "-------", "-----", "--------", "-------------", "----------", "--------", "--------")

	var verdict row
	for _, width := range widths {
		r := measure(width, *keyLen)
		if width == 20 {
			verdict = r
		}
		fmt.Printf("%-8d %-9d %-8d %-7d %-9s %-14.3f %-11s %-9.0f %-8s\n",
			r.width, r.members, r.payload, r.frame,
			fmt.Sprintf("%.2f%%", r.overhead*100), r.metaPerEl,
			fmt.Sprintf("%.0fx", r.amortize), r.coldPerEl,
			fmt.Sprintf("%.0f%%", r.freedFrac*100))
	}

	// The verdict is pinned to the 20-byte member the spec sizes against (section
	// 6.2's set row). If -quick dropped a run that included it, fall back to it here.
	if verdict.members == 0 {
		verdict = measure(20, *keyLen)
	}
	fmt.Printf("\nVerdict point (20-byte member, section 6.2's set row):\n")
	fmt.Printf("  %d members per 4 KiB chunk (spec target ~170), frame overhead %.2f%% (spec target ~1%%)\n",
		verdict.members, verdict.overhead*100)
	fmt.Printf("  resident cold metadata %.3f B/member, %.0fx below element-per-row's %.0f B (spec 0.16-0.32 B)\n",
		verdict.metaPerEl, verdict.amortize, elementPerRowResident)
	fmt.Printf("  a demote frees %.0f%% of a member's resident footprint to disk (%.0f B -> %.0f B resident)\n",
		verdict.freedFrac*100, verdict.hotPerEl, verdict.coldPerEl)
}
