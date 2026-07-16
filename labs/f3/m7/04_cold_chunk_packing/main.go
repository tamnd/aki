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

// List encoding constants, copied from the packages they mirror. A list demotes a
// whole ring chunk rather than gathering members: its blob and directory are already
// contiguous, so the pass copies the live frames straight into one cold frame keyed
// by the per-list demote sequence (list/cold.go demote), keeps no per-element
// resident record (a list carries position by layout, not per-member records), and
// keeps no offset table (a read rides the ring handle's cold offset, not a directory
// slot). The one resident cost a demoted chunk leaves is its directory descriptor.
const (
	listBlobCap  = 4096 // native.go chunkBlobCap: the list chunk blob budget
	listElemCap  = 128  // native.go chunkElemCap: the per-chunk element ceiling
	listDirBytes = 2    // native.go dir []uint16: two resident bytes per element slot
	// listFootprint mirrors native.go chunkFootprint: a resident list chunk's fixed
	// backing arrays, the blob budget plus the full uint16 directory, allocated at
	// full width regardless of fill. The whole of it leaves on a demote.
	listFootprint = listBlobCap + listElemCap*listDirBytes
	// listDescBytes is the one resident descriptor a demoted list chunk keeps, keyed
	// by the demote sequence (list/cold.go listCold.dir, one Insert per shed chunk).
	// Unlike the set there is no offset-table slot, so the descriptor is the whole
	// resident cost of a cold list chunk.
	listDescBytes = 48
)

// Hash encoding constants, copied from the packages they mirror. A hash is key-value,
// so its demote is the collection analog of the string band's value-log split: it
// keeps the field bytes resident (the probe key) and sheds only the value bytes to a
// cold chunk (hash/cold.go demote). So the packed payload is the field-and-value PAIR
// (hash/cold.go appendEntry, uvarint(flen)+field+uvarint(vlen)+value), the field
// riding along for the M8 recovery walk, and the demoted record stays resident with
// its field bytes still in the slab, only its value read routed through a pread. The
// resident cost after a demote is the record cell, its draw-vector slot, the field
// bytes it keeps, and its share of one directory descriptor and one offset slot. The
// freed fraction is the value bytes alone, lower than the set's whole-member shed, the
// price the hash pays to keep HEXISTS, HKEYS, and HSTRLEN zero preads.
const (
	fentryBytes  = 20 // hash/field.go fentry: foff+voff+vslot+flen+band+vlen, padded
	hashVecBytes = 4  // hash/field.go vec: one uint32 draw-vector slot per field
)

// Stream encoding constants, copied from the packages they mirror. A stream's native
// band is already an append log of packed blocks, and a block's blob is byte-for-byte
// the master-delta wire form a cold read decodes, so a demote spills the whole blob
// through store.AppendChunk with no repack at all (stream/cold.go demote). This gives
// the tightest packing of the series: the factor is 1.0 by construction (the payload
// IS the resident blob), so the only on-disk overhead is the cold frame header plus
// the key and the 16-byte first-id discriminator. A block demotes whole, so it keeps
// no per-entry resident record; the resident cost after a demote is the block's header
// cell and its master schema, kept so covers and the directory floor resolve a cold
// block without a pread, plus its share of one demote-sequence descriptor. So the
// stream frees the blob (~96% of a byte-bound block) rather than the list's whole
// ~98.9%: it trades a slightly larger resident residue for pread-free block resolution.
// A narrow-value block seals on the 128-entry cap and under-fills the 4 KiB budget, so
// its fixed header cell is a larger share and it frees a touch less; the freed fraction
// climbs toward ~96% as a wider value fills the byte budget.
const (
	streamBlockBudget = 4096 // block.go blockBudget: the per-block blob byte budget
	streamBlockCap    = 128  // block.go blockCap: the per-block entry ceiling
	streamDiscBytes   = 16   // stream/cold.go discID: a 16-byte first id (ms || seq), on disk
	streamHeaderBytes = 96   // stream.go blockHeaderBytes: the resident cell per block
	streamDescBytes   = 48   // tier/directory.go: one resident demote-sequence descriptor per shed block
	streamNameLen     = 1    // the single field name the master carries once for a same-schema block
)

// streamMasterLen prices the master (first) entry of a block: the flag byte, the
// zero id delta (uvarint 0 plus signed-varint 0), the field count, the one field name
// with its length prefix, and the value with its length prefix (block.go appendMaster).
func streamMasterLen(width int) int {
	return 1 + uvarintLen(0) + 1 + uvarintLen(1) + uvarintLen(streamNameLen) + streamNameLen + uvarintLen(width) + width
}

// streamEntryLen prices the i-th same-schema entry of a block (i counts from the
// master at 0): the flag byte, the id delta against the block first id (consecutive
// ms so the ms delta is uvarint(i), seq 0 so the signed seq delta is one byte), and
// the value with its length prefix. The field name is implied by the master, so a
// same-schema entry carries none (block.go appendEntry, same branch).
func streamEntryLen(i, width int) int {
	return 1 + uvarintLen(i) + 1 + uvarintLen(width) + width
}

// streamBlockPack models one block's fill for a fixed value width, sealing on
// whichever binds first, the 4 KiB blob budget or the 128-entry cap (block.go
// appendEntry rejects an entry that would breach either, so the blob stays at or
// under the budget). It returns the entries the block holds and its blob byte count,
// which is exactly the cold payload since the demote spills the blob verbatim.
func streamBlockPack(width int) (entries, payloadBytes int) {
	payloadBytes = streamMasterLen(width)
	entries = 1
	for entries < streamBlockCap {
		next := streamEntryLen(entries, width)
		if payloadBytes+next > streamBlockBudget {
			break
		}
		payloadBytes += next
		entries++
	}
	return entries, payloadBytes
}

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

// listRow is one member-width point in the list sweep. It shares the frame and
// packing shape of a set row but models the whole-chunk demote: no per-element
// resident record, no offset table, so the resident cost after a demote is only the
// chunk's share of one directory descriptor.
type listRow struct {
	width     int
	members   int     // frames packed into one list chunk
	payload   int     // payload bytes those frames occupy
	frame     int     // whole on-disk frame bytes (header + key + demote-seq + payload)
	overhead  float64 // frame overhead as a fraction of the frame
	metaPerEl float64 // resident cold metadata bytes per cold frame (descriptor share)
	amortize  float64 // element-per-row resident cost over the chunk's, the packing win
	hotPerEl  float64 // fully-resident bytes per frame (chunk footprint share)
	coldPerEl float64 // resident bytes per frame after a whole-chunk demote
	freedFrac float64 // resident fraction a demote frees to disk
}

// listPack models one list chunk's fill for a fixed member width. A list chunk seals
// on whichever binds first, the blob byte budget or the element cap (native.go
// canAppendTail), so a narrow member hits the 128-element ceiling well before 4 KiB.
func listPack(width int) (members, payloadBytes int) {
	entry := uvarintLen(width) + width
	members = listBlobCap / entry
	if members > listElemCap {
		members = listElemCap
	}
	return members, members * entry
}

func measureList(width, keyLen int) listRow {
	members, payload := listPack(width)
	frame := chunkHdr + keyLen + discBytes + payload
	metaPerEl := float64(listDescBytes) / float64(members)
	hotPerEl := float64(listFootprint) / float64(members)
	return listRow{
		width:     width,
		members:   members,
		payload:   payload,
		frame:     frame,
		overhead:  float64(frame-payload) / float64(frame),
		metaPerEl: metaPerEl,
		amortize:  elementPerRowResident / metaPerEl,
		hotPerEl:  hotPerEl,
		coldPerEl: metaPerEl,
		freedFrac: (hotPerEl - metaPerEl) / hotPerEl,
	}
}

// hashRow is one width point in the hash sweep. The width is both the field and the
// value byte length: a chunk packs field-and-value pairs, but only the value leaves
// resident memory, so the freed fraction is the value's share of the pair's footprint.
type hashRow struct {
	width     int
	members   int     // field-value pairs packed into one 4 KiB chunk
	payload   int     // payload bytes those pairs occupy
	frame     int     // whole on-disk frame bytes (header + key + disc + payload)
	overhead  float64 // frame overhead as a fraction of the frame
	metaPerEl float64 // resident cold metadata bytes per cold field (descriptor + offset share)
	amortize  float64 // element-per-row resident cost over the chunk's, the packing win
	hotPerEl  float64 // fully-resident bytes per field (record + vec + field + value)
	coldPerEl float64 // resident bytes per field after a demote (record + vec + field kept + meta)
	freedFrac float64 // resident fraction a demote frees to disk (the value bytes)
}

// hashPack models one hash chunk's fill for a fixed field and value width. Each pair
// costs the two length prefixes and the two byte runs (hash/cold.go appendEntry), and
// the chunk overruns the byte target by at most one pair, the same fill-then-check
// flush the set takes.
func hashPack(width int) (members, payloadBytes int) {
	entry := uvarintLen(width) + width + uvarintLen(width) + width
	for payloadBytes < chunkByteTarget {
		payloadBytes += entry
		members++
	}
	return members, payloadBytes
}

func measureHash(width, keyLen int) hashRow {
	members, payload := hashPack(width)
	frame := chunkHdr + keyLen + discBytes + payload
	metaPerEl := float64(descBytes+offsBytes) / float64(members)
	hotPerEl := float64(fentryBytes + hashVecBytes + width + width)
	coldPerEl := float64(fentryBytes+hashVecBytes+width) + metaPerEl
	return hashRow{
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

// streamRow is one value-width point in the stream sweep. It shares the frame shape
// of the other rows but models the whole-block demote of an already-packed blob: no
// per-entry resident record, no offset table, and no repack, so the payload is the
// resident blob verbatim (packing factor 1.0) and the resident cost after a demote is
// only the block header cell plus the chunk's share of one demote-sequence descriptor.
type streamRow struct {
	width     int
	members   int     // entries packed into one block
	payload   int     // blob bytes those entries occupy, the cold payload verbatim
	frame     int     // whole on-disk frame bytes (header + key + first-id disc + payload)
	overhead  float64 // frame overhead as a fraction of the frame
	metaPerEl float64 // resident cold metadata bytes per cold entry (descriptor share)
	amortize  float64 // element-per-row resident cost over the block's, the packing win
	hotPerEl  float64 // fully-resident bytes per entry (blob share + block header cell)
	coldPerEl float64 // resident bytes per entry after a whole-block demote
	freedFrac float64 // resident fraction a demote frees to disk (the blob bytes)
}

func measureStream(width, keyLen int) streamRow {
	members, payload := streamBlockPack(width)
	frame := chunkHdr + keyLen + streamDiscBytes + payload
	metaPerEl := float64(streamDescBytes) / float64(members)
	// A resident block keeps its blob (payload share per entry) plus the fixed header
	// cell; a cold block keeps only the header cell and its descriptor share.
	hotPerEl := float64(payload+streamHeaderBytes) / float64(members)
	coldPerEl := float64(streamHeaderBytes)/float64(members) + metaPerEl
	return streamRow{
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

	// The list column: a list demotes a whole chunk, so it keeps no per-element
	// resident record and no offset table, only one directory descriptor per shed
	// chunk. Its packing shares the frame shape but its freed fraction is the pitch.
	fmt.Printf("\nlist cold chunk packing, whole-chunk demote (one descriptor per chunk, no offset table)\n\n")
	fmt.Printf(hdr, "member", "frames/", "payload", "frame", "frame", "resident meta", "vs element", "resident", "freed to")
	fmt.Printf(hdr, "bytes", "chunk", "bytes", "bytes", "overhead", "B/member", "per-row", "B/member", "disk")
	fmt.Printf(hdr, "------", "--------", "-------", "-----", "--------", "-------------", "----------", "--------", "--------")

	var lverdict listRow
	for _, width := range widths {
		r := measureList(width, *keyLen)
		if width == 20 {
			lverdict = r
		}
		fmt.Printf("%-8d %-9d %-8d %-7d %-9s %-14.3f %-11s %-9.3f %-8s\n",
			r.width, r.members, r.payload, r.frame,
			fmt.Sprintf("%.2f%%", r.overhead*100), r.metaPerEl,
			fmt.Sprintf("%.0fx", r.amortize), r.coldPerEl,
			fmt.Sprintf("%.1f%%", r.freedFrac*100))
	}
	if lverdict.members == 0 {
		lverdict = measureList(20, *keyLen)
	}
	fmt.Printf("\nList verdict point (20-byte member):\n")
	fmt.Printf("  %d frames per chunk (the %d-element cap binds before the 4 KiB byte target for a narrow member)\n",
		lverdict.members, listElemCap)
	fmt.Printf("  resident cold metadata %.3f B/member (descriptor only, no offset table), %.0fx below element-per-row\n",
		lverdict.metaPerEl, lverdict.amortize)
	fmt.Printf("  a whole-chunk demote frees %.1f%% of the chunk's resident footprint at every width (%.1f B -> %.2f B resident per member)\n",
		lverdict.freedFrac*100, lverdict.hotPerEl, lverdict.coldPerEl)

	// The hash column: a hash packs field-and-value pairs but keeps the field bytes
	// resident, so its freed fraction is the value's share alone, the price of a
	// zero-pread probe. The width labels the field and the value byte length together.
	fmt.Printf("\nhash cold chunk packing, field-and-value pairs, fields kept resident (only values shed)\n\n")
	fmt.Printf(hdr, "f+v", "pairs/", "payload", "frame", "frame", "resident meta", "vs element", "resident", "freed to")
	fmt.Printf(hdr, "bytes", "chunk", "bytes", "bytes", "overhead", "B/field", "per-row", "B/field", "disk")
	fmt.Printf(hdr, "------", "--------", "-------", "-----", "--------", "-------------", "----------", "--------", "--------")

	var hverdict hashRow
	for _, width := range widths {
		r := measureHash(width, *keyLen)
		if width == 20 {
			hverdict = r
		}
		fmt.Printf("%-8d %-9d %-8d %-7d %-9s %-14.3f %-11s %-9.0f %-8s\n",
			r.width, r.members, r.payload, r.frame,
			fmt.Sprintf("%.2f%%", r.overhead*100), r.metaPerEl,
			fmt.Sprintf("%.0fx", r.amortize), r.coldPerEl,
			fmt.Sprintf("%.0f%%", r.freedFrac*100))
	}
	if hverdict.members == 0 {
		hverdict = measureHash(20, *keyLen)
	}
	fmt.Printf("\nHash verdict point (20-byte field and value):\n")
	fmt.Printf("  %d pairs per 4 KiB chunk, frame overhead %.2f%%\n", hverdict.members, hverdict.overhead*100)
	fmt.Printf("  resident cold metadata %.3f B/field (descriptor + offset), %.0fx below element-per-row\n",
		hverdict.metaPerEl, hverdict.amortize)
	fmt.Printf("  a demote frees %.0f%% of a field's footprint to disk, the value bytes (%.0f B -> %.0f B resident; the field stays resident for a zero-pread probe)\n",
		hverdict.freedFrac*100, hverdict.hotPerEl, hverdict.coldPerEl)

	// The stream column: a stream block's blob is already the wire form, so a demote
	// spills it whole with no repack (packing factor 1.0, the tightest packing of the
	// series) and keeps only the block header cell, its master schema, and one
	// descriptor per shed block. It frees ~96% of a byte-bound block, a touch below the
	// list's whole-chunk demote, the price of pread-free block resolution.
	fmt.Printf("\nstream cold chunk packing, whole-block demote of an already-packed blob (payload = blob verbatim, factor 1.0)\n\n")
	fmt.Printf(hdr, "value", "entries/", "payload", "frame", "frame", "resident meta", "vs element", "resident", "freed to")
	fmt.Printf(hdr, "bytes", "block", "bytes", "bytes", "overhead", "B/entry", "per-row", "B/entry", "disk")
	fmt.Printf(hdr, "------", "--------", "-------", "-----", "--------", "-------------", "----------", "--------", "--------")

	var sverdict streamRow
	for _, width := range widths {
		r := measureStream(width, *keyLen)
		if width == 20 {
			sverdict = r
		}
		fmt.Printf("%-8d %-9d %-8d %-7d %-9s %-14.3f %-11s %-9.3f %-8s\n",
			r.width, r.members, r.payload, r.frame,
			fmt.Sprintf("%.2f%%", r.overhead*100), r.metaPerEl,
			fmt.Sprintf("%.0fx", r.amortize), r.coldPerEl,
			fmt.Sprintf("%.1f%%", r.freedFrac*100))
	}
	if sverdict.members == 0 {
		sverdict = measureStream(20, *keyLen)
	}
	fmt.Printf("\nStream verdict point (20-byte value, single field):\n")
	fmt.Printf("  %d entries per block, blob spilled verbatim as the cold payload (packing factor 1.0, the tightest of the series)\n", sverdict.members)
	fmt.Printf("  frame overhead %.2f%% (cold frame header only, no repack), resident cold metadata %.3f B/entry, %.0fx below element-per-row\n",
		sverdict.overhead*100, sverdict.metaPerEl, sverdict.amortize)
	fmt.Printf("  a whole-block demote frees %.1f%% of the block's resident footprint to disk (%.1f B -> %.2f B resident per entry; the header cell and master schema stay resident for pread-free block resolution)\n",
		sverdict.freedFrac*100, sverdict.hotPerEl, sverdict.coldPerEl)
}
