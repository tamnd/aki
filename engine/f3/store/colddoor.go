package store

// The cold-read doorkeeper (spec 2064/f3/06 section 5): a small two-generation
// Bloom filter over cold keys that gates promotion on a second sighting. A cold
// record read once serves its frame and stays cold, so a one-hit cold read does
// not fill the arena with a key the working set will evict again; a cold record
// read twice within the window promotes back to the arena, where it stops
// paying a pread per read. This is the read-side asymmetry of doc 06 section
// 7.3: a write promotes unconditionally (strong hotness), a read must earn it.
//
// It is the one probabilistic frequency structure in the engine and lives only
// on this path. A false positive promotes a once-read key one sighting early,
// which costs one bring-up the next demotion pass undoes, so a few bits per key
// at a percent-level false-positive rate is safe: the failure mode is cheap and
// self-correcting, not a correctness event.

const (
	// coldDoorBits is the default filter width per generation, a power of two.
	// One mebibit per half is 128 KiB, 256 KiB across both, per shard: the doc
	// 06 section 5.2 sizing (~0.5 MiB tracks a million distinct cold touches).
	// The lab sweep tunes it against the promotion-after-cold-read rate.
	coldDoorBits = 1 << 20
	// coldDoorHashes is the bits a mark sets and a test checks per generation.
	coldDoorHashes = 2
	// coldDoorGolden mixes a second independent position out of the same
	// fingerprint, the odd multiplier from the wyhash family.
	coldDoorGolden = 0x9e3779b97f4a7c15
)

// coldDoor is a two-generation Bloom doorkeeper. A mark sets coldDoorHashes bits
// in the current half; a test passes when all those bits are set in either half,
// so a key stays a member for up to two windows. When the current half has taken
// window marks, the other half is zeroed and becomes current, sliding the window
// without a decay pass (the TinyLFU rotation). Owner-local, no atomics.
type coldDoor struct {
	half   [2][]uint64
	cur    int
	marks  uint64
	window uint64
	mask   uint64 // bit-index mask, one less than the power-of-two bit count
}

// newColdDoor builds a doorkeeper of bits per generation, rounded up to a power
// of two. The rotation window is a quarter of the bit count, which keeps each
// half under half full at rotation so the false-positive rate stays low.
func newColdDoor(bits uint64) *coldDoor {
	n := uint64(1)
	for n < bits {
		n <<= 1
	}
	words := n / 64
	return &coldDoor{
		half:   [2][]uint64{make([]uint64, words), make([]uint64, words)},
		window: n / 4,
		mask:   n - 1,
	}
}

// pos derives the coldDoorHashes bit indices for a key fingerprint.
func (d *coldDoor) pos(fp uint64) (uint64, uint64) {
	return fp & d.mask, (fp * coldDoorGolden) & d.mask
}

// present reports whether both bits are set in half h.
func (d *coldDoor) present(h int, p1, p2 uint64) bool {
	a := d.half[h]
	return a[p1>>6]&(1<<(p1&63)) != 0 && a[p2>>6]&(1<<(p2&63)) != 0
}

// test reports whether the key has been marked within the current window.
func (d *coldDoor) test(fp uint64) bool {
	p1, p2 := d.pos(fp)
	return d.present(0, p1, p2) || d.present(1, p1, p2)
}

// mark records a first sighting of the key in the current half and rotates the
// window when the half fills.
func (d *coldDoor) mark(fp uint64) {
	p1, p2 := d.pos(fp)
	a := d.half[d.cur]
	a[p1>>6] |= 1 << (p1 & 63)
	a[p2>>6] |= 1 << (p2 & 63)
	d.marks++
	if d.marks >= d.window {
		d.cur ^= 1
		clear(d.half[d.cur])
		d.marks = 0
	}
}

// reset zeros both generations and rewinds the cursor, for Store.Reset.
func (d *coldDoor) reset() {
	clear(d.half[0])
	clear(d.half[1])
	d.cur = 0
	d.marks = 0
}
