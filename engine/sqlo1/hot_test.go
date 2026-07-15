package sqlo1

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"
)

func TestHdrSize(t *testing.T) {
	// The compile-time assert in hdr.go is the real gate; this spells the
	// number out in test output for anyone touching the struct.
	if s := unsafe.Sizeof(hdr{}); s != 48 {
		t.Fatalf("hdr is %d bytes, want 48", s)
	}
}

func TestArenaAllocData(t *testing.T) {
	var a arena
	vals := [][]byte{
		nil,
		[]byte("x"),
		bytes.Repeat([]byte("k"), 8),
		bytes.Repeat([]byte("v"), 100),
		bytes.Repeat([]byte("w"), arenaChunkSize-arenaAlign), // largest standard
		bytes.Repeat([]byte("z"), arenaChunkSize),            // oversize
	}
	refs := make([]uint32, len(vals))
	for i, v := range vals {
		refs[i] = a.alloc(v)
		if refs[i] == 0 {
			t.Fatalf("val %d: got ref 0, reserved for no-ref", i)
		}
	}
	for i, v := range vals {
		if got := a.data(refs[i]); !bytes.Equal(got, v) {
			t.Fatalf("val %d: read back %d bytes, want %d", i, len(got), len(v))
		}
	}
}

func TestArenaClassReuse(t *testing.T) {
	var a arena
	r1 := a.alloc(bytes.Repeat([]byte("a"), 20)) // footprint 32
	a.release(r1)
	r2 := a.alloc(bytes.Repeat([]byte("b"), 24)) // same class
	if r2 != r1 {
		t.Fatalf("same-class realloc got ref %#x, want recycled %#x", r2, r1)
	}
	if string(a.data(r2)) != string(bytes.Repeat([]byte("b"), 24)) {
		t.Fatal("recycled slot returned stale bytes")
	}
	r3 := a.alloc(bytes.Repeat([]byte("c"), 100)) // different class, no reuse
	if r3 == r1 {
		t.Fatal("cross-class alloc reused a smaller slot")
	}
}

func TestArenaUpdateInPlace(t *testing.T) {
	var a arena
	ref := a.alloc([]byte("hello")) // footprint 16, capacity 8
	if !a.update(ref, []byte("byebye")) {
		t.Fatal("update within capacity refused")
	}
	if string(a.data(ref)) != "byebye" {
		t.Fatalf("data after update = %q", a.data(ref))
	}
	if a.update(ref, bytes.Repeat([]byte("x"), 9)) {
		t.Fatal("update past capacity accepted")
	}
	if string(a.data(ref)) != "byebye" {
		t.Fatal("failed update clobbered the payload")
	}
}

func TestArenaOversizeChunkRecycled(t *testing.T) {
	var a arena
	big := bytes.Repeat([]byte("z"), arenaChunkSize*2)
	ref := a.alloc(big)
	slot := ref >> 16
	chunks := len(a.chunks)
	a.release(ref)
	if a.chunks[slot] != nil {
		t.Fatal("oversize chunk not released")
	}
	ref2 := a.alloc(big)
	if ref2>>16 != slot || len(a.chunks) != chunks {
		t.Fatalf("oversize realloc used slot %d of %d chunks, want recycled slot %d of %d",
			ref2>>16, len(a.chunks), slot, chunks)
	}
}

func TestArenaOversizeFirstThenBump(t *testing.T) {
	// Regression: when the arena's first allocation was oversize, the
	// bump path used to write into that oversize chunk at offset 0 and
	// alias its ref.
	var a arena
	big := bytes.Repeat([]byte("z"), arenaChunkSize)
	rBig := a.alloc(big)
	rSmall := a.alloc([]byte("small"))
	if rSmall == rBig {
		t.Fatalf("standard alloc aliased the oversize ref %#x", rBig)
	}
	if !bytes.Equal(a.data(rBig), big) {
		t.Fatal("oversize payload corrupted by a standard alloc")
	}
	if string(a.data(rSmall)) != "small" {
		t.Fatalf("standard payload = %q", a.data(rSmall))
	}
}

func TestHotTableBasics(t *testing.T) {
	ht := NewHotTable(8)
	if !ht.Put([]byte("k1"), []byte("v1"), TagString) {
		t.Fatal("put k1 refused")
	}
	if v, ok := ht.Get([]byte("k1")); !ok || string(v) != "v1" {
		t.Fatalf("get k1 = %q %v", v, ok)
	}
	if _, ok := ht.Get([]byte("nope")); ok {
		t.Fatal("get of a missing key hit")
	}
	if !ht.Put([]byte("k1"), []byte("v2-longer-than-before"), TagString) {
		t.Fatal("overwrite refused")
	}
	if v, _ := ht.Get([]byte("k1")); string(v) != "v2-longer-than-before" {
		t.Fatalf("get after overwrite = %q", v)
	}
	if !ht.Del([]byte("k1")) || ht.Del([]byte("k1")) {
		t.Fatal("del must hit once then miss")
	}
	if ht.Len() != 0 {
		t.Fatalf("len = %d after delete, want 0", ht.Len())
	}
	if ht.Put(bytes.Repeat([]byte("k"), maxKlen+1), nil, TagString) {
		t.Fatal("key past klen reach accepted")
	}
}

func TestHotTableFull(t *testing.T) {
	ht := NewHotTable(4)
	for i := range 4 {
		if !ht.Put(fmt.Appendf(nil, "k%d", i), []byte("v"), TagString) {
			t.Fatalf("put %d refused below capacity", i)
		}
	}
	if ht.Put([]byte("k4"), []byte("v"), TagString) {
		t.Fatal("put past capacity accepted")
	}
	if !ht.Put([]byte("k0"), []byte("vv"), TagString) {
		t.Fatal("overwrite refused at capacity")
	}
	if !ht.Del([]byte("k1")) || !ht.Put([]byte("k4"), []byte("v"), TagString) {
		t.Fatal("slot not reusable after delete")
	}
}

// TestHotTableCollisions drives the side table directly with a forced
// hash, which maphash cannot produce on demand.
func TestHotTableCollisions(t *testing.T) {
	ht := NewHotTable(8)
	const h = uint64(0xDEADBEEF)
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, k := range keys {
		s := uint32(len(ht.hdrs))
		ht.hdrs = append(ht.hdrs, hdr{keyRef: ht.keys.alloc(k), valRef: ht.vals.alloc(k), klen: uint16(len(k)), state: stateDirty})
		if _, ok := ht.index[h]; ok {
			ht.dups[h] = append(ht.dups[h], s)
		} else {
			ht.index[h] = s
		}
	}
	for _, k := range keys {
		s, ok := ht.lookup(h, k)
		if !ok || !ht.keyIs(s, k) {
			t.Fatalf("colliding key %q not found", k)
		}
	}
	if _, ok := ht.lookup(h, []byte("d")); ok {
		t.Fatal("missing key resolved through the side table")
	}

	// Delete the primary occupant: a dup must be promoted into the index.
	sA := ht.index[h]
	ht.keys.release(ht.hdrs[sA].keyRef)
	ht.vals.release(ht.hdrs[sA].valRef)
	ht.hdrs[sA] = hdr{}
	ht.freeSlots = append(ht.freeSlots, sA)
	ht.index[h] = ht.dups[h][1]
	ht.shrinkDups(h, 1)
	for _, k := range keys[1:] {
		if _, ok := ht.lookup(h, k); !ok {
			t.Fatalf("key %q lost after primary delete", k)
		}
	}
	if _, ok := ht.lookup(h, keys[0]); ok {
		t.Fatal("deleted colliding key still resolves")
	}
}

// TestHotTableShadow drives random point ops against a plain map and
// compares the full contents at the end; sizes cross the in-place,
// realloc, and oversize boundaries.
func TestHotTableShadow(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	ht := NewHotTable(512)
	shadow := make(map[string][]byte)
	sizes := []int{0, 1, 7, 8, 9, 100, 1000, 9000, arenaChunkSize, arenaChunkSize + 3}

	for range 20000 {
		key := fmt.Appendf(nil, "key-%d", rng.Intn(400))
		switch rng.Intn(10) {
		case 0, 1:
			_, present := shadow[string(key)]
			if ht.Del(key) != present {
				t.Fatalf("del %q disagreed with shadow", key)
			}
			delete(shadow, string(key))
		case 2, 3, 4:
			v, ok := ht.Get(key)
			want, wok := shadow[string(key)]
			if ok != wok || !bytes.Equal(v, want) {
				t.Fatalf("get %q = %d bytes %v, want %d bytes %v", key, len(v), ok, len(want), wok)
			}
		default:
			n := sizes[rng.Intn(len(sizes))]
			val := bytes.Repeat([]byte{byte(rng.Intn(256))}, n)
			if !ht.Put(key, val, TagString) {
				t.Fatalf("put %q refused with %d live of 512", key, ht.Len())
			}
			shadow[string(key)] = val
		}
	}

	if ht.Len() != len(shadow) {
		t.Fatalf("len = %d, shadow has %d", ht.Len(), len(shadow))
	}
	for k, want := range shadow {
		v, ok := ht.Get([]byte(k))
		if !ok || !bytes.Equal(v, want) {
			t.Fatalf("final scan: %q = %d bytes %v, want %d bytes", k, len(v), ok, len(want))
		}
	}
}
