package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// rigNow is the hashRig's frozen clock (NowMs in newHashRig); TTL
// tests place expiries around it.
const rigNow = int64(1) << 41

// segRigHash builds a segmented hash under key with n fat fields
// f000..f(n-1) holding val(i) and returns the value maker; the caller
// asserts hashtable encoding when it matters.
func segRigHash(t *testing.T, r *hashRig, key string, n int) func(i int) string {
	t.Helper()
	val := func(i int) string {
		return fmt.Sprintf("v-%d-%s", i, strings.Repeat("x", 120))
	}
	for i := range n {
		r.hset(key, fmt.Sprintf("f%03d", i), val(i))
	}
	return val
}

func TestHSetNX(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)

	set, err := r.h.HSetNX(ctx, []byte("h"), []byte("f"), []byte("one"))
	if err != nil || !set {
		t.Fatalf("HSetNX create = %v, %v; want true", set, err)
	}
	set, err = r.h.HSetNX(ctx, []byte("h"), []byte("f"), []byte("two"))
	if err != nil || set {
		t.Fatalf("HSetNX existing = %v, %v; want false", set, err)
	}
	if v, _ := r.hget("h", "f"); v != "one" {
		t.Fatalf("value after refused HSetNX = %q, want %q", v, "one")
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.h.HSetNX(ctx, []byte("str"), []byte("f"), []byte("v")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HSetNX on a string = %v, want ErrWrongType", err)
	}
}

func TestHMGetInline(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	r.hset("h", "a", "1")
	r.hset("h", "b", "2")
	r.hset("h", "c", "3")

	collect := func(key string, fields ...string) []string {
		t.Helper()
		fs := make([][]byte, len(fields))
		for i, f := range fields {
			fs[i] = []byte(f)
		}
		var out []string
		err := r.h.HMGet(ctx, []byte(key), fs, func(v []byte, ok bool) {
			if !ok {
				out = append(out, "<nil>")
				return
			}
			out = append(out, string(v))
		})
		if err != nil {
			t.Fatalf("HMGet(%q, %v): %v", key, fields, err)
		}
		return out
	}

	got := collect("h", "a", "missing", "c", "a")
	want := []string{"1", "<nil>", "3", "1"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("HMGet[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	got = collect("absent", "a", "b")
	if got[0] != "<nil>" || got[1] != "<nil>" {
		t.Fatalf("HMGet on absent key = %v, want all misses", got)
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	err := r.h.HMGet(ctx, []byte("str"), [][]byte{[]byte("a")}, func([]byte, bool) {
		t.Fatal("emit called on a wrongtype key")
	})
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("HMGet on a string = %v, want ErrWrongType", err)
	}
}

func TestHMGetSegmented(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	val := segRigHash(t, r, "wide", 200)
	if enc, _, _ := r.h.Encoding(ctx, []byte("wide")); enc != "hashtable" {
		t.Fatalf("encoding = %q, want hashtable", enc)
	}

	// Every field plus interleaved misses and duplicates, in bursts
	// that force multi-segment batches through the fence dedupe.
	for lo := 0; lo < 200; lo += 32 {
		var fs [][]byte
		var want []string
		for i := lo; i < lo+32 && i < 200; i++ {
			fs = append(fs, fmt.Appendf(nil, "f%03d", i))
			want = append(want, val(i))
			fs = append(fs, fmt.Appendf(nil, "nope%03d", i))
			want = append(want, "<nil>")
		}
		fs = append(fs, fmt.Appendf(nil, "f%03d", lo))
		want = append(want, val(lo))
		var got []string
		err := r.h.HMGet(ctx, []byte("wide"), fs, func(v []byte, ok bool) {
			if !ok {
				got = append(got, "<nil>")
				return
			}
			got = append(got, string(v))
		})
		if err != nil {
			t.Fatalf("HMGet burst at %d: %v", lo, err)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("burst %d field %d (%s) = %.40q, want %.40q", lo, i, fs[i], got[i], want[i])
			}
		}
	}
}

func TestHIncrBy(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)

	n, err := r.h.HIncrBy(ctx, []byte("h"), []byte("n"), 5)
	if err != nil || n != 5 {
		t.Fatalf("HIncrBy on absent = %d, %v; want 5", n, err)
	}
	n, err = r.h.HIncrBy(ctx, []byte("h"), []byte("n"), -12)
	if err != nil || n != -7 {
		t.Fatalf("HIncrBy = %d, %v; want -7", n, err)
	}
	if v, _ := r.hget("h", "n"); v != "-7" {
		t.Fatalf("stored value = %q, want -7", v)
	}

	r.hset("h", "s", "notanumber")
	if _, err := r.h.HIncrBy(ctx, []byte("h"), []byte("s"), 1); !errors.Is(err, ErrHashNotInt) {
		t.Fatalf("HIncrBy on a string field = %v, want ErrHashNotInt", err)
	}
	// string2ll strictness: leading plus and zeros are not integers.
	r.hset("h", "plus", "+1")
	if _, err := r.h.HIncrBy(ctx, []byte("h"), []byte("plus"), 1); !errors.Is(err, ErrHashNotInt) {
		t.Fatalf("HIncrBy on %q = %v, want ErrHashNotInt", "+1", err)
	}

	r.hset("h", "big", "9223372036854775807")
	if _, err := r.h.HIncrBy(ctx, []byte("h"), []byte("big"), 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("HIncrBy overflow = %v, want ErrOverflow", err)
	}
	if v, _ := r.hget("h", "big"); v != "9223372036854775807" {
		t.Fatalf("value after refused overflow = %q", v)
	}
}

func TestHIncrByFloat(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)

	v, err := r.h.HIncrByFloat(ctx, []byte("h"), []byte("f"), 10.5)
	if err != nil || string(v) != "10.5" {
		t.Fatalf("HIncrByFloat on absent = %q, %v; want 10.5", v, err)
	}
	v, err = r.h.HIncrByFloat(ctx, []byte("h"), []byte("f"), 0.1)
	if err != nil || string(v) != "10.6" {
		t.Fatalf("HIncrByFloat = %q, %v; want 10.6", v, err)
	}
	// No trailing zeros or dot in the reply format.
	r.hset("h", "three", "2")
	v, err = r.h.HIncrByFloat(ctx, []byte("h"), []byte("three"), 1)
	if err != nil || string(v) != "3" {
		t.Fatalf("HIncrByFloat integer result = %q, %v; want 3", v, err)
	}

	r.hset("h", "s", "notafloat")
	if _, err := r.h.HIncrByFloat(ctx, []byte("h"), []byte("s"), 1); !errors.Is(err, ErrHashNotFloat) {
		t.Fatalf("HIncrByFloat on a string field = %v, want ErrHashNotFloat", err)
	}
	r.hset("h", "inf", "inf")
	if _, err := r.h.HIncrByFloat(ctx, []byte("h"), []byte("inf"), 1); !errors.Is(err, ErrNaNInf) {
		t.Fatalf("HIncrByFloat to infinity = %v, want ErrNaNInf", err)
	}
}

func TestHGetDel(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	r.hset("h", "a", "1")
	r.hset("h", "b", "2")

	v, ok, err := r.h.HGetDel(ctx, []byte("h"), []byte("a"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("HGetDel = %q, %v, %v; want 1", v, ok, err)
	}
	if _, ok := r.hget("h", "a"); ok {
		t.Fatal("field a survived HGetDel")
	}
	if _, ok, _ := r.h.HGetDel(ctx, []byte("h"), []byte("a")); ok {
		t.Fatal("HGetDel found a deleted field")
	}
	// Removing the last field kills the key, Redis's empty-hash rule.
	if v, ok, err := r.h.HGetDel(ctx, []byte("h"), []byte("b")); err != nil || !ok || string(v) != "2" {
		t.Fatalf("HGetDel last = %q, %v, %v", v, ok, err)
	}
	if _, ok, _ := r.h.Encoding(ctx, []byte("h")); ok {
		t.Fatal("key survived losing its last field")
	}
}

func TestHGetEx(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	r.hset("h", "a", "1")
	r.hset("h", "b", "2")
	future := rigNow + 60_000

	// A plain read edits nothing.
	v, ok, err := r.h.HGetEx(ctx, []byte("h"), []byte("a"), false, 0)
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("HGetEx read = %q, %v, %v", v, ok, err)
	}
	if _, eExp, _, _ := r.h.getEntry(ctx, []byte("h"), []byte("a")); eExp != 0 {
		t.Fatalf("plain HGetEx left a TTL: %d", eExp)
	}

	// Set, observe, persist.
	if _, ok, err := r.h.HGetEx(ctx, []byte("h"), []byte("a"), true, future); err != nil || !ok {
		t.Fatalf("HGetEx set TTL: %v, %v", ok, err)
	}
	if _, eExp, _, _ := r.h.getEntry(ctx, []byte("h"), []byte("a")); eExp != future {
		t.Fatalf("field TTL = %d, want %d", eExp, future)
	}
	if v, _ := r.hget("h", "a"); v != "1" {
		t.Fatalf("value after TTL edit = %q", v)
	}
	if _, ok, err := r.h.HGetEx(ctx, []byte("h"), []byte("a"), true, 0); err != nil || !ok {
		t.Fatalf("HGetEx persist: %v, %v", ok, err)
	}
	if _, eExp, _, _ := r.h.getEntry(ctx, []byte("h"), []byte("a")); eExp != 0 {
		t.Fatalf("field TTL after persist = %d, want 0", eExp)
	}

	// A past expiry deletes the field after the read, the backstop.
	v, ok, err = r.h.HGetEx(ctx, []byte("h"), []byte("b"), true, rigNow-1)
	if err != nil || !ok || string(v) != "2" {
		t.Fatalf("HGetEx past = %q, %v, %v", v, ok, err)
	}
	if _, ok := r.hget("h", "b"); ok {
		t.Fatal("field b survived a past expiry")
	}
	if n, _ := r.h.HLen(ctx, []byte("h")); n != 1 {
		t.Fatalf("HLen = %d, want 1", n)
	}

	// TTL preservation across HINCRBY (Redis 7.4+ semantics).
	if _, err := r.h.HIncrBy(ctx, []byte("h"), []byte("a"), 1); err != nil {
		t.Fatal(err)
	}
	if _, eExp, _, _ := r.h.getEntry(ctx, []byte("h"), []byte("a")); eExp != 0 {
		t.Fatalf("HIncrBy invented a TTL: %d", eExp)
	}
	if _, _, err := r.h.HGetEx(ctx, []byte("h"), []byte("a"), true, future); err != nil {
		t.Fatal(err)
	}
	if _, err := r.h.HIncrBy(ctx, []byte("h"), []byte("a"), 1); err != nil {
		t.Fatal(err)
	}
	if _, eExp, _, _ := r.h.getEntry(ctx, []byte("h"), []byte("a")); eExp != future {
		t.Fatalf("field TTL after HIncrBy = %d, want %d (preserved)", eExp, future)
	}
	// HSET clears it, Redis's HSET rule.
	r.hset("h", "a", "9")
	if _, eExp, _, _ := r.h.getEntry(ctx, []byte("h"), []byte("a")); eExp != 0 {
		t.Fatalf("field TTL after HSET = %d, want 0 (cleared)", eExp)
	}
}

func TestHGetExSegmented(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	val := segRigHash(t, r, "wide", 200)
	if enc, _, _ := r.h.Encoding(ctx, []byte("wide")); enc != "hashtable" {
		t.Fatalf("encoding = %q, want hashtable", enc)
	}
	future := rigNow + 60_000

	// TTL a few fields spread across segments; the root min_expire
	// must lower (H-I6's stale-late ban) and every decode validator on
	// the reopen path checks the images stay consistent.
	for _, f := range []string{"f000", "f077", "f199"} {
		v, ok, err := r.h.HGetEx(ctx, []byte("wide"), []byte(f), true, future)
		if err != nil || !ok {
			t.Fatalf("HGetEx(%s): %v, %v", f, ok, err)
		}
		i := 0
		fmt.Sscanf(f, "f%03d", &i)
		if string(v) != val(i) {
			t.Fatalf("HGetEx(%s) value mismatch", f)
		}
	}
	if _, eExp, ok, _ := r.h.getEntry(ctx, []byte("wide"), []byte("f077")); !ok || eExp != future {
		t.Fatalf("f077 TTL = %d, %v; want %d", eExp, ok, future)
	}
	// A pure update with the TTL riding through: HIncrByFloat is not
	// applicable to fat strings, so overwrite via HGetEx PERSIST and
	// re-set to prove both directions move the root min legally.
	if _, ok, err := r.h.HGetEx(ctx, []byte("wide"), []byte("f000"), true, 0); err != nil || !ok {
		t.Fatalf("persist f000: %v, %v", ok, err)
	}
	if _, eExp, _, _ := r.h.getEntry(ctx, []byte("wide"), []byte("f000")); eExp != 0 {
		t.Fatalf("f000 TTL after persist = %d", eExp)
	}

	// The cold view a restart sees must decode cleanly and agree.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	h2 := r.reopen()
	if _, eExp, ok, err := h2.getEntry(ctx, []byte("wide"), []byte("f199")); err != nil || !ok || eExp != future {
		t.Fatalf("cold f199 TTL = %d, %v, %v; want %d", eExp, ok, err, future)
	}
	if n, err := h2.HLen(ctx, []byte("wide")); err != nil || n != 200 {
		t.Fatalf("cold HLen = %d, %v; want 200", n, err)
	}
	for _, f := range []string{"f000", "f042", "f077", "f199"} {
		i := 0
		fmt.Sscanf(f, "f%03d", &i)
		v, ok, err := h2.HGet(ctx, []byte("wide"), []byte(f))
		if err != nil || !ok || string(v) != val(i) {
			t.Fatalf("cold HGet(%s): ok=%v err=%v", f, ok, err)
		}
	}
}
