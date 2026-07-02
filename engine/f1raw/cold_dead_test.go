package f1raw

import (
	"bytes"
	"strings"
	"testing"
)

// These tests cover the cold-log dead-byte accounting: the dead counter each unlink site
// bumps when it drops a separated record, and the ColdBytes introspection that reports it.
// The counter is dormant (nothing acts on it yet), so what matters is that it stays honest:
// every superseded cold value is counted exactly once, an inline supersession counts
// nothing, and dead never exceeds the log's total. A later compaction milestone reads this
// counter to decide when the dead fraction is worth a rewrite, so an undercount would defer
// a needed compaction and an overcount would trigger a pointless one.

// TestColdDeadOnOverwrite overwrites one large separated value with another. The first
// value's bytes are now unreferenced, so dead must equal the first value's length, while
// total holds both appends (the log is append-only, nothing is freed in place).
func TestColdDeadOnOverwrite(t *testing.T) {
	s := newColdStore(t, 512)
	first := strings.Repeat("1", 2048)
	second := strings.Repeat("2", 3072)
	if err := s.Set([]byte("k"), []byte(first)); err != nil {
		t.Fatalf("Set first: %v", err)
	}
	if _, dead := s.ColdBytes(); dead != 0 {
		t.Fatalf("dead after first write = %d, want 0", dead)
	}
	if err := s.Set([]byte("k"), []byte(second)); err != nil {
		t.Fatalf("Set second: %v", err)
	}
	total, dead := s.ColdBytes()
	if dead != uint64(len(first)) {
		t.Fatalf("dead after overwrite = %d, want %d (the superseded first value)", dead, len(first))
	}
	if total != uint64(len(first)+len(second)) {
		t.Fatalf("total = %d, want %d (both appends, append-only log)", total, len(first)+len(second))
	}
}

// TestColdDeadOnDelete deletes a separated key. Its cold bytes are unreferenced after the
// index entry drops, so dead must rise to the value's length.
func TestColdDeadOnDelete(t *testing.T) {
	s := newColdStore(t, 512)
	big := strings.Repeat("z", 2048)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !s.Delete([]byte("k")) {
		t.Fatal("Delete of present separated key returned false")
	}
	if _, dead := s.ColdBytes(); dead != uint64(len(big)) {
		t.Fatalf("dead after delete = %d, want %d", dead, len(big))
	}
}

// TestColdDeadOnInlineDeleteNoCount deletes a small inline key. There is no cold value to
// reclaim, so the delete must not touch the dead counter even though the store has a cold
// log open.
func TestColdDeadOnInlineDeleteNoCount(t *testing.T) {
	s := newColdStore(t, 512)
	if err := s.Set([]byte("k"), []byte("small")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !s.Delete([]byte("k")) {
		t.Fatal("Delete of present inline key returned false")
	}
	if total, dead := s.ColdBytes(); total != 0 || dead != 0 {
		t.Fatalf("inline delete moved cold accounting: total=%d dead=%d, want 0/0", total, dead)
	}
}

// TestColdDeadInlineOverwriteNoCount overwrites an inline value with another inline value.
// The inPlace path never separates and never supersedes a cold value, so dead stays zero.
func TestColdDeadInlineOverwriteNoCount(t *testing.T) {
	s := newColdStore(t, 512)
	if err := s.Set([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	if err := s.Set([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Set v2: %v", err)
	}
	if total, dead := s.ColdBytes(); total != 0 || dead != 0 {
		t.Fatalf("inline overwrite moved cold accounting: total=%d dead=%d, want 0/0", total, dead)
	}
}

// TestColdDeadSeparatedToInline overwrites a large separated value with a small inline one.
// The separated record is superseded by a fresh inline record, so its cold bytes become
// dead: the supersession is counted at publish's entry swap regardless of the new record's
// inline/separated shape.
func TestColdDeadSeparatedToInline(t *testing.T) {
	s := newColdStore(t, 512)
	big := strings.Repeat("z", 2048)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set big: %v", err)
	}
	if err := s.Set([]byte("k"), []byte("small")); err != nil {
		t.Fatalf("Set small: %v", err)
	}
	if _, dead := s.ColdBytes(); dead != uint64(len(big)) {
		t.Fatalf("dead after separated->inline overwrite = %d, want %d", dead, len(big))
	}
}

// TestColdDeadOnCollDelete deletes a separated collection element (a large hash field). The
// collection unlink site must count its cold bytes the same way the string delete does.
func TestColdDeadOnCollDelete(t *testing.T) {
	s := newColdStore(t, 512)
	big := bytes.Repeat([]byte("f"), 2048)
	if _, err := s.PutKind([]byte("f"), big, benchKindHashField); err != nil {
		t.Fatalf("PutKind: %v", err)
	}
	if !s.DeleteKind([]byte("f"), benchKindHashField) {
		t.Fatal("DeleteKind of present separated field returned false")
	}
	if _, dead := s.ColdBytes(); dead != uint64(len(big)) {
		t.Fatalf("dead after collection delete = %d, want %d", dead, len(big))
	}
}

// TestColdDeadOnTake pops a separated collection element with TakeKind (the fused
// read-then-delete a list pop runs). The value is returned to the caller, then its cold
// bytes are dead, so dead must equal the value length after the take.
func TestColdDeadOnTake(t *testing.T) {
	s := newColdStore(t, 512)
	big := bytes.Repeat([]byte("p"), 2048)
	if _, err := s.PutKind([]byte("e"), big, benchKindHashField); err != nil {
		t.Fatalf("PutKind: %v", err)
	}
	got, ok := s.TakeKind([]byte("e"), nil, benchKindHashField)
	if !ok || !bytes.Equal(got, big) {
		t.Fatalf("TakeKind = (%d bytes, %v), want the separated value intact", len(got), ok)
	}
	if _, dead := s.ColdBytes(); dead != uint64(len(big)) {
		t.Fatalf("dead after take = %d, want %d", dead, len(big))
	}
}

// TestColdDeadOnReset writes several separated values, then flushes with Reset. Every record
// is unlinked, so every cold byte is dead: Reset sets dead to the current tail so a flushed
// dataset's cold bytes never read as live.
func TestColdDeadOnReset(t *testing.T) {
	s := newColdStore(t, 512)
	val := strings.Repeat("q", 2048)
	const n = 8
	for i := 0; i < n; i++ {
		if err := s.Set([]byte{'k', byte(i)}, []byte(val)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	total, _ := s.ColdBytes()
	if total != uint64(n*len(val)) {
		t.Fatalf("total before reset = %d, want %d", total, n*len(val))
	}
	s.Reset()
	total2, dead := s.ColdBytes()
	if total2 != total {
		t.Fatalf("Reset changed total from %d to %d (log left in place in M1)", total, total2)
	}
	if dead != total {
		t.Fatalf("dead after reset = %d, want %d (every cold byte unlinked)", dead, total)
	}
}

// TestColdDeadCountedOnce overwrites the same separated key repeatedly. Each overwrite
// supersedes exactly one prior value, so dead must equal the sum of the superseded values
// (everything but the last), never double-counting a record, and never exceed total.
func TestColdDeadCountedOnce(t *testing.T) {
	s := newColdStore(t, 512)
	sizes := []int{2048, 3072, 1024, 4096, 2560}
	sum := 0
	for i, sz := range sizes {
		if err := s.Set([]byte("k"), bytes.Repeat([]byte("x"), sz)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		total, dead := s.ColdBytes()
		if dead > total {
			t.Fatalf("dead %d exceeded total %d at step %d", dead, total, i)
		}
		if i > 0 {
			sum += sizes[i-1]
		}
		if int(dead) != sum {
			t.Fatalf("dead after %d writes = %d, want %d (all but the live value)", i+1, dead, sum)
		}
	}
	// The one live value is the last size; live = total - dead must equal it.
	total, dead := s.ColdBytes()
	if int(total-dead) != sizes[len(sizes)-1] {
		t.Fatalf("live = total-dead = %d, want the last value %d", total-dead, sizes[len(sizes)-1])
	}
}

// TestColdNoLogNoAccounting confirms a store with no cold log reports zero for both counters
// and that markSepDead on the string and collection paths is a safe no-op there: a pure
// in-memory store never separates, so nothing is ever counted.
func TestColdNoLogNoAccounting(t *testing.T) {
	s := New(1<<10, 1<<20)
	defer s.Close()
	big := strings.Repeat("z", 4096)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	s.Delete([]byte("k"))
	if total, dead := s.ColdBytes(); total != 0 || dead != 0 {
		t.Fatalf("no-cold-log store reported total=%d dead=%d, want 0/0", total, dead)
	}
}
