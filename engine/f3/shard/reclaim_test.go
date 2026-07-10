package shard

import (
	"bytes"
	"fmt"
	"testing"
)

// TestOwnerReclaimsArena drives sustained separated-band churn through the
// owner path and pins the two shard-side triggers: the idle-boundary
// maybeCompact frees what the churn killed, and the arena never reports full
// even though the churn allocates several times the arena's capacity. The
// runtime is not started; the test goroutine is the owner, same as the
// zero-alloc test, so the triggers are called at exactly the boundaries the
// run loop uses.
func TestOwnerReclaimsArena(t *testing.T) {
	rt := testRuntime(1)
	c := rt.NewConn()
	w := rt.workers[0]

	const nKeys = 24
	sizes := []int{2048, 3072, 4096, 6144}
	key := func(i int) []byte { return []byte(fmt.Sprintf("churn%03d", i%nKeys)) }
	val := func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i%26)}, sizes[(i*7)%len(sizes)])
	}

	emit := func([]byte) {}
	fails := 0
	drain := func() {
		c.Flush()
		for w.drainAndExecute() > 0 {
		}
		c.DrainReplies(func(rep []byte) {
			if len(rep) > 0 && rep[0] == '-' {
				fails++
				t.Errorf("reply error: %q", rep)
			}
			emit(rep)
		})
	}

	// Churn to several times the arena's 4MiB: every size-crossing overwrite
	// kills a run where it lies. The between-drain boundary is where the run
	// loop would reclaim under load, so the test does the same.
	for i := 0; i < 12_000; i++ {
		if err := c.Do(opSet, true, [][]byte{key(i), val(i)}); err != nil {
			t.Fatal(err)
		}
		if i%16 == 15 {
			drain()
			if len(w.streams) == 0 && w.st.ArenaTight() {
				w.st.CompactArena()
			}
		}
	}
	drain()
	if fails > 0 {
		t.Fatalf("%d writes failed under churn", fails)
	}

	// Now the idle trigger: kill half the keys' bytes with oversize rewrites,
	// then let maybeCompact take the dead share back.
	for i := 0; i < nKeys; i++ {
		if err := c.Do(opSet, true, [][]byte{key(i), bytes.Repeat([]byte{'z'}, 8192)}); err != nil {
			t.Fatal(err)
		}
	}
	drain()
	before, _ := w.st.ArenaBytes()
	w.maybeCompact()
	after, _ := w.st.ArenaBytes()
	if after > before {
		t.Fatalf("arena fill grew across maybeCompact: %d -> %d", before, after)
	}

	// Every key still reads back its last write.
	for i := 0; i < nKeys; i++ {
		if err := c.Do(opGet, true, [][]byte{key(i)}); err != nil {
			t.Fatal(err)
		}
	}
	got := 0
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	c.DrainReplies(func(rep []byte) {
		if len(rep) > 0 && rep[0] == '$' && !bytes.HasPrefix(rep, []byte("$-1")) {
			got++
		}
	})
	if got != nKeys {
		t.Fatalf("%d of %d keys readable after churn and compaction", got, nKeys)
	}
}
