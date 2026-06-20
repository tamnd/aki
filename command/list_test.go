package command

import "testing"

func TestListPushOrderAndRange(t *testing.T) {
	r, c := startData(t)
	// LPUSH puts each argument at the head, so the run reverses.
	if got := sendLine(t, r, c, "LPUSH k a b c"); got != ":3" {
		t.Fatalf("LPUSH = %q want :3", got)
	}
	want := []string{"c", "b", "a"}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, want) {
		t.Fatalf("LRANGE after LPUSH = %v want %v", got, want)
	}
	// RPUSH appends in order.
	_ = sendLine(t, r, c, "RPUSH k x y z")
	want = []string{"c", "b", "a", "x", "y", "z"}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, want) {
		t.Fatalf("LRANGE after RPUSH = %v want %v", got, want)
	}
	if got := sendLine(t, r, c, "LLEN k"); got != ":6" {
		t.Fatalf("LLEN = %q want :6", got)
	}
}

func TestListRangeNegativeAndClamp(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c d e")
	if got := array(t, r, c, "LRANGE k -3 -1"); !equalSlice(got, []string{"c", "d", "e"}) {
		t.Fatalf("LRANGE -3 -1 = %v", got)
	}
	if got := array(t, r, c, "LRANGE k 0 100"); !equalSlice(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("LRANGE 0 100 = %v", got)
	}
	if got := sendLine(t, r, c, "LRANGE k 5 10"); got != "*0" {
		t.Fatalf("LRANGE out of range = %q want *0", got)
	}
	if got := sendLine(t, r, c, "LRANGE missing 0 -1"); got != "*0" {
		t.Fatalf("LRANGE missing = %q want *0", got)
	}
}

func TestListPopSingle(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c")
	if got := bulk(t, r, c, "LPOP k"); got != "a" {
		t.Fatalf("LPOP = %q want a", got)
	}
	if got := bulk(t, r, c, "RPOP k"); got != "c" {
		t.Fatalf("RPOP = %q want c", got)
	}
	if got := bulk(t, r, c, "LPOP missing"); got != "<nil>" {
		t.Fatalf("LPOP missing = %q want nil", got)
	}
}

func TestListPopCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c d e")
	if got := array(t, r, c, "LPOP k 2"); !equalSlice(got, []string{"a", "b"}) {
		t.Fatalf("LPOP k 2 = %v", got)
	}
	// RPOP count returns elements in pop order, so the tail comes out reversed.
	if got := array(t, r, c, "RPOP k 2"); !equalSlice(got, []string{"e", "d"}) {
		t.Fatalf("RPOP k 2 = %v", got)
	}
	// Count above the length returns the rest and deletes the key.
	if got := array(t, r, c, "LPOP k 10"); !equalSlice(got, []string{"c"}) {
		t.Fatalf("LPOP k 10 = %v", got)
	}
	if got := sendLine(t, r, c, "EXISTS k"); got != ":0" {
		t.Fatalf("key should be gone, EXISTS = %q", got)
	}
}

func TestListPopCountZeroAndNil(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a")
	if got := sendLine(t, r, c, "LPOP k 0"); got != "*0" {
		t.Fatalf("LPOP k 0 = %q want *0", got)
	}
	if got := sendLine(t, r, c, "LPOP missing 2"); got != "*-1" {
		t.Fatalf("LPOP missing 2 = %q want *-1", got)
	}
	if got := sendLine(t, r, c, "LPOP k -1"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("LPOP k -1 = %q", got)
	}
}

func TestListPushXEmptyAndExisting(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "LPUSHX k a"); got != ":0" {
		t.Fatalf("LPUSHX missing = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS k"); got != ":0" {
		t.Fatalf("LPUSHX should not create key, EXISTS = %q", got)
	}
	_ = sendLine(t, r, c, "RPUSH k a")
	if got := sendLine(t, r, c, "RPUSHX k b"); got != ":2" {
		t.Fatalf("RPUSHX existing = %q want :2", got)
	}
}

func TestListWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	if got := sendLine(t, r, c, "LPUSH s a"); got != "-"+wrongTypeError {
		t.Fatalf("LPUSH on string = %q", got)
	}
	if got := sendLine(t, r, c, "LLEN s"); got != "-"+wrongTypeError {
		t.Fatalf("LLEN on string = %q", got)
	}
	if got := sendLine(t, r, c, "LPOP s"); got != "-"+wrongTypeError {
		t.Fatalf("LPOP on string = %q", got)
	}
}

func TestListPopTooManyArgs(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a")
	if got := sendLine(t, r, c, "LPOP k 1 2"); got != "-ERR wrong number of arguments for 'lpop' command" {
		t.Fatalf("LPOP too many args = %q", got)
	}
}

// equalSlice reports whether two string slices are element-wise equal.
func equalSlice(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
