package command

import (
	"slices"
	"testing"
)

// TestSortNumeric checks the default numeric ascending sort over a list.
func TestSortNumeric(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH nums 3 1 2 10")
	if got := readArray(t, r, c, "SORT nums"); !slices.Equal(got, []string{"1", "2", "3", "10"}) {
		t.Fatalf("SORT nums = %v", got)
	}
}

// TestSortDescAndLimit checks DESC ordering and the LIMIT window together.
func TestSortDescAndLimit(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH nums 3 1 2 10 5")
	if got := readArray(t, r, c, "SORT nums DESC LIMIT 1 2"); !slices.Equal(got, []string{"5", "3"}) {
		t.Fatalf("SORT DESC LIMIT = %v", got)
	}
}

// TestSortAlpha checks lexical sorting and the numeric error on non-numbers.
func TestSortAlpha(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH words banana apple cherry")
	if got := readArray(t, r, c, "SORT words ALPHA"); !slices.Equal(got, []string{"apple", "banana", "cherry"}) {
		t.Fatalf("SORT ALPHA = %v", got)
	}
	// Without ALPHA the non-numeric elements cannot be ordered.
	if got := sendLine(t, r, c, "SORT words"); got != "-ERR One or more scores can't be converted into double" {
		t.Fatalf("SORT non-numeric = %q", got)
	}
}

// TestSortByAndGet checks the BY weight pattern and GET projection, including the
// "#" placeholder and a missing GET key returning nil.
func TestSortByAndGet(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH ids 1 2 3")
	_ = sendLine(t, r, c, "MSET weight_1 30 weight_2 10 weight_3 20")
	_ = sendLine(t, r, c, "MSET name_1 alice name_3 carol")
	// Order by weight: 2 (10), 3 (20), 1 (30). Project the id and its name.
	got := readArray(t, r, c, "SORT ids BY weight_* GET # GET name_*")
	want := []string{"2", "<nil>", "3", "carol", "1", "alice"}
	if !slices.Equal(got, want) {
		t.Fatalf("SORT BY GET = %v want %v", got, want)
	}
}

// TestSortByHashPattern checks the key->field form of the BY and GET patterns.
func TestSortByHashPattern(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH ids 1 2")
	_ = sendLine(t, r, c, "HSET h_1 w 5 label one")
	_ = sendLine(t, r, c, "HSET h_2 w 1 label two")
	if got := readArray(t, r, c, "SORT ids BY h_*->w GET h_*->label"); !slices.Equal(got, []string{"two", "one"}) {
		t.Fatalf("SORT BY hash = %v", got)
	}
}

// TestSortStore checks that STORE writes a list and returns its length, and that
// the stored key reads back in sorted order.
func TestSortStore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH nums 3 1 2")
	if got := sendLine(t, r, c, "SORT nums STORE dest"); got != ":3" {
		t.Fatalf("SORT STORE = %q", got)
	}
	if got := readArray(t, r, c, "LRANGE dest 0 -1"); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Fatalf("stored list = %v", got)
	}
	if got := sendLine(t, r, c, "SORT_RO nums STORE dest"); got != "-ERR syntax error" {
		t.Fatalf("SORT_RO STORE = %q want syntax error", got)
	}
}

// TestSortNosortBy checks that a BY pattern with no '*' leaves the list order
// untouched.
func TestSortNosortBy(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH nums 3 1 2")
	if got := readArray(t, r, c, "SORT nums BY nosort"); !slices.Equal(got, []string{"3", "1", "2"}) {
		t.Fatalf("SORT BY nosort = %v", got)
	}
}

// TestSortMissingKey checks that sorting a missing key returns an empty array.
func TestSortMissingKey(t *testing.T) {
	r, c := startData(t)
	if got := readArray(t, r, c, "SORT nope"); len(got) != 0 {
		t.Fatalf("SORT missing = %v want empty", got)
	}
}
