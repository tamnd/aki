package command

import (
	"sort"
	"testing"
)

func TestHIncrBy(t *testing.T) {
	r, c := startData(t)
	// Missing field starts at 0.
	if got := sendLine(t, r, c, "HINCRBY h n 5"); got != ":5" {
		t.Fatalf("HINCRBY new = %q want :5", got)
	}
	if got := sendLine(t, r, c, "HINCRBY h n -2"); got != ":3" {
		t.Fatalf("HINCRBY = %q want :3", got)
	}
	_ = sendLine(t, r, c, "HSET h s notanumber")
	if got := sendLine(t, r, c, "HINCRBY h s 1"); got != "-ERR hash value is not an integer" {
		t.Fatalf("HINCRBY non-int = %q", got)
	}
	_ = sendLine(t, r, c, "HSET h big 9223372036854775807")
	if got := sendLine(t, r, c, "HINCRBY h big 1"); got != "-ERR increment or decrement would overflow" {
		t.Fatalf("HINCRBY overflow = %q", got)
	}
}

func TestHIncrByFloat(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h v 10.5")
	if got := bulk(t, r, c, "HINCRBYFLOAT h v 0.1"); got != "10.6" {
		t.Fatalf("HINCRBYFLOAT = %q want 10.6", got)
	}
	if got := bulk(t, r, c, "HINCRBYFLOAT h v -5"); got != "5.6" {
		t.Fatalf("HINCRBYFLOAT = %q want 5.6", got)
	}
	// Missing field starts at 0.
	if got := bulk(t, r, c, "HINCRBYFLOAT h fresh 2.5"); got != "2.5" {
		t.Fatalf("HINCRBYFLOAT new = %q want 2.5", got)
	}
	_ = sendLine(t, r, c, "HSET h s notafloat")
	if got := sendLine(t, r, c, "HINCRBYFLOAT h s 1"); got != "-ERR hash value is not a float" {
		t.Fatalf("HINCRBYFLOAT non-float = %q", got)
	}
}

func TestHRandFieldNoCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1 b 2 c 3")
	got := bulk(t, r, c, "HRANDFIELD h")
	if got != "a" && got != "b" && got != "c" {
		t.Fatalf("HRANDFIELD = %q want one of a/b/c", got)
	}
	if got := bulk(t, r, c, "HRANDFIELD nokey"); got != "<nil>" {
		t.Fatalf("HRANDFIELD missing key = %q want nil", got)
	}
}

func TestHRandFieldPositiveCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1 b 2 c 3")
	// A positive count returns distinct fields.
	got := array(t, r, c, "HRANDFIELD h 2")
	if len(got) != 2 {
		t.Fatalf("HRANDFIELD h 2 len = %d want 2", len(got))
	}
	if got[0] == got[1] {
		t.Fatalf("HRANDFIELD distinct expected, got dup %v", got)
	}
	// A count past the size returns all fields once.
	got = array(t, r, c, "HRANDFIELD h 10")
	sort.Strings(got)
	if !equalSlice(got, []string{"a", "b", "c"}) {
		t.Fatalf("HRANDFIELD h 10 = %v want all", got)
	}
}

func TestHRandFieldNegativeCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1")
	// A negative count allows duplicates and returns exactly its magnitude.
	got := array(t, r, c, "HRANDFIELD h -4")
	if len(got) != 4 {
		t.Fatalf("HRANDFIELD h -4 len = %d want 4", len(got))
	}
	for _, f := range got {
		if f != "a" {
			t.Fatalf("HRANDFIELD = %v want all a", got)
		}
	}
}

func TestHRandFieldWithValues(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1 b 2 c 3")
	got := array(t, r, c, "HRANDFIELD h 3 WITHVALUES")
	if len(got) != 6 {
		t.Fatalf("HRANDFIELD WITHVALUES len = %d want 6", len(got))
	}
	pairs := map[string]string{}
	for i := 0; i < len(got); i += 2 {
		pairs[got[i]] = got[i+1]
	}
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range want {
		if pairs[k] != v {
			t.Fatalf("HRANDFIELD WITHVALUES %s = %q want %q", k, pairs[k], v)
		}
	}
	if got := sendLine(t, r, c, "HRANDFIELD h WITHVALUES"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("HRANDFIELD WITHVALUES without count = %q", got)
	}
}

func TestHashExtraWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	for _, cmd := range []string{"HINCRBY s f 1", "HINCRBYFLOAT s f 1", "HRANDFIELD s 2"} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
