package command

import "testing"

func TestHSetAndHGet(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "HSET h name Alice age 30"); got != ":2" {
		t.Fatalf("HSET = %q want :2", got)
	}
	// Updating an existing field adds 0 and can add a new one at the same time.
	if got := sendLine(t, r, c, "HSET h name Bob city NYC"); got != ":1" {
		t.Fatalf("HSET update+add = %q want :1", got)
	}
	if got := bulk(t, r, c, "HGET h name"); got != "Bob" {
		t.Fatalf("HGET name = %q want Bob", got)
	}
	if got := bulk(t, r, c, "HGET h missing"); got != "<nil>" {
		t.Fatalf("HGET missing field = %q want nil", got)
	}
	if got := bulk(t, r, c, "HGET nokey name"); got != "<nil>" {
		t.Fatalf("HGET missing key = %q want nil", got)
	}
}

func TestHSetOddArgs(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "HSET h f1 v1 f2"); got != "-ERR wrong number of arguments for 'hset' command" {
		t.Fatalf("HSET odd = %q", got)
	}
}

func TestHMSet(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "HMSET h a 1 b 2"); got != "+OK" {
		t.Fatalf("HMSET = %q want +OK", got)
	}
	if got := bulk(t, r, c, "HGET h b"); got != "2" {
		t.Fatalf("HGET b = %q want 2", got)
	}
}

func TestHSetNX(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "HSETNX h f v1"); got != ":1" {
		t.Fatalf("HSETNX new = %q want :1", got)
	}
	if got := sendLine(t, r, c, "HSETNX h f v2"); got != ":0" {
		t.Fatalf("HSETNX existing = %q want :0", got)
	}
	if got := bulk(t, r, c, "HGET h f"); got != "v1" {
		t.Fatalf("HGET f = %q want v1 (unchanged)", got)
	}
}

func TestHMGet(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1 b 2")
	got := array(t, r, c, "HMGET h a missing b")
	want := []string{"1", "<nil>", "2"}
	if !equalSlice(got, want) {
		t.Fatalf("HMGET = %v want %v", got, want)
	}
	// A missing key replies all nils.
	got = array(t, r, c, "HMGET nokey x y")
	if !equalSlice(got, []string{"<nil>", "<nil>"}) {
		t.Fatalf("HMGET missing key = %v", got)
	}
}

func TestHGetAll(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1 b 2")
	got := array(t, r, c, "HGETALL h")
	if !equalSlice(got, []string{"a", "1", "b", "2"}) {
		t.Fatalf("HGETALL = %v", got)
	}
	if got := sendLine(t, r, c, "HGETALL nokey"); got != "*0" {
		t.Fatalf("HGETALL missing = %q want *0", got)
	}
}

func TestHDel(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1 b 2 c 3")
	if got := sendLine(t, r, c, "HDEL h a missing c"); got != ":2" {
		t.Fatalf("HDEL = %q want :2", got)
	}
	if got := array(t, r, c, "HGETALL h"); !equalSlice(got, []string{"b", "2"}) {
		t.Fatalf("after HDEL = %v", got)
	}
	// Removing the last field deletes the key.
	if got := sendLine(t, r, c, "HDEL h b"); got != ":1" {
		t.Fatalf("HDEL last = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("emptied hash should be deleted, EXISTS = %q", got)
	}
}

func TestHLenExistsStrLen(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h name Alice age 30")
	if got := sendLine(t, r, c, "HLEN h"); got != ":2" {
		t.Fatalf("HLEN = %q want :2", got)
	}
	if got := sendLine(t, r, c, "HLEN nokey"); got != ":0" {
		t.Fatalf("HLEN missing = %q want :0", got)
	}
	if got := sendLine(t, r, c, "HEXISTS h name"); got != ":1" {
		t.Fatalf("HEXISTS present = %q want :1", got)
	}
	if got := sendLine(t, r, c, "HEXISTS h nope"); got != ":0" {
		t.Fatalf("HEXISTS absent = %q want :0", got)
	}
	if got := sendLine(t, r, c, "HSTRLEN h name"); got != ":5" {
		t.Fatalf("HSTRLEN name = %q want :5", got)
	}
	if got := sendLine(t, r, c, "HSTRLEN h nope"); got != ":0" {
		t.Fatalf("HSTRLEN absent = %q want :0", got)
	}
}

func TestHKeysHVals(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h a 1 b 2 c 3")
	if got := array(t, r, c, "HKEYS h"); !equalSlice(got, []string{"a", "b", "c"}) {
		t.Fatalf("HKEYS = %v", got)
	}
	if got := array(t, r, c, "HVALS h"); !equalSlice(got, []string{"1", "2", "3"}) {
		t.Fatalf("HVALS = %v", got)
	}
	if got := sendLine(t, r, c, "HKEYS nokey"); got != "*0" {
		t.Fatalf("HKEYS missing = %q want *0", got)
	}
}

func TestHashWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	for _, cmd := range []string{
		"HSET s f v", "HSETNX s f v", "HGET s f", "HMGET s f",
		"HGETALL s", "HDEL s f", "HLEN s", "HEXISTS s f",
		"HKEYS s", "HVALS s", "HSTRLEN s f",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
