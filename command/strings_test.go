package command

import "testing"

func TestSetNXOption(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k first")
	// NX on an existing key does nothing and returns null.
	if got := sendLine(t, r, c, "SET k second NX"); got != "$-1" {
		t.Fatalf("SET k second NX = %q want null", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "first" {
		t.Fatalf("GET k = %q want first", got)
	}
	// NX on a fresh key sets it.
	if got := sendLine(t, r, c, "SET fresh v NX"); got != "+OK" {
		t.Fatalf("SET fresh v NX = %q want +OK", got)
	}
}

func TestSetXXOption(t *testing.T) {
	r, c := startData(t)
	// XX on a missing key does nothing and returns null.
	if got := sendLine(t, r, c, "SET k v XX"); got != "$-1" {
		t.Fatalf("SET k v XX = %q want null", got)
	}
	_ = sendLine(t, r, c, "SET k first")
	if got := sendLine(t, r, c, "SET k second XX"); got != "+OK" {
		t.Fatalf("SET k second XX = %q want +OK", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "second" {
		t.Fatalf("GET k = %q want second", got)
	}
}

func TestSetGetOption(t *testing.T) {
	r, c := startData(t)
	// GET on a missing key returns null and still sets the new value.
	if got := bulk(t, r, c, "SET k new GET"); got != "<nil>" {
		t.Fatalf("SET k new GET (missing) = %q want nil", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "new" {
		t.Fatalf("GET k = %q want new", got)
	}
	// GET on an existing key returns the old value and writes the new one.
	if got := bulk(t, r, c, "SET k newer GET"); got != "new" {
		t.Fatalf("SET k newer GET = %q want new", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "newer" {
		t.Fatalf("GET k = %q want newer", got)
	}
}

func TestSetNXGetReturnsOldValueWithoutWriting(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k keep")
	// NX fails because the key exists; GET still returns the old value and the
	// stored value is unchanged.
	if got := bulk(t, r, c, "SET k blocked NX GET"); got != "keep" {
		t.Fatalf("SET k blocked NX GET = %q want keep", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "keep" {
		t.Fatalf("GET k = %q want keep", got)
	}
}

func TestSetInvalidExpire(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SET k v EX 0"); got != "-ERR invalid expire time in 'set' command" {
		t.Fatalf("SET k v EX 0 = %q", got)
	}
	if got := sendLine(t, r, c, "SET k v PX -1"); got != "-ERR invalid expire time in 'set' command" {
		t.Fatalf("SET k v PX -1 = %q", got)
	}
}

func TestSetConflictingOptions(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SET k v NX XX"); got != "-ERR syntax error" {
		t.Fatalf("SET k v NX XX = %q", got)
	}
	if got := sendLine(t, r, c, "SET k v EX 10 PX 20"); got != "-ERR syntax error" {
		t.Fatalf("SET k v EX 10 PX 20 = %q", got)
	}
}

func TestSetNXCommand(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SETNX k v"); got != ":1" {
		t.Fatalf("SETNX new = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SETNX k other"); got != ":0" {
		t.Fatalf("SETNX existing = %q want :0", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "v" {
		t.Fatalf("GET k = %q want v", got)
	}
}

func TestSetEX(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SETEX k 100 v"); got != "+OK" {
		t.Fatalf("SETEX = %q want +OK", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "v" {
		t.Fatalf("GET k = %q want v", got)
	}
	if got := sendLine(t, r, c, "SETEX k 0 v"); got != "-ERR invalid expire time in 'setex' command" {
		t.Fatalf("SETEX k 0 v = %q", got)
	}
	if got := sendLine(t, r, c, "SETEX k notnum v"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("SETEX k notnum v = %q", got)
	}
}

func TestPSetEX(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PSETEX k 100000 v"); got != "+OK" {
		t.Fatalf("PSETEX = %q want +OK", got)
	}
	if got := sendLine(t, r, c, "PSETEX k 0 v"); got != "-ERR invalid expire time in 'psetex' command" {
		t.Fatalf("PSETEX k 0 v = %q", got)
	}
}

func TestGetSet(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "GETSET k first"); got != "<nil>" {
		t.Fatalf("GETSET missing = %q want nil", got)
	}
	if got := bulk(t, r, c, "GETSET k second"); got != "first" {
		t.Fatalf("GETSET = %q want first", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "second" {
		t.Fatalf("GET k = %q want second", got)
	}
}

func TestGetDel(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := bulk(t, r, c, "GETDEL k"); got != "v" {
		t.Fatalf("GETDEL = %q want v", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "<nil>" {
		t.Fatalf("GET k after GETDEL = %q want nil", got)
	}
	if got := bulk(t, r, c, "GETDEL missing"); got != "<nil>" {
		t.Fatalf("GETDEL missing = %q want nil", got)
	}
}

func TestGetEX(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	// No option behaves as a plain read.
	if got := bulk(t, r, c, "GETEX k"); got != "v" {
		t.Fatalf("GETEX k = %q want v", got)
	}
	// PERSIST returns the value; the key stays.
	if got := bulk(t, r, c, "GETEX k PERSIST"); got != "v" {
		t.Fatalf("GETEX k PERSIST = %q want v", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "v" {
		t.Fatalf("GET k = %q want v", got)
	}
	// EX takes the value still readable afterward.
	if got := bulk(t, r, c, "GETEX k EX 100"); got != "v" {
		t.Fatalf("GETEX k EX 100 = %q want v", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "v" {
		t.Fatalf("GET k after GETEX EX = %q want v", got)
	}
	if got := sendLine(t, r, c, "GETEX k EX 0"); got != "-ERR invalid expire time in 'getex' command" {
		t.Fatalf("GETEX k EX 0 = %q", got)
	}
}
