package command

import "testing"

func TestRename(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a hello")
	if got := sendLine(t, r, c, "RENAME a b"); got != "+OK" {
		t.Fatalf("RENAME = %q", got)
	}
	if got := bulk(t, r, c, "GET b"); got != "hello" {
		t.Fatalf("GET b = %q want hello", got)
	}
	if got := sendLine(t, r, c, "EXISTS a"); got != ":0" {
		t.Fatalf("EXISTS a after rename = %q want :0", got)
	}
}

func TestRenameKeepsTTL(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a v")
	big := "99999999999999"
	_ = sendLine(t, r, c, "PEXPIREAT a "+big)
	_ = sendLine(t, r, c, "RENAME a b")
	if got := sendLine(t, r, c, "PEXPIRETIME b"); got != ":"+big {
		t.Fatalf("PEXPIRETIME b = %q want :%s", got, big)
	}
}

func TestRenameOverwrites(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a one")
	_ = sendLine(t, r, c, "SET b two")
	_ = sendLine(t, r, c, "RENAME a b")
	if got := bulk(t, r, c, "GET b"); got != "one" {
		t.Fatalf("GET b after overwrite = %q want one", got)
	}
}

func TestRenameSameKey(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a v")
	if got := sendLine(t, r, c, "RENAME a a"); got != "+OK" {
		t.Fatalf("RENAME a a = %q want +OK", got)
	}
	if got := bulk(t, r, c, "GET a"); got != "v" {
		t.Fatalf("GET a = %q want v", got)
	}
}

func TestRenameMissing(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "RENAME nope b"); got != "-ERR no such key" {
		t.Fatalf("RENAME missing = %q", got)
	}
}

func TestRenameNX(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a v")
	if got := sendLine(t, r, c, "RENAMENX a b"); got != ":1" {
		t.Fatalf("RENAMENX fresh = %q want :1", got)
	}
	_ = sendLine(t, r, c, "SET a v2")
	if got := sendLine(t, r, c, "RENAMENX a b"); got != ":0" {
		t.Fatalf("RENAMENX taken = %q want :0", got)
	}
	if got := sendLine(t, r, c, "RENAMENX nope b"); got != "-ERR no such key" {
		t.Fatalf("RENAMENX missing = %q", got)
	}
}

func TestTouch(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	_ = sendLine(t, r, c, "SET b 2")
	if got := sendLine(t, r, c, "TOUCH a b missing a"); got != ":3" {
		t.Fatalf("TOUCH = %q want :3", got)
	}
}

func TestUnlink(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	_ = sendLine(t, r, c, "SET b 2")
	if got := sendLine(t, r, c, "UNLINK a b c"); got != ":2" {
		t.Fatalf("UNLINK = %q want :2", got)
	}
}

func TestMove(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "MOVE k 1"); got != ":1" {
		t.Fatalf("MOVE = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS k"); got != ":0" {
		t.Fatalf("EXISTS k in db0 after MOVE = %q want :0", got)
	}
	_ = sendLine(t, r, c, "SELECT 1")
	if got := bulk(t, r, c, "GET k"); got != "v" {
		t.Fatalf("GET k in db1 = %q want v", got)
	}
}

func TestMoveDestExists(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k src")
	_ = sendLine(t, r, c, "SELECT 1")
	_ = sendLine(t, r, c, "SET k dst")
	_ = sendLine(t, r, c, "SELECT 0")
	if got := sendLine(t, r, c, "MOVE k 1"); got != ":0" {
		t.Fatalf("MOVE onto existing = %q want :0", got)
	}
	// The source is left in place.
	if got := bulk(t, r, c, "GET k"); got != "src" {
		t.Fatalf("GET k after failed MOVE = %q want src", got)
	}
}

func TestMoveErrors(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "MOVE k 0"); got != "-ERR source and destination objects are the same" {
		t.Fatalf("MOVE same db = %q", got)
	}
	if got := sendLine(t, r, c, "MOVE k 99"); got != "-ERR DB index is out of range" {
		t.Fatalf("MOVE bad db = %q", got)
	}
	if got := sendLine(t, r, c, "MOVE k notanint"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("MOVE bad int = %q", got)
	}
}

func TestCopySameDB(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a hello")
	if got := sendLine(t, r, c, "COPY a b"); got != ":1" {
		t.Fatalf("COPY = %q want :1", got)
	}
	if got := bulk(t, r, c, "GET b"); got != "hello" {
		t.Fatalf("GET b = %q want hello", got)
	}
	// The source survives a copy.
	if got := bulk(t, r, c, "GET a"); got != "hello" {
		t.Fatalf("GET a after copy = %q want hello", got)
	}
}

func TestCopyReplace(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a one")
	_ = sendLine(t, r, c, "SET b two")
	if got := sendLine(t, r, c, "COPY a b"); got != ":0" {
		t.Fatalf("COPY onto existing without REPLACE = %q want :0", got)
	}
	if got := sendLine(t, r, c, "COPY a b REPLACE"); got != ":1" {
		t.Fatalf("COPY REPLACE = %q want :1", got)
	}
	if got := bulk(t, r, c, "GET b"); got != "one" {
		t.Fatalf("GET b after REPLACE = %q want one", got)
	}
}

func TestCopyToDB(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "COPY k k DB 2"); got != ":1" {
		t.Fatalf("COPY DB 2 = %q want :1", got)
	}
	_ = sendLine(t, r, c, "SELECT 2")
	if got := bulk(t, r, c, "GET k"); got != "v" {
		t.Fatalf("GET k in db2 = %q want v", got)
	}
}

func TestCopySameObject(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "COPY k k"); got != "-ERR source and destination objects are the same" {
		t.Fatalf("COPY same object = %q", got)
	}
}

func TestCopyMissingSource(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "COPY nope dst"); got != ":0" {
		t.Fatalf("COPY missing source = %q want :0", got)
	}
}
