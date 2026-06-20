package command

import (
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

func TestSetBitAndGetBit(t *testing.T) {
	r, c := startData(t)
	// The spec worked example: setting bit 7 of an empty key builds "\x01".
	if got := sendLine(t, r, c, "SETBIT mybit 7 1"); got != ":0" {
		t.Fatalf("SETBIT 7 1 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "GETBIT mybit 7"); got != ":1" {
		t.Fatalf("GETBIT 7 = %q want :1", got)
	}
	if got := bulk(t, r, c, "GET mybit"); got != "\x01" {
		t.Fatalf("GET mybit = %q want \\x01", got)
	}
	// Setting the most significant bit zero-extends nothing and gives 0x81.
	if got := sendLine(t, r, c, "SETBIT mybit 0 1"); got != ":0" {
		t.Fatalf("SETBIT 0 1 = %q want :0", got)
	}
	if got := bulk(t, r, c, "GET mybit"); got != "\x81" {
		t.Fatalf("GET mybit = %q want \\x81", got)
	}
	// SETBIT returns the previous bit value.
	if got := sendLine(t, r, c, "SETBIT mybit 7 0"); got != ":1" {
		t.Fatalf("SETBIT 7 0 = %q want :1", got)
	}
}

func TestGetBitPastEnd(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k foobar")
	if got := sendLine(t, r, c, "GETBIT k 1000"); got != ":0" {
		t.Fatalf("GETBIT past end = %q want :0", got)
	}
	if got := sendLine(t, r, c, "GETBIT missing 0"); got != ":0" {
		t.Fatalf("GETBIT missing = %q want :0", got)
	}
}

func TestSetBitBadArgs(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SETBIT k -1 1"); got != "-ERR bit offset is not an integer or out of range" {
		t.Fatalf("SETBIT -1 = %q", got)
	}
	if got := sendLine(t, r, c, "SETBIT k 0 2"); got != "-ERR bit is not an integer or it is out of range" {
		t.Fatalf("SETBIT value 2 = %q", got)
	}
}

func TestBitCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k foobar")
	if got := sendLine(t, r, c, "BITCOUNT k"); got != ":26" {
		t.Fatalf("BITCOUNT = %q want :26", got)
	}
	if got := sendLine(t, r, c, "BITCOUNT k 0 0"); got != ":4" {
		t.Fatalf("BITCOUNT 0 0 = %q want :4", got)
	}
	if got := sendLine(t, r, c, "BITCOUNT k 1 1"); got != ":6" {
		t.Fatalf("BITCOUNT 1 1 = %q want :6", got)
	}
	if got := sendLine(t, r, c, "BITCOUNT k 0 0 BYTE"); got != ":4" {
		t.Fatalf("BITCOUNT 0 0 BYTE = %q want :4", got)
	}
	if got := sendLine(t, r, c, "BITCOUNT k 5 30 BIT"); got != ":17" {
		t.Fatalf("BITCOUNT 5 30 BIT = %q want :17", got)
	}
	if got := sendLine(t, r, c, "BITCOUNT missing"); got != ":0" {
		t.Fatalf("BITCOUNT missing = %q want :0", got)
	}
}

func TestBitCountSyntax(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k foobar")
	if got := sendLine(t, r, c, "BITCOUNT k 0"); got != "-ERR syntax error" {
		t.Fatalf("BITCOUNT k 0 = %q", got)
	}
	if got := sendLine(t, r, c, "BITCOUNT k 0 0 NIBBLE"); got != "-ERR syntax error" {
		t.Fatalf("BITCOUNT bad unit = %q", got)
	}
}

func TestBitPos(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET mykey "\xff\xf0\x00"`)
	if got := sendLine(t, r, c, "BITPOS mykey 0"); got != ":12" {
		t.Fatalf("BITPOS 0 = %q want :12", got)
	}
	if got := sendLine(t, r, c, "BITPOS mykey 1"); got != ":0" {
		t.Fatalf("BITPOS 1 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "BITPOS mykey 0 2"); got != ":16" {
		t.Fatalf("BITPOS 0 2 = %q want :16", got)
	}
}

func TestBitPosAllOnes(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET allones "\xff\xff\xff"`)
	// No zero bit anywhere, and no end given, so the answer is one past the end.
	if got := sendLine(t, r, c, "BITPOS allones 0"); got != ":24" {
		t.Fatalf("BITPOS allones 0 = %q want :24", got)
	}
	// With an explicit end the not-found answer is -1 instead.
	if got := sendLine(t, r, c, "BITPOS allones 0 0 -1"); got != ":-1" {
		t.Fatalf("BITPOS allones 0 0 -1 = %q want :-1", got)
	}
}

func TestBitPosEmptyAndMissing(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET empty ""`)
	if got := sendLine(t, r, c, "BITPOS empty 0"); got != ":0" {
		t.Fatalf("BITPOS empty 0 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "BITPOS empty 1"); got != ":-1" {
		t.Fatalf("BITPOS empty 1 = %q want :-1", got)
	}
	if got := sendLine(t, r, c, "BITPOS missing 0"); got != ":0" {
		t.Fatalf("BITPOS missing 0 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "BITPOS missing 1"); got != ":-1" {
		t.Fatalf("BITPOS missing 1 = %q want :-1", got)
	}
}

func TestBitPosBit(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET mykey "\xff\xf0\x00"`)
	if got := sendLine(t, r, c, "BITPOS mykey 0 8 -1 BIT"); got != ":12" {
		t.Fatalf("BITPOS 0 8 -1 BIT = %q want :12", got)
	}
}

func TestBitOp(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET key1 abc")
	_ = sendLine(t, r, c, "SET key2 abd")
	if got := sendLine(t, r, c, "BITOP AND dest key1 key2"); got != ":3" {
		t.Fatalf("BITOP AND = %q want :3", got)
	}
	if got := bulk(t, r, c, "GET dest"); got != "ab`" {
		t.Fatalf("BITOP AND result = %q", got)
	}
	if got := sendLine(t, r, c, "BITOP OR dest key1 key2"); got != ":3" {
		t.Fatalf("BITOP OR = %q want :3", got)
	}
	if got := bulk(t, r, c, "GET dest"); got != "abg" {
		t.Fatalf("BITOP OR result = %q", got)
	}
}

func TestBitOpNot(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET src "\xff\x00"`)
	if got := sendLine(t, r, c, "BITOP NOT dest src"); got != ":2" {
		t.Fatalf("BITOP NOT = %q want :2", got)
	}
	if got := bulk(t, r, c, "GET dest"); got != "\x00\xff" {
		t.Fatalf("BITOP NOT result = %q", got)
	}
	if got := sendLine(t, r, c, "BITOP NOT dest a b"); got != "-ERR BITOP NOT must be called with a single source key." {
		t.Fatalf("BITOP NOT two keys = %q", got)
	}
}

func TestBitOpUnknownAndEmpty(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BITOP NAND dest a"); got != "-ERR syntax error" {
		t.Fatalf("BITOP NAND = %q", got)
	}
	// All-missing sources delete the destination and return 0.
	_ = sendLine(t, r, c, "SET dest preset")
	if got := sendLine(t, r, c, "BITOP AND dest miss1 miss2"); got != ":0" {
		t.Fatalf("BITOP all missing = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS dest"); got != ":0" {
		t.Fatalf("dest should be deleted, EXISTS = %q", got)
	}
}

func TestBitmapWrongType(t *testing.T) {
	// No list/set/hash commands exist yet, so seed a non-string value straight
	// into the keyspace and check the bit commands reject it with WRONGTYPE.
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	eng := NewEngine(ks)
	if err := eng.update(0, func(db *keyspace.DB) error {
		return db.Set([]byte("mylist"), []byte("x"), keyspace.TypeList, keyspace.EncRaw, -1)
	}); err != nil {
		t.Fatalf("seed list: %v", err)
	}
	r, c := start(t, Config{Engine: eng})

	if got := sendLine(t, r, c, "SETBIT mylist 0 1"); got != "-"+wrongTypeError {
		t.Fatalf("SETBIT on list = %q", got)
	}
	if got := sendLine(t, r, c, "GETBIT mylist 0"); got != "-"+wrongTypeError {
		t.Fatalf("GETBIT on list = %q", got)
	}
	if got := sendLine(t, r, c, "BITCOUNT mylist"); got != "-"+wrongTypeError {
		t.Fatalf("BITCOUNT on list = %q", got)
	}
	if got := sendLine(t, r, c, "BITPOS mylist 1"); got != "-"+wrongTypeError {
		t.Fatalf("BITPOS on list = %q", got)
	}
}
