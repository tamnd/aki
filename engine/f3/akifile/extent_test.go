package akifile

import (
	"errors"
	"reflect"
	"testing"
)

func TestExtentRoundTrip(t *testing.T) {
	es := []Extent{
		{Kind: ExtentHeader, StartOff: 0, Length: PageSize},
		{Kind: ExtentAppend, StartOff: PageSize, Length: 1 << 20},
		{Kind: ExtentFree, Flags: 1, StartOff: 1<<20 + PageSize, Length: 8192},
		{Kind: ExtentPendingFree, StartOff: 1 << 30, Length: 4096},
	}
	b := MarshalExtents(es)
	if len(b) != len(es)*ExtentSize {
		t.Fatalf("marshalled %d bytes, want %d", len(b), len(es)*ExtentSize)
	}
	got, err := ParseExtents(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !reflect.DeepEqual(got, es) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, es)
	}
}

func TestExtentEmpty(t *testing.T) {
	got, err := ParseExtents(nil)
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty table decoded %d extents", len(got))
	}
}

// TestExtentTruncated is a table whose byte length is not a whole number of
// 24-byte entries, i.e. a torn or corrupt table.
func TestExtentTruncated(t *testing.T) {
	b := MarshalExtents([]Extent{{Kind: ExtentAppend}})
	if _, err := ParseExtents(b[:len(b)-3]); !errors.Is(err, ErrLength) {
		t.Fatalf("err = %v, want ErrLength", err)
	}
}
