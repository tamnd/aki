package command

import (
	"testing"

	"github.com/tamnd/aki/vfs"
)

const testLib = "#!lua name=mylib\nredis.register_function('myfunc', function() return 1 end)"

// loadOneLibrary builds and registers a single library source on d, the same way
// FUNCTION LOAD does, so the test does not have to drive the full command path.
func loadOneLibrary(t *testing.T, d *Dispatcher, src string) {
	t.Helper()
	d.loadFunctionLibraries([]string{src})
	if len(d.functions.named()) == 0 {
		t.Fatalf("library did not register: %q", src)
	}
}

// TestFunctionPersistRoundTrip loads a library, persists it, then reopens the file
// and loads the functions back. The library and its function must come back.
func TestFunctionPersistRoundTrip(t *testing.T) {
	fs := vfs.NewMem()

	d, p := openEngine(t, fs, "fn.aki", true)
	loadOneLibrary(t, d, testLib)
	d.persistFunctions()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, p2 := openEngine(t, fs, "fn.aki", false)
	defer func() { _ = p2.Close() }()
	if err := d2.LoadFunctionsFromKeyspace(); err != nil {
		t.Fatalf("load: %v", err)
	}
	got := d2.functions.named()
	if got["mylib"] != testLib {
		t.Fatalf("mylib source = %q want %q", got["mylib"], testLib)
	}
	d2.functions.mu.RLock()
	owner := d2.functions.fnIndex["myfunc"]
	d2.functions.mu.RUnlock()
	if owner != "mylib" {
		t.Fatalf("myfunc owner = %q want mylib", owner)
	}
}

// TestFunctionPersistFlushClearsEntries loads a library, persists, clears the
// registry, persists again, and checks nothing comes back after a reopen.
func TestFunctionPersistFlushClearsEntries(t *testing.T) {
	fs := vfs.NewMem()

	d, p := openEngine(t, fs, "fn.aki", true)
	loadOneLibrary(t, d, testLib)
	d.persistFunctions()
	d.functions.mu.Lock()
	d.functions.libs = map[string]*funcLib{}
	d.functions.fnIndex = map[string]string{}
	d.functions.mu.Unlock()
	d.persistFunctions()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, p2 := openEngine(t, fs, "fn.aki", false)
	defer func() { _ = p2.Close() }()
	if err := d2.LoadFunctionsFromKeyspace(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if n := len(d2.functions.named()); n != 0 {
		t.Fatalf("registry has %d libraries after flush and reopen, want 0", n)
	}
}

// TestFunctionPersistNoEngine confirms the persist and load paths are safe no-ops
// when no engine is attached.
func TestFunctionPersistNoEngine(t *testing.T) {
	d := New(Config{})
	d.persistFunctions()
	if err := d.LoadFunctionsFromKeyspace(); err != nil {
		t.Fatalf("load with no engine: %v", err)
	}
}
