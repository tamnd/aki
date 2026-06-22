package command

import (
	"testing"

	"github.com/tamnd/aki/vfs"
)

const testScript = "return 1 + 1"

// TestScriptPersistRoundTrip caches a script, persists it, then reopens the file
// and loads scripts back. The body must come back under the same digest.
func TestScriptPersistRoundTrip(t *testing.T) {
	fs := vfs.NewMem()

	d, p := openEngine(t, fs, "sc.aki", true)
	sum := d.scripts.put(testScript)
	d.persistScript(sum, testScript)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, p2 := openEngine(t, fs, "sc.aki", false)
	defer func() { _ = p2.Close() }()
	if err := d2.LoadScriptsFromKeyspace(); err != nil {
		t.Fatalf("load: %v", err)
	}
	body, ok := d2.scripts.get(sum)
	if !ok || body != testScript {
		t.Fatalf("get %s = %q ok %v want %q", sum, body, ok, testScript)
	}
}

// TestScriptPersistFlushClears caches and persists a script, clears the persisted
// copy, then proves nothing comes back after a reopen.
func TestScriptPersistFlushClears(t *testing.T) {
	fs := vfs.NewMem()

	d, p := openEngine(t, fs, "sc.aki", true)
	sum := d.scripts.put(testScript)
	d.persistScript(sum, testScript)
	d.clearPersistedScripts()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, p2 := openEngine(t, fs, "sc.aki", false)
	defer func() { _ = p2.Close() }()
	if err := d2.LoadScriptsFromKeyspace(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := d2.scripts.get(sum); ok {
		t.Fatal("script still present after flush and reopen")
	}
}

// TestScriptPersistNoEngine confirms the persist, clear and load paths are safe
// no-ops with no engine attached.
func TestScriptPersistNoEngine(t *testing.T) {
	d := New(Config{})
	d.persistScript("deadbeef", testScript)
	d.clearPersistedScripts()
	if err := d.LoadScriptsFromKeyspace(); err != nil {
		t.Fatalf("load with no engine: %v", err)
	}
}
