package command

// scriptEntryPrefix namespaces cached scripts in the .aki system table. Each
// script is one entry, keyed scriptEntryPrefix+sha1, valued with its body.
const scriptEntryPrefix = "script:"

// Only SCRIPT LOAD scripts are persisted, not the bodies that an inline EVAL
// caches as a side effect. SCRIPT LOAD is the explicit "keep this" verb, so
// persisting just those keeps EVALSHA working across a plain restart without
// turning every distinct inline EVAL into a commit.

// persistScript stores one cached script in the .aki system table. It is a no-op
// when no engine is attached. A write failure is logged, not returned, so SCRIPT
// LOAD still replies with the digest even if the durable copy lags.
func (d *Dispatcher) persistScript(sum, body string) {
	if d.engine == nil {
		return
	}
	if err := d.engine.systemSet(scriptEntryPrefix, sum, body); err != nil {
		d.logWarning("failed to persist script to the data file", logField{"err", err.Error()})
	}
}

// clearPersistedScripts drops every persisted script. SCRIPT FLUSH calls it so the
// durable copy matches the now-empty in-memory cache.
func (d *Dispatcher) clearPersistedScripts() {
	if d.engine == nil {
		return
	}
	if err := d.engine.systemReplace(scriptEntryPrefix, nil); err != nil {
		d.logWarning("failed to clear persisted scripts", logField{"err", err.Error()})
	}
}

// LoadScriptsFromKeyspace loads the persisted scripts back into the cache at
// startup. Each body is re-added through put, which recomputes the same digest, so
// the key in the table and the cache key always agree. An empty table leaves the
// cache empty.
func (d *Dispatcher) LoadScriptsFromKeyspace() error {
	if d.engine == nil {
		return nil
	}
	entries, err := d.engine.systemEntries(scriptEntryPrefix)
	if err != nil {
		return err
	}
	for _, body := range entries {
		d.scripts.put(body)
	}
	return nil
}
