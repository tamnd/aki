package command

// fnEntryPrefix namespaces function libraries in the .aki system table. Each
// library is one entry, keyed fnEntryPrefix+libname, valued with its source. The
// source is all that is needed to rebuild the library, the same form FUNCTION2
// records carry in an RDB.
const fnEntryPrefix = "fn:"

// named renders the loaded libraries as a map of library name to source. The
// caller mirrors it into the .aki system table.
func (fr *functionRegistry) named() map[string]string {
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	out := make(map[string]string, len(fr.libs))
	for name, lib := range fr.libs {
		out[name] = lib.source
	}
	return out
}

// persistFunctions mirrors the loaded function libraries into the .aki system
// table. It is a no-op when no engine is attached. A write failure is logged, not
// returned, so the FUNCTION command still succeeds on the wire even if the durable
// copy lags. Functions also ride the RDB and the AOF; this is the extra copy that
// survives a plain restart of a server with no RDB or AOF.
func (d *Dispatcher) persistFunctions() {
	if d.engine == nil {
		return
	}
	if err := d.engine.systemReplace(fnEntryPrefix, d.functions.named()); err != nil {
		d.logWarning("failed to persist functions to the data file", logField{"err", err.Error()})
	}
}

// PersistFunctions mirrors the loaded function libraries into the .aki system
// table. The server uses it at startup to make an imported dump.rdb's functions
// durable in the data file.
func (d *Dispatcher) PersistFunctions() { d.persistFunctions() }

// LoadFunctionsFromKeyspace rebuilds the function registry from the libraries
// persisted in the .aki system table. It runs once at startup before any RDB
// import, so an explicit --load-rdb still applies on top. An empty table leaves
// the registry empty, the normal first-boot state.
func (d *Dispatcher) LoadFunctionsFromKeyspace() error {
	if d.engine == nil {
		return nil
	}
	entries, err := d.engine.systemEntries(fnEntryPrefix)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	sources := make([]string, 0, len(entries))
	for _, src := range entries {
		sources = append(sources, src)
	}
	d.loadFunctionLibraries(sources)
	return nil
}
