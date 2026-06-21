package command

import (
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/rdb"
)

// This file exposes the keyspace-to-RDB bridge the CLI uses for offline import
// and export (spec 2064 doc 17 sections 9 and 10). The server uses the same
// helpers to load a dump.rdb at first open. They reuse the per-key serialization
// the DUMP and RESTORE commands rely on, so a key written by import is built the
// same way a key written by a command is.

// SnapshotKeyspace copies every live key in every database of ks into an
// rdb.Snapshot. The caller is expected to hold whatever lock the keyspace needs;
// the offline tools own the keyspace exclusively and the server takes the engine
// lock through snapshotAll.
func SnapshotKeyspace(ks *keyspace.Keyspace) (rdb.Snapshot, error) {
	snap := rdb.Snapshot{}
	for i := range ks.DBCount() {
		db, err := ks.DB(i)
		if err != nil {
			return rdb.Snapshot{}, err
		}
		entries, err := reloadEntries(db)
		if err != nil {
			return rdb.Snapshot{}, err
		}
		if len(entries) > 0 {
			snap.DBs = append(snap.DBs, rdb.DBData{Index: i, Entries: entries})
		}
	}
	return snap, nil
}

// LoadSnapshot writes the entries from snap into ks. When onlyDB is zero or
// greater only that source database is imported. When replace is false a key that
// already exists is left untouched; when true it is overwritten. It returns the
// number of keys written. The caller commits the keyspace.
func LoadSnapshot(ks *keyspace.Keyspace, snap rdb.Snapshot, onlyDB int, replace bool) (int, error) {
	written := 0
	for _, dbData := range snap.DBs {
		if onlyDB >= 0 && dbData.Index != onlyDB {
			continue
		}
		db, err := ks.DB(dbData.Index)
		if err != nil {
			return written, err
		}
		for _, e := range dbData.Entries {
			if !replace {
				exists, eerr := db.Exists(e.Key)
				if eerr != nil {
					return written, eerr
				}
				if exists {
					continue
				}
			}
			if serr := storeRestored(db, e.Key, e.Value, e.ExpireMS); serr != nil {
				return written, serr
			}
			if e.HasIdle {
				db.SetIdle(e.Key, e.Idle)
			}
			if e.HasFreq {
				db.SetFreq(e.Key, e.Freq)
			}
			written++
		}
	}
	return written, nil
}

// CountSnapshot returns the number of keys in a snapshot, optionally limited to a
// single source database. The import dry run uses it.
func CountSnapshot(snap rdb.Snapshot, onlyDB int) int {
	n := 0
	for _, dbData := range snap.DBs {
		if onlyDB >= 0 && dbData.Index != onlyDB {
			continue
		}
		n += len(dbData.Entries)
	}
	return n
}
