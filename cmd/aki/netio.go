package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/resp"
	"github.com/tamnd/aki/respclient"
)

// dumpFromServer reads every key from a running instance into an rdb.Snapshot.
// It walks each database with SCAN, pulls the serialized value with DUMP, and
// reads the remaining TTL with PTTL, turning the live keyspace into the same
// snapshot shape the offline reader produces. When onlyDB is zero or greater
// only that database is read. auth, when set, is sent before anything else.
func dumpFromServer(addr, auth string, databases, onlyDB int, timeout time.Duration) (rdb.Snapshot, error) {
	cl, err := respclient.Dial(addr, timeout)
	if err != nil {
		return rdb.Snapshot{}, fmt.Errorf("connect %s: %w", addr, err)
	}
	defer cl.Close()

	if auth != "" {
		if reply, aerr := cl.CallStr("AUTH", auth); aerr != nil {
			return rdb.Snapshot{}, fmt.Errorf("auth: %w", aerr)
		} else if reply.Type == resp.TypeError {
			return rdb.Snapshot{}, fmt.Errorf("auth: %s", reply.Err)
		}
	}

	nowMs := time.Now().UnixMilli()
	var snap rdb.Snapshot
	for db := 0; db < databases; db++ {
		if onlyDB >= 0 && db != onlyDB {
			continue
		}
		if reply, serr := cl.CallStr("SELECT", strconv.Itoa(db)); serr != nil {
			return rdb.Snapshot{}, fmt.Errorf("select %d: %w", db, serr)
		} else if reply.Type == resp.TypeError {
			return rdb.Snapshot{}, fmt.Errorf("select %d: %s", db, reply.Err)
		}

		entries, derr := scanDatabase(cl, nowMs)
		if derr != nil {
			return rdb.Snapshot{}, fmt.Errorf("db %d: %w", db, derr)
		}
		if len(entries) > 0 {
			idx := db
			if onlyDB >= 0 {
				idx = 0
			}
			snap.DBs = append(snap.DBs, rdb.DBData{Index: idx, Entries: entries})
		}
	}
	return snap, nil
}

// scanDatabase walks the currently selected database with SCAN and serializes
// every key it finds. A key that disappears between SCAN and DUMP is skipped, and
// a value type DUMP cannot serialize on the remote is reported and skipped so one
// odd key does not abort the whole export.
func scanDatabase(cl *respclient.Client, nowMs int64) ([]rdb.Entry, error) {
	var entries []rdb.Entry
	cursor := "0"
	for {
		reply, err := cl.CallStr("SCAN", cursor, "COUNT", "512")
		if err != nil {
			return nil, err
		}
		if reply.Type == resp.TypeError {
			return nil, fmt.Errorf("scan: %s", reply.Err)
		}
		if len(reply.Elems) != 2 {
			return nil, fmt.Errorf("scan: unexpected reply shape")
		}
		cursor = string(reply.Elems[0].Str)
		for _, k := range reply.Elems[1].Elems {
			entry, ok, eerr := dumpOneKey(cl, k.Str, nowMs)
			if eerr != nil {
				return nil, eerr
			}
			if ok {
				entries = append(entries, entry)
			}
		}
		if cursor == "0" {
			break
		}
	}
	return entries, nil
}

// dumpOneKey serializes a single key from the remote. ok is false when the key is
// gone or its type cannot be serialized.
func dumpOneKey(cl *respclient.Client, key []byte, nowMs int64) (rdb.Entry, bool, error) {
	dump, err := cl.Call([]byte("DUMP"), key)
	if err != nil {
		return rdb.Entry{}, false, err
	}
	if dump.Type == resp.TypeError {
		fmt.Printf("skip %s: %s\n", key, dump.Err)
		return rdb.Entry{}, false, nil
	}
	if dump.IsNull {
		return rdb.Entry{}, false, nil
	}
	val, derr := rdb.Unmarshal(dump.Str)
	if derr != nil {
		fmt.Printf("skip %s: %v\n", key, derr)
		return rdb.Entry{}, false, nil
	}

	pttl, perr := cl.CallStr("PTTL", string(key))
	if perr != nil {
		return rdb.Entry{}, false, perr
	}
	expire := int64(-1)
	if pttl.Type == resp.TypeInteger && pttl.Integer >= 0 {
		expire = nowMs + pttl.Integer
	}
	return rdb.Entry{Key: append([]byte(nil), key...), Value: val, ExpireMS: expire}, true, nil
}

// importToServer ships every key in a snapshot to a running instance with
// RESTORE. It selects each source database on the remote, sends the remaining
// TTL in milliseconds, and adds REPLACE when asked so an existing key is
// overwritten instead of rejected. It returns the number of keys written.
func importToServer(addr, auth string, snap rdb.Snapshot, onlyDB int, replace bool, timeout time.Duration) (int, error) {
	cl, err := respclient.Dial(addr, timeout)
	if err != nil {
		return 0, fmt.Errorf("connect %s: %w", addr, err)
	}
	defer cl.Close()

	if auth != "" {
		if reply, aerr := cl.CallStr("AUTH", auth); aerr != nil {
			return 0, fmt.Errorf("auth: %w", aerr)
		} else if reply.Type == resp.TypeError {
			return 0, fmt.Errorf("auth: %s", reply.Err)
		}
	}

	nowMs := time.Now().UnixMilli()
	written := 0
	for _, dbData := range snap.DBs {
		if onlyDB >= 0 && dbData.Index != onlyDB {
			continue
		}
		if reply, serr := cl.CallStr("SELECT", strconv.Itoa(dbData.Index)); serr != nil {
			return written, fmt.Errorf("select %d: %w", dbData.Index, serr)
		} else if reply.Type == resp.TypeError {
			return written, fmt.Errorf("select %d: %s", dbData.Index, reply.Err)
		}
		for _, e := range dbData.Entries {
			n, rerr := restoreOneKey(cl, e, nowMs, replace)
			if rerr != nil {
				return written, rerr
			}
			written += n
		}
	}
	return written, nil
}

// restoreOneKey sends one RESTORE to the remote. It returns 1 when the key was
// written and 0 when it was skipped because it already existed without REPLACE.
func restoreOneKey(cl *respclient.Client, e rdb.Entry, nowMs int64, replace bool) (int, error) {
	blob, merr := rdb.Marshal(e.Value)
	if merr != nil {
		fmt.Printf("skip %s: %v\n", e.Key, merr)
		return 0, nil
	}
	ttl := int64(0)
	if e.ExpireMS >= 0 {
		ttl = e.ExpireMS - nowMs
		if ttl < 1 {
			ttl = 1
		}
	}
	args := [][]byte{[]byte("RESTORE"), e.Key, []byte(strconv.FormatInt(ttl, 10)), blob}
	if replace {
		args = append(args, []byte("REPLACE"))
	}
	reply, err := cl.Call(args...)
	if err != nil {
		return 0, err
	}
	if reply.Type == resp.TypeError {
		if !replace && len(reply.Err) >= 7 && reply.Err[:7] == "BUSYKEY" {
			return 0, nil
		}
		return 0, fmt.Errorf("restore %s: %s", e.Key, reply.Err)
	}
	return 1, nil
}
