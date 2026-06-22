package command

import (
	"sort"
	"strconv"
	"strings"
)

// This file holds the model oracle for the property test in property_test.go.
// The model is a small, obviously-correct implementation of the subset of Redis
// commands the generator produces. It is correct by inspection, not by speed.
// The property test runs the same random command sequence against the model and
// against a live in-process aki dispatcher and asserts the replies match.
//
// Doc 23 section 5 specifies this design. The model here covers strings, lists,
// hashes, and sets, which is the subset with simple, unambiguous reply shapes.

// errReply is the canonical form of a RESP error. Only the leading code (the
// first word) is compared, which is how a real client distinguishes WRONGTYPE
// from ERR without depending on the exact message wording.
type errReply struct{ code string }

var (
	wrongType = errReply{"WRONGTYPE"}
	notIntErr = errReply{"ERR"}
)

type mtype int

const (
	mNone mtype = iota
	mString
	mList
	mHash
	mSet
)

type mval struct {
	typ  mtype
	s    string
	list []string
	hash map[string]string
	set  map[string]bool
}

// modelDB is a single-database model. The property test uses db 0 only.
type modelDB struct {
	keys map[string]*mval
}

func newModelDB() *modelDB { return &modelDB{keys: map[string]*mval{}} }

// exec applies one command and returns its canonical reply.
func (db *modelDB) exec(argv []string) any {
	if len(argv) == 0 {
		return errReply{"ERR"}
	}
	cmd := strings.ToUpper(argv[0])
	a := argv[1:]
	switch cmd {
	case "SET":
		db.keys[a[0]] = &mval{typ: mString, s: a[1]}
		return "OK"
	case "SETNX":
		if db.keys[a[0]] != nil {
			return int64(0)
		}
		db.keys[a[0]] = &mval{typ: mString, s: a[1]}
		return int64(1)
	case "GET":
		v := db.keys[a[0]]
		if v == nil {
			return nil
		}
		if v.typ != mString {
			return wrongType
		}
		return v.s
	case "APPEND":
		v := db.keys[a[0]]
		if v == nil {
			db.keys[a[0]] = &mval{typ: mString, s: a[1]}
			return int64(len(a[1]))
		}
		if v.typ != mString {
			return wrongType
		}
		v.s += a[1]
		return int64(len(v.s))
	case "STRLEN":
		v := db.keys[a[0]]
		if v == nil {
			return int64(0)
		}
		if v.typ != mString {
			return wrongType
		}
		return int64(len(v.s))
	case "INCR":
		return db.incrBy(a[0], 1)
	case "DECR":
		return db.incrBy(a[0], -1)
	case "INCRBY":
		n, err := strconv.ParseInt(a[1], 10, 64)
		if err != nil {
			return notIntErr
		}
		return db.incrBy(a[0], n)
	case "DEL":
		if db.keys[a[0]] != nil {
			delete(db.keys, a[0])
			return int64(1)
		}
		return int64(0)
	case "EXISTS":
		if db.keys[a[0]] != nil {
			return int64(1)
		}
		return int64(0)
	case "TYPE":
		v := db.keys[a[0]]
		if v == nil {
			return "none"
		}
		return mTypeName(v.typ)
	case "RPUSH", "LPUSH":
		return db.push(cmd == "LPUSH", a[0], a[1:])
	case "RPOP":
		return db.pop(false, a[0])
	case "LPOP":
		return db.pop(true, a[0])
	case "LLEN":
		v := db.keys[a[0]]
		if v == nil {
			return int64(0)
		}
		if v.typ != mList {
			return wrongType
		}
		return int64(len(v.list))
	case "LINDEX":
		return db.lindex(a[0], a[1])
	case "LRANGE":
		return db.lrange(a[0], a[1], a[2])
	case "HSET":
		return db.hset(a[0], a[1:])
	case "HGET":
		v := db.keys[a[0]]
		if v == nil {
			return nil
		}
		if v.typ != mHash {
			return wrongType
		}
		val, ok := v.hash[a[1]]
		if !ok {
			return nil
		}
		return val
	case "HDEL":
		return db.hdel(a[0], a[1])
	case "HLEN":
		v := db.keys[a[0]]
		if v == nil {
			return int64(0)
		}
		if v.typ != mHash {
			return wrongType
		}
		return int64(len(v.hash))
	case "HEXISTS":
		v := db.keys[a[0]]
		if v == nil {
			return int64(0)
		}
		if v.typ != mHash {
			return wrongType
		}
		if _, ok := v.hash[a[1]]; ok {
			return int64(1)
		}
		return int64(0)
	case "HGETALL":
		v := db.keys[a[0]]
		if v == nil {
			return []any{}
		}
		if v.typ != mHash {
			return wrongType
		}
		out := make([]any, 0, len(v.hash)*2)
		for f, val := range v.hash {
			out = append(out, f, val)
		}
		return out
	case "SADD":
		return db.sadd(a[0], a[1:])
	case "SREM":
		return db.srem(a[0], a[1])
	case "SCARD":
		v := db.keys[a[0]]
		if v == nil {
			return int64(0)
		}
		if v.typ != mSet {
			return wrongType
		}
		return int64(len(v.set))
	case "SISMEMBER":
		v := db.keys[a[0]]
		if v == nil {
			return int64(0)
		}
		if v.typ != mSet {
			return wrongType
		}
		if v.set[a[1]] {
			return int64(1)
		}
		return int64(0)
	case "SMEMBERS":
		v := db.keys[a[0]]
		if v == nil {
			return []any{}
		}
		if v.typ != mSet {
			return wrongType
		}
		out := make([]any, 0, len(v.set))
		for m := range v.set {
			out = append(out, m)
		}
		return out
	}
	return errReply{"ERR"}
}

func (db *modelDB) incrBy(key string, delta int64) any {
	v := db.keys[key]
	if v == nil {
		db.keys[key] = &mval{typ: mString, s: strconv.FormatInt(delta, 10)}
		return delta
	}
	if v.typ != mString {
		return wrongType
	}
	cur, ok := parseRedisInt(v.s)
	if !ok {
		return notIntErr
	}
	cur += delta
	v.s = strconv.FormatInt(cur, 10)
	return cur
}

// parseRedisInt mirrors Redis string2ll: a base-10 integer with no leading
// zeros (except "0" itself), no leading plus, and no surrounding space. Go's
// strconv.ParseInt is more lenient (it accepts "01" and "+1"), so INCR and DECR
// need this stricter parse to match real Redis on a stored value like "01".
func parseRedisInt(s string) (int64, bool) {
	if s == "0" {
		return 0, true
	}
	i := 0
	if len(s) > 0 && s[0] == '-' {
		i = 1
	}
	if i >= len(s) || s[i] < '1' || s[i] > '9' {
		return 0, false
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (db *modelDB) push(left bool, key string, vals []string) any {
	v := db.keys[key]
	if v == nil {
		v = &mval{typ: mList}
		db.keys[key] = v
	} else if v.typ != mList {
		return wrongType
	}
	for _, e := range vals {
		if left {
			v.list = append([]string{e}, v.list...)
		} else {
			v.list = append(v.list, e)
		}
	}
	return int64(len(v.list))
}

func (db *modelDB) pop(left bool, key string) any {
	v := db.keys[key]
	if v == nil {
		return nil
	}
	if v.typ != mList {
		return wrongType
	}
	if len(v.list) == 0 {
		return nil
	}
	var out string
	if left {
		out = v.list[0]
		v.list = v.list[1:]
	} else {
		out = v.list[len(v.list)-1]
		v.list = v.list[:len(v.list)-1]
	}
	if len(v.list) == 0 {
		delete(db.keys, key)
	}
	return out
}

func (db *modelDB) lindex(key, idxStr string) any {
	v := db.keys[key]
	if v == nil {
		return nil
	}
	if v.typ != mList {
		return wrongType
	}
	i, err := strconv.Atoi(idxStr)
	if err != nil {
		return notIntErr
	}
	n := len(v.list)
	if i < 0 {
		i += n
	}
	if i < 0 || i >= n {
		return nil
	}
	return v.list[i]
}

func (db *modelDB) lrange(key, startStr, stopStr string) any {
	v := db.keys[key]
	if v == nil {
		return []any{}
	}
	if v.typ != mList {
		return wrongType
	}
	start, err1 := strconv.Atoi(startStr)
	stop, err2 := strconv.Atoi(stopStr)
	if err1 != nil || err2 != nil {
		return notIntErr
	}
	n := len(v.list)
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	out := []any{}
	if start > stop || n == 0 {
		return out
	}
	for _, e := range v.list[start : stop+1] {
		out = append(out, e)
	}
	return out
}

func (db *modelDB) hset(key string, fv []string) any {
	v := db.keys[key]
	if v == nil {
		v = &mval{typ: mHash, hash: map[string]string{}}
		db.keys[key] = v
	} else if v.typ != mHash {
		return wrongType
	}
	added := int64(0)
	for i := 0; i+1 < len(fv); i += 2 {
		if _, ok := v.hash[fv[i]]; !ok {
			added++
		}
		v.hash[fv[i]] = fv[i+1]
	}
	return added
}

func (db *modelDB) hdel(key, field string) any {
	v := db.keys[key]
	if v == nil {
		return int64(0)
	}
	if v.typ != mHash {
		return wrongType
	}
	if _, ok := v.hash[field]; !ok {
		return int64(0)
	}
	delete(v.hash, field)
	if len(v.hash) == 0 {
		delete(db.keys, key)
	}
	return int64(1)
}

func (db *modelDB) sadd(key string, members []string) any {
	v := db.keys[key]
	if v == nil {
		v = &mval{typ: mSet, set: map[string]bool{}}
		db.keys[key] = v
	} else if v.typ != mSet {
		return wrongType
	}
	added := int64(0)
	for _, m := range members {
		if !v.set[m] {
			v.set[m] = true
			added++
		}
	}
	return added
}

func (db *modelDB) srem(key, member string) any {
	v := db.keys[key]
	if v == nil {
		return int64(0)
	}
	if v.typ != mSet {
		return wrongType
	}
	if !v.set[member] {
		return int64(0)
	}
	delete(v.set, member)
	if len(v.set) == 0 {
		delete(db.keys, key)
	}
	return int64(1)
}

func mTypeName(t mtype) string {
	switch t {
	case mString:
		return "string"
	case mList:
		return "list"
	case mHash:
		return "hash"
	case mSet:
		return "set"
	}
	return "none"
}

// normalize folds order out of replies whose element order is unspecified, so a
// model reply and an aki reply compare equal regardless of iteration order.
func normalize(cmd string, v any) any {
	switch strings.ToUpper(cmd) {
	case "HGETALL":
		a, ok := v.([]any)
		if !ok {
			return v
		}
		m := map[string]string{}
		for i := 0; i+1 < len(a); i += 2 {
			k, _ := a[i].(string)
			val, _ := a[i+1].(string)
			m[k] = val
		}
		return m
	case "SMEMBERS":
		a, ok := v.([]any)
		if !ok {
			return v
		}
		ss := make([]string, 0, len(a))
		for _, e := range a {
			s, _ := e.(string)
			ss = append(ss, s)
		}
		sort.Strings(ss)
		return ss
	}
	return v
}

// canonEqual compares two canonical replies, treating errors by their leading
// code only and arrays elementwise.
func canonEqual(a, b any) bool {
	switch av := a.(type) {
	case errReply:
		bv, ok := b.(errReply)
		return ok && av.code == bv.code
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !canonEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]string:
		bv, ok := b.(map[string]string)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, val := range av {
			if bv[k] != val {
				return false
			}
		}
		return true
	case []string:
		bv, ok := b.([]string)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
		return true
	case nil:
		return b == nil
	default:
		return a == b
	}
}
