package command

import "github.com/tamnd/aki/keyspace"

// zsetPopN pops up to count members from one end of a sorted set: the lowest
// scores when fromMax is false, the highest when fromMax is true. The popped
// members come back in reply order, lowest first for MIN and highest first for
// MAX. On a coll-form set it pops the boundary rows in place through zsetCollPop,
// which seeks to the right end of the score index and deletes only the popped
// rows, so a pop off a million-member set touches the boundary window rather than
// cloning every member. It falls back to a blob decode only below the listpack
// threshold, where the whole set is small by definition. emptied reports whether
// the pop removed the last member. The caller has confirmed the key exists and is
// a sorted set through zsetHeader.
func zsetPopN(db *keyspace.DB, key []byte, hdr keyspace.ValueHeader, count int64, fromMax bool, lim encLimits) (popped []zmember, emptied bool, err error) {
	if hdr.IsColl() {
		popped, err = zsetCollPop(db, key, count, fromMax)
		if err != nil || len(popped) == 0 {
			return nil, false, err
		}
		// CollUpdate tore the key down if the pop emptied it; re-probe to reflect
		// that for the del notification.
		_, stillThere, herr := zsetHeader(db, key)
		if herr != nil {
			return nil, false, herr
		}
		return popped, !stillThere, nil
	}
	members, ehdr, found, err := getZSet(db, key)
	if err != nil {
		return nil, false, err
	}
	if !found || len(members) == 0 {
		return nil, false, nil
	}
	n := int(min(count, int64(len(members))))
	var kept []zmember
	if fromMax {
		// Highest scores sit at the tail; return them highest first.
		popped = make([]zmember, n)
		for i := range n {
			popped[i] = members[len(members)-1-i]
		}
		kept = members[:len(members)-n]
	} else {
		popped = append(popped, members[:n]...)
		kept = members[n:]
	}
	if len(kept) == 0 {
		if _, e := db.Delete(key); e != nil {
			return nil, false, e
		}
		return popped, true, nil
	}
	if e := db.Set(key, zsetEncode(kept), keyspace.TypeZSet,
		zsetEncoding(lim, kept, ehdr.Encoding), keepTTL(ehdr, found)); e != nil {
		return nil, false, e
	}
	return popped, false, nil
}
