package zset

import "sort"

// nativeStore is the seam for the native zset band (spec 2064/f3/12 section 2):
// the counted B+ tree plus member hash. This slice ships a placeholder, a
// member-to-score map for O(1) ZSCORE plus an ordered slice for rank and range,
// so the one-way conversion and the OBJECT ENCODING skiplist transition are
// exercised now. A later M2 slice replaces this file with the counted tree
// behind the same method set; zset.go calls only these methods, so nothing
// above the seam changes.
//
// The placeholder keeps the ordered slice in the zset total order (score
// ascending, member bytes ascending) so entries and rank read it directly. Its
// inserts are O(n) memmoves, which is fine for a stand-in the tree replaces.
type nativeStore struct {
	byMember map[string]float64
	order    []natEntry
}

type natEntry struct {
	member string
	score  float64
}

func newNativeStore(hint int) *nativeStore {
	return &nativeStore{
		byMember: make(map[string]float64, hint),
		order:    make([]natEntry, 0, hint),
	}
}

func (n *nativeStore) card() int { return len(n.order) }

func (n *nativeStore) score(m []byte) (float64, bool) {
	v, ok := n.byMember[string(m)]
	return v, ok
}

// appendSorted takes a member already greater than every current one, the fast
// path the band promotion uses to load an ordered blob.
func (n *nativeStore) appendSorted(m []byte, score float64) {
	ms := string(m)
	n.byMember[ms] = score
	n.order = append(n.order, natEntry{member: ms, score: score})
}

func (n *nativeStore) insert(m []byte, score float64) {
	ms := string(m)
	n.byMember[ms] = score
	i := n.seek(ms, score)
	n.order = append(n.order, natEntry{})
	copy(n.order[i+1:], n.order[i:])
	n.order[i] = natEntry{member: ms, score: score}
}

func (n *nativeStore) rescore(m []byte, score float64) {
	n.rem(m)
	n.insert(m, score)
}

func (n *nativeStore) rem(m []byte) bool {
	ms := string(m)
	old, ok := n.byMember[ms]
	if !ok {
		return false
	}
	delete(n.byMember, ms)
	i := n.seek(ms, old)
	n.order = append(n.order[:i], n.order[i+1:]...)
	return true
}

// seek returns the index of the first entry at or after (score, ms).
func (n *nativeStore) seek(ms string, score float64) int {
	return sort.Search(len(n.order), func(i int) bool {
		e := n.order[i]
		return !lessStr(e.score, e.member, score, ms)
	})
}

func (n *nativeStore) each(fn func(m []byte, score float64)) {
	for i := range n.order {
		fn([]byte(n.order[i].member), n.order[i].score)
	}
}

// rankScan advances idx to the count of members sorting before m. The member is
// known present, so the walk always finds it.
func (n *nativeStore) rankScan(m []byte, idx *int) {
	ms := string(m)
	for i := range n.order {
		if n.order[i].member == ms {
			*idx = i
			return
		}
	}
}

// lessStr is lessPair over string members, the ordering the placeholder's slice
// is kept in.
func lessStr(sA float64, mA string, sB float64, mB string) bool {
	if sA != sB {
		return sA < sB
	}
	return mA < mB
}
