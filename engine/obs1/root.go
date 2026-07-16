// The root object (spec 2064/obs1 doc 03 section 3): the only well-known
// key and the only CAS-replaced one, read once per boot per node. The root
// is convenience and the chain is authority: losing every root update
// forever costs boot speed, never correctness, so the update protocol
// gives up the moment someone else has advanced further.
//
// Two spellings, chosen by the create-time probe (doc 03 section 9): the
// CAS world replaces the `root` key with If-Match, and the fallback world
// appends a dense sequence `rootv/<seq16>` with CAS-create, booters
// walking GET-next to the tail. Everything above the two writers is
// identical.
package obs1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// rootFVersion is the layout this parser reads and this writer writes.
const rootFVersion = 1

// Root is the root object payload. Settings is the canonical create-time
// settings blob, opaque at this layer: the parser carries it verbatim and
// the encoding lands with the settings themselves.
type Root struct {
	DBID      [16]byte
	CreatedMS uint64
	G         uint16 // slot group count, frozen at create
	D         uint8  // log domain count, 1 until domains are enabled
	Flags     uint8  // bit 0: strict-durability-only database
	CkptSeq   uint64 // seq of the newest checkpoint at last root update
	CkptDD    uint16 // its domain
	Settings  []byte
}

// rootFixed is the payload size before the settings blob and final crc.
const rootFixed = 16 + 8 + 2 + 1 + 1 + 8 + 2 + 4

// AppendRoot appends the encoded root object, header included, to b.
func AppendRoot(b []byte, writer uint64, r Root) []byte {
	b = AppendHeader(b, Header{Format: FormatRoot, FVersion: rootFVersion, Writer: writer})
	p := len(b)
	b = append(b, r.DBID[:]...)
	b = binary.LittleEndian.AppendUint64(b, r.CreatedMS)
	b = binary.LittleEndian.AppendUint16(b, r.G)
	b = append(b, r.D, r.Flags)
	b = binary.LittleEndian.AppendUint64(b, r.CkptSeq)
	b = binary.LittleEndian.AppendUint16(b, r.CkptDD)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Settings)))
	b = append(b, r.Settings...)
	return binary.LittleEndian.AppendUint32(b, crc32c(b[p:]))
}

// ParseRoot reads a root object. Every reject is a clean error: wrong or
// cross-typed header, an fversion this layout does not know, a settings
// length running past the object, a corrupt payload crc.
func ParseRoot(b []byte) (Root, Header, error) {
	h, err := ParseHeaderAs(b, FormatRoot)
	if err != nil {
		return Root{}, Header{}, err
	}
	if h.FVersion != rootFVersion {
		return Root{}, Header{}, fmt.Errorf("obs1: root fversion %d, this build reads %d", h.FVersion, rootFVersion)
	}
	p := b[HeaderSize:]
	if len(p) < rootFixed+4 {
		return Root{}, Header{}, fmt.Errorf("obs1: root payload is %d bytes, want at least %d", len(p), rootFixed+4)
	}
	slen := binary.LittleEndian.Uint32(p[38:42])
	if uint64(len(p)) != uint64(rootFixed)+uint64(slen)+4 {
		return Root{}, Header{}, fmt.Errorf("obs1: root settings length %d does not fill the %d payload bytes", slen, len(p))
	}
	body, crc := p[:len(p)-4], binary.LittleEndian.Uint32(p[len(p)-4:])
	if got := crc32c(body); got != crc {
		return Root{}, Header{}, fmt.Errorf("obs1: root payload crc 0x%08x, computed 0x%08x", crc, got)
	}
	r := Root{
		CreatedMS: binary.LittleEndian.Uint64(p[16:24]),
		G:         binary.LittleEndian.Uint16(p[24:26]),
		D:         p[26],
		Flags:     p[27],
		CkptSeq:   binary.LittleEndian.Uint64(p[28:36]),
		CkptDD:    binary.LittleEndian.Uint16(p[36:38]),
	}
	copy(r.DBID[:], p[0:16])
	if slen > 0 {
		r.Settings = append([]byte(nil), p[rootFixed:rootFixed+int(slen)]...)
	}
	return r, h, nil
}

// seq16 renders a dense sequence number the doc 03 way: 16 zero-padded
// decimal digits, so lexicographic order equals numeric order.
func seq16(n uint64) string {
	return fmt.Sprintf("%016d", n)
}

// dbKey joins the database prefix and a namespace key.
func dbKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "/" + key
}

// rootTag is the self-recognition tag a fallback append carries, so an
// ambiguous CAS-create resolves by Recheck instead of a guess.
func rootTag(writer uint64, seq uint64) WriteTag {
	return WriteTag{Writer: fmt.Sprintf("%016x", writer), Batch: seq16(seq)}
}

// CreateRoot writes the create-time root (writer 0), CAS-created so a
// racing second create loses with ErrPrecondition instead of clobbering.
func CreateRoot(ctx context.Context, s Store, prefix string, fallback bool, r Root) error {
	key := dbKey(prefix, "root")
	if fallback {
		key = dbKey(prefix, "rootv/"+seq16(0))
	}
	_, err := s.PutIfAbsent(ctx, key, AppendRoot(nil, 0, r), rootTag(0, 0))
	return err
}

// LoadRoot reads the newest root a booter can see. In the CAS world that
// is one GET; in the fallback world it walks rootv/ GET-next to the tail
// per C-I6, never a LIST. A database with no root is ErrNotFound.
func LoadRoot(ctx context.Context, s Store, prefix string, fallback bool) (Root, error) {
	if !fallback {
		b, _, err := s.Get(ctx, dbKey(prefix, "root"))
		if err != nil {
			return Root{}, err
		}
		r, _, err := ParseRoot(b)
		return r, err
	}
	var last []byte
	for seq := uint64(0); ; seq++ {
		b, _, err := s.Get(ctx, dbKey(prefix, "rootv/"+seq16(seq)))
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return Root{}, err
		}
		last = b
	}
	if last == nil {
		return Root{}, fmt.Errorf("obs1: no root under %q: %w", prefix, ErrNotFound)
	}
	r, _, err := ParseRoot(last)
	return r, err
}

// AdvanceRoot publishes r after its checkpoint is durable. The doc 03
// protocol: read, and if what is there already knows a checkpoint at
// least as new, stop, someone else got further; otherwise swap and let a
// 412 send us back around. An ambiguous swap re-reads and lets the
// content decide, because whoever's root is newest is correct no matter
// whose PUT landed.
func AdvanceRoot(ctx context.Context, s Store, prefix string, fallback bool, writer uint64, r Root) error {
	if fallback {
		return advanceRootV(ctx, s, prefix, writer, r)
	}
	key := dbKey(prefix, "root")
	for {
		cur, info, err := s.Get(ctx, key)
		if err != nil {
			return err
		}
		have, _, err := ParseRoot(cur)
		if err != nil {
			return err
		}
		if !rootOlder(have, r) {
			return nil
		}
		_, err = s.PutIfMatch(ctx, key, AppendRoot(nil, writer, r), info.ETag, rootTag(writer, r.CkptSeq))
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrPrecondition), errors.Is(err, ErrAmbiguous), errors.Is(err, ErrNotFound):
			// Someone swapped first, the wire cut, or the key vanished
			// under us: re-read and let the ckpt comparison decide.
			continue
		default:
			return err
		}
	}
}

// advanceRootV is the dense-sequence spelling: find the tail, and while
// our checkpoint is newer than the tail's, CAS-create the next slot. A
// lost create means someone else filled it; read what they wrote and
// re-decide. An ambiguous create resolves by Recheck when the store
// offers it, else by the same read-and-re-decide.
func advanceRootV(ctx context.Context, s Store, prefix string, writer uint64, r Root) error {
	seq := uint64(0)
	var have *Root
	for {
		b, _, err := s.Get(ctx, dbKey(prefix, "rootv/"+seq16(seq)))
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return err
		}
		rr, _, err := ParseRoot(b)
		if err != nil {
			return err
		}
		have, seq = &rr, seq+1
	}
	for {
		if have == nil {
			return fmt.Errorf("obs1: no root under %q: %w", prefix, ErrNotFound)
		}
		if !rootOlder(*have, r) {
			return nil
		}
		tag := rootTag(writer, r.CkptSeq)
		_, err := s.PutIfAbsent(ctx, dbKey(prefix, "rootv/"+seq16(seq)), AppendRoot(nil, writer, r), tag)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrAmbiguous) {
			out, body, _, rerr := s.Recheck(ctx, dbKey(prefix, "rootv/"+seq16(seq)), tag)
			if rerr != nil {
				return rerr
			}
			switch out {
			case RecheckOurs:
				return nil
			case RecheckAbsent:
				continue // nothing landed; the same create is safe
			default: // theirs: read what won and re-decide
				rr, _, perr := ParseRoot(body)
				if perr != nil {
					return perr
				}
				have, seq = &rr, seq+1
				continue
			}
		}
		if errors.Is(err, ErrPrecondition) || errors.Is(err, ErrConflict) {
			b, _, gerr := s.Get(ctx, dbKey(prefix, "rootv/"+seq16(seq)))
			if gerr != nil {
				return gerr
			}
			rr, _, perr := ParseRoot(b)
			if perr != nil {
				return perr
			}
			have, seq = &rr, seq+1
			continue
		}
		return err
	}
}

// rootOlder says whether next beats have's knowledge of the chain:
// domain first, then seq, matching the chain's total order.
func rootOlder(have, next Root) bool {
	if have.CkptDD != next.CkptDD {
		return have.CkptDD < next.CkptDD
	}
	return have.CkptSeq < next.CkptSeq
}
