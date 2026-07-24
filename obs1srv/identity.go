// Node identity (spec 2064/obs1 doc 02 section 4.1): a random u64 minted
// at first boot and persisted in the cache directory when there is one,
// with an incarnation bumped once per process start. A node with no cache
// directory mints a fresh id per process, which is correct: with nothing
// persisted it cannot claim continuity with any earlier life. This lives
// in the serving layer because engine/obs1 stays off the disk APIs (W-I4)
// and the cache directory is the server's to own.
package obs1srv

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tamnd/aki/engine/obs1"
)

// NodeIdentity is who a process is on the chain: the writer id its
// batches carry and the incarnation its member row and batches declare.
type NodeIdentity struct {
	Node        uint64
	Incarnation uint32
}

// identityFile is the cache-dir file name; the format is one line,
// self-labeling, so a human can read it and a foreign file rejects.
const identityFile = "node-id"

func mintNodeID() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("obs1srv: minting a node id: %w", err)
		}
		if id := binary.LittleEndian.Uint64(b[:]); id != 0 {
			return id, nil
		}
	}
}

// LoadNodeIdentity returns this process's identity. With a cache dir the
// id persists across restarts and the incarnation bumps by one per call;
// the write goes through a temp file and rename so a crash mid-update
// leaves the old line intact. With dir empty the identity is per-process:
// a fresh random id at incarnation 1.
func LoadNodeIdentity(dir string) (NodeIdentity, error) {
	if dir == "" {
		id, err := mintNodeID()
		if err != nil {
			return NodeIdentity{}, err
		}
		return NodeIdentity{Node: id, Incarnation: 1}, nil
	}
	path := filepath.Join(dir, identityFile)
	var cur NodeIdentity
	switch b, err := os.ReadFile(path); {
	case err == nil:
		var node uint64
		var inc uint32
		if _, serr := fmt.Sscanf(strings.TrimSpace(string(b)), "obs1-node %016x %d", &node, &inc); serr != nil || node == 0 {
			return NodeIdentity{}, fmt.Errorf("obs1srv: %s is not a node identity file", path)
		}
		cur = NodeIdentity{Node: node, Incarnation: inc}
	case os.IsNotExist(err):
		id, merr := mintNodeID()
		if merr != nil {
			return NodeIdentity{}, merr
		}
		cur = NodeIdentity{Node: id, Incarnation: 0}
	default:
		return NodeIdentity{}, err
	}
	cur.Incarnation++
	tmp, err := os.CreateTemp(dir, identityFile+".*")
	if err != nil {
		return NodeIdentity{}, err
	}
	line := fmt.Sprintf("obs1-node %016x %d\n", cur.Node, cur.Incarnation)
	if _, err := tmp.WriteString(line); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return NodeIdentity{}, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return NodeIdentity{}, err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		_ = os.Remove(tmp.Name())
		return NodeIdentity{}, err
	}
	return cur, nil
}

// JoinRecord builds the member(join) this identity appends at boot.
func (id NodeIdentity) JoinRecord(resp, mesh string, weight uint16, version string) obs1.MemberRecord {
	return obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{
		Node: id.Node, Incarnation: id.Incarnation,
		Resp: resp, Mesh: mesh, Weight: weight, Version: version,
	}}
}

// LeaveRecord builds the member(leave) a graceful shutdown appends.
func (id NodeIdentity) LeaveRecord() obs1.MemberRecord {
	return obs1.MemberRecord{Op: obs1.MemberLeave, Member: obs1.Member{Node: id.Node, Incarnation: id.Incarnation}}
}
