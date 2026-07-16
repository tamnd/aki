// The CAS-fidelity probe (spec 2064/obs1 doc 01 section 12, items 1 and
// 2): before a bucket is trusted, a scripted sequence runs against a
// scratch prefix and records what the provider actually does, not what its
// docs claim. Degradation is per feature (doc 03 section 9): a provider
// without CAS-create cannot host obs1 at all, and one without CAS-replace
// runs with the rootv/ dense-seq fallback.
package obs1

import (
	"context"
	"errors"
	"fmt"
)

// Capabilities is a probe's verdict on one bucket. Notes carry what the
// failing step saw, verbatim, for the doc 03 section 9 matrix row.
type Capabilities struct {
	CASCreate   bool
	CASReplace  bool
	CreateNote  string
	ReplaceNote string
}

// CanHost says whether the provider can hold an obs1 bucket at all. The
// commit chain is CAS-create and nothing substitutes for it; a create
// surface that lies is refused, not worked around.
func (p Capabilities) CanHost() bool { return p.CASCreate }

// RootVFallback says in-place root swaps must give way to the rootv/
// dense-seq pointer chain, the doc 03 section 9 CAS-replace degradation.
func (p Capabilities) RootVFallback() bool { return p.CASCreate && !p.CASReplace }

// ProbeCAS runs the scripted sequence against a scratch prefix: create on
// a missing key, duplicate create, swap with the live token, swap with a
// stale one. Each primitive is probed independently, so a broken create
// (the MinIO pre-2025-09-07 shape) still yields a replace verdict. A
// definite wrong answer flips the capability off; an ambiguous or
// cancelled exchange returns an error instead, because a network that is
// down proves nothing about the provider.
func ProbeCAS(ctx context.Context, s Store, prefix string) (Capabilities, error) {
	key := prefix + "/casprobe"
	caps := Capabilities{CASCreate: true, CASReplace: true}

	// A leftover from a crashed probe would fail the create step for the
	// wrong reason; the trailing delete keeps the prefix clean for the next
	// probe the same way.
	if err := s.Delete(ctx, key); err != nil {
		return Capabilities{}, fmt.Errorf("obs1: probe scratch cleanup: %w", err)
	}
	defer func() { _ = s.Delete(ctx, key) }()

	switch _, err := s.PutIfAbsent(ctx, key, []byte("probe-create"), WriteTag{Writer: "obs1-probe", Batch: "create"}); {
	case err == nil:
	case inconclusive(err):
		return Capabilities{}, err
	default:
		caps.CASCreate = false
		caps.CreateNote = "create on a missing key failed: " + err.Error()
	}
	if caps.CASCreate {
		switch _, err := s.PutIfAbsent(ctx, key, []byte("probe-dup"), WriteTag{Writer: "obs1-probe", Batch: "dup"}); {
		case errors.Is(err, ErrPrecondition), errors.Is(err, ErrConflict):
		case err == nil:
			caps.CASCreate = false
			caps.CreateNote = "duplicate create overwrote silently"
		case inconclusive(err):
			return Capabilities{}, err
		default:
			caps.CASCreate = false
			caps.CreateNote = "duplicate create failed unexpectedly: " + err.Error()
		}
	}

	// The replace half starts from a plain Put so it has a live token even
	// when create is broken.
	info, err := s.Put(ctx, key, []byte("probe-base"))
	if err != nil {
		return Capabilities{}, fmt.Errorf("obs1: probe base put: %w", err)
	}
	live := info.ETag
	switch _, err := s.PutIfMatch(ctx, key, []byte("probe-swap"), live, WriteTag{Writer: "obs1-probe", Batch: "swap"}); {
	case err == nil:
	case inconclusive(err):
		return Capabilities{}, err
	default:
		caps.CASReplace = false
		caps.ReplaceNote = "swap with the live token failed: " + err.Error()
	}
	if caps.CASReplace {
		// The successful swap just moved the token, so live is stale now.
		switch _, err := s.PutIfMatch(ctx, key, []byte("probe-stale"), live, WriteTag{Writer: "obs1-probe", Batch: "stale"}); {
		case errors.Is(err, ErrPrecondition), errors.Is(err, ErrConflict):
		case err == nil:
			caps.CASReplace = false
			caps.ReplaceNote = "swap with a stale token succeeded"
		case inconclusive(err):
			return Capabilities{}, err
		default:
			caps.CASReplace = false
			caps.ReplaceNote = "stale swap failed unexpectedly: " + err.Error()
		}
	}
	return caps, nil
}

// inconclusive marks outcomes that say nothing about the provider: a cut
// wire on a create, or the caller giving up.
func inconclusive(err error) bool {
	return errors.Is(err, ErrAmbiguous) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
