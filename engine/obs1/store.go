// Store is the one small seam from spec 2064/obs1 doc 11 section 1: every
// consumer above the client (commit chain, folders, sweeper, checkpoints)
// talks to this interface, and the O0a simulator implements it too, which
// is what makes the differential suite and every later measured number
// possible.
package obs1

import "context"

// Store is the object-store surface obs1 is built on. The Client is the
// wire implementation; the simulator is the other one. Semantics are the
// Client's documented ones: typed sentinel errors, ErrAmbiguous never
// blindly replayed, tokens opaque.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, ObjectInfo, error)
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, ObjectInfo, error)
	GetTail(ctx context.Context, key string, n int64) ([]byte, ObjectInfo, error)

	Put(ctx context.Context, key string, body []byte) (ObjectInfo, error)
	PutIfAbsent(ctx context.Context, key string, body []byte, tag WriteTag) (ObjectInfo, error)
	PutIfMatch(ctx context.Context, key string, body []byte, token string, tag WriteTag) (ObjectInfo, error)
	Recheck(ctx context.Context, key string, tag WriteTag) (RecheckOutcome, []byte, ObjectInfo, error)

	Delete(ctx context.Context, key string) error
	DeleteObjects(ctx context.Context, keys []string) error

	CreateMultipart(ctx context.Context, key string, tag WriteTag) (string, error)
	UploadPart(ctx context.Context, key, uploadID string, n int, body []byte) (string, error)
	CompleteMultipart(ctx context.Context, key, uploadID string, parts []Part) (ObjectInfo, error)
	AbortMultipart(ctx context.Context, key, uploadID string) error
}

var _ Store = (*Client)(nil)
