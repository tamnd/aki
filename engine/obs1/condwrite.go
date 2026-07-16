// Conditional writes (spec 2064/obs1 doc 02 section 2.4): If-None-Match
// create, If-Match swap, and the self-recognition pass that turns an
// ambiguous PUT outcome into a definite one. Any CAS caller (the commit
// chain first, checkpoints and manifests later) rides these.
package obs1

import (
	"context"
	"errors"
	"net/http"
)

// WriteTag identifies one writer's one attempt. It rides as object
// metadata so a re-read can tell "our PUT landed" from "someone beat us"
// after a cut wire. A zero tag travels as no headers and never
// self-recognizes.
type WriteTag struct {
	Writer string // stable per lease holder or process
	Batch  string // unique per attempt: a seq, a ulid, whatever the caller keys on
}

const (
	writerHeader = "x-amz-meta-obs1-writer"
	batchHeader  = "x-amz-meta-obs1-batch"
)

func (t WriteTag) headers(h map[string]string) map[string]string {
	if t != (WriteTag{}) {
		h[writerHeader] = t.Writer
		h[batchHeader] = t.Batch
	}
	return h
}

// PutIfAbsent creates key only if nothing is there (If-None-Match: * in
// the S3 dialect). An object already present is ErrPrecondition; a
// concurrent conditional write still settling is ErrConflict, and the
// caller re-reads instead of replaying; a cut wire is ErrAmbiguous,
// resolved with Recheck.
func (c *Client) PutIfAbsent(ctx context.Context, key string, body []byte, tag WriteTag) (ObjectInfo, error) {
	h := tag.headers(c.dialect.Create())
	return c.put(ctx, key, body, h)
}

// PutIfMatch replaces key only while its CAS token still matches, the swap
// half of CAS. Pass the token exactly as a read or write returned it in
// Info.ETag, quotes and all. A lost race is ErrPrecondition.
func (c *Client) PutIfMatch(ctx context.Context, key string, body []byte, token string, tag WriteTag) (ObjectInfo, error) {
	h := tag.headers(c.dialect.Replace(token))
	return c.put(ctx, key, body, h)
}

func (c *Client) put(ctx context.Context, key string, body []byte, h map[string]string) (ObjectInfo, error) {
	var info ObjectInfo
	err := c.do(ctx, s3req{method: http.MethodPut, key: key, body: body, extra: h}, func(resp *http.Response) error {
		info = c.objectInfo(resp, int64(len(body)))
		return nil
	})
	return info, err
}

// RecheckOutcome is Recheck's verdict on an ambiguous PUT.
type RecheckOutcome int

const (
	// RecheckOurs: our PUT landed; the write is done.
	RecheckOurs RecheckOutcome = iota + 1
	// RecheckOther: an object is there and it is not ours. For a create
	// the caller applies it and retries at the next seq; for a swap the
	// returned ETag says whether it is the old object or a rival's new one.
	RecheckOther
	// RecheckAbsent: nothing landed; the same PUT is safe to send again.
	RecheckAbsent
)

// Recheck resolves an ErrAmbiguous by re-reading key and comparing write
// tags (doc 02 section 2.4, the 5xx/timeout path). The mandatory rule it
// encodes: a timed-out PUT may have succeeded, and blindly retrying would
// 412 against our own object. RecheckOther returns the body so a chain
// caller can apply the records it lost to.
func (c *Client) Recheck(ctx context.Context, key string, tag WriteTag) (RecheckOutcome, []byte, ObjectInfo, error) {
	if tag == (WriteTag{}) {
		return 0, nil, ObjectInfo{}, errors.New("obs1: Recheck needs a non-zero tag")
	}
	body, info, err := c.Get(ctx, key)
	switch {
	case errors.Is(err, ErrNotFound):
		return RecheckAbsent, nil, ObjectInfo{}, nil
	case err != nil:
		return 0, nil, ObjectInfo{}, err
	case info.Tag == tag:
		return RecheckOurs, body, info, nil
	}
	return RecheckOther, body, info, nil
}
