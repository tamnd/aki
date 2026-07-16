// The object-store client core (spec 2064/obs1 doc 11 section 1): GET, PUT,
// DELETE against one bucket, hand rolled on net/http. Conditional writes,
// ranged reads, batching, and the provider seam land in the next O0a slices;
// this file is the request loop everything else rides on.
package obs1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClientConfig wires a Client. Endpoint is a base URL like
// https://s3.us-east-1.amazonaws.com or http://127.0.0.1:9000; PathStyle
// puts the bucket in the path instead of the host, which is what MinIO and
// most emulators speak.
type ClientConfig struct {
	Endpoint     string
	Region       string
	Bucket       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	PathStyle    bool

	// Retry defaults to DefaultRetry; AttemptTimeout bounds one HTTP
	// exchange, not the whole retry loop, and defaults to 15s.
	Retry          RetryPolicy
	AttemptTimeout time.Duration

	// HTTPClient is shared across Clients in tests and labs; nil gets a
	// dedicated one with sane pool limits for a node's fan-out.
	HTTPClient *http.Client

	// Dialect spells the provider's CAS headers and token; the zero value
	// means DialectS3.
	Dialect Dialect
}

// Client talks to one bucket. It is safe for concurrent use; all state is
// read-only after NewClient.
type Client struct {
	base           url.URL // scheme and host, bucket already applied for path style
	pathPrefix     string  // "/bucket" for path style, "" for vhost style
	region         string
	creds          credentials
	retry          RetryPolicy
	attemptTimeout time.Duration
	http           *http.Client
	dialect        Dialect
	now            func() time.Time // test hook for deterministic signing
}

// ObjectInfo is what a read reveals about the object it hit. ETag is the
// provider's CAS token in the dialect's spelling (the ETag on S3, the
// generation on GCS), opaque and never an integrity check. Tag is the
// writer's self-recognition mark if the write carried one.
type ObjectInfo struct {
	ETag string
	Size int64
	Tag  WriteTag
}

// objectInfo lifts the common response fields.
func (c *Client) objectInfo(resp *http.Response, size int64) ObjectInfo {
	return ObjectInfo{
		ETag: c.dialect.Token(resp.Header),
		Size: size,
		Tag: WriteTag{
			Writer: resp.Header.Get(writerHeader),
			Batch:  resp.Header.Get(batchHeader),
		},
	}
}

// NewClient validates cfg and builds a Client.
func NewClient(cfg ClientConfig) (*Client, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("obs1: bad endpoint %q", cfg.Endpoint)
	}
	if cfg.Bucket == "" || cfg.Region == "" {
		return nil, errors.New("obs1: bucket and region are required")
	}
	c := &Client{
		base:           *u,
		region:         cfg.Region,
		creds:          credentials{cfg.AccessKey, cfg.SecretKey, cfg.SessionToken},
		retry:          cfg.Retry,
		attemptTimeout: cfg.AttemptTimeout,
		http:           cfg.HTTPClient,
		dialect:        cfg.Dialect,
		now:            time.Now,
	}
	if c.dialect.Name == "" {
		c.dialect = DialectS3
	}
	if cfg.PathStyle {
		c.pathPrefix = "/" + cfg.Bucket
	} else {
		c.base.Host = cfg.Bucket + "." + u.Host
	}
	if c.retry == (RetryPolicy{}) {
		c.retry = DefaultRetry
	}
	if c.attemptTimeout == 0 {
		c.attemptTimeout = 15 * time.Second
	}
	if c.http == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		// The connpool lab (labs/obs1/o0a/01_connpool) settled this: reuse
		// holds either way, but 256 keeps fresh TLS handshakes out of the
		// hot path at 512-way fan-out and cuts re-dials 2 to 3x.
		t.MaxIdleConnsPerHost = 256
		c.http = &http.Client{Transport: t}
	}
	return c, nil
}

// urlFor builds the request URL for a key. Keys are raw strings; encoding
// happens exactly once, here, so the signer and the wire always agree.
func (c *Client) urlFor(key string) *url.URL {
	u := c.base
	u.Path = c.pathPrefix + "/" + key
	u.RawPath = c.pathPrefix + "/" + escapeKey(key)
	return &u
}

// escapeKey percent-encodes a key for the URI path, keeping '/' literal:
// each segment gets the same RFC 3986 encoding the signature uses.
func escapeKey(key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = escapeV4(s)
	}
	return strings.Join(segs, "/")
}

// Get fetches a whole object. A missing key is ErrNotFound.
func (c *Client) Get(ctx context.Context, key string) ([]byte, ObjectInfo, error) {
	var body []byte
	var info ObjectInfo
	err := c.do(ctx, s3req{method: http.MethodGet, key: key, replay: true}, func(resp *http.Response) error {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return &transportError{err} // a cut mid-body is retryable on a read
		}
		body = b
		info = c.objectInfo(resp, int64(len(b)))
		return nil
	})
	return body, info, err
}

// Put writes a whole object, unconditionally (last writer wins). The
// conditional variants live in condwrite.go. An ambiguous outcome is
// returned as ErrAmbiguous, never retried blindly: the caller knows how to
// re-read and self-recognize (doc 02 section 2.4).
func (c *Client) Put(ctx context.Context, key string, body []byte) (ObjectInfo, error) {
	return c.put(ctx, key, body, nil)
}

// Delete removes a key. Deleting a missing key succeeds, matching S3.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.do(ctx, s3req{method: http.MethodDelete, key: key, replay: true}, func(*http.Response) error { return nil })
}

// transportError marks an attempt that died on the wire, so the loop can
// tell it from a classified store response.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// s3req is one operation for the retry loop. replay says a cut wire may
// be blindly retried: true for reads, deletes, and idempotent part
// uploads; false for anything that creates an object whose outcome the
// caller must self-recognize (doc 02 section 2.4).
type s3req struct {
	method string
	key    string
	query  url.Values
	body   []byte
	extra  map[string]string
	replay bool
}

// do runs one operation through the retry loop. onOK consumes a 2xx
// response while its body is still open.
func (c *Client) do(ctx context.Context, r s3req, onOK func(*http.Response) error) error {
	var lastErr error
	for attempt := 0; attempt < c.retry.Attempts; attempt++ {
		if attempt > 0 {
			slow := errors.Is(lastErr, ErrSlowDown)
			if err := sleep(ctx, c.retry.backoff(attempt, slow)); err != nil {
				return err
			}
		}
		lastErr = c.attempt(ctx, r, onOK)
		switch {
		case lastErr == nil:
			return nil
		case isTransport(lastErr):
			if !r.replay {
				// The request may have taken effect; only the caller can
				// re-read and decide, so surface it instead of replaying.
				return fmt.Errorf("%w: %s %s: %w", ErrAmbiguous, r.method, r.key, lastErr)
			}
			continue
		case retryable(lastErr):
			continue
		default:
			return lastErr
		}
	}
	return fmt.Errorf("obs1: %s %s: attempts exhausted: %w", r.method, r.key, lastErr)
}

// attempt is one signed HTTP exchange.
func (c *Client) attempt(ctx context.Context, r s3req, onOK func(*http.Response) error) error {
	actx, cancel := context.WithTimeout(ctx, c.attemptTimeout)
	defer cancel()

	payloadHash := emptySHA256
	var reader io.Reader
	if r.body != nil {
		sum := sha256.Sum256(r.body)
		payloadHash = hex.EncodeToString(sum[:])
		reader = bytes.NewReader(r.body)
	}
	u := c.urlFor(r.key)
	u.RawQuery = r.query.Encode()
	req, err := http.NewRequestWithContext(actx, r.method, u.String(), reader)
	if err != nil {
		return err
	}
	if r.body != nil {
		req.ContentLength = int64(len(r.body))
	}
	for k, v := range r.extra {
		req.Header.Set(k, v)
	}
	signV4(req, c.creds, c.region, "s3", payloadHash, c.now())

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // the caller gave up; not a wire verdict
		}
		return &transportError{err}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return onOK(resp)
	}
	eb, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	return storeErr(resp.StatusCode, eb, resp.Header.Get("x-amz-request-id"))
}

func isTransport(err error) bool {
	var te *transportError
	return errors.As(err, &te)
}
