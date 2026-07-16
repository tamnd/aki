// Ranged reads and batch deletes (spec 2064/obs1 doc 05 section 1 rung 5,
// doc 06 section 5). Ranges are planned from manifests and footers, so the
// caller always knows offset and length; the tail form covers the one case
// it does not, reading a fixed-size footer off a segment it has not opened.
package obs1

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"context"
)

// GetRange fetches n bytes starting at off. A range starting at or past
// the end of the object is ErrRange; a range running past the end returns
// the bytes that exist, matching S3. Info reports the size of the returned
// slice, not the whole object.
func (c *Client) GetRange(ctx context.Context, key string, off, n int64) ([]byte, ObjectInfo, error) {
	if off < 0 || n <= 0 {
		return nil, ObjectInfo{}, fmt.Errorf("obs1: bad range %d+%d for %s", off, n, key)
	}
	return c.getRange(ctx, key, fmt.Sprintf("bytes=%d-%d", off, off+n-1))
}

// GetTail fetches the last n bytes of an object (bytes=-n), the footer
// read a taking-over node issues before it knows a segment's size.
func (c *Client) GetTail(ctx context.Context, key string, n int64) ([]byte, ObjectInfo, error) {
	if n <= 0 {
		return nil, ObjectInfo{}, fmt.Errorf("obs1: bad tail %d for %s", n, key)
	}
	return c.getRange(ctx, key, fmt.Sprintf("bytes=-%d", n))
}

func (c *Client) getRange(ctx context.Context, key, rng string) ([]byte, ObjectInfo, error) {
	var body []byte
	var info ObjectInfo
	err := c.do(ctx, s3req{
		method: http.MethodGet,
		key:    key,
		extra:  map[string]string{"Range": rng},
		replay: true,
	}, func(resp *http.Response) error {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return &transportError{err}
		}
		body = b
		info = objectInfo(resp, int64(len(b)))
		return nil
	})
	return body, info, err
}

// deleteBatchMax is the DeleteObjects request ceiling, fixed by S3.
const deleteBatchMax = 1000

// deleteReq and deleteResult are the DeleteObjects document shapes. Quiet
// mode makes the response list errors only.
type deleteReq struct {
	XMLName xml.Name    `xml:"Delete"`
	Quiet   bool        `xml:"Quiet"`
	Objects []deleteKey `xml:"Object"`
}

type deleteKey struct {
	Key string `xml:"Key"`
}

type deleteResult struct {
	Errors []struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

// DeleteObjects removes up to 1000 keys in one request, the sweeper's unit
// of work (doc 06 section 5: deletion requests are free, so the batch size
// is the only knob). Missing keys succeed, matching S3. Per-key failures
// come back as one error naming every key that survived; the caller re-runs
// the batch, since deletes are idempotent.
func (c *Client) DeleteObjects(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) > deleteBatchMax {
		return fmt.Errorf("obs1: DeleteObjects called with %d keys, max %d", len(keys), deleteBatchMax)
	}
	dr := deleteReq{Quiet: true}
	for _, k := range keys {
		dr.Objects = append(dr.Objects, deleteKey{Key: k})
	}
	body, err := xml.Marshal(dr)
	if err != nil {
		return err
	}
	sum := md5.Sum(body)

	var res deleteResult
	err = c.do(ctx, s3req{
		method: http.MethodPost,
		query:  url.Values{"delete": {""}},
		body:   body,
		// Content-MD5 is mandatory on this API; the payload SHA in the
		// signature does not replace it.
		extra:  map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(sum[:])},
		replay: true, // deletes are idempotent, a replayed batch converges
	}, func(resp *http.Response) error {
		rb, err := io.ReadAll(resp.Body)
		if err != nil {
			return &transportError{err}
		}
		res = deleteResult{}
		return xml.Unmarshal(rb, &res)
	})
	if err != nil {
		return err
	}
	if len(res.Errors) > 0 {
		var sb strings.Builder
		for i, e := range res.Errors {
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "%s (%s)", e.Key, e.Code)
		}
		return fmt.Errorf("obs1: DeleteObjects left %d of %d keys: %s", len(res.Errors), len(keys), sb.String())
	}
	return nil
}
