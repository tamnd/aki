// Multipart upload (spec 2064/obs1 doc 03 section 5): segments stay under
// the single-PUT limit by construction, so multipart is a throughput
// option for the folder above 64 MiB, never a requirement. The retry
// grammar per call: create and abort replay (an orphaned upload id is the
// GC sweeper's problem, doc 06), a part replays (re-uploading part N of an
// upload id is idempotent), complete does not (it creates the visible
// object, the same self-recognition rule as any PUT).
package obs1

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// MinPartSize is S3's floor for every part but the last.
const MinPartSize = 5 << 20

// Part names one uploaded part for CompleteMultipart. ETag is returned by
// UploadPart and passed back verbatim.
type Part struct {
	N    int
	ETag string
}

type initiateResult struct {
	UploadID string `xml:"UploadId"`
}

// CreateMultipart starts an upload and returns its id. The write tag rides
// as metadata on the final object, so Recheck works after an ambiguous
// complete just as it does after an ambiguous PUT.
func (c *Client) CreateMultipart(ctx context.Context, key string, tag WriteTag) (string, error) {
	var res initiateResult
	err := c.do(ctx, s3req{
		method: http.MethodPost,
		key:    key,
		query:  url.Values{"uploads": {""}},
		extra:  tag.headers(map[string]string{}),
		replay: true, // worst case an orphan upload id, swept by GC
	}, func(resp *http.Response) error {
		rb, err := io.ReadAll(resp.Body)
		if err != nil {
			return &transportError{err}
		}
		res = initiateResult{}
		return xml.Unmarshal(rb, &res)
	})
	if err != nil {
		return "", err
	}
	if res.UploadID == "" {
		return "", fmt.Errorf("obs1: create multipart %s: empty upload id", key)
	}
	return res.UploadID, nil
}

// UploadPart sends part n (1-based) and returns its ETag for the manifest
// handed to CompleteMultipart.
func (c *Client) UploadPart(ctx context.Context, key, uploadID string, n int, body []byte) (string, error) {
	var etag string
	err := c.do(ctx, s3req{
		method: http.MethodPut,
		key:    key,
		query:  url.Values{"partNumber": {strconv.Itoa(n)}, "uploadId": {uploadID}},
		body:   body,
		replay: true, // resending (uploadID, n) overwrites the same part
	}, func(resp *http.Response) error {
		etag = resp.Header.Get("ETag")
		return nil
	})
	return etag, err
}

type completeReq struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeResult struct {
	XMLName xml.Name
	ETag    string `xml:"ETag"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// CompleteMultipart stitches the parts into the visible object. S3 may
// answer 200 and then stream an error document instead of a result (the
// long-poll shape), so the body decides, not the status: an InternalError
// there is retried like a 500, anything else surfaces classified. A cut
// wire is ErrAmbiguous and the caller runs Recheck, same as a PUT.
func (c *Client) CompleteMultipart(ctx context.Context, key, uploadID string, parts []Part) (ObjectInfo, error) {
	cr := completeReq{}
	for _, p := range parts {
		cr.Parts = append(cr.Parts, completePart{PartNumber: p.N, ETag: p.ETag})
	}
	body, err := xml.Marshal(cr)
	if err != nil {
		return ObjectInfo{}, err
	}
	var info ObjectInfo
	err = c.do(ctx, s3req{
		method: http.MethodPost,
		key:    key,
		query:  url.Values{"uploadId": {uploadID}},
		body:   body,
	}, func(resp *http.Response) error {
		rb, err := io.ReadAll(resp.Body)
		if err != nil {
			return &transportError{err} // cut mid-body: outcome unknown
		}
		var res completeResult
		if err := xml.Unmarshal(rb, &res); err != nil {
			return err
		}
		if res.XMLName.Local == "Error" {
			return storeErr(http.StatusInternalServerError, rb, resp.Header.Get("x-amz-request-id"))
		}
		info = c.objectInfo(resp, 0)
		if res.ETag != "" {
			info.ETag = res.ETag
		}
		return nil
	})
	return info, err
}

// AbortMultipart drops an upload and its parts. Abort means ensure-gone,
// so an unknown or already-gone upload id succeeds: providers disagree on
// the wire (AWS answers 404 NoSuchUpload, MinIO answers 204) and the
// sweeper only needs gone, so the client folds the 404 to nil here.
func (c *Client) AbortMultipart(ctx context.Context, key, uploadID string) error {
	err := c.do(ctx, s3req{
		method: http.MethodDelete,
		key:    key,
		query:  url.Values{"uploadId": {uploadID}},
		replay: true,
	}, func(*http.Response) error { return nil })
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}
