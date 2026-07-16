// The error taxonomy (spec 2064/obs1 doc 01 section 2.1, doc 02 section
// 2.4): every client caller branches on exactly these conditions, so they
// are types here, not status codes leaking through the engine.
package obs1

import (
	"encoding/xml"
	"errors"
	"fmt"
)

// Sentinel conditions, tested with errors.Is. Each names one branch of the
// append protocol or the read path; anything else surfaces as a *StoreErr
// with its raw code.
var (
	// ErrNotFound: the key does not exist (404). A chain reader treats this
	// as "the tail so far"; a CAS retrier treats it as "slot still free".
	ErrNotFound = errors.New("obs1: object not found")

	// ErrPrecondition: the conditional header lost (412). The definitive
	// "someone else already wrote seq N" signal; never ambiguous.
	ErrPrecondition = errors.New("obs1: precondition failed")

	// ErrConflict: 409 ConditionalRequestConflict, a concurrent conditional
	// write on the same key was in flight. Distinct from 412 because the
	// request may or may not have taken effect; recovery is re-read then
	// re-decide, the same path as an ambiguous timeout.
	ErrConflict = errors.New("obs1: conditional request conflict")

	// ErrSlowDown: the service asked for backpressure (503 SlowDown). The
	// retry policy stretches its backoff for this one instead of hammering.
	ErrSlowDown = errors.New("obs1: slow down")

	// ErrAmbiguous: the request left this process and no definitive answer
	// came back (timeout, connection cut mid-response). For a mutation the
	// outcome is unknown and the caller must re-read; doc 02 section 2.4
	// makes that re-check mandatory for chain appends.
	ErrAmbiguous = errors.New("obs1: outcome unknown")
)

// StoreErr is a definitive error response from the object store, wrapping
// whichever sentinel matches so errors.Is keeps working while the raw code,
// status, and request id stay available for logs.
type StoreErr struct {
	Status    int
	Code      string // the <Code> field of the XML error body, if any
	Message   string
	RequestID string
	sentinel  error // nil when no sentinel condition applies
}

func (e *StoreErr) Error() string {
	return fmt.Sprintf("obs1: store error: http %d code=%q %s (request id %s)",
		e.Status, e.Code, e.Message, e.RequestID)
}

func (e *StoreErr) Unwrap() error { return e.sentinel }

// xmlError is the S3 error document shape.
type xmlError struct {
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	RequestID string `xml:"RequestId"`
}

// storeErr classifies a non-2xx response into the taxonomy. body may be
// empty (HEAD, or a provider that sends none).
func storeErr(status int, body []byte, requestID string) *StoreErr {
	var doc xmlError
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &doc) // an unparsable body just leaves Code empty
	}
	if doc.RequestID != "" {
		requestID = doc.RequestID
	}
	e := &StoreErr{Status: status, Code: doc.Code, Message: doc.Message, RequestID: requestID}
	switch {
	case status == 404:
		e.sentinel = ErrNotFound
	case status == 412:
		e.sentinel = ErrPrecondition
	case status == 409 && doc.Code == "ConditionalRequestConflict":
		e.sentinel = ErrConflict
	case doc.Code == "SlowDown" || doc.Code == "RequestLimitExceeded":
		e.sentinel = ErrSlowDown
	}
	return e
}

// retryable reports whether the retry loop may replay the request as-is: the
// throttle and 5xx family, plus 400 RequestTimeout (the S3 idle-upload cut,
// a fresh attempt is well defined). The CAS-visible conditions (404, 412,
// 409) are never retried here; their loops live in the callers, which is the
// doc 02 append protocol.
func retryable(err error) bool {
	if errors.Is(err, ErrSlowDown) {
		return true
	}
	var se *StoreErr
	if errors.As(err, &se) {
		if se.Status >= 500 {
			return se.sentinel == nil || errors.Is(se, ErrSlowDown)
		}
		return se.Status == 400 && se.Code == "RequestTimeout"
	}
	return false
}
