// Provider dialects (spec 2064/obs1 doc 03 section 9). obs1 needs exactly
// two conditional primitives, CAS-create and CAS-replace, and providers
// differ only in which headers spell them and which response header carries
// the CAS token. That difference is data, not code paths.
//
// The S3 dialect covers AWS, S3 Express, R2, and MinIO. The GCS dialect
// rides the same wire client: the GCS XML API accepts SigV4 with HMAC
// interop keys, swapping only the x-goog condition headers and the
// generation token. Azure Blob is a different REST surface entirely
// (SharedKey auth, x-ms headers) and needs a native client before its row
// in the doc 03 matrix can move past recorded.
package obs1

import "net/http"

// Dialect names a provider family's spelling of the two CAS primitives.
type Dialect struct {
	// Name appears in probe reports and errors.
	Name string
	// Create returns the headers for write-if-absent.
	Create func() map[string]string
	// Replace returns the headers for write-if-token-matches.
	Replace func(token string) map[string]string
	// Token extracts the CAS token a successful write or read reveals:
	// the ETag on S3, the generation on GCS. Tokens are opaque to every
	// caller and never used for integrity (doc 03 section 9).
	Token func(h http.Header) string
}

// DialectS3 is the reference dialect: AWS S3, S3 Express, R2, MinIO.
var DialectS3 = Dialect{
	Name:    "s3",
	Create:  func() map[string]string { return map[string]string{"If-None-Match": "*"} },
	Replace: func(token string) map[string]string { return map[string]string{"If-Match": token} },
	Token:   func(h http.Header) string { return h.Get("ETag") },
}

// DialectGCS is the GCS XML API over SigV4 interop keys. Generation 0
// means "no live generation", which is exactly write-if-absent, and a
// generation match is strictly stronger than an ETag match (doc 03
// section 9: generations never repeat, ETags can).
var DialectGCS = Dialect{
	Name:    "gcs",
	Create:  func() map[string]string { return map[string]string{"x-goog-if-generation-match": "0"} },
	Replace: func(token string) map[string]string { return map[string]string{"x-goog-if-generation-match": token} },
	Token:   func(h http.Header) string { return h.Get("x-goog-generation") },
}
