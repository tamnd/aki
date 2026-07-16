// SigV4 request signing (spec 2064/obs1 doc 11 section 1): the client is
// hand rolled on net/http, so the signing algorithm lives here in a page.
// Header-based signing only; presigned URLs are not an obs1 surface.
package obs1

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// credentials is the static key pair a node runs with. Rotation happens by
// process restart or a caller-side swap; the client never refreshes on its
// own (doc 11 keeps credential plumbing out of scope).
type credentials struct {
	accessKey    string
	secretKey    string
	sessionToken string // set when the deployment hands out STS creds
}

// emptySHA256 is the hash of a zero-byte payload, used by GET and DELETE.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// signV4 signs req in place for the given service scope. payloadHash is the
// lowercase hex SHA-256 of the request body (emptySHA256 for none). The
// x-amz-date, x-amz-content-sha256, host, and (when present) token headers
// are the signed set; extra x-amz-* headers already on the request are
// signed too, which is how the conditional-write headers ride along later.
func signV4(req *http.Request, creds credentials, region, service string, payloadHash string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	scopeDate := amzDate[:8]

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if creds.sessionToken != "" {
		req.Header.Set("x-amz-security-token", creds.sessionToken)
	}

	// Canonical headers: host plus everything set on the request at signing
	// time, lowercase names, trimmed values, sorted by name. Signing the
	// whole set (not just x-amz-*) matches the AWS documented examples and
	// covers Range, Content-Type, and the conditional headers by default;
	// what the transport adds later (User-Agent, Accept-Encoding) is never
	// in the map here, so it rides unsigned, which is allowed.
	names := []string{"host"}
	for name := range req.Header {
		if n := strings.ToLower(name); n != "authorization" {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, n := range names {
		v := req.Host
		if n != "host" {
			v = strings.Join(req.Header.Values(http.CanonicalHeaderKey(n)), ",")
			v = strings.TrimSpace(v)
		}
		canonHeaders.WriteString(n)
		canonHeaders.WriteByte(':')
		canonHeaders.WriteString(v)
		canonHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")

	canonical := req.Method + "\n" +
		canonicalURI(req.URL) + "\n" +
		canonicalQuery(req.URL) + "\n" +
		canonHeaders.String() + "\n" +
		signedHeaders + "\n" +
		payloadHash

	scope := scopeDate + "/" + region + "/" + service + "/aws4_request"
	sum := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])

	k := hmacSHA256([]byte("AWS4"+creds.secretKey), scopeDate)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	k = hmacSHA256(k, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(k, toSign))

	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+creds.accessKey+"/"+scope+
			", SignedHeaders="+signedHeaders+
			", Signature="+signature)
}

// canonicalURI is the URI-encoded path with '/' kept literal. S3 wants the
// path encoded exactly once here, and the client always builds request URLs
// from raw key strings, so encoding the raw path segment by segment is the
// single encode.
func canonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	return path
}

// canonicalQuery sorts parameters by name then value, both RFC 3986 encoded
// with space as %20.
func canonicalQuery(u *url.URL) string {
	if u.RawQuery == "" {
		return ""
	}
	values, _ := url.ParseQuery(u.RawQuery)
	type kv struct{ k, v string }
	var pairs []kv
	for k, vs := range values {
		for _, v := range vs {
			pairs = append(pairs, kv{escapeV4(k), escapeV4(v)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

// escapeV4 is RFC 3986 percent-encoding with the AWS unreserved set:
// url.QueryEscape except space is %20 and '~' stays literal.
func escapeV4(s string) string {
	e := url.QueryEscape(s)
	e = strings.ReplaceAll(e, "+", "%20")
	e = strings.ReplaceAll(e, "%7E", "~")
	return e
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}
