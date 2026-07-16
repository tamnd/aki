// A trimmed copy of the engine/obs1 SigV4 signer, enough for the minio
// arm's setup PUTs and signed GETs. The lab lands before the engine client
// by design (the lab justifies the client's pool constant), and labs stay
// self-contained binaries, so the copy is deliberate; the engine version is
// the one conformance-tested against the AWS documented vectors.
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type credentials struct {
	accessKey string
	secretKey string
}

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func newReader(b []byte) io.Reader { return bytes.NewReader(b) }

// signBody signs a request whose full payload is b (nil for none).
func signBody(req *http.Request, b []byte, creds credentials) {
	hash := emptySHA256
	if b != nil {
		sum := sha256.Sum256(b)
		hash = hex.EncodeToString(sum[:])
	}
	signV4hash(req, creds, "us-east-1", "s3", hash)
}

// signV4 signs a body-less request (the lab's GETs).
func signV4(req *http.Request, creds credentials, region, service string) {
	signV4hash(req, creds, region, service, emptySHA256)
}

func signV4hash(req *http.Request, creds credentials, region, service, payloadHash string) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	scopeDate := amzDate[:8]

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

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
		if n == "host" && v == "" {
			v = req.URL.Host
		}
		if n != "host" {
			v = strings.TrimSpace(strings.Join(req.Header.Values(http.CanonicalHeaderKey(n)), ","))
		}
		canonHeaders.WriteString(n + ":" + v + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	canonical := req.Method + "\n" +
		req.URL.EscapedPath() + "\n" +
		req.URL.RawQuery + "\n" +
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
	sig := hex.EncodeToString(hmacSHA256(k, toSign))

	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+creds.accessKey+"/"+scope+
			", SignedHeaders="+signedHeaders+", Signature="+sig)
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}
