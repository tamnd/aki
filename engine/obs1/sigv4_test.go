package obs1

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// The four documented examples from "Authenticating Requests: Using the
// Authorization Header" in the S3 API reference: examplebucket, us-east-1,
// 2013-05-24, the shared example key pair. If these four signatures come
// out byte-identical the canonical request, string to sign, and key
// derivation are all right; the MinIO differential suite then keeps the
// signer honest against a real implementation.
func TestSignV4DocumentedExamples(t *testing.T) {
	creds := credentials{
		accessKey: "AKIAIOSFODNN7EXAMPLE",
		secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	at := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		method  string
		url     string
		headers map[string]string
		payload string // hash; empty means the zero-byte hash
		want    string // the documented signature
	}{
		{
			name:   "get object",
			method: "GET",
			url:    "https://examplebucket.s3.amazonaws.com/test.txt",
			headers: map[string]string{
				"Range": "bytes=0-9",
			},
			want: "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
		},
		{
			name:    "get bucket lifecycle",
			method:  "GET",
			url:     "https://examplebucket.s3.amazonaws.com/?lifecycle",
			headers: map[string]string{},
			want:    "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
		},
		{
			name:    "list objects",
			method:  "GET",
			url:     "https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J",
			headers: map[string]string{},
			want:    "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
		},
		{
			name:   "put object",
			method: "PUT",
			url:    "https://examplebucket.s3.amazonaws.com/test%24file.text",
			headers: map[string]string{
				"Date":                "Fri, 24 May 2013 00:00:00 GMT",
				"x-amz-storage-class": "REDUCED_REDUNDANCY",
			},
			payload: "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
			want:    "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, tc.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			hash := tc.payload
			if hash == "" {
				hash = emptySHA256
			}
			signV4(req, creds, "us-east-1", "s3", hash, at)

			auth := req.Header.Get("Authorization")
			_, got, ok := strings.Cut(auth, "Signature=")
			if !ok {
				t.Fatalf("no signature in %q", auth)
			}
			if got != tc.want {
				t.Errorf("signature\n got %s\nwant %s\nauth %s", got, tc.want, auth)
			}
		})
	}
}

// A session token must enter the signed header set like any other x-amz-*
// header; this just pins that it lands in both places.
func TestSignV4SessionToken(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://b.s3.amazonaws.com/k", nil)
	signV4(req, credentials{"AK", "SK", "TOKEN"}, "us-east-1", "s3", emptySHA256, time.Unix(0, 0))
	if req.Header.Get("x-amz-security-token") != "TOKEN" {
		t.Error("token header missing")
	}
	if !strings.Contains(req.Header.Get("Authorization"), "x-amz-security-token") {
		t.Error("token not in signed headers")
	}
}

func TestEscapeKey(t *testing.T) {
	cases := map[string]string{
		"chain/00/000000000042": "chain/00/000000000042",
		"a b+c~d":               "a%20b%2Bc~d",
		"seg/α":                 "seg/%CE%B1",
	}
	for in, want := range cases {
		if got := escapeKey(in); got != want {
			t.Errorf("escapeKey(%q) = %q, want %q", in, got, want)
		}
	}
}
