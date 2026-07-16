// connpool measures stdlib net/http connection reuse and TLS session cost
// under concurrent GET fan-out (spec 2064/obs1 milestone O0a lab 01). One
// configuration per run, one CSV row to stdout.
package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	arm := flag.String("arm", "inproc-http", "inproc-http | inproc-tls | fresh | minio")
	endpoint := flag.String("endpoint", envOr("AKI_OBS1_S3", "http://127.0.0.1:19000"), "S3 endpoint for the minio arm")
	user := flag.String("user", envOr("AKI_OBS1_S3_USER", "minioadmin"), "access key for the minio arm")
	pass := flag.String("pass", envOr("AKI_OBS1_S3_PASS", "minioadmin"), "secret key for the minio arm")
	conc := flag.Int("conc", 16, "concurrent workers")
	pool := flag.Int("pool", 64, "MaxIdleConnsPerHost")
	ops := flag.Int("ops", 0, "total requests (0 = 200 per worker, at least 2000)")
	size := flag.Int("size", 4096, "object size in bytes")
	quick := flag.Bool("quick", false, "one small run per in-process arm")
	header := flag.Bool("header", false, "print the CSV header and exit")
	flag.Parse()

	if *header {
		fmt.Println("arm,conc,pool,size,ops,ops_per_s,p50_us,p99_us,max_us,reused_frac,dials")
		return
	}
	if *quick {
		for _, a := range []string{"inproc-http", "inproc-tls", "fresh"} {
			run(a, "", "", "", 4, 16, 400, 1024)
		}
		return
	}
	n := *ops
	if n == 0 {
		n = max(2000, *conc*200)
	}
	run(*arm, *endpoint, *user, *pass, *conc, *pool, n, *size)
}

func run(arm, endpoint, user, pass string, conc, pool, ops, size int) {
	var url string
	var client *http.Client
	var sign func(*http.Request)

	switch arm {
	case "inproc-http", "inproc-tls", "fresh":
		body := make([]byte, size)
		if _, err := rand.Read(body); err != nil {
			fatal(err)
		}
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := w.Write(body); err != nil {
				return // client went away; the counters tell the story
			}
		})
		var srv *httptest.Server
		if arm == "inproc-tls" {
			srv = httptest.NewTLSServer(h)
			client = srv.Client() // trusts the test certificate
		} else {
			srv = httptest.NewServer(h)
			client = &http.Client{}
		}
		defer srv.Close()
		url = srv.URL + "/obj"
		t := client.Transport
		if t == nil {
			t = http.DefaultTransport
		}
		tr := t.(*http.Transport).Clone()
		tr.MaxIdleConnsPerHost = pool
		tr.MaxIdleConns = 4 * pool
		tr.DisableKeepAlives = arm == "fresh"
		client.Transport = tr
	case "minio":
		creds := credentials{accessKey: user, secretKey: pass}
		bucket := fmt.Sprintf("obs1-connpool-%d", time.Now().UnixNano())
		key := "obj"
		setupMinio(endpoint, bucket, key, size, creds)
		url = endpoint + "/" + bucket + "/" + key
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.MaxIdleConnsPerHost = pool
		tr.MaxIdleConns = 4 * pool
		client = &http.Client{Transport: tr}
		sign = func(req *http.Request) { signV4(req, creds, "us-east-1", "s3") }
	default:
		fatal(fmt.Errorf("unknown arm %q", arm))
	}

	var reused, dials atomic.Int64
	lat := make([][]int64, conc)
	perWorker := ops / conc
	start := time.Now()
	var wg sync.WaitGroup
	for w := range conc {
		wg.Go(func() {
			lat[w] = make([]int64, 0, perWorker)
			trace := &httptrace.ClientTrace{
				GotConn: func(ci httptrace.GotConnInfo) {
					if ci.Reused {
						reused.Add(1)
					} else {
						dials.Add(1)
					}
				},
			}
			for range perWorker {
				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					fatal(err)
				}
				req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
				if sign != nil {
					sign(req)
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					fatal(err)
				}
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != 200 {
					fatal(fmt.Errorf("GET %s: http %d", url, resp.StatusCode))
				}
				lat[w] = append(lat[w], time.Since(t0).Microseconds())
			}
		})
	}
	wg.Wait()
	total := time.Since(start)

	var all []int64
	for _, l := range lat {
		all = append(all, l...)
	}
	slices.Sort(all)
	n := len(all)
	opsPerS := float64(n) / total.Seconds()
	reusedFrac := float64(reused.Load()) / float64(reused.Load()+dials.Load())
	fmt.Printf("%s,%d,%d,%d,%d,%.0f,%d,%d,%d,%.4f,%d\n",
		arm, conc, pool, size, n, opsPerS,
		all[n/2], all[n*99/100], all[n-1], reusedFrac, dials.Load())
}

// setupMinio creates the lab's bucket and object with signed raw requests.
func setupMinio(endpoint, bucket, key string, size int, creds credentials) {
	body := make([]byte, size)
	if _, err := rand.Read(body); err != nil {
		fatal(err)
	}
	put := func(url string, b []byte) {
		req, err := http.NewRequest(http.MethodPut, url, nil)
		if err != nil {
			fatal(err)
		}
		if b != nil {
			req.Body = io.NopCloser(newReader(b))
			req.ContentLength = int64(len(b))
		}
		signBody(req, b, creds)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			fatal(fmt.Errorf("PUT %s: http %d %s", url, resp.StatusCode, msg))
		}
	}
	put(endpoint+"/"+bucket, nil)
	put(endpoint+"/"+bucket+"/"+key, body)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "connpool:", err)
	os.Exit(1)
}
