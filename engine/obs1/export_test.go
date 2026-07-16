package obs1

import "testing"

// CreateTestBucket exposes the raw bucket-level PUT to the external test
// package, where the differential suite lives so it can import both this
// package and the simulator.
func CreateTestBucket(t *testing.T, endpoint, bucket, user, pass string) {
	createBucket(t, endpoint, bucket, user, pass)
}
