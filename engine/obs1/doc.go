// Package obs1 is the engine of the obs1 driver: a distributed,
// Redis-compatible store whose persistent state lives entirely in an
// S3-class bucket (spec 2064/obs1). The commit chain, the leases, the
// object formats, the fold, and the hand-rolled object-store client all
// land here; the package stays flat and imports the standard library only,
// no AWS SDK, enforced by scripts/obs1-import-boundary.sh.
//
// Empty at O0a slice 1; the client core lands in slice 2.
package obs1
