// Package obs1labs anchors the labs/obs1 tree so the CI lane's build,
// vet, and test patterns resolve before the first lab lands. Labs live
// one directory per milestone under here, in landing order (01_, 02_,
// ...), and every lab runs on this machine first: local MinIO and the
// simulator are the lab providers, the gate box only sees scheduled
// gate runs (spec 2064/obs1 doc 10).
package obs1labs
