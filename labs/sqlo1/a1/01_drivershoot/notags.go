//go:build !drv_modernc && !drv_zombiezen && !drv_ncruces

package main

import "errors"

// The no-tag build exists so plain go vet and gofmt stay green; a real
// run picks exactly one driver at build time.
const driverName = "none"

func openShootDB(path string, pageSize, cacheKiB int) (shootDB, error) {
	return nil, errors.New("build with -tags drv_modernc, drv_zombiezen, or drv_ncruces")
}
