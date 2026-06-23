//go:build !windows

package command

import "syscall"

// cpuSeconds returns the process system and user CPU time in seconds, read from
// getrusage on unix. The windows build reads the same numbers from
// GetProcessTimes in info_windows.go.
func cpuSeconds() (sys, user float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	user = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sys = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return sys, user
}
