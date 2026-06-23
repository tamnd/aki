//go:build windows

package command

import "syscall"

// cpuSeconds returns the process system and user CPU time in seconds. Windows
// has no getrusage, so it comes from GetProcessTimes, whose kernel and user
// Filetime values count 100-nanosecond ticks since the process started.
func cpuSeconds() (sys, user float64) {
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, 0
	}
	var creation, exit, kernel, userT syscall.Filetime
	if err := syscall.GetProcessTimes(h, &creation, &exit, &kernel, &userT); err != nil {
		return 0, 0
	}
	sys = float64(kernel.Nanoseconds()) / 1e9
	user = float64(userT.Nanoseconds()) / 1e9
	return sys, user
}
