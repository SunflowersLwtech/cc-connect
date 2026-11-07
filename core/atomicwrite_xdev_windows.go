//go:build windows

package core

import "syscall"

// errCrossDevice is the error os.Rename wraps when oldpath and newpath are on
// different volumes: ERROR_NOT_SAME_DEVICE (17). syscall.EXDEV exists on
// Windows too, but no Windows API returns it.
var errCrossDevice error = syscall.Errno(17)
