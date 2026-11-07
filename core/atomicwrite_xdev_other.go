//go:build !windows

package core

import "syscall"

// errCrossDevice is the error os.Rename wraps when oldpath and newpath are on
// different filesystems.
var errCrossDevice error = syscall.EXDEV
