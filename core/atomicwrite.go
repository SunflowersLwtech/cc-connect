package core

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// renameHook, when set, replaces os.Rename in AtomicWriteFile. Tests use it to
// simulate a system temp directory on another filesystem than the target;
// it is atomic because other tests' background goroutines may be writing.
var renameHook atomic.Pointer[func(oldpath, newpath string) error]

func renameFile(oldpath, newpath string) error {
	if h := renameHook.Load(); h != nil {
		return (*h)(oldpath, newpath)
	}
	return os.Rename(oldpath, newpath)
}

// sameDirTempDirs remembers target directories that are on another filesystem
// than the system temp directory, so later writes into them skip the doomed
// first attempt (which would otherwise copy every payload twice) and write
// their temp file next to the target straight away.
//
// A directory is remembered only when the rename from the system temp
// directory failed with a cross-device error (EXDEV; ERROR_NOT_SAME_DEVICE on
// Windows) and the rename from next to the target then worked. Being on
// another filesystem is a property of the directory and holds for every later
// write. Any other rename failure (the target is a directory, a permission
// problem, the temp file removed under us) says nothing about the next write;
// that write still tries a temp file next to the target, but the directory is
// not remembered, because remembering it would make every later write leave
// its temp file in that directory, which same-filesystem setups must never do.
var sameDirTempDirs sync.Map

// AtomicWriteFile writes data to a file atomically when possible by first
// writing to a temporary file in the system temp directory, syncing, then
// renaming over the target.  On the same filesystem the rename is fully
// atomic.
//
// When os.TempDir() and the target directory are on different filesystems
// (typically a container whose data directory is a bind mount) the rename
// fails with EXDEV.  Writing the target in place instead would truncate it
// first: readers see a half-written file while the write runs, and a process
// killed mid-write leaves it truncated for good.  So we retry with a temp file
// in the target directory and rename that, and only write in place when that
// rename fails too.  Same-filesystem setups (including tests, whose
// t.TempDir() lives under os.TempDir()) never reach that path, so callers
// which write from background goroutines still never leave temp files in
// their directories there — this avoids spurious "t.TempDir() cleanup:
// directory not empty" failures during parallel tests.
//
// A process killed between writing a temp file next to the target and
// renaming it leaves that file behind; RemoveStaleAtomicWriteTemps removes
// such files at startup.
func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dstDir := filepath.Dir(path)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}

	crossDevice := false
	if _, ok := sameDirTempDirs.Load(dstDir); !ok {
		tmpPath, err := writeTempFile("", "cc-atomic-*", data, perm)
		if err != nil {
			return err
		}
		// Try atomic rename (works when temp and target are on the same fs).
		err = renameFile(tmpPath, path)
		if err == nil {
			return nil
		}
		_ = os.Remove(tmpPath)
		crossDevice = errors.Is(err, errCrossDevice)
	}

	// The rename failed: retry with a temp file next to the target.
	tmpPath, err := writeTempFile(dstDir, sameDirTempPattern(path), data, perm)
	if err == nil {
		if err = renameFile(tmpPath, path); err == nil {
			if crossDevice {
				sameDirTempDirs.Store(dstDir, struct{}{})
			}
			return nil
		}
		_ = os.Remove(tmpPath)
	}

	// Neither rename worked. Fall back to direct write — no orphan temp files.
	return os.WriteFile(path, data, perm)
}

// sameDirTempPattern is the os.CreateTemp pattern of the temp file that
// AtomicWriteFile writes next to path. CreateTemp replaces the * with decimal
// digits; isSameDirTempName recognises the result.
func sameDirTempPattern(path string) string {
	return "." + filepath.Base(path) + ".tmp-*"
}

// isSameDirTempName reports whether name is a file name sameDirTempPattern
// produces: "." + a non-empty base name + ".tmp-" + decimal digits.
func isSameDirTempName(name string) bool {
	i := strings.LastIndex(name, ".tmp-")
	if i < 2 || name[0] != '.' {
		return false
	}
	digits := name[i+len(".tmp-"):]
	if digits == "" {
		return false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// RemoveStaleAtomicWriteTemps removes, from root and every directory below
// it, the temp files AtomicWriteFile writes next to a target (see
// sameDirTempPattern) that were last modified more than minAge ago, and
// returns how many it removed. It is meant for the data directory at
// startup: such a file is left only when a process is killed between writing
// it and renaming it, and nothing else ever removes it (each can be as large
// as the store it was replacing).
//
// minAge keeps it away from a temp file another process is writing at that
// moment, e.g. a command-line invocation sharing the data directory; a temp
// file exists only for the duration of one write. Symlinks and anything that
// is not a regular file are left alone. A missing root is not an error; other
// errors are collected and the walk goes on, the first one is returned.
func RemoveStaleAtomicWriteTemps(root string, minAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-minAge)
	removed := 0
	var firstErr error
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if !(path == root && errors.Is(err, fs.ErrNotExist)) {
				note(err)
			}
			return nil
		}
		if !d.Type().IsRegular() || !isSameDirTempName(d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				note(err)
			}
			return nil
		}
		if !info.ModTime().Before(cutoff) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				note(err)
			}
			return nil
		}
		removed++
		return nil
	})
	if walkErr != nil {
		note(walkErr)
	}
	return removed, firstErr
}

// writeTempFile writes data to a new temp file in dir (the system temp
// directory when dir is ""), syncs it and applies perm. On error the temp
// file is removed.
func writeTempFile(dir, pattern string, data []byte, perm os.FileMode) (string, error) {
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return "", err
	}
	if err := os.Chmod(tmpPath, perm); err != nil {
		os.Remove(tmpPath)
		return "", err
	}
	return tmpPath, nil
}
