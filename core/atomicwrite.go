package core

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

// sameDirTempDirs remembers target directories that cannot be reached by
// renaming from the system temp directory but can be by renaming from a temp
// file next to the target, so later writes skip the doomed first attempt
// (which would otherwise copy every payload twice).
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
func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dstDir := filepath.Dir(path)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}

	if _, ok := sameDirTempDirs.Load(dstDir); !ok {
		tmpPath, err := writeTempFile("", "cc-atomic-*", data, perm)
		if err != nil {
			return err
		}
		// Try atomic rename (works when temp and target are on the same fs).
		if err := renameFile(tmpPath, path); err == nil {
			return nil
		}
		_ = os.Remove(tmpPath)
	}

	// Rename failed (likely EXDEV on different mount points).
	tmpPath, err := writeTempFile(dstDir, "."+filepath.Base(path)+".tmp-*", data, perm)
	if err == nil {
		if err = renameFile(tmpPath, path); err == nil {
			sameDirTempDirs.Store(dstDir, struct{}{})
			return nil
		}
		_ = os.Remove(tmpPath)
	}

	// Neither rename worked. Fall back to direct write — no orphan temp files.
	return os.WriteFile(path, data, perm)
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
