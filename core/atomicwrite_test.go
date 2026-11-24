package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
)

func TestAtomicWriteFile_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	data := []byte("hello world")

	if err := AtomicWriteFile(path, data, 0644); err != nil {
		t.Fatalf("AtomicWriteFile: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestAtomicWriteFile_Overwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	if err := AtomicWriteFile(path, []byte("first"), 0644); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := AtomicWriteFile(path, []byte("second"), 0644); err != nil {
		t.Fatalf("second write: %v", err)
	}

	got, _ := os.ReadFile(path)
	if string(got) != "second" {
		t.Errorf("got %q, want %q", got, "second")
	}
}

func TestAtomicWriteFile_Permissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix file permissions not supported on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	if err := AtomicWriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatalf("AtomicWriteFile: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("perm = %o, want 0600", perm)
	}
}

func TestAtomicWriteFile_NoTempLeftOnSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	_ = AtomicWriteFile(path, []byte("data"), 0644)

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "test.txt" {
			t.Errorf("unexpected file left: %s", e.Name())
		}
	}
}

// TestAtomicWriteFile_NoTempLeftWhenRenameFails is a regression test for a
// `.tmp-*` leak in the rename-failure path. Before the fix, AtomicWriteFile
// returned the rename error directly without removing the tmp file it had
// just created — so repeated failures would litter the parent directory
// with stale `.tmp-*` files and confuse callers that scan that directory
// (cron store, session store, etc.).
//
// We force a rename failure by writing to a path that already exists as a
// directory; os.Rename refuses to replace a non-empty directory with a
// regular file on every supported platform.
func TestAtomicWriteFile_NoTempLeftWhenRenameFails(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "blocked")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir target dir: %v", err)
	}

	if err := AtomicWriteFile(target, []byte("payload"), 0o644); err == nil {
		t.Fatal("AtomicWriteFile should fail when target is an existing directory")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != "blocked" {
			t.Errorf("rename failure left orphan file %q in %s; cleanup is missing", e.Name(), dir)
		}
	}
}

// simulateCrossDeviceTempDir makes renames from another directory into dir
// fail the way renames out of os.TempDir() do when it is on a different
// filesystem than dir (EXDEV), e.g. in a container whose data directory is a
// bind mount. It returns a counter of those failed attempts.
func simulateCrossDeviceTempDir(t *testing.T, dir string) *atomic.Int32 {
	t.Helper()
	var crossDir atomic.Int32
	hook := func(oldpath, newpath string) error {
		if filepath.Dir(newpath) == dir && filepath.Dir(oldpath) != dir {
			crossDir.Add(1)
			return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: errors.New("invalid cross-device link")}
		}
		return os.Rename(oldpath, newpath)
	}
	renameHook.Store(&hook)
	t.Cleanup(func() { renameHook.Store(nil) })
	return &crossDir
}

// When the system temp dir is on another filesystem, the target must still be
// replaced by a rename, not truncated and rewritten in place: a reader that
// opened the old file keeps seeing all of the old content, and a process
// killed mid-write can never leave a truncated file under the path.
func TestAtomicWriteFile_CrossDeviceTempDirStillReplacesByRename(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("renaming over an open file is not allowed on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	old := bytes.Repeat([]byte("old "), 1024)
	if err := os.WriteFile(path, old, 0o644); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	crossDir := simulateCrossDeviceTempDir(t, dir)
	want := []byte("new content")
	if err := AtomicWriteFile(path, want, 0o644); err != nil {
		t.Fatalf("AtomicWriteFile: %v", err)
	}
	if crossDir.Load() == 0 {
		t.Fatal("no rename from the system temp dir was attempted; the fallback path was not exercised")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("path holds %q, want %q", got, want)
	}
	seen, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, old) {
		t.Fatalf("a reader of the old file saw %d bytes (%.20q…) instead of the %d old ones: the file was rewritten in place, not replaced", len(seen), seen, len(old))
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "jobs.json" {
			t.Errorf("unexpected file left: %s", e.Name())
		}
	}
}

// Once a directory is known to need a temp file next to the target, later
// writes go there directly instead of first copying the payload through the
// system temp dir.
func TestAtomicWriteFile_CrossDeviceRemembersDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	crossDir := simulateCrossDeviceTempDir(t, dir)

	for i := 0; i < 3; i++ {
		if err := AtomicWriteFile(path, []byte(fmt.Sprint(i)), 0o644); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if n := crossDir.Load(); n != 1 {
		t.Fatalf("renamed from the system temp dir %d times over 3 writes, want 1", n)
	}
	if got, _ := os.ReadFile(path); string(got) != "2" {
		t.Fatalf("path holds %q, want %q", got, "2")
	}
}
