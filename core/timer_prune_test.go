package core

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// writeTimerJobsFile writes jobs.json the way an older build left it, before
// any store is opened on dataDir.
func writeTimerJobsFile(t *testing.T, dataDir string, jobs []*TimerJob) {
	t.Helper()
	dir := filepath.Join(dataDir, "timers")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(jobs, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func timerJobAt(id string, scheduled time.Time, firedAt *time.Time) *TimerJob {
	j := &TimerJob{
		ID:          id,
		Project:     "p",
		SessionKey:  "feishu:chat:user",
		ScheduledAt: scheduled,
		Prompt:      "prompt of " + id,
		Description: "mesh from " + id,
		CreatedAt:   scheduled.Add(-30 * time.Second),
	}
	if firedAt != nil {
		j.Fired = true
		j.FiredAt = *firedAt
		j.LastError = `session "feishu:chat:user" is busy`
	}
	return j
}

func ago(d time.Duration) *time.Time { t := time.Now().Add(-d); return &t }

// readTimerJobsFile returns the jobs in jobs.json on disk, by id.
func readTimerJobsFile(t *testing.T, dataDir string) map[string]*TimerJob {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "timers", "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var jobs []*TimerJob
	if err := json.Unmarshal(data, &jobs); err != nil {
		t.Fatalf("jobs.json is not valid JSON: %v", err)
	}
	out := make(map[string]*TimerJob)
	for _, j := range jobs {
		out[j.ID] = j
	}
	return out
}

// readTimerArchive returns every line of every archive file, keyed by file name.
func readTimerArchive(t *testing.T, dataDir string) map[string][]string {
	t.Helper()
	dir := filepath.Join(dataDir, "timers", "archive")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string][]string)
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > 0 && data[len(data)-1] != '\n' {
			t.Errorf("%s does not end with a newline", e.Name())
		}
		out[e.Name()] = strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	}
	return out
}

func sortedIDs(m map[string]*TimerJob) []string {
	var ids []string
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Opening a store on a jobs.json full of history moves the fired jobs past
// the retention window into the archive file of the month they fired in,
// byte for byte as they were, and keeps every pending job however old.
func TestTimerStore_PruneMovesOldFiredJobsToMonthlyArchive(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	jobs := []*TimerJob{
		timerJobAt("pend-future", now.Add(time.Hour), nil),
		timerJobAt("pend-overdue", now.Add(-40*24*time.Hour), nil), // never fired, must stay
		timerJobAt("fired-40d", now.Add(-40*24*time.Hour), ago(40*24*time.Hour)),
		timerJobAt("fired-1h", now.Add(-time.Hour), ago(time.Hour)),
		timerJobAt("fired-4d", now.Add(-4*24*time.Hour), ago(4*24*time.Hour)),
		timerJobAt("fired-47h", now.Add(-47*time.Hour), ago(47*time.Hour)),
		timerJobAt("nofiredat-5d", now.Add(-5*24*time.Hour), nil),
		timerJobAt("nofiredat-2h", now.Add(-2*time.Hour), nil),
	}
	// Fired before fired_at was recorded: only scheduled_at says when.
	for _, j := range jobs[6:] {
		j.Fired = true
	}
	writeTimerJobsFile(t, dir, jobs)
	byID := make(map[string]*TimerJob)
	for _, j := range jobs {
		byID[j.ID] = j
	}
	wantMoved := []string{"fired-40d", "fired-4d", "nofiredat-5d"}
	wantKept := []string{"fired-1h", "fired-47h", "nofiredat-2h", "pend-future", "pend-overdue"}

	store, err := NewTimerStore(dir)
	if err != nil {
		t.Fatalf("NewTimerStore: %v", err)
	}

	onDisk := readTimerJobsFile(t, dir)
	if got := sortedIDs(onDisk); strings.Join(got, ",") != strings.Join(wantKept, ",") {
		t.Fatalf("jobs.json holds %v, want %v", got, wantKept)
	}
	for _, id := range []string{"pend-future", "pend-overdue"} {
		if onDisk[id].Fired || store.Get(id) == nil || store.Get(id).Fired {
			t.Errorf("pending job %s was fired or lost", id)
		}
	}
	for _, id := range wantMoved {
		if store.Get(id) != nil {
			t.Errorf("store still serves archived job %s", id)
		}
	}

	archive := readTimerArchive(t, dir)
	seen := make(map[string]int)
	for file, lines := range archive {
		for _, line := range lines {
			var j TimerJob
			if err := json.Unmarshal([]byte(line), &j); err != nil {
				t.Fatalf("%s has a line that is not a job: %q (%v)", file, line, err)
			}
			seen[j.ID]++
			orig, ok := byID[j.ID]
			if !ok {
				t.Fatalf("%s holds unknown job %q", file, j.ID)
			}
			if wantFile := "jobs-" + timerFiredTime(orig).Format("2006-01") + ".jsonl"; file != wantFile {
				t.Errorf("job %s archived in %s, want %s", j.ID, file, wantFile)
			}
			want, _ := json.Marshal(orig)
			if !bytes.Equal([]byte(line), want) {
				t.Errorf("archived %s differs from the job that was moved:\n got %s\nwant %s", j.ID, line, want)
			}
		}
	}
	for _, id := range wantMoved {
		if seen[id] != 1 {
			t.Errorf("job %s is in the archive %d times, want 1", id, seen[id])
		}
	}
	if len(seen) != len(wantMoved) {
		t.Errorf("archive holds %d jobs, want %d (%v)", len(seen), len(wantMoved), seen)
	}

	// Reopening finds nothing more to move and does not archive twice.
	if _, err := NewTimerStore(dir); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, lines := range readTimerArchive(t, dir) {
		total += len(lines)
	}
	if total != len(wantMoved) {
		t.Errorf("after reopening the archive holds %d lines, want %d", total, len(wantMoved))
	}
}

// Beyond the newest firedKeepMax fired jobs, older fired ones move even when
// they are inside the retention window; pending jobs never count or move.
func TestTimerStore_PruneKeepsOnlyNewestFiredUpToCap(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	var jobs []*TimerJob
	for i := 0; i < 4; i++ {
		jobs = append(jobs, timerJobAt("pend-"+string(rune('a'+i)), now.Add(-time.Duration(i+1)*time.Hour*30), nil))
	}
	for i := 1; i <= 5; i++ {
		jobs = append(jobs, timerJobAt("fired-"+string(rune('0'+i)), now.Add(-time.Duration(i)*time.Minute), ago(time.Duration(i)*time.Minute)))
	}
	writeTimerJobsFile(t, dir, jobs)
	store, err := NewTimerStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(store.List()); n != len(jobs) {
		t.Fatalf("defaults moved jobs on open: %d left of %d", n, len(jobs))
	}

	store.firedKeepMax = 2
	moved, err := store.PruneFired(time.Now())
	if err != nil {
		t.Fatalf("PruneFired: %v", err)
	}
	if moved != 3 {
		t.Errorf("PruneFired moved %d, want 3", moved)
	}
	want := "fired-1,fired-2,pend-a,pend-b,pend-c,pend-d"
	if got := strings.Join(sortedIDs(readTimerJobsFile(t, dir)), ","); got != want {
		t.Errorf("jobs.json holds %s, want %s", got, want)
	}
	if n := len(store.ListPending()); n != 4 {
		t.Errorf("%d pending jobs left, want 4", n)
	}
}

// A previous append that was cut short leaves a partial last line; the next
// batch starts on a fresh line so its first job stays readable.
func TestTimerStore_PruneStartsArchiveBatchOnFreshLine(t *testing.T) {
	dir := t.TempDir()
	old := ago(10 * 24 * time.Hour)
	writeTimerJobsFile(t, dir, []*TimerJob{timerJobAt("old", *old, old)})
	file := filepath.Join(dir, "timers", "archive", "jobs-"+old.Format("2006-01")+".jsonl")
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(`{"id":"cut-sh`), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := NewTimerStore(dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("archive has %d lines, want the partial one and the job: %q", len(lines), data)
	}
	var j TimerJob
	if err := json.Unmarshal([]byte(lines[1]), &j); err != nil || j.ID != "old" {
		t.Fatalf("archived job is unreadable after a partial line: %q (%v)", lines[1], err)
	}
}

// If the archive cannot be written, the fired jobs stay in jobs.json rather
// than being dropped.
func TestTimerStore_PruneKeepsJobsWhenArchiveFails(t *testing.T) {
	dir := t.TempDir()
	writeTimerJobsFile(t, dir, []*TimerJob{
		timerJobAt("old", *ago(10 * 24 * time.Hour), ago(10*24*time.Hour)),
		timerJobAt("pend", time.Now().Add(time.Hour), nil),
	})
	// A file where the archive directory should be makes MkdirAll fail.
	if err := os.WriteFile(filepath.Join(dir, "timers", "archive"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := NewTimerStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if store.Get("old") == nil {
		t.Fatal("job dropped from the store although it could not be archived")
	}
	if n, err := store.PruneFired(time.Now()); n != 0 || err != nil {
		t.Errorf("PruneFired = %d, %v; want 0, nil", n, err)
	}
	if err := store.Add(timerJobAt("new", time.Now().Add(time.Hour), nil)); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(sortedIDs(readTimerJobsFile(t, dir)), ","); got != "new,old,pend" {
		t.Errorf("jobs.json holds %s, want new,old,pend", got)
	}
}

// A running store moves jobs that age out while it runs, through the saves
// that Add and MarkFired already do; it does not wait for a restart.
func TestTimerStore_PruneRunsDuringSaves(t *testing.T) {
	dir := t.TempDir()
	store, err := NewTimerStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Now()
	store.now = func() time.Time { return clock }

	if err := store.Add(timerJobAt("a", time.Now(), nil)); err != nil {
		t.Fatal(err)
	}
	store.MarkFired("a", nil)
	if store.Get("a") == nil {
		t.Fatal("a fresh fired job was moved straight away")
	}

	clock = clock.Add(defaultTimerFiredRetention + timerPruneInterval)
	if err := store.Add(timerJobAt("b", clock.Add(time.Hour), nil)); err != nil {
		t.Fatal(err)
	}
	if store.Get("a") != nil {
		t.Fatal("job past its retention is still in the store after a save")
	}
	if got := strings.Join(sortedIDs(readTimerJobsFile(t, dir)), ","); got != "b" {
		t.Errorf("jobs.json holds %s, want b", got)
	}
	n := 0
	for _, lines := range readTimerArchive(t, dir) {
		n += len(lines)
	}
	if n != 1 {
		t.Errorf("archive holds %d lines, want 1", n)
	}
}
