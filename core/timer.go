package core

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// TimerJob represents a persisted one-shot timer.
type TimerJob struct {
	ID          string    `json:"id"`
	Project     string    `json:"project"`
	SessionKey  string    `json:"session_key"`
	ScheduledAt time.Time `json:"scheduled_at"` // absolute fire time
	Prompt      string    `json:"prompt"`
	Exec        string    `json:"exec,omitempty"`     // shell command; mutually exclusive with Prompt
	WorkDir     string    `json:"work_dir,omitempty"` // working directory for exec; empty = agent work_dir
	Description string    `json:"description"`
	Silent      *bool     `json:"silent,omitempty"`       // suppress start notification; nil = use global default
	Mute        bool      `json:"mute,omitempty"`         // suppress ALL messages (start + result)
	SessionMode string    `json:"session_mode,omitempty"` // "" or "reuse" = share active session; "new_per_run" = fresh session each run
	Mode        string    `json:"mode,omitempty"`         // permission mode override; "" = use project default
	TimeoutMins *int      `json:"timeout_mins,omitempty"` // nil = default 30m; 0 = no limit; >0 = minutes
	CreatedAt   time.Time `json:"created_at"`
	Fired       bool      `json:"fired"`
	FiredAt     time.Time `json:"fired_at,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
}

// IsShellJob returns true if the job runs a shell command directly.
func (j *TimerJob) IsShellJob() bool {
	return j.Exec != ""
}

const defaultTimerJobTimeout = 30 * time.Minute

// ExecutionTimeout returns how long the scheduler waits for the job goroutine to finish.
func (j *TimerJob) ExecutionTimeout() time.Duration {
	if j.TimeoutMins == nil {
		return defaultTimerJobTimeout
	}
	if *j.TimeoutMins <= 0 {
		return 0
	}
	return time.Duration(*j.TimeoutMins) * time.Minute
}

// UsesNewSessionPerRun reports whether the timer should use a new engine session.
func (j *TimerJob) UsesNewSessionPerRun() bool {
	return NormalizeCronSessionMode(j.SessionMode) == "new_per_run"
}

func validateTimerJob(j *TimerJob) error {
	if strings.TrimSpace(j.SessionKey) == "" {
		return fmt.Errorf("session_key is required")
	}
	if j.ScheduledAt.IsZero() {
		return fmt.Errorf("scheduled_at is required")
	}
	if j.Prompt == "" && j.Exec == "" {
		return fmt.Errorf("either prompt or exec is required")
	}
	if j.Prompt != "" && j.Exec != "" {
		return fmt.Errorf("prompt and exec are mutually exclusive")
	}
	mode := NormalizeCronSessionMode(j.SessionMode)
	if mode != "" && mode != "new_per_run" {
		return fmt.Errorf("invalid session_mode %q (want reuse, new_per_run, or new-per-run)", j.SessionMode)
	}
	if j.Mode != "" {
		switch j.Mode {
		case "default", "bypassPermissions", "acceptEdits", "plan", "auto", "dontAsk":
		default:
			return fmt.Errorf("invalid mode %q", j.Mode)
		}
	}
	if j.TimeoutMins != nil && *j.TimeoutMins < 0 {
		return fmt.Errorf("timeout_mins must be >= 0")
	}
	return nil
}

// Fired timers stay in jobs.json for a while so that /timer/info and scripts
// reading the file can see how a run ended. After that they are moved to an
// append-only monthly archive (timers/archive/jobs-YYYY-MM.jsonl, one job per
// line) that is never read back. Without this every fired timer stays in
// jobs.json forever, and every Add and MarkFired rewrites all of that history
// while holding the store lock.
const (
	// defaultTimerFiredRetention is how long a fired timer stays in jobs.json.
	defaultTimerFiredRetention = 72 * time.Hour
	// defaultTimerFiredKeepMax caps how many fired timers stay in jobs.json
	// however recent they are, so a burst of timers cannot make each save
	// slow again.
	defaultTimerFiredKeepMax = 5000
	// timerPruneInterval is how often saves look for fired timers to move.
	timerPruneInterval = time.Hour
)

// TimerStore persists timer jobs to a JSON file.
type TimerStore struct {
	path       string
	archiveDir string
	mu         sync.Mutex
	jobs       []*TimerJob

	firedRetention time.Duration // 0 = no age limit
	firedKeepMax   int           // 0 = no count limit
	lastPrune      time.Time
	now            func() time.Time
}

func NewTimerStore(dataDir string) (*TimerStore, error) {
	dir := filepath.Join(dataDir, "timers")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "jobs.json")
	s := &TimerStore{
		path:           path,
		archiveDir:     filepath.Join(dir, "archive"),
		firedRetention: defaultTimerFiredRetention,
		firedKeepMax:   defaultTimerFiredKeepMax,
		now:            time.Now,
	}
	s.load()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pruneLocked(s.now()) > 0 {
		if err := s.save(); err != nil {
			slog.Warn("timer: failed to save after archiving fired jobs", "error", err)
		}
	}
	return s, nil
}

func (s *TimerStore) load() {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	if err := json.Unmarshal(data, &s.jobs); err != nil {
		slog.Error("timer: failed to load jobs", "path", s.path, "error", err)
	}
}

// save writes jobs.json. At most once per timerPruneInterval it first moves
// fired jobs past their retention to the archive, so s.jobs may shrink.
func (s *TimerStore) save() error {
	if now := s.now(); now.Sub(s.lastPrune) >= timerPruneInterval {
		s.pruneLocked(now)
	}
	data, err := json.MarshalIndent(s.jobs, "", "  ")
	if err != nil {
		return err
	}
	return AtomicWriteFile(s.path, data, 0o644)
}

func (s *TimerStore) Add(job *TimerJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, job)
	return s.save()
}

func (s *TimerStore) Remove(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, j := range s.jobs {
		if j.ID == id {
			s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
			if err := s.save(); err != nil {
				slog.Warn("timer: failed to save after remove", "error", err)
			}
			return true
		}
	}
	return false
}

func (s *TimerStore) SetMute(id string, mute bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.ID == id {
			j.Mute = mute
			if err := s.save(); err != nil {
				slog.Warn("timer: save after mute toggle", "error", err)
			}
			return true
		}
	}
	return false
}

func (s *TimerStore) MarkFired(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.ID == id {
			j.Fired = true
			j.FiredAt = time.Now()
			if err != nil {
				j.LastError = err.Error()
			} else {
				j.LastError = ""
			}
			if saveErr := s.save(); saveErr != nil {
				slog.Warn("timer: failed to save after mark fired", "error", saveErr)
			}
			return
		}
	}
}

func (s *TimerStore) List() []*TimerJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*TimerJob, len(s.jobs))
	copy(out, s.jobs)
	return out
}

func (s *TimerStore) ListByProject(project string) []*TimerJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*TimerJob
	for _, j := range s.jobs {
		if j.Project == project {
			out = append(out, j)
		}
	}
	return out
}

func (s *TimerStore) ListBySessionKey(sessionKey string) []*TimerJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*TimerJob
	for _, j := range s.jobs {
		if j.SessionKey == sessionKey {
			out = append(out, j)
		}
	}
	return out
}

func (s *TimerStore) Get(id string) *TimerJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.ID == id {
			return j
		}
	}
	return nil
}

// PruneFired moves fired jobs that are older than the retention window, or
// beyond the newest firedKeepMax fired jobs, from jobs.json to the monthly
// archive, and returns how many it moved. Pending jobs are never moved.
func (s *TimerStore) PruneFired(now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.pruneLocked(now)
	if n == 0 {
		return 0, nil
	}
	return n, s.save()
}

// pruneLocked drops the jobs PruneFired describes from s.jobs after
// appending them to the archive. If the archive cannot be written the jobs
// stay in jobs.json, so a job is never in neither place; a crash between
// the append and the next save can leave it in both.
func (s *TimerStore) pruneLocked(now time.Time) int {
	s.lastPrune = now
	var fired []*TimerJob
	for _, j := range s.jobs {
		if j.Fired {
			fired = append(fired, j)
		}
	}
	sort.SliceStable(fired, func(a, b int) bool { return timerFiredTime(fired[a]).After(timerFiredTime(fired[b])) })
	cutoff := now.Add(-s.firedRetention)
	move := make(map[*TimerJob]bool)
	for i, j := range fired {
		if (s.firedKeepMax > 0 && i >= s.firedKeepMax) || (s.firedRetention > 0 && timerFiredTime(j).Before(cutoff)) {
			move[j] = true
		}
	}
	if len(move) == 0 {
		return 0
	}

	var moved, kept []*TimerJob
	for _, j := range s.jobs {
		if move[j] {
			moved = append(moved, j)
		} else {
			kept = append(kept, j)
		}
	}
	if err := s.archive(moved); err != nil {
		slog.Warn("timer: failed to archive fired jobs; keeping them in jobs.json", "count", len(moved), "error", err)
		return 0
	}
	s.jobs = kept
	slog.Info("timer: archived fired jobs", "count", len(moved), "kept", len(kept), "dir", s.archiveDir)
	return len(moved)
}

// timerFiredTime is when a fired job fired, or when it was due if that was
// not recorded.
func timerFiredTime(j *TimerJob) time.Time {
	if !j.FiredAt.IsZero() {
		return j.FiredAt
	}
	return j.ScheduledAt
}

// archive appends jobs, one JSON object per line, to the archive file of the
// month each one fired in.
func (s *TimerStore) archive(jobs []*TimerJob) error {
	if err := os.MkdirAll(s.archiveDir, 0o755); err != nil {
		return err
	}
	byMonth := make(map[string][]byte)
	var months []string
	for _, j := range jobs {
		line, err := json.Marshal(j)
		if err != nil {
			return err
		}
		m := timerFiredTime(j).Format("2006-01")
		if _, ok := byMonth[m]; !ok {
			months = append(months, m)
		}
		byMonth[m] = append(append(byMonth[m], line...), '\n')
	}
	sort.Strings(months)
	for _, m := range months {
		if err := appendTimerArchive(filepath.Join(s.archiveDir, "jobs-"+m+".jsonl"), byMonth[m]); err != nil {
			return err
		}
	}
	return nil
}

func appendTimerArchive(path string, lines []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	// An earlier append cut short leaves a partial last line; start on a new
	// line so it does not swallow the first job of this batch.
	if st, err := f.Stat(); err != nil {
		return err
	} else if st.Size() > 0 {
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, st.Size()-1); err != nil {
			return err
		}
		if last[0] != '\n' {
			lines = append([]byte{'\n'}, lines...)
		}
	}
	if _, err := f.Write(lines); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// ListPending returns all non-fired timer jobs.
func (s *TimerStore) ListPending() []*TimerJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*TimerJob
	for _, j := range s.jobs {
		if !j.Fired {
			out = append(out, j)
		}
	}
	return out
}

// TimerScheduler runs one-shot timer jobs using time.AfterFunc.
type TimerScheduler struct {
	store    *TimerStore
	engines  map[string]*Engine
	mu       sync.RWMutex
	timers   map[string]*time.Timer // job ID → active timer
	defaultSilent      bool
	defaultSessionMode string
}

// missedJobGracePeriod is how long after a missed fire time we still execute.
// Older missed jobs are logged and skipped.
const missedJobGracePeriod = 5 * time.Minute

func NewTimerScheduler(store *TimerStore) *TimerScheduler {
	return &TimerScheduler{
		store:   store,
		engines: make(map[string]*Engine),
		timers:  make(map[string]*time.Timer),
	}
}

func (ts *TimerScheduler) RegisterEngine(name string, e *Engine) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.engines[name] = e
}

// SetDefaultSilent sets the default silent mode for timer jobs.
// Must be called before Start (not safe for concurrent use).
func (ts *TimerScheduler) SetDefaultSilent(silent bool) {
	ts.defaultSilent = silent
}

// SetDefaultSessionMode sets the default session mode for timer jobs.
// Must be called before Start (not safe for concurrent use).
func (ts *TimerScheduler) SetDefaultSessionMode(mode string) {
	ts.defaultSessionMode = NormalizeCronSessionMode(mode)
}

// IsSilent returns whether the timer job should suppress the start notification.
func (ts *TimerScheduler) IsSilent(job *TimerJob) bool {
	if job.Silent != nil {
		return *job.Silent
	}
	return ts.defaultSilent
}

// UsesNewSession returns whether the job should create a fresh session.
func (ts *TimerScheduler) UsesNewSession(job *TimerJob) bool {
	if job.SessionMode != "" {
		return job.UsesNewSessionPerRun()
	}
	return ts.defaultSessionMode == "new_per_run"
}

func (ts *TimerScheduler) Start() error {
	jobs := ts.store.List()
	now := time.Now()
	var scheduled, missed, skipped int
	for _, job := range jobs {
		if job.Fired {
			continue
		}
		delay := job.ScheduledAt.Sub(now)
		if delay <= 0 {
			// Past due
			if -delay <= missedJobGracePeriod {
				// Just missed — fire immediately
				slog.Info("timer: firing missed job immediately", "id", job.ID, "overdue", -delay)
				ts.scheduleAt(job, 0)
				missed++
			} else {
				slog.Warn("timer: skipping stale job", "id", job.ID, "scheduled_at", job.ScheduledAt, "overdue", -delay)
				ts.store.MarkFired(job.ID, fmt.Errorf("missed by %v (stale)", -delay))
				skipped++
			}
		} else {
			ts.scheduleAt(job, delay)
			scheduled++
		}
	}
	slog.Info("timer: scheduler started", "scheduled", scheduled, "missed_fired", missed, "skipped_stale", skipped, "total", len(jobs))
	return nil
}

func (ts *TimerScheduler) Stop() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for id, t := range ts.timers {
		t.Stop()
		delete(ts.timers, id)
	}
}

func (ts *TimerScheduler) AddJob(job *TimerJob) error {
	if err := validateTimerJob(job); err != nil {
		return err
	}
	job.SessionMode = NormalizeCronSessionMode(job.SessionMode)
	if err := ts.store.Add(job); err != nil {
		return err
	}
	delay := time.Until(job.ScheduledAt)
	if delay <= 0 {
		// Already due — fire immediately
		slog.Info("timer: new job already due, firing immediately", "id", job.ID)
		ts.scheduleAt(job, 0)
	} else {
		ts.scheduleAt(job, delay)
	}
	return nil
}

func (ts *TimerScheduler) RemoveJob(id string) bool {
	ts.mu.Lock()
	if t, ok := ts.timers[id]; ok {
		t.Stop()
		delete(ts.timers, id)
	}
	// Remove from store while still holding ts.mu to prevent executeJob
	// from running a job that was just cancelled (the AfterFunc callback
	// also needs ts.mu, so it will see the store entry is gone).
	removed := ts.store.Remove(id)
	ts.mu.Unlock()
	return removed
}

func (ts *TimerScheduler) SetMute(id string, mute bool) bool {
	return ts.store.SetMute(id, mute)
}

func (ts *TimerScheduler) Store() *TimerStore {
	return ts.store
}

func (ts *TimerScheduler) scheduleAt(job *TimerJob, delay time.Duration) {
	jobID := job.ID
	ts.mu.Lock()
	// Stop any existing timer for this job
	if old, ok := ts.timers[jobID]; ok {
		old.Stop()
	}
	// Register timer in map before it can fire, so RemoveJob can always cancel it.
	t := time.AfterFunc(delay, func() {
		ts.mu.Lock()
		delete(ts.timers, jobID)
		ts.mu.Unlock()
		ts.executeJob(jobID)
	})
	ts.timers[jobID] = t
	ts.mu.Unlock()
}

func (ts *TimerScheduler) executeJob(jobID string) {
	job := ts.store.Get(jobID)
	if job == nil || job.Fired {
		return
	}

	ts.mu.RLock()
	engine, ok := ts.engines[job.Project]
	ts.mu.RUnlock()

	if !ok {
		slog.Error("timer: project not found", "job", jobID, "project", job.Project)
		ts.store.MarkFired(jobID, fmt.Errorf("project %q not found", job.Project))
		return
	}

	slog.Info("timer: executing job", "id", jobID, "project", job.Project, "prompt", truncateStr(job.Prompt, 60))

	done := make(chan error, 1)
	go func() {
		done <- engine.ExecuteTimerJob(job)
	}()

	var err error
	timeout := job.ExecutionTimeout()
	if timeout > 0 {
		select {
		case err = <-done:
		case <-time.After(timeout):
			err = fmt.Errorf("job timed out after %v", timeout)
		}
	} else {
		err = <-done
	}

	ts.store.MarkFired(jobID, err)

	if err != nil {
		slog.Error("timer: job failed", "id", jobID, "error", err)
	} else {
		slog.Info("timer: job completed", "id", jobID)
	}
}

func GenerateTimerID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("generate timer id: %w", err))
	}
	return hex.EncodeToString(b)
}

// ParseDelayOrTime parses a relative duration ("2h", "30m", "1h30m") or
// an absolute ISO time ("2026-05-15T14:00", "2026-05-15T14:00:00+08:00")
// and returns the absolute fire time.
func ParseDelayOrTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty delay or time")
	}

	// Try as a Go duration first (e.g., "2h", "30m", "1h30m", "2h30m15s")
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return time.Time{}, fmt.Errorf("delay must be positive")
		}
		return time.Now().Add(d), nil
	}

	// Try ISO time formats
	// RFC3339 includes timezone (e.g. "2026-05-15T14:00:00+08:00"),
	// so it's parsed directly. The other layouts have no timezone
	// and are interpreted in the system's local timezone.
	layouts := []struct {
		layout string
		local  bool
	}{
		{time.RFC3339, false},
		{"2006-01-02T15:04:05", true},
		{"2006-01-02T15:04", true},
		{"2006-01-02 15:04:05", true},
		{"2006-01-02 15:04", true},
	}
	for _, l := range layouts {
		if l.local {
			if t, err := time.ParseInLocation(l.layout, s, time.Local); err == nil {
				return t, nil
			}
		} else {
			if t, err := time.Parse(l.layout, s); err == nil {
				return t, nil
			}
		}
	}

	return time.Time{}, fmt.Errorf("invalid delay or time %q (use duration like 2h30m, or ISO time like 2026-05-15T14:00)", s)
}

// FormatTimerRemaining returns a human-readable string for time remaining
// until the scheduled fire time.
func FormatTimerRemaining(scheduledAt time.Time) string {
	d := time.Until(scheduledAt)
	if d <= 0 {
		return "overdue"
	}
	if d < time.Minute {
		secs := int(d.Seconds() + 0.5) // round up for countdown display
		if secs >= 60 {
			return "1m"
		}
		return fmt.Sprintf("%ds", secs)
	}
	if d < time.Hour {
		mins := int(d.Minutes() + 0.5) // round up for countdown display
		if mins >= 60 {
			return "1h"
		}
		return fmt.Sprintf("%dm", mins)
	}
	totalMins := int(d.Minutes() + 0.5) // round up
	hours := totalMins / 60
	mins := totalMins % 60
	if mins == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh%dm", hours, mins)
}
