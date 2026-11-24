package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// permRecordSession is an AgentSession that records every permission answer.
type permRecordSession struct {
	events   chan Event
	mu       sync.Mutex
	answers  []PermissionResult
	answered chan struct{}
}

func newPermRecordSession() *permRecordSession {
	return &permRecordSession{events: make(chan Event, 8), answered: make(chan struct{}, 8)}
}

func (s *permRecordSession) Send(string, string, []ImageAttachment, []FileAttachment) error {
	return nil
}
func (s *permRecordSession) RespondPermission(_ string, r PermissionResult) error {
	s.mu.Lock()
	s.answers = append(s.answers, r)
	s.mu.Unlock()
	s.answered <- struct{}{}
	return nil
}
func (s *permRecordSession) Events() <-chan Event     { return s.events }
func (s *permRecordSession) CurrentSessionID() string { return "perm-agent-session" }
func (s *permRecordSession) Alive() bool              { return true }
func (s *permRecordSession) Close() error             { return nil }
func (s *permRecordSession) getAnswers() []PermissionResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PermissionResult(nil), s.answers...)
}

type permTimeoutEnv struct {
	t       *testing.T
	e       *Engine
	p       *stubPlatformEngine
	sess    *permRecordSession
	state   *interactiveState
	key     string
	done    chan struct{}
	hookOut string
}

// startPermTimeoutTurn runs one interactive turn against a recording agent
// session, with a synchronous command hook that writes permission.timed_out
// events to a file.
func startPermTimeoutTurn(t *testing.T, timeout time.Duration) *permTimeoutEnv {
	t.Helper()
	p := &stubPlatformEngine{n: "test"}
	return startPermTimeoutTurnOn(t, timeout, p, p)
}

// startPermTimeoutTurnOn is startPermTimeoutTurn on platform plat; sent records
// what plat sends as plain messages.
func startPermTimeoutTurnOn(t *testing.T, timeout time.Duration, plat Platform, sent *stubPlatformEngine) *permTimeoutEnv {
	t.Helper()
	p := sent
	e := NewEngine("test", &stubAgent{}, []Platform{plat}, "", LangEnglish)
	e.SetPermissionTimeout(timeout)
	hookOut := filepath.Join(t.TempDir(), "timed-out.json")
	async := false
	e.SetHooks(NewHookManager("test", []HookConfig{{Event: string(HookEventPermissionTimedOut), Type: "command", Command: "cat > " + hookOut, Async: &async}}, "sh", "-c", ""))
	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	sess := newPermRecordSession()
	state := &interactiveState{agentSession: sess, platform: plat, replyCtx: "ctx"}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()
	sendDone := make(chan error, 1)
	sendDone <- nil
	env := &permTimeoutEnv{t: t, e: e, p: p, sess: sess, state: state, key: key, done: make(chan struct{}), hookOut: hookOut}
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "m1", time.Now(), nil, sendDone, nil, 0)
		close(env.done)
	}()
	return env
}

func (env *permTimeoutEnv) askBash(command string) {
	env.sess.events <- Event{
		Type:         EventPermissionRequest,
		RequestID:    "req-1",
		ToolName:     "Bash",
		ToolInput:    command,
		ToolInputRaw: map[string]any{"command": command},
	}
}

func (env *permTimeoutEnv) waitAnswer(within time.Duration) PermissionResult {
	env.t.Helper()
	select {
	case <-env.sess.answered:
	case <-time.After(within):
		env.t.Fatalf("no permission answer within %s", within)
	}
	a := env.sess.getAnswers()
	return a[len(a)-1]
}

func (env *permTimeoutEnv) noAnswerFor(d time.Duration) {
	env.t.Helper()
	select {
	case <-env.sess.answered:
		env.t.Fatalf("unexpected permission answer: %+v", env.sess.getAnswers())
	case <-time.After(d):
	}
}

func (env *permTimeoutEnv) finishTurn() {
	env.t.Helper()
	env.sess.events <- Event{Type: EventResult, Content: "ok", Done: true}
	select {
	case <-env.done:
	case <-time.After(3 * time.Second):
		env.t.Fatal("turn did not finish after the permission prompt")
	}
}

func waitPermPending(t *testing.T, state *interactiveState) *pendingPermission {
	t.Helper()
	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		state.mu.Lock()
		p := state.pending
		state.mu.Unlock()
		if p != nil {
			return p
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("permission request never became pending")
	return nil
}

// Nobody answers: the request is denied with a message asking for another
// way, the chat is told, the hook gets the session and command, and the turn
// goes on.
func TestPermissionTimeout_DeniesAndTurnContinues(t *testing.T) {
	env := startPermTimeoutTurn(t, 80*time.Millisecond)
	env.askBash(`timeout 3 bash -c "</dev/tcp/10.0.0.1/22"`)
	a := env.waitAnswer(2 * time.Second)
	if a.Behavior != "deny" {
		t.Fatalf("behavior = %q, want deny", a.Behavior)
	}
	for _, want := range []string{"another way that does not need approval", "换一种不需要审批的写法"} {
		if !strings.Contains(a.Message, want) {
			t.Errorf("deny message %q lacks %q", a.Message, want)
		}
	}
	env.state.mu.Lock()
	stillPending := env.state.pending != nil
	env.state.mu.Unlock()
	if stillPending {
		t.Error("pending permission left in place after the timeout")
	}
	// The chat notice and the hook come right after the agent is answered.
	var raw []byte
	notified := false
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end) && (!notified || raw == nil); time.Sleep(10 * time.Millisecond) {
		for _, m := range env.p.getSent() {
			if strings.Contains(m, "denied automatically") && strings.Contains(m, "Bash") {
				notified = true
			}
		}
		if b, err := os.ReadFile(env.hookOut); err == nil && len(b) > 0 && json.Valid(b) {
			raw = b
		}
	}
	if !notified {
		t.Errorf("chat not told about the timeout: %q", env.p.getSent())
	}
	if raw == nil {
		t.Fatal("permission.timed_out hook did not run")
	}
	var ev HookEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("hook stdin is not an event: %v (%s)", err, raw)
	}
	if ev.Event != HookEventPermissionTimedOut || ev.SessionKey != env.key {
		t.Errorf("hook event = %s / %s", ev.Event, ev.SessionKey)
	}
	if ev.Extra["command"] != `timeout 3 bash -c "</dev/tcp/10.0.0.1/22"` || ev.Extra["tool_name"] != "Bash" || ev.Extra["session_id"] == "" {
		t.Errorf("hook extra = %+v", ev.Extra)
	}
	env.finishTurn()
	if n := len(env.sess.getAnswers()); n != 1 {
		t.Errorf("%d answers, want exactly one", n)
	}
}

// A click on the old card after the timeout neither answers again nor is
// forwarded to the agent as text.
func TestPermissionTimeout_LateClickIsDropped(t *testing.T) {
	env := startPermTimeoutTurn(t, 50*time.Millisecond)
	env.askBash("rm -f ./x")
	if a := env.waitAnswer(2 * time.Second); a.Behavior != "deny" {
		t.Fatalf("first answer %q, want deny", a.Behavior)
	}
	handled := env.e.handlePendingPermission(env.p, &Message{SessionKey: env.key, ReplyCtx: "ctx", IsPermissionResponse: true}, "allow", "")
	if !handled {
		t.Error("late permission click was not consumed (would reach the agent as text)")
	}
	env.noAnswerFor(150 * time.Millisecond)
	env.finishTurn()
}

// The timeout and the user's answer race: whoever claims the request first
// answers it, the other does nothing.
func TestPermissionTimeout_AnswerAfterClaimIsDropped(t *testing.T) {
	env := startPermTimeoutTurn(t, 0)
	env.askBash("ls")
	pending := waitPermPending(t, env.state)
	if !pending.claim() { // the timeout got there first
		t.Fatal("fresh request already claimed")
	}
	if !env.e.handlePendingPermission(env.p, &Message{SessionKey: env.key, ReplyCtx: "ctx"}, "allow", "") {
		t.Error("answer to a claimed request was not consumed")
	}
	env.noAnswerFor(150 * time.Millisecond)
	pending.resolve()
	env.finishTurn()
}

// A user answer before the timeout is the only answer; the timer firing later
// changes nothing.
func TestPermissionTimeout_UserAnswerFirst(t *testing.T) {
	env := startPermTimeoutTurn(t, 200*time.Millisecond)
	env.askBash("ls")
	waitPermPending(t, env.state)
	if !env.e.handlePendingPermission(env.p, &Message{SessionKey: env.key, ReplyCtx: "ctx"}, "allow", "") {
		t.Fatal("allow not handled")
	}
	if a := env.waitAnswer(time.Second); a.Behavior != "allow" {
		t.Fatalf("answer %q, want allow", a.Behavior)
	}
	env.noAnswerFor(400 * time.Millisecond)
	env.finishTurn()
}

// AskUserQuestion is a question for the user, not a tool permission: it waits.
func TestPermissionTimeout_AskUserQuestionWaits(t *testing.T) {
	env := startPermTimeoutTurn(t, 50*time.Millisecond)
	env.sess.events <- Event{
		Type:         EventPermissionRequest,
		RequestID:    "req-q",
		ToolName:     "AskUserQuestion",
		ToolInputRaw: map[string]any{"questions": []any{}},
		Questions:    []UserQuestion{{Question: "Which?", Options: []UserQuestionOption{{Label: "A"}, {Label: "B"}}}},
	}
	waitPermPending(t, env.state)
	env.noAnswerFor(300 * time.Millisecond)
	if !env.e.handlePendingPermission(env.p, &Message{SessionKey: env.key, ReplyCtx: "ctx"}, "1", "") {
		t.Fatal("answer not handled")
	}
	if a := env.waitAnswer(time.Second); a.Behavior != "allow" {
		t.Fatalf("answer %q, want allow", a.Behavior)
	}
	env.finishTurn()
}

// 0 keeps the old behaviour: wait for the user however long it takes.
func TestPermissionTimeout_ZeroWaitsForever(t *testing.T) {
	env := startPermTimeoutTurn(t, 0)
	env.askBash("ls")
	waitPermPending(t, env.state)
	env.noAnswerFor(300 * time.Millisecond)
	if !env.e.handlePendingPermission(env.p, &Message{SessionKey: env.key, ReplyCtx: "ctx"}, "deny", "") {
		t.Fatal("deny not handled")
	}
	if a := env.waitAnswer(time.Second); a.Behavior != "deny" || strings.Contains(a.Message, "automatically") {
		t.Fatalf("answer %+v, want the user's deny", a)
	}
	env.finishTurn()
	if _, err := os.Stat(env.hookOut); err == nil {
		t.Error("permission.timed_out hook ran without a timeout")
	}
}

// permCardPlatform sends cards as messages with IDs (CardMessageSender) and
// records every card sent and every patch.
type permCardPlatform struct {
	stubPlatformEngine
	cmu     sync.Mutex
	cards   []*Card
	patches []permCardPatch
}

type permCardPatch struct {
	id   string
	card *Card
}

func (p *permCardPlatform) SendCard(ctx context.Context, rctx any, c *Card) error {
	_, err := p.SendCardMessage(ctx, rctx, c)
	return err
}
func (p *permCardPlatform) ReplyCard(ctx context.Context, rctx any, c *Card) error {
	return p.SendCard(ctx, rctx, c)
}
func (p *permCardPlatform) SendCardMessage(_ context.Context, _ any, c *Card) (string, error) {
	p.cmu.Lock()
	defer p.cmu.Unlock()
	p.cards = append(p.cards, c)
	return fmt.Sprintf("om_card_%d", len(p.cards)), nil
}
func (p *permCardPlatform) PatchCardMessage(_ context.Context, id string, c *Card) error {
	p.cmu.Lock()
	defer p.cmu.Unlock()
	p.patches = append(p.patches, permCardPatch{id, c})
	return nil
}
func (p *permCardPlatform) snapshot() ([]*Card, []permCardPatch) {
	p.cmu.Lock()
	defer p.cmu.Unlock()
	return append([]*Card(nil), p.cards...), append([]permCardPatch(nil), p.patches...)
}

// On a timeout the permission card itself is replaced by one without
// buttons that says it expired, so nobody can click it afterwards.
func TestPermissionTimeout_ExpiresTheCard(t *testing.T) {
	cp := &permCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	env := startPermTimeoutTurnOn(t, 80*time.Millisecond, cp, &cp.stubPlatformEngine)
	cmd := `exec 3<>/dev/tcp/127.0.0.1/9 # timeout test, do not click`
	env.askBash(cmd)
	if a := env.waitAnswer(2 * time.Second); a.Behavior != "deny" {
		t.Fatalf("answer %q, want deny", a.Behavior)
	}
	var cards []*Card
	var patches []permCardPatch
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end) && len(patches) == 0; time.Sleep(10 * time.Millisecond) {
		cards, patches = cp.snapshot()
	}
	if len(cards) != 1 || !cards[0].HasButtons() {
		t.Fatalf("permission card not sent as one card with buttons: %d cards", len(cards))
	}
	if len(patches) != 1 {
		t.Fatalf("%d patches of the card, want 1", len(patches))
	}
	pt := patches[0]
	if pt.id != "om_card_1" {
		t.Errorf("patched message %q, want the permission card om_card_1", pt.id)
	}
	if pt.card.HasButtons() {
		t.Error("expired card still has buttons")
	}
	if pt.card.Header == nil || !strings.Contains(pt.card.Header.Title, "Expired") || !strings.Contains(pt.card.Header.Title, "denied automatically") {
		t.Errorf("expired card header = %+v", pt.card.Header)
	}
	if !strings.Contains(pt.card.RenderText(), "127.0.0.1/9") {
		t.Errorf("expired card does not show the request: %q", pt.card.RenderText())
	}
	if !env.e.handlePendingPermission(cp, &Message{SessionKey: env.key, ReplyCtx: "ctx", IsPermissionResponse: true}, "allow", "") {
		t.Error("late click not consumed")
	}
	env.noAnswerFor(150 * time.Millisecond)
	env.finishTurn()
}

// A user answer leaves the card alone: only a timeout marks it expired.
func TestPermissionTimeout_AnsweredCardNotPatched(t *testing.T) {
	cp := &permCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	env := startPermTimeoutTurnOn(t, 300*time.Millisecond, cp, &cp.stubPlatformEngine)
	env.askBash("ls")
	waitPermPending(t, env.state)
	if !env.e.handlePendingPermission(cp, &Message{SessionKey: env.key, ReplyCtx: "ctx"}, "allow", "") {
		t.Fatal("allow not handled")
	}
	env.waitAnswer(time.Second)
	env.noAnswerFor(500 * time.Millisecond)
	if _, patches := cp.snapshot(); len(patches) != 0 {
		t.Errorf("card patched after the user answered: %d", len(patches))
	}
	env.finishTurn()
}
