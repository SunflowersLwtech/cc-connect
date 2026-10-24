package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Hook events that identify messages by platform ID, so a hook can keep every
// message the owner sends (including ones sent while the agent is busy) and
// tie replies and follow-ups to the conversation they belong to.

type hookCollector struct {
	mu     sync.Mutex
	events []HookEvent
	srv    *httptest.Server
}

func newHookCollector(t *testing.T) *hookCollector {
	t.Helper()
	c := &hookCollector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev HookEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			t.Errorf("hook body: %v", err)
			return
		}
		c.mu.Lock()
		c.events = append(c.events, ev)
		c.mu.Unlock()
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *hookCollector) hooks(events ...HookEventType) *HookManager {
	var cfg []HookConfig
	for _, ev := range events {
		cfg = append(cfg, HookConfig{Event: string(ev), Type: "http", URL: c.srv.URL})
	}
	return NewHookManager("test", cfg, "sh", "-c", "")
}

// waitFor returns the events of one type once there are at least n of them.
func (c *hookCollector) waitFor(t *testing.T, ev HookEventType, n int) []HookEvent {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.mu.Lock()
		var got []HookEvent
		for _, e := range c.events {
			if e.Event == ev {
				got = append(got, e)
			}
		}
		c.mu.Unlock()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %d %s hook events, want %d: %+v", len(got), ev, n, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func extraString(ev HookEvent, key string) string {
	s, _ := ev.Extra[key].(string)
	return s
}

// A message that reaches the engine while the agent is busy is reported with
// its ID whichever way it then goes: queued, dropped as a stale redelivery,
// or injected into the running turn with /ps.
func TestMessageReceivedHook_ReportsMessagesWhileBusy(t *testing.T) {
	cases := []struct {
		name      string
		content   string
		createdMs int64
		check     func(t *testing.T, state *interactiveState, as *queuingAgentSession)
	}{
		{"queued", "also check the logs", 6_000, func(t *testing.T, state *interactiveState, _ *queuingAgentSession) {
			state.mu.Lock()
			defer state.mu.Unlock()
			if len(state.pendingMessages) != 1 {
				t.Fatalf("queue depth = %d, want 1 (the message was not queued)", len(state.pendingMessages))
			}
		}},
		{"stale redelivery", "an old copy", 3_000, func(t *testing.T, state *interactiveState, _ *queuingAgentSession) {
			state.mu.Lock()
			defer state.mu.Unlock()
			if len(state.pendingMessages) != 0 {
				t.Fatalf("queue depth = %d, want 0 (the stale copy was queued)", len(state.pendingMessages))
			}
		}},
		{"/ps", "/ps change of plan", 6_000, func(t *testing.T, _ *interactiveState, as *queuingAgentSession) {
			as.sendMu.Lock()
			defer as.sendMu.Unlock()
			if len(as.sendCalls) != 1 || as.sendCalls[0] != "change of plan" {
				t.Fatalf("agent got %q, want the /ps text injected", as.sendCalls)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &stubPlatformEngine{n: "test"}
			as := newQueuingSession("busy")
			e := NewEngine("test", &controllableAgent{nextSession: as}, []Platform{p}, "", LangEnglish)
			t.Cleanup(func() { _ = e.Stop() })
			c := newHookCollector(t)
			e.SetHooks(c.hooks(HookEventMessageReceived))

			key := "test:owner"
			session := e.sessions.GetOrCreateActive(key)
			if _, ok := session.TryLock(); !ok {
				t.Fatal("could not lock session")
			}
			state := &interactiveState{
				agentSession:                   as,
				busySession:                    session,
				platform:                       p,
				replyCtx:                       "ctx",
				lastCompletedUserMessageTimeMs: 5_000,
			}
			e.interactiveMu.Lock()
			e.interactiveStates[key] = state
			e.interactiveMu.Unlock()

			e.handleMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "om_mid_turn", UserID: "u1",
				UserName: "owner", Content: tc.content, ReplyCtx: "ctx-new", UserMessageTimeMs: tc.createdMs})

			tc.check(t, state, as)
			ev := c.waitFor(t, HookEventMessageReceived, 1)[0]
			if extraString(ev, "message_id") != "om_mid_turn" || ev.Content != tc.content {
				t.Fatalf("message.received extra=%v content=%q, want message_id om_mid_turn and the text", ev.Extra, ev.Content)
			}
		})
	}
}

// A reply to an earlier message reports which message it answers, the chain's
// root and the topic; the user's own text stays separate from the quote.
func TestMessageReceivedHook_ReportsReplyRefs(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	c := newHookCollector(t)
	e.SetHooks(c.hooks(HookEventMessageReceived))
	session := e.sessions.GetOrCreateActive("test:owner")
	if _, ok := session.TryLock(); !ok {
		t.Fatal("could not lock session")
	}

	e.handleMessage(p, &Message{SessionKey: "test:owner", Platform: "test", MessageID: "om_follow_up", UserID: "u1",
		Content: "do that one too", ExtraContent: "[Quoted message from bot]:\nearlier answer",
		ReplyToMessageID: "om_bot_answer", ThreadRootID: "om_first", PlatformThreadID: "omt_topic",
		UserMessageTimeMs: 1_792_800_000_000, ReplyCtx: "ctx"})

	ev := c.waitFor(t, HookEventMessageReceived, 1)[0]
	want := map[string]string{"message_id": "om_follow_up", "reply_to_message_id": "om_bot_answer",
		"root_message_id": "om_first", "platform_thread_id": "omt_topic"}
	for k, v := range want {
		if extraString(ev, k) != v {
			t.Fatalf("extra[%s] = %v, want %q (extra=%v)", k, ev.Extra[k], v, ev.Extra)
		}
	}
	if ms, _ := ev.Extra["created_ms"].(float64); int64(ms) != 1_792_800_000_000 {
		t.Fatalf("extra[created_ms] = %v", ev.Extra["created_ms"])
	}
	if ev.Content != "do that one too" {
		t.Fatalf("content = %q, want only the user's own text", ev.Content)
	}
}

// Each reply names the user message whose turn produced it, also when the
// turn was started by a message that had been queued.
func TestMessageSentHook_InReplyToFollowsQueuedTurn(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("sent-refs")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	c := newHookCollector(t)
	e.SetHooks(c.hooks(HookEventMessageSent))

	key := "test:owner"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx-1",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-2", content: "second question", messageID: "om_2"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	sess.events <- Event{Type: EventResult, Content: "first answer", Done: true}
	go func() {
		if waitQueuingSends(sess, 1) {
			sess.events <- Event{Type: EventResult, Content: "second answer", Done: true}
		}
	}()
	sendDone := make(chan error, 1)
	sendDone <- nil
	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "om_1", time.Now(), nil, sendDone, "ctx-1", 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processInteractiveEvents did not complete")
	}

	var got []string
	for _, ev := range c.waitFor(t, HookEventMessageSent, 2) {
		got = append(got, ev.Content+" <- "+extraString(ev, "in_reply_to"))
	}
	sort.Strings(got)
	if strings.Join(got, "; ") != "first answer <- om_1; second answer <- om_2" {
		t.Fatalf("message.sent = %q", got)
	}
}

// A command hook gets the whole event, Extra included, as JSON on stdin.
func TestCommandHook_ReceivesEventJSONOnStdin(t *testing.T) {
	out := filepath.Join(t.TempDir(), "event.json")
	async := false
	hm := NewHookManager("test", []HookConfig{{Event: "message.received", Type: "command",
		Command: fmt.Sprintf("cat > '%s'", out), Async: &async}}, "sh", "-c", "")
	hm.Emit(HookEvent{Event: HookEventMessageReceived, Content: "你好", Extra: map[string]any{"message_id": "om_stdin"}})

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read hook output: %v", err)
	}
	var ev HookEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("stdin was not the event JSON: %q (%v)", raw, err)
	}
	if extraString(ev, "message_id") != "om_stdin" || ev.Content != "你好" || ev.Project != "test" {
		t.Fatalf("stdin event = %+v", ev)
	}
}

type deliveringPlatform struct {
	stubPlatformEngine
	mu sync.Mutex
	fn func(DeliveredMessage)
}

func (p *deliveringPlatform) SetDeliveryObserver(fn func(DeliveredMessage)) {
	p.mu.Lock()
	p.fn = fn
	p.mu.Unlock()
}

func (p *deliveringPlatform) deliver(d DeliveredMessage) {
	p.mu.Lock()
	fn := p.fn
	p.mu.Unlock()
	if fn != nil {
		fn(d)
	}
}

// Messages a platform reports as sent become message.delivered events.
func TestEngineStart_ReportsDeliveredMessages(t *testing.T) {
	p := &deliveringPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	c := newHookCollector(t)
	e.SetHooks(c.hooks(HookEventMessageDelivered))
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	p.deliver(DeliveredMessage{MessageID: "om_bot_1", InReplyTo: "om_user", ChatID: "oc_chat"})

	ev := c.waitFor(t, HookEventMessageDelivered, 1)[0]
	if ev.Platform != "feishu" || extraString(ev, "message_id") != "om_bot_1" ||
		extraString(ev, "in_reply_to") != "om_user" || extraString(ev, "chat_id") != "oc_chat" {
		t.Fatalf("message.delivered = %+v", ev)
	}
}

// A voice message is reported twice under one ID: first with no text, marked
// as waiting for transcription, then with the transcript.
func TestReceivedHookExtra_MarksVoiceAwaitingTranscription(t *testing.T) {
	audio := &Message{MessageID: "om_voice", Audio: &AudioAttachment{Format: "ogg"}}
	if got := receivedHookExtra(audio, true); got["pending_transcription"] != true || got["message_id"] != "om_voice" {
		t.Fatalf("before transcription: %v", got)
	}
	transcribed := &Message{MessageID: "om_voice", Content: "说完了", FromVoice: true}
	got := receivedHookExtra(transcribed, false)
	if got["from_voice"] != true || got["pending_transcription"] != nil {
		t.Fatalf("after transcription: %v", got)
	}
	for _, k := range []string{"reply_to_message_id", "root_message_id", "platform_thread_id", "created_ms"} {
		if _, ok := got[k]; ok {
			t.Fatalf("extra has %s for a message without it: %v", k, got)
		}
	}
}
