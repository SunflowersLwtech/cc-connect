package core

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// A turn the agent runs on its own (e.g. Claude Code answering a background
// task notification) while the engine is still delivering the previous reply
// sits in the event buffer until the next turn starts. Before a queued turn,
// the engine used to discard the whole buffer, and with it that reply.

func waitQueuingSends(sess *queuingAgentSession, n int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sess.sendMu.Lock()
		got := len(sess.sendCalls)
		sess.sendMu.Unlock()
		if got >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func recordedSends(p *replyCtxRecordingPlatform) []string {
	var out []string
	for _, ev := range p.recordedEvents() {
		out = append(out, fmt.Sprintf("%v: %s", ev.replyCtx, ev.content))
	}
	return out
}

func historyLines(s *Session) []string {
	var out []string
	for _, h := range s.GetHistory(0) {
		out = append(out, h.Role+": "+h.Content)
	}
	return out
}

func assertLines(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s:\n  got  %q\n  want %q", what, got, want)
	}
}

// runQueuedTurnWithBuffer runs turn 1 with msg2 queued. buffered is what the
// agent emitted after turn 1's result, before the engine read on.
func runQueuedTurnWithBuffer(t *testing.T, buffered []Event) (*replyCtxRecordingPlatform, *Session) {
	t.Helper()
	p := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	sess := newQueuingSession("buffered-turns")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx-turn1",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-msg2", content: "msg2"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	sess.events <- Event{Type: EventResult, Content: "reply1", Done: true}
	for _, ev := range buffered {
		sess.events <- ev
	}
	go func() {
		if waitQueuingSends(sess, 1) {
			sess.events <- Event{Type: EventResult, Content: "reply2", Done: true}
		}
	}()

	session.AddHistory("user", "msg1")
	sendDone := make(chan error, 1)
	sendDone <- nil
	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "m1", time.Now(), nil, sendDone, "ctx-turn1", 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("processInteractiveEvents did not complete; sent so far %q", recordedSends(p))
	}

	sess.sendMu.Lock()
	calls := append([]string(nil), sess.sendCalls...)
	sess.sendMu.Unlock()
	if len(calls) != 1 || !strings.Contains(calls[0], "msg2") {
		t.Fatalf("agent sends = %q, want only msg2", calls)
	}
	return p, session
}

func TestQueuedTurn_RelaysBufferedUnsolicitedTurn(t *testing.T) {
	p, session := runQueuedTurnWithBuffer(t, []Event{
		{Type: EventText, Content: "background "},
		{Type: EventToolUse, ToolName: "Bash"},
		{Type: EventToolResult, ToolStatus: "completed"},
		{Type: EventText, Content: "task finished"},
		{Type: EventResult, Done: true},
	})

	// Delivered where turn 1's reply went, before msg2's reply.
	assertLines(t, "platform sends", recordedSends(p), []string{
		"ctx-turn1: reply1",
		"ctx-turn1: background task finished",
		"ctx-msg2: reply2",
	})
	assertLines(t, "history", historyLines(session), []string{
		"user: msg1",
		"assistant: reply1",
		"assistant: background task finished",
		"user: msg2",
		"assistant: reply2",
	})
}

func TestQueuedTurn_BufferedEvents(t *testing.T) {
	cases := []struct {
		name     string
		buffered []Event
		want     []string
	}{
		{
			name:     "deferred error after the reply is dropped",
			buffered: []Event{{Type: EventError, Error: errors.New("exit status 1")}},
			want:     []string{"reply1", "reply2"},
		},
		{
			name:     "start of a turn still running is dropped",
			buffered: []Event{{Type: EventText, Content: "half a turn"}, {Type: EventToolUse, ToolName: "Bash"}},
			want:     []string{"reply1", "reply2"},
		},
		{
			name: "an error ends the turn before it",
			buffered: []Event{
				{Type: EventText, Content: "dead turn "},
				{Type: EventError, Error: errors.New("boom")},
				{Type: EventText, Content: "next turn"},
				{Type: EventResult, Done: true},
			},
			want: []string{"reply1", "next turn", "reply2"},
		},
		{
			name: "compaction result does not end the turn",
			buffered: []Event{
				{Type: EventText, Content: "before "},
				{Type: EventResult, Done: false},
				{Type: EventText, Content: "after"},
				{Type: EventResult, Done: true},
			},
			want: []string{"reply1", "before after", "reply2"},
		},
		{
			name: "result content wins over streamed text",
			buffered: []Event{
				{Type: EventText, Content: "draft"},
				{Type: EventResult, Content: "final", Done: true},
			},
			want: []string{"reply1", "final", "reply2"},
		},
		{
			name: "every complete turn is relayed in order",
			buffered: []Event{
				{Type: EventResult, Content: "first", Done: true},
				{Type: EventText, Content: "second"},
				{Type: EventResult, Done: true},
				{Type: EventText, Content: "third, unfinished"},
			},
			want: []string{"reply1", "first", "second", "reply2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := runQueuedTurnWithBuffer(t, tc.buffered)
			var got []string
			for _, ev := range p.recordedEvents() {
				got = append(got, ev.content)
			}
			assertLines(t, "platform sends", got, tc.want)
		})
	}
}

// drainPendingMessages starts queued turns after processInteractiveEvents
// has returned; it clears the buffer the same way.
func TestDrainPendingMessages_RelaysBufferedUnsolicitedTurn(t *testing.T) {
	p := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	sess := newQueuingSession("drain-pending")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	gen, ok := session.TryLock()
	if !ok {
		t.Fatal("TryLock failed")
	}
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx-turn1",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-msg2", content: "msg2"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	sess.events <- Event{Type: EventText, Content: "background result"}
	sess.events <- Event{Type: EventResult, Done: true}
	go func() {
		if waitQueuingSends(sess, 1) {
			sess.events <- Event{Type: EventResult, Content: "reply2", Done: true}
		}
	}()

	done := make(chan bool, 1)
	go func() { done <- e.drainPendingMessages(state, session, e.sessions, key, gen) }()
	select {
	case unlocked := <-done:
		if !unlocked {
			t.Fatal("drainPendingMessages did not unlock the session")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("drainPendingMessages did not complete; sent so far %q", recordedSends(p))
	}

	assertLines(t, "platform sends", recordedSends(p), []string{
		"ctx-turn1: background result",
		"ctx-msg2: reply2",
	})
	assertLines(t, "history", historyLines(session), []string{
		"assistant: background result",
		"user: msg2",
		"assistant: reply2",
	})
}

// The compress command is also sent after clearing the buffer.
func TestRunCompress_RelaysBufferedUnsolicitedTurn(t *testing.T) {
	p := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	sess := newQueuingSession("compress-buffered")
	e := NewEngine("test", &stubCompressorAgent{cmd: "/compact"}, []Platform{p}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:user1"
	state := &interactiveState{agentSession: sess, platform: p, replyCtx: "ctx-turn1"}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	sess.events <- Event{Type: EventText, Content: "background result"}
	sess.events <- Event{Type: EventResult, Done: true}

	e.cmdCompress(p, &Message{SessionKey: key, Content: "/compress", ReplyCtx: "ctx-compress"})
	if !waitQueuingSends(sess, 1) {
		t.Fatalf("compress command never sent; sent so far %q", recordedSends(p))
	}
	sess.events <- Event{Type: EventResult, Done: true}

	doneText := e.i18n.T(MsgCompressDone)
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(strings.Join(recordedSends(p), "\n"), doneText) {
		if time.Now().After(deadline) {
			t.Fatalf("compress never finished; sent %q", recordedSends(p))
		}
		time.Sleep(10 * time.Millisecond)
	}

	sends := recordedSends(p)
	relayed := -1
	for i, s := range sends {
		if s == "ctx-turn1: background result" {
			relayed = i
		}
	}
	if relayed < 0 {
		t.Fatalf("background result not relayed to the previous turn's context; sent %q", sends)
	}
	if !strings.Contains(sends[len(sends)-1], doneText) {
		t.Fatalf("background result relayed after the compress finished; sent %q", sends)
	}
	sess.sendMu.Lock()
	calls := append([]string(nil), sess.sendCalls...)
	sess.sendMu.Unlock()
	assertLines(t, "agent sends", calls, []string{"/compact"})
}

// A buffered turn that fell back to another model: the user hears about it,
// and with model_fallback_action = "restore" the session is switched back
// before the queued message reaches the agent.
func TestQueuedTurn_BufferedTurnModelFallbackRestoredBeforeQueuedMessage(t *testing.T) {
	p := &stubPlatformEngine{n: "feishu"}
	agentSession := &liveModelSession{controllableAgentSession: newControllableSession("buffered-restore"), platform: p}
	h := newModelSwitchHarness(t, p, p, agentSession)
	h.e.SetModelFallbackAction("restore")
	h.state.pendingMessages = []queuedMessage{{platform: p, replyCtx: "ctx-msg2", content: "msg2"}}

	events := agentSession.events
	events <- Event{Type: EventResult, Content: "reply1", Done: true}
	events <- Event{Type: EventModelSwitch, ModelSwitch: opusCyberFallback()}
	events <- Event{Type: EventText, Content: "answer from the fallback model"}
	events <- Event{Type: EventResult, Done: true}
	go func() {
		// The queued turn's reply arrives once the session has switched
		// back, or after a second if it never does.
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if calls, _ := agentSession.switches(); len(calls) > 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		events <- Event{Type: EventResult, Content: "reply2", Done: true}
	}()

	done := make(chan struct{})
	go func() {
		h.e.processInteractiveEvents(h.state, h.session, h.e.sessions, h.key, "m1", time.Now(), nil, nil, h.state.replyCtx, 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("processInteractiveEvents did not complete; sent so far %q", p.getSent())
	}

	assertSent(t, p.getSent(), []string{"reply1", wantRestoringNotice, "answer from the fallback model", wantRestored, "reply2"})
	calls, sentBefore := agentSession.switches()
	if fmt.Sprint(calls) != "[claude-opus-5-5]" {
		t.Fatalf("SetLiveModel calls = %v, want [claude-opus-5-5]", calls)
	}
	if sentBefore[0] != 3 {
		t.Fatalf("switched back with %d messages out, want 3 (after the buffered turn's reply)", sentBefore[0])
	}
}

// Background turns follow the foreground NO_REPLY rule: a bare marker sends
// nothing, a trailing one is cut off; the history keeps what the agent said.
func TestQueuedTurn_BufferedNoReplyTurnStaysSilent(t *testing.T) {
	p, session := runQueuedTurnWithBuffer(t, []Event{
		{Type: EventText, Content: "NO_REPLY"},
		{Type: EventResult, Content: "NO_REPLY", Done: true},
		{Type: EventText, Content: "checked the deploy, all green\n\nNO_REPLY"},
		{Type: EventResult, Done: true},
	})
	assertLines(t, "platform sends", recordedSends(p), []string{
		"ctx-turn1: reply1",
		"ctx-turn1: checked the deploy, all green",
		"ctx-msg2: reply2",
	})
	assertLines(t, "history", historyLines(session), []string{
		"user: msg1",
		"assistant: reply1",
		"assistant: NO_REPLY",
		"assistant: checked the deploy, all green\n\nNO_REPLY",
		"user: msg2",
		"assistant: reply2",
	})
}

// The unsolicited reader shares the delivery, so the same rule applies there.
func TestUnsolicitedReader_NoReplyTurnStaysSilent(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("reader-noreply")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	key := "test:ch1:u1"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{agentSession: sess, platform: p, replyCtx: "ctx"}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()
	e.startUnsolicitedReader(state, session, e.sessions, key, "")
	defer e.stopUnsolicitedReader(state)

	sess.events <- Event{Type: EventResult, Content: "NO_REPLY", Done: true}
	sess.events <- Event{Type: EventResult, Content: "background check done", Done: true}
	sent := waitForPlatformSend(p, 1, 2*time.Second)
	assertLines(t, "platform sends", sent, []string{"background check done"})
	assertLines(t, "history", historyLines(session), []string{
		"assistant: NO_REPLY",
		"assistant: background check done",
	})
}

type permAnsweringSession struct {
	*queuingAgentSession
	mu      sync.Mutex
	answers []string
}

// RespondPermission records the answer and how many prompts the agent had
// been sent at that point.
func (s *permAnsweringSession) RespondPermission(id string, r PermissionResult) error {
	s.sendMu.Lock()
	before := len(s.sendCalls)
	s.sendMu.Unlock()
	s.mu.Lock()
	s.answers = append(s.answers, fmt.Sprintf("%s:%s after %d sends", id, r.Behavior, before))
	s.mu.Unlock()
	return nil
}

// A background turn stopped at a permission request is answered before the
// queued message goes out, instead of being dropped and leaving the agent
// blocked until the idle timeout.
func TestQueuedTurn_BufferedPermissionRequestIsAnswered(t *testing.T) {
	notice := func(tool string) string {
		return "ctx-turn1: " + fmt.Sprintf(NewI18n(LangEnglish).T(MsgBackgroundAutoDenied), tool)
	}
	cases := []struct {
		name       string
		tool       string
		yolo       bool
		wantAnswer string
		wantSends  []string
	}{
		{"question, nobody to ask", "AskUserQuestion", false, "rq1:deny after 0 sends",
			[]string{"ctx-turn1: reply1", notice("AskUserQuestion"), "ctx-msg2: reply2"}},
		{"question under /yolo is still denied", "AskUserQuestion", true, "rq1:deny after 0 sends",
			[]string{"ctx-turn1: reply1", notice("AskUserQuestion"), "ctx-msg2: reply2"}},
		{"tool, nobody to ask", "Bash", false, "rq1:deny after 0 sends",
			[]string{"ctx-turn1: reply1", notice("Bash"), "ctx-msg2: reply2"}},
		{"tool under /yolo", "Bash", true, "rq1:allow after 0 sends",
			[]string{"ctx-turn1: reply1", "ctx-msg2: reply2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
			sess := &permAnsweringSession{queuingAgentSession: newQueuingSession("buffered-perm")}
			e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, "", LangEnglish)
			t.Cleanup(func() { _ = e.Stop() })
			key := "test:user1"
			session := e.sessions.GetOrCreateActive(key)
			state := &interactiveState{agentSession: sess, platform: p, replyCtx: "ctx-turn1", approveAll: tc.yolo,
				pendingMessages: []queuedMessage{{platform: p, replyCtx: "ctx-msg2", content: "msg2"}}}
			e.interactiveMu.Lock()
			e.interactiveStates[key] = state
			e.interactiveMu.Unlock()

			sess.events <- Event{Type: EventResult, Content: "reply1", Done: true}
			sess.events <- Event{Type: EventText, Content: "background: which one?"}
			ev := Event{Type: EventPermissionRequest, ToolName: tc.tool, RequestID: "rq1"}
			if tc.tool == "AskUserQuestion" {
				ev.Questions = []UserQuestion{{Question: "A or B?", Options: []UserQuestionOption{{Label: "A"}, {Label: "B"}}}}
			}
			sess.events <- ev
			go func() {
				if waitQueuingSends(sess.queuingAgentSession, 1) {
					sess.events <- Event{Type: EventResult, Content: "reply2", Done: true}
				}
			}()
			sendDone := make(chan error, 1)
			sendDone <- nil
			done := make(chan struct{})
			go func() {
				e.processInteractiveEvents(state, session, e.sessions, key, "m1", time.Now(), nil, sendDone, "ctx-turn1", 0)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("did not complete; sent %q", recordedSends(p))
			}

			sess.mu.Lock()
			answers := append([]string(nil), sess.answers...)
			sess.mu.Unlock()
			assertLines(t, "answers to the agent", answers, []string{tc.wantAnswer})
			assertLines(t, "platform sends", recordedSends(p), tc.wantSends)
		})
	}
}

// Each buffered turn is sent on its own; text does not carry over to the next.
func TestQueuedTurn_ConsecutiveBufferedTurnsDoNotShareText(t *testing.T) {
	p, _ := runQueuedTurnWithBuffer(t, []Event{
		{Type: EventText, Content: "one"},
		{Type: EventResult, Done: true},
		{Type: EventText, Content: "two"},
		{Type: EventResult, Done: true},
	})
	assertLines(t, "platform sends", recordedSends(p), []string{
		"ctx-turn1: reply1", "ctx-turn1: one", "ctx-turn1: two", "ctx-msg2: reply2",
	})
}

// A long background reply is split to the platform's message limit.
func TestUnsolicitedReader_SplitsLongReply(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("reader-long")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	key := "test:ch1:u1"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{agentSession: sess, platform: p, replyCtx: "ctx"}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()
	e.startUnsolicitedReader(state, session, e.sessions, key, "")
	defer e.stopUnsolicitedReader(state)

	long := strings.Repeat("line of background output\n", 400) // ~10k chars
	sess.events <- Event{Type: EventResult, Content: long, Done: true}
	sent := waitForPlatformSend(p, 2, 2*time.Second)
	if len(sent) < 2 {
		t.Fatalf("want the long reply split, got %d sends", len(sent))
	}
	for _, s := range sent {
		if len(s) > maxPlatformMessageLen {
			t.Fatalf("sent a %d-char message, limit %d (%d sends)", len(s), maxPlatformMessageLen, len(sent))
		}
	}
}
