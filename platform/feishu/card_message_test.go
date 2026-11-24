package feishu

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
)

// cardMessageServer fakes the IM API: a create or reply answers with message
// ID om_card_1, and every request is recorded.
type cardMessageServer struct {
	mu   sync.Mutex
	reqs []string // "METHOD path body"
}

func (s *cardMessageServer) start(t *testing.T) (*httptest.Server, *interactivePlatform) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "expire": 7200, "tenant_access_token": "tok"})
			return
		}
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reqs = append(s.reqs, r.Method+" "+r.URL.Path+" "+string(b))
		s.mu.Unlock()
		writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "data": map[string]any{"message_id": "om_card_1"}})
	}))
	t.Cleanup(srv.Close)
	p := &Platform{
		platformName: "feishu",
		domain:       srv.URL,
		appID:        "cli_card_msg",
		appSecret:    "secret",
		client:       lark.NewClient("cli_card_msg", "secret", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())),
		replayClient: lark.NewClient("cli_card_msg", "secret", lark.WithEnableTokenCache(false), lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())),
	}
	return srv, &interactivePlatform{Platform: p}
}

func (s *cardMessageServer) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.reqs...)
}

func permCard() *core.Card {
	return core.NewCard().Title("Permission", "orange").Markdown("Bash: ls").
		ButtonsEqual(core.CardButton{Text: "Allow", Value: "perm:allow"}, core.CardButton{Text: "Deny", Value: "perm:deny"}).Build()
}

func TestSendCardMessage_ReturnsTheMessageID(t *testing.T) {
	s := &cardMessageServer{}
	_, p := s.start(t)
	id, err := p.SendCardMessage(context.Background(), replyContext{chatID: "oc_chat", sessionKey: "feishu:oc_chat:ou_user"}, permCard())
	if err != nil {
		t.Fatalf("SendCardMessage: %v", err)
	}
	if id != "om_card_1" {
		t.Errorf("message ID %q, want om_card_1", id)
	}
	reqs := s.requests()
	if len(reqs) != 1 || !strings.HasPrefix(reqs[0], "POST /open-apis/im/v1/messages ") || !strings.Contains(reqs[0], "interactive") {
		t.Errorf("requests = %q, want one interactive message create", reqs)
	}
}

// In a thread session the card goes in as a reply in the thread, like SendCard.
func TestSendCardMessage_ThreadRepliesAndReturnsID(t *testing.T) {
	s := &cardMessageServer{}
	_, p := s.start(t)
	p.threadIsolation = true
	rc := replyContext{chatID: "oc_chat", messageID: "om_trigger", sessionKey: "feishu:oc_chat:root:om_root"}
	id, err := p.SendCardMessage(context.Background(), rc, permCard())
	if err != nil || id != "om_card_1" {
		t.Fatalf("SendCardMessage = %q, %v; want om_card_1", id, err)
	}
	reqs := s.requests()
	if len(reqs) != 1 || !strings.HasPrefix(reqs[0], "POST /open-apis/im/v1/messages/om_trigger/reply ") {
		t.Errorf("requests = %q, want one reply to om_trigger", reqs)
	}
}

func TestPatchCardMessage_PatchesThatMessage(t *testing.T) {
	s := &cardMessageServer{}
	_, p := s.start(t)
	expired := core.NewCard().Title("⏰ Expired: nobody answered within 10 min, denied automatically", "grey").Markdown("Bash: ls").Build()
	if err := p.PatchCardMessage(context.Background(), "om_card_1", expired); err != nil {
		t.Fatalf("PatchCardMessage: %v", err)
	}
	reqs := s.requests()
	if len(reqs) != 1 || !strings.HasPrefix(reqs[0], "PATCH /open-apis/im/v1/messages/om_card_1 ") {
		t.Fatalf("requests = %q, want one PATCH of om_card_1", reqs)
	}
	if !strings.Contains(reqs[0], "Expired") || strings.Contains(reqs[0], "perm:allow") {
		t.Errorf("patched content = %q, want the expired card without buttons", reqs[0])
	}
	if err := p.PatchCardMessage(context.Background(), "", expired); err == nil {
		t.Error("patch without a message ID did not fail")
	}
}
