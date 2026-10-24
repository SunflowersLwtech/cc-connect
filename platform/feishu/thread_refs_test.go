package feishu

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// A message the owner sends as a reply to an earlier message carries the IDs
// of that message, the chain's root and the topic into core.Message, so the
// engine can report which conversation it continues.
func TestOnMessageCarriesReplyAndTopicIDs(t *testing.T) {
	cases := []struct {
		name                 string
		parent, root, topic  string
		wantParent, wantRoot string
		wantTopic            string
	}{
		{name: "reply to a bot message", parent: "om_bot_reply", root: "om_first", topic: "omt_topic",
			wantParent: "om_bot_reply", wantRoot: "om_first", wantTopic: "omt_topic"},
		{name: "plain message", wantParent: "", wantRoot: "", wantTopic: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan *core.Message, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal":
					writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "expire": 7200, "tenant_access_token": "tenant-token"})
				case r.URL.Path == "/open-apis/im/v1/messages/"+tc.parent:
					writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "data": map[string]any{"items": []map[string]any{{
						"msg_type": "text", "parent_id": "",
						"sender": map[string]any{"id": "cli_bot", "sender_type": "app"},
						"body":   map[string]any{"content": `{"text":"earlier answer"}`},
					}}}})
				case strings.HasPrefix(r.URL.Path, "/open-apis/contact/v3/users/"),
					strings.HasPrefix(r.URL.Path, "/open-apis/im/v1/chats/"):
					writeJSON(t, w, map[string]any{"code": 0, "msg": "success"})
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer srv.Close()

			p := &Platform{
				platformName: "feishu",
				domain:       srv.URL,
				appID:        "cli_refs",
				appSecret:    "secret-refs",
				botOpenID:    "ou_bot",
				dedup:        &core.MessageDedup{},
				client:       lark.NewClient("cli_refs", "secret-refs", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())),
				handler:      func(_ core.Platform, msg *core.Message) { got <- msg },
			}
			chatType, senderType, msgType := "p2p", "user", "text"
			content := `{"text":"接着上面那件说"}`
			createTime := strconv.FormatInt(time.Now().Add(time.Second).UnixMilli(), 10)
			ev := &larkim.EventMessage{
				MessageId: strPtr("om_new"), ChatId: strPtr("oc_chat"), ChatType: &chatType,
				MessageType: &msgType, Content: &content, CreateTime: &createTime,
			}
			if tc.parent != "" {
				ev.ParentId, ev.RootId, ev.ThreadId = strPtr(tc.parent), strPtr(tc.root), strPtr(tc.topic)
			}
			if err := p.onMessage(context.Background(), &larkim.P2MessageReceiveV1{Event: &larkim.P2MessageReceiveV1Data{
				Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: strPtr("ou_owner")}, SenderType: &senderType},
				Message: ev,
			}}); err != nil {
				t.Fatalf("onMessage: %v", err)
			}
			select {
			case msg := <-got:
				if msg.MessageID != "om_new" || msg.ReplyToMessageID != tc.wantParent || msg.ThreadRootID != tc.wantRoot || msg.PlatformThreadID != tc.wantTopic {
					t.Fatalf("got message=%q reply_to=%q root=%q topic=%q, want om_new/%q/%q/%q",
						msg.MessageID, msg.ReplyToMessageID, msg.ThreadRootID, msg.PlatformThreadID, tc.wantParent, tc.wantRoot, tc.wantTopic)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for the dispatched message")
			}
		})
	}
}

type deliveryLog struct {
	mu  sync.Mutex
	got []core.DeliveredMessage
}

func (d *deliveryLog) add(m core.DeliveredMessage) {
	d.mu.Lock()
	d.got = append(d.got, m)
	d.mu.Unlock()
}

func (d *deliveryLog) all() []core.DeliveredMessage {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]core.DeliveredMessage(nil), d.got...)
}

// Every message the bot creates is reported with its new ID and the user
// message it answers; a refused request reports nothing.
func TestOutgoingMessagesReportTheirIDs(t *testing.T) {
	var refuse bool
	var mu sync.Mutex
	var kinds []string
	n := 0
	requests := func() string {
		mu.Lock()
		defer mu.Unlock()
		return strings.Join(kinds, " ")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "expire": 7200, "tenant_access_token": "tenant-token"})
			return
		}
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/open-apis/im/v1/messages") {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/reply") {
			kinds = append(kinds, "reply")
		} else {
			kinds = append(kinds, "create")
		}
		if refuse {
			writeJSON(t, w, map[string]any{"code": 230002, "msg": "Bot/User can NOT be out of the chat."})
			return
		}
		n++
		writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "data": map[string]any{"message_id": "om_out_" + strconv.Itoa(n)}})
	}))
	defer srv.Close()

	p := &Platform{
		platformName:       "feishu",
		domain:             srv.URL,
		appID:              "cli_out",
		appSecret:          "secret-out",
		useInteractiveCard: true,
		client:             lark.NewClient("cli_out", "secret-out", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())),
	}
	log := &deliveryLog{}
	p.SetDeliveryObserver(log.add)
	rc := replyContext{messageID: "om_user", chatID: "oc_chat", sessionKey: "feishu:oc_chat:ou_owner"}
	ctx := context.Background()

	// Sent as replies to the user's message (the default).
	if err := p.Send(ctx, rc, "plain answer"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := p.SendPreviewStart(ctx, rc, "thinking…"); err != nil {
		t.Fatalf("SendPreviewStart: %v", err)
	}
	// Sent as new chat messages: with no_reply_to_trigger, and for output no
	// user message started (a reconstructed context without a message ID).
	p.noReplyToTrigger = true
	if err := p.Send(ctx, rc, "standalone answer"); err != nil {
		t.Fatalf("Send (no_reply_to_trigger): %v", err)
	}
	if _, err := p.SendPreviewStart(ctx, rc, "thinking again…"); err != nil {
		t.Fatalf("SendPreviewStart (no_reply_to_trigger): %v", err)
	}
	if err := p.Send(ctx, replyContext{chatID: "oc_chat"}, "background result"); err != nil {
		t.Fatalf("Send (no message ID): %v", err)
	}
	want := []core.DeliveredMessage{
		{MessageID: "om_out_1", InReplyTo: "om_user", ChatID: "oc_chat"},
		{MessageID: "om_out_2", InReplyTo: "om_user", ChatID: "oc_chat"},
		{MessageID: "om_out_3", InReplyTo: "om_user", ChatID: "oc_chat"},
		{MessageID: "om_out_4", InReplyTo: "om_user", ChatID: "oc_chat"},
		{MessageID: "om_out_5", InReplyTo: "", ChatID: "oc_chat"},
	}
	got := log.all()
	if len(got) != len(want) {
		t.Fatalf("delivered = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered = %+v, want %+v", got, want)
		}
	}
	if reqs := requests(); reqs != "reply reply create create create" {
		t.Fatalf("requests = %q, want two replies then three new messages", reqs)
	}

	mu.Lock()
	refuse = true
	mu.Unlock()
	if err := p.Send(ctx, rc, "lost answer"); err == nil {
		t.Fatal("Send succeeded although the server refused it")
	}
	if got := log.all(); len(got) != len(want) {
		t.Fatalf("a refused send was reported as delivered: %+v", got[len(want):])
	}
}
