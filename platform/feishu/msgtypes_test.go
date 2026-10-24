package feishu

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// videoChatContent is the content of a real video-meeting card sent to the
// bot in a p2p chat (read back with GET /open-apis/im/v1/messages).
const videoChatContent = `{"topic":"bossai进度代办","meet_number":"991483516","start_time":"1792026635000","end_time":null}`

// 1792026635000 ms = 2026-10-15 01:10:35 UTC = 09:10 in Beijing.
const videoChatText = "[飞书视频会议卡片] 主题：bossai进度代办；会议号：991483516；开始时间：2026-10-15 09:10（北京时间）"

func p2pEvent(messageID, chatID, userID, msgType, content string) *larkim.P2MessageReceiveV1 {
	ev := p2pTextEvent(messageID, chatID, userID, "")
	ev.Event.Message.MessageType = &msgType
	ev.Event.Message.Content = &content
	return ev
}

// A video-meeting card sent to the bot must reach the agent. Before the fix
// dispatchMessage had no case for video_chat and dropped it with a debug log,
// so at the default info level there was neither a reply nor a log line.
func TestOnMessage_VideoChatCardReachesAgent(t *testing.T) {
	ls := newLookupServer(t, lookupNoName, lookupNamed)
	got := make(chan *core.Message, 1)
	p := ls.platform(func(_ core.Platform, msg *core.Message) { got <- msg })

	if err := p.onMessage(context.Background(), p2pEvent("om_vc", "oc_p2p", "ou_alice", "video_chat", videoChatContent)); err != nil {
		t.Fatalf("onMessage: %v", err)
	}
	select {
	case msg := <-got:
		if msg.Content != videoChatText {
			t.Fatalf("Content = %q, want %q", msg.Content, videoChatText)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("video_chat card never reached the engine handler")
	}
}

// Every other type without dedicated handling reaches the agent as its raw
// content, cut to otherMessageContentLimit characters. System messages are the
// one exception: they are not written by the user.
func TestDispatchMessage_OtherTypesForwardedAsText(t *testing.T) {
	longSummary := strings.Repeat("项", 2500)
	longTodo := `{"task_id":"t1","summary":{"title":"` + longSummary + `"}}`

	cases := []struct {
		name, msgType, content string
		check                  func(t *testing.T, got string)
	}{
		{
			name: "share_chat", msgType: "share_chat", content: `{"chat_id":"oc_shared"}`,
			check: func(t *testing.T, got string) {
				if want := `[飞书 share_chat 消息] {"chat_id":"oc_shared"}`; got != want {
					t.Fatalf("Content = %q, want %q", got, want)
				}
			},
		},
		{
			name: "video_chat without known fields falls back to raw JSON", msgType: "video_chat", content: `{"foo":1}`,
			check: func(t *testing.T, got string) {
				if want := `[飞书 video_chat 消息] {"foo":1}`; got != want {
					t.Fatalf("Content = %q, want %q", got, want)
				}
			},
		},
		{
			name: "long content is truncated by characters", msgType: "todo", content: longTodo,
			check: func(t *testing.T, got string) {
				const prefix = "[飞书 todo 消息] "
				marker := "…（已截断，原文 " + strconv.Itoa(utf8.RuneCountInString(longTodo)) + " 字符）"
				if !strings.HasPrefix(got, prefix) || !strings.HasSuffix(got, marker) {
					t.Fatalf("Content = %.80q…%q, want prefix %q and suffix %q", got, got[max(0, len(got)-60):], prefix, marker)
				}
				body := strings.TrimSuffix(strings.TrimPrefix(got, prefix), marker)
				if n := utf8.RuneCountInString(body); n != otherMessageContentLimit {
					t.Fatalf("forwarded %d characters of content, want %d", n, otherMessageContentLimit)
				}
				if !utf8.ValidString(got) {
					t.Fatal("truncation split a multi-byte character")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ls := newLookupServer(t, lookupNoName, lookupNamed)
			var got []*core.Message
			p := ls.platform(func(_ core.Platform, msg *core.Message) { got = append(got, msg) })
			p.dispatchMessage(context.Background(), tc.msgType, tc.content, nil, "om_"+tc.msgType, "feishu:oc_p2p:ou_alice",
				"ou_alice", "oc_p2p", replyContext{messageID: "om_" + tc.msgType, chatID: "oc_p2p"}, "", 0)
			if len(got) != 1 {
				t.Fatalf("%s message dispatched %d times, want 1", tc.msgType, len(got))
			}
			tc.check(t, got[0].Content)
		})
	}

	t.Run("system message is not forwarded", func(t *testing.T) {
		ls := newLookupServer(t, lookupNoName, lookupNamed)
		var got []*core.Message
		p := ls.platform(func(_ core.Platform, msg *core.Message) { got = append(got, msg) })
		p.dispatchMessage(context.Background(), "system", `{"template":"{from_user} invited {to_chatters} to this chat"}`, nil,
			"om_sys", "feishu:oc_p2p:ou_alice", "ou_alice", "oc_p2p", replyContext{messageID: "om_sys", chatID: "oc_p2p"}, "", 0)
		if len(got) != 0 {
			t.Fatalf("system message dispatched with Content %q, want it skipped", got[0].Content)
		}
	})
}

// A reply that quotes a video-meeting card must carry the card's details in
// the quote, not the bare "[video_chat]" placeholder.
func TestDispatchMessage_QuotedVideoChat(t *testing.T) {
	const parentID = "om_vc_parent"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal":
			writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "expire": 7200, "tenant_access_token": "t-token"})
		case r.URL.Path == "/open-apis/im/v1/messages/"+parentID:
			writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "data": map[string]any{"items": []map[string]any{{
				"msg_type": "video_chat",
				"sender":   map[string]any{"id": "ou_alice", "sender_type": "user"},
				"body":     map[string]any{"content": videoChatContent},
			}}}})
		case strings.HasPrefix(r.URL.Path, "/open-apis/contact/v3/users/"):
			writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "data": map[string]any{"user": map[string]any{"name": "Alice"}}})
		default:
			writeJSON(t, w, map[string]any{"code": 0, "msg": "success"})
		}
	}))
	t.Cleanup(srv.Close)

	var got []*core.Message
	p := &Platform{
		platformName: "feishu",
		domain:       srv.URL,
		appID:        "cli_quote_vc",
		appSecret:    "secret-quote-vc",
		botOpenID:    "ou_bot",
		dedup:        &core.MessageDedup{},
		client:       lark.NewClient("cli_quote_vc", "secret-quote-vc", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())),
		handler:      func(_ core.Platform, msg *core.Message) { got = append(got, msg) },
	}
	p.dispatchMessage(context.Background(), "text", `{"text":"这个会几点开始？"}`, nil, "om_reply", "feishu:oc_p2p:ou_alice",
		"ou_alice", "oc_p2p", replyContext{messageID: "om_reply", chatID: "oc_p2p"}, parentID, 0)

	if len(got) != 1 {
		t.Fatalf("reply dispatched %d times, want 1", len(got))
	}
	if !strings.Contains(got[0].ExtraContent, videoChatText) || strings.Contains(got[0].ExtraContent, "[video_chat]") {
		t.Fatalf("quoted content = %q, want it to contain %q", got[0].ExtraContent, videoChatText)
	}
}

// Messages inside a forwarded bundle get the same treatment. Types whose
// content is only resource keys keep their short placeholder.
func TestFormatMergeForwardTree_OtherTypes(t *testing.T) {
	item := func(id, msgType, content string) *larkim.Message {
		return &larkim.Message{
			MessageId:  strPtr(id),
			MsgType:    strPtr(msgType),
			CreateTime: strPtr("1792027571969"),
			Sender:     &larkim.Sender{Id: strPtr("ou_alice")},
			Body:       &larkim.MessageBody{Content: strPtr(content)},
		}
	}
	children := map[string][]*larkim.Message{"om_root": {
		item("om_1", "video_chat", videoChatContent),
		item("om_2", "share_user", `{"user_id":"ou_bob"}`),
		item("om_3", "audio", `{"file_key":"file_v3","duration":2000}`),
	}}
	var sb strings.Builder
	(&Platform{platformName: "feishu"}).formatMergeForwardTree("om_root", children, map[string]string{"ou_alice": "Alice"}, &sb, new([]core.ImageAttachment), new([]core.FileAttachment), 0)
	out := sb.String()
	for _, want := range []string{
		"Alice: " + videoChatText + "\n",
		`Alice: [飞书 share_user 消息] {"user_id":"ou_bob"}` + "\n",
		"Alice: [audio message]\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("forwarded bundle = %q, want it to contain %q", out, want)
		}
	}
}
