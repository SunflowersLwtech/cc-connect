package feishu

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// freezeConn stands in for a connection that dies without a FIN or RST: once
// frozen, everything the server writes is dropped, so the client hears
// nothing more, not even PING acks, while the server keeps reading.
type freezeConn struct {
	net.Conn
	frozen atomic.Bool
}

func (c *freezeConn) Write(b []byte) (int, error) {
	if c.frozen.Load() {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

type freezeListener struct{ net.Listener }

func (l freezeListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &freezeConn{Conn: c}, nil
}

type freezeConnKey struct{}

type messageRequest struct {
	path  string
	uuid  string
	proto int
}

// fakeOpenAPI is a TLS + HTTP/2 stand-in for open.feishu.cn. Token requests
// always succeed; message requests go to onMessage with their 1-based index.
type fakeOpenAPI struct {
	srv       *httptest.Server
	mu        sync.Mutex
	requests  []messageRequest
	onMessage func(n int, w http.ResponseWriter, r *http.Request, conn *freezeConn)
}

func newFakeOpenAPI(t *testing.T, onMessage func(n int, w http.ResponseWriter, r *http.Request, conn *freezeConn)) *fakeOpenAPI {
	t.Helper()
	api := &fakeOpenAPI{onMessage: onMessage}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(api.serve))
	srv.Listener = freezeListener{srv.Listener}
	srv.EnableHTTP2 = true
	srv.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		if tc, ok := c.(*tls.Conn); ok {
			if fc, ok := tc.NetConn().(*freezeConn); ok {
				return context.WithValue(ctx, freezeConnKey{}, fc)
			}
		}
		return ctx
	}
	srv.StartTLS()
	api.srv = srv
	// Cleanups run LIFO: drop client connections first so handlers still
	// waiting on a request see it cancelled, then Close can wait for them.
	t.Cleanup(srv.Close)
	t.Cleanup(srv.CloseClientConnections)

	// Trust the server through http.DefaultTransport, which is where both the
	// SDK's default client and a client cloned from it get their TLS config.
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	pool.AddCert(srv.Certificate())
	dt := http.DefaultTransport.(*http.Transport)
	old := dt.TLSClientConfig
	dt.TLSClientConfig = &tls.Config{RootCAs: pool, NextProtos: []string{"h2", "http/1.1"}}
	t.Cleanup(func() {
		dt.TLSClientConfig = old
		dt.CloseIdleConnections()
	})
	return api
}

func (api *fakeOpenAPI) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "msg": "success", "expire": 7200, "tenant_access_token": "t-token"})
		return
	}
	body, _ := io.ReadAll(r.Body)
	var fields struct {
		UUID string `json:"uuid"`
	}
	_ = json.Unmarshal(body, &fields)
	api.mu.Lock()
	api.requests = append(api.requests, messageRequest{path: r.URL.Path, uuid: fields.UUID, proto: r.ProtoMajor})
	n := len(api.requests)
	api.mu.Unlock()
	conn, _ := r.Context().Value(freezeConnKey{}).(*freezeConn)
	api.onMessage(n, w, r, conn)
}

func (api *fakeOpenAPI) messageRequests() []messageRequest {
	api.mu.Lock()
	defer api.mu.Unlock()
	return append([]messageRequest(nil), api.requests...)
}

func (api *fakeOpenAPI) platform(t *testing.T) *Platform {
	t.Helper()
	pa, err := New(map[string]any{"app_id": "cli_timeout", "app_secret": "secret", "domain": api.srv.URL, "enable_feishu_card": true})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	switch p := pa.(type) {
	case *Platform:
		return p
	case *interactivePlatform:
		return p.Platform
	}
	t.Fatalf("platform type = %T", pa)
	return nil
}

func answerMessage(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "msg": "success", "data": map[string]any{"message_id": "om_sent"}})
}

func hangUntilCancelled(r *http.Request) {
	<-r.Context().Done()
}

func setFeishuTimeouts(t *testing.T, sendPing, ping, attempt, retryInitial time.Duration) {
	t.Helper()
	oldSendPing, oldPing, oldAttempt, oldInitial := feishuH2SendPingTimeout, feishuH2PingTimeout, feishuAttemptTimeout, transientRetryInitial
	feishuH2SendPingTimeout, feishuH2PingTimeout, feishuAttemptTimeout, transientRetryInitial = sendPing, ping, attempt, retryInitial
	t.Cleanup(func() {
		feishuH2SendPingTimeout, feishuH2PingTimeout, feishuAttemptTimeout, transientRetryInitial = oldSendPing, oldPing, oldAttempt, oldInitial
	})
}

func runWithin(t *testing.T, limit time.Duration, what string, api *fakeOpenAPI, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("%s still blocked after %s; message requests so far: %+v", what, limit, api.messageRequests())
		return nil
	}
}

var testReplyCtx = replyContext{messageID: "om_parent", chatID: "oc_chat", sessionKey: "feishu:oc_chat:ou_user"}

// The reported incident: the connection carrying a reply goes silent. Only
// the HTTP/2 health check can notice within the limit here (the per-attempt
// deadline is left at 30s), and the retry must reuse the uuid so Feishu
// drops the copy if the first request did get through.
func TestReplyMessage_RecoversFromSilentHTTP2Connection(t *testing.T) {
	setFeishuTimeouts(t, 200*time.Millisecond, 200*time.Millisecond, 30*time.Second, 10*time.Millisecond)
	api := newFakeOpenAPI(t, func(n int, w http.ResponseWriter, r *http.Request, conn *freezeConn) {
		if n == 1 {
			conn.frozen.Store(true)
			return
		}
		answerMessage(w)
	})
	p := api.platform(t)

	err := runWithin(t, 10*time.Second, "reply", api, func() error {
		return p.replyMessage(context.Background(), testReplyCtx, "text", `{"text":"hello"}`)
	})
	if err != nil {
		t.Fatalf("reply error = %v, want success on retry", err)
	}
	reqs := api.messageRequests()
	if len(reqs) != 2 {
		t.Fatalf("message requests = %+v, want 2 (one lost, one retried)", reqs)
	}
	if reqs[0].proto != 2 {
		t.Fatalf("first request used HTTP/%d, want HTTP/2", reqs[0].proto)
	}
	if reqs[0].uuid == "" || reqs[0].uuid != reqs[1].uuid {
		t.Fatalf("uuids = %q, %q; want the same non-empty uuid on both attempts", reqs[0].uuid, reqs[1].uuid)
	}
}

// The connection stays healthy (PINGs are answered) but the server never
// answers the request: only the per-attempt deadline helps.
func TestReplyMessage_RetriesWhenServerNeverAnswers(t *testing.T) {
	setFeishuTimeouts(t, 30*time.Second, 30*time.Second, 300*time.Millisecond, 10*time.Millisecond)
	api := newFakeOpenAPI(t, func(n int, w http.ResponseWriter, r *http.Request, conn *freezeConn) {
		if n == 1 {
			hangUntilCancelled(r)
			return
		}
		answerMessage(w)
	})
	p := api.platform(t)

	err := runWithin(t, 10*time.Second, "reply", api, func() error {
		return p.replyMessage(context.Background(), testReplyCtx, "text", `{"text":"hello"}`)
	})
	if err != nil {
		t.Fatalf("reply error = %v, want success on retry", err)
	}
	reqs := api.messageRequests()
	if len(reqs) != 2 {
		t.Fatalf("message requests = %+v, want 2 (one timed out, one retried)", reqs)
	}
	if reqs[0].uuid == "" || reqs[0].uuid != reqs[1].uuid {
		t.Fatalf("uuids = %q, %q; want the same non-empty uuid on both attempts", reqs[0].uuid, reqs[1].uuid)
	}
}

// A caller that gives up while a retry is pending gets its answer at once
// instead of after the backoff and another attempt.
func TestReplyMessage_StopsRetryingWhenCallerCancels(t *testing.T) {
	setFeishuTimeouts(t, 30*time.Second, 30*time.Second, 200*time.Millisecond, 2*time.Second)
	api := newFakeOpenAPI(t, func(n int, w http.ResponseWriter, r *http.Request, conn *freezeConn) {
		hangUntilCancelled(r)
	})
	p := api.platform(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(500*time.Millisecond, cancel) // during the backoff after attempt 1
	start := time.Now()
	err := runWithin(t, 10*time.Second, "reply", api, func() error {
		return p.replyMessage(ctx, testReplyCtx, "text", `{"text":"hello"}`)
	})
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reply error = %v, want context.Canceled", err)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("reply returned %s after the caller cancelled at 500ms, want it to stop at once", elapsed.Round(time.Millisecond))
	}
	if reqs := api.messageRequests(); len(reqs) != 1 {
		t.Fatalf("message requests = %+v, want 1", reqs)
	}
}

// Every logical send gets its own uuid: replies, new messages and previews.
func TestMessageSends_CarryDistinctUUIDs(t *testing.T) {
	api := newFakeOpenAPI(t, func(n int, w http.ResponseWriter, r *http.Request, conn *freezeConn) {
		answerMessage(w)
	})
	p := api.platform(t)
	ctx := context.Background()

	if err := p.replyMessage(ctx, testReplyCtx, "text", `{"text":"one"}`); err != nil {
		t.Fatalf("reply: %v", err)
	}
	if err := p.replyMessage(ctx, testReplyCtx, "text", `{"text":"two"}`); err != nil {
		t.Fatalf("reply: %v", err)
	}
	if err := p.createMessage(ctx, replyContext{chatID: "oc_chat"}, "text", `{"text":"three"}`, "send"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := p.SendPreviewStart(ctx, testReplyCtx, "thinking"); err != nil {
		t.Fatalf("preview (reply): %v", err)
	}
	if _, err := p.SendPreviewStart(ctx, replyContext{chatID: "oc_chat", sessionKey: "feishu:oc_chat:ou_user"}, "thinking"); err != nil {
		t.Fatalf("preview (send): %v", err)
	}

	reqs := api.messageRequests()
	if len(reqs) != 5 {
		t.Fatalf("message requests = %+v, want 5", reqs)
	}
	seen := map[string]string{}
	for _, req := range reqs {
		if req.uuid == "" || len(req.uuid) > 50 {
			t.Errorf("%s uuid = %q, want 1-50 characters", req.path, req.uuid)
			continue
		}
		if prev, dup := seen[req.uuid]; dup {
			t.Errorf("uuid %q used by both %s and %s, want one uuid per message", req.uuid, prev, req.path)
		}
		seen[req.uuid] = req.path
	}
	if !strings.HasSuffix(reqs[0].path, "/reply") || reqs[2].path != "/open-apis/im/v1/messages" {
		t.Errorf("paths = %+v, want reply, reply, create, reply, create", reqs)
	}
}

// Reactions are not retried, but they must not hang either: the typing
// indicator is added on the dispatch path.
func TestAddReaction_GivesUpWhenServerNeverAnswers(t *testing.T) {
	setFeishuTimeouts(t, 30*time.Second, 30*time.Second, 300*time.Millisecond, 10*time.Millisecond)
	api := newFakeOpenAPI(t, func(n int, w http.ResponseWriter, r *http.Request, conn *freezeConn) {
		hangUntilCancelled(r)
	})
	p := api.platform(t)

	_ = runWithin(t, 5*time.Second, "add reaction", api, func() error {
		if id := p.addReactionWithEmoji("om_parent", "OnIt"); id != "" {
			t.Errorf("reaction id = %q, want empty after the request timed out", id)
		}
		return nil
	})
}
