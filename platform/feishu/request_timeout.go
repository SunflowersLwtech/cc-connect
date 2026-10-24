package feishu

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Without an explicit HTTP client the SDK uses http.DefaultClient: no
// timeout, and HTTP/2 without health checks. Requests to Feishu are
// multiplexed on one connection, so when that connection dies silently every
// request on it hangs until the kernel gives up retransmitting (about 15
// minutes with the default tcp_retries2), and replies to the user hang with
// it. Two layers bound that:
//
//   - the transport pings an HTTP/2 connection that has gone quiet and closes
//     it when the ping is not answered, failing every request on it at once
//     (this also covers uploads and downloads, which a live connection keeps
//     answering however long they take);
//   - short message requests (reply, send, patch, delete, reactions) get a
//     deadline per attempt, which also covers a server that keeps the
//     connection alive but never answers.
//
// A dead connection is closed within 10s+15s, before the 30s per-attempt
// deadline expires, so the retry after a timed-out attempt opens a new
// connection instead of landing on the dead one again (cancelling a request
// does not evict its connection). Declared as vars so tests can shrink them.
var (
	feishuH2SendPingTimeout = 10 * time.Second
	feishuH2PingTimeout     = 15 * time.Second
	feishuAttemptTimeout    = 30 * time.Second
)

// newFeishuHTTPClient builds the HTTP client for OpenAPI calls. It is a var
// so tests can add trust for a local TLS server on top of it.
var newFeishuHTTPClient = func() *http.Client {
	var t *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t = dt.Clone()
	} else {
		t = &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true}
	}
	t.HTTP2 = &http.HTTP2Config{
		SendPingTimeout: feishuH2SendPingTimeout,
		PingTimeout:     feishuH2PingTimeout,
	}
	return &http.Client{Transport: t}
}

// errFeishuAttemptTimeout marks an attempt that ran out of its own deadline
// while the caller's context was still live. The SDK reports a client-side
// timeout as *larkcore.ClientTimeoutError, which carries neither Timeout()
// nor the cause, so without this marker isTransientError would not retry it.
var errFeishuAttemptTimeout = errors.New("feishu request attempt timed out")

// withAttemptTimeout runs fn with its own deadline derived from ctx.
func withAttemptTimeout(ctx context.Context, fn func(context.Context) error) error {
	actx, cancel := context.WithTimeout(ctx, feishuAttemptTimeout)
	defer cancel()
	err := fn(actx)
	if err != nil && ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w after %s: %v", errFeishuAttemptTimeout, feishuAttemptTimeout, err)
	}
	return err
}

// withAttemptRetry is withTransientRetry with a deadline on every attempt.
func (p *Platform) withAttemptRetry(ctx context.Context, operation string, fn func(context.Context) error) error {
	return p.withTransientRetry(ctx, operation, func() error {
		return withAttemptTimeout(ctx, fn)
	})
}

// newMessageUUID returns the dedup key for one logical message send. Feishu
// creates at most one message per uuid within an hour, so retrying a request
// whose response was lost does not post the message twice. Build the request
// once, outside the retry loop, so every attempt carries the same uuid.
// crypto/rand.Text gives 26 characters; the API allows up to 50.
func newMessageUUID() string {
	return crand.Text()
}
