package core

import "sync"

// Backported test fixture: copied unchanged from upstream main
// core/engine_receipt_ack_test.go (#1812), which v1.5.1-beta.3 does not have.
// askq_stale_callback_test.go (#2061) embeds it.

// receiptAgentSession makes the external session fixture safe for engine
// teardown racing with the processing goroutine's liveness checks.
type receiptAgentSession struct {
	*queuingAgentSession
	closeOnce sync.Once
}

func (s *receiptAgentSession) Alive() bool {
	select {
	case <-s.closed:
		return false
	default:
		return true
	}
}

func (s *receiptAgentSession) Close() error {
	s.closeOnce.Do(func() { close(s.closed); close(s.events) })
	return nil
}
