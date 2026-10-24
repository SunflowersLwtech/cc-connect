package core

// Extra fields that let a hook tie messages together by platform message ID:
// which message the user replied to, which user message a reply answers, and
// which platform messages carried that reply.

// receivedHookExtra describes an incoming message for message.received.
// pendingTranscription marks a voice message that will be reported again,
// with the same message_id, once speech-to-text has produced its text.
func receivedHookExtra(msg *Message, pendingTranscription bool) map[string]any {
	extra := map[string]any{"message_id": msg.MessageID}
	if msg.ReplyToMessageID != "" {
		extra["reply_to_message_id"] = msg.ReplyToMessageID
	}
	if msg.ThreadRootID != "" {
		extra["root_message_id"] = msg.ThreadRootID
	}
	if msg.PlatformThreadID != "" {
		extra["platform_thread_id"] = msg.PlatformThreadID
	}
	if msg.UserMessageTimeMs > 0 {
		extra["created_ms"] = msg.UserMessageTimeMs
	}
	if msg.FromVoice {
		extra["from_voice"] = true
	}
	if pendingTranscription {
		extra["pending_transcription"] = true
	}
	return extra
}

// sentHookExtra describes an agent reply for message.sent: the ID of the user
// message whose turn produced it (empty for turns no user message started).
func sentHookExtra(inReplyTo string) map[string]any {
	if inReplyTo == "" {
		return nil
	}
	return map[string]any{"in_reply_to": inReplyTo}
}

// deliveredHookEvent turns a platform's delivery report into message.delivered.
func deliveredHookEvent(platform string, d DeliveredMessage) HookEvent {
	extra := map[string]any{"message_id": d.MessageID}
	if d.InReplyTo != "" {
		extra["in_reply_to"] = d.InReplyTo
	}
	if d.ChatID != "" {
		extra["chat_id"] = d.ChatID
	}
	return HookEvent{Event: HookEventMessageDelivered, Platform: platform, Extra: extra}
}
