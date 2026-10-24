package feishu

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// otherMessageContentLimit caps how much of a message's raw content JSON is
// passed on for a type without dedicated handling. Cards and calendar shares
// can carry large payloads; the agent only needs enough to see what was sent.
const otherMessageContentLimit = 2000

// beijingTime renders Feishu timestamps for the agent. A fixed zone, because a
// static binary in a minimal container may have no tz database.
var beijingTime = time.FixedZone("UTC+8", 8*60*60)

// isResourceMessageType reports whether msgType is one dispatchMessage handles
// whose content is only resource keys (no readable text). Inside quotes and
// forwarded bundles these keep their short placeholder.
func isResourceMessageType(msgType string) bool {
	switch msgType {
	case "image", "file", "audio", "media", "sticker", "merge_forward":
		return true
	}
	return false
}

// describeOtherMessage renders a message whose type cc-connect has no dedicated
// handling for (video_chat, share_chat, share_user, location, todo, vote,
// calendar shares, interactive cards, …) as text the agent can read. These used
// to be dropped with only a debug log, so the user got no reply at all.
func describeOtherMessage(msgType, content string) string {
	if msgType == "video_chat" {
		if s := describeVideoChat(content); s != "" {
			return s
		}
	}
	return fmt.Sprintf("[飞书 %s 消息] %s", msgType, truncateRunes(strings.TrimSpace(content), otherMessageContentLimit))
}

// describeVideoChat formats a video-meeting card. Measured payload:
// {"topic":"…","meet_number":"991483516","start_time":"1792026635000","end_time":null}
// (times in milliseconds, as strings). Returns "" when nothing is recognisable,
// so the caller falls back to the raw JSON.
func describeVideoChat(content string) string {
	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(content), &body); err != nil {
		return ""
	}
	var parts []string
	if v := jsonScalar(body["topic"]); v != "" {
		parts = append(parts, "主题："+v)
	}
	if v := jsonScalar(body["meet_number"]); v != "" {
		parts = append(parts, "会议号："+v)
	}
	if v := feishuTimestamp(jsonScalar(body["start_time"])); v != "" {
		parts = append(parts, "开始时间："+v+"（北京时间）")
	}
	if v := feishuTimestamp(jsonScalar(body["end_time"])); v != "" {
		parts = append(parts, "结束时间："+v+"（北京时间）")
	}
	if len(parts) == 0 {
		return ""
	}
	return "[飞书视频会议卡片] " + strings.Join(parts, "；")
}

// jsonScalar returns a JSON string or number as text, and "" for null, absent
// or structured values.
func jsonScalar(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return ""
}

// feishuTimestamp formats a Unix timestamp in milliseconds (or seconds) as
// Beijing time, or returns "" when v is not a positive integer.
func feishuTimestamp(v string) string {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return ""
	}
	var t time.Time
	if n >= 1e12 {
		t = time.UnixMilli(n)
	} else {
		t = time.Unix(n, 0)
	}
	return t.In(beijingTime).Format("2006-01-02 15:04")
}

// truncateRunes cuts s to at most limit characters (not bytes, so a multi-byte
// character is never split) and says so when it did.
func truncateRunes(s string, limit int) string {
	if utf8.RuneCountInString(s) <= limit {
		return s
	}
	runes := []rune(s)
	return string(runes[:limit]) + fmt.Sprintf("…（已截断，原文 %d 字符）", len(runes))
}
