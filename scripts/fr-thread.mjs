#!/usr/bin/env node
// cc-connect event hook → foldrender desk threads. Reads one event's JSON on
// stdin (command hooks get it there), maps it, calls `foldrender thread`:
//   message.received  {message_id, reply_to_message_id?} → thread new --ref id
//     [--reply-to-ref parent]; the words follow the answered message's thread.
//   message.sent      {in_reply_to} → thread reply --reply-to-ref id.
//   message.delivered {message_id, in_reply_to} → thread ref t-… --add id when
//     the reply's address is already kept in the state file, else queued and
//     flushed on later runs (the reply lands before the chat id is known).
// Never in the way: every failure is silent, exit 0, 8s at most per call.
// Env: FOLDRENDER_DESK_TOKEN (the desk's agent token), FR_THREAD_STATE_DIR
// (the queue/state dir), FR_THREAD_BIN (the command, default foldrender),
// FR_THREAD_SESSION (linked to each thread it opens).
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, appendFileSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const TIMEOUT = 8000;
const stateDir = process.env.FR_THREAD_STATE_DIR || join(process.env.HOME || "/tmp", ".local/state/fr-thread");
const bin = process.env.FR_THREAD_BIN || "foldrender";

function readStdin() {
  try { return JSON.parse(readFileSync(0, "utf8") || "{}"); } catch { return null; }
}
function run(args) {
  try {
    const r = spawnSync(bin, args, { timeout: TIMEOUT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] });
    return r.status === 0 ? (r.stdout || "") : null;
  } catch { return null; }
}
function keyOf(name) {
  try { return readFileSync(join(stateDir, name), "utf8").trim() || null; } catch { return null; }
}
function keep(name, v) {
  try { mkdirSync(stateDir, { recursive: true }); writeFileSync(join(stateDir, name), `${v}\n`); } catch { /* never in the way */ }
}
function queueDelivered(inReplyTo, messageId) {
  try { mkdirSync(stateDir, { recursive: true }); appendFileSync(join(stateDir, "delivered.jsonl"), `${JSON.stringify({ in_reply_to: inReplyTo, message_id: messageId })}\n`); } catch { /* */ }
}
function flushDelivered() {
  const f = join(stateDir, "delivered.jsonl");
  let lines;
  try { lines = readFileSync(f, "utf8").split("\n").filter(Boolean); } catch { return; }
  const left = [];
  for (const line of lines) {
    let d; try { d = JSON.parse(line); } catch { continue; }
    const addr = keyOf(`sent-${d.in_reply_to}`);
    if (!addr) { left.push(line); continue; }
    // addr is t-… or t-…#seq: the reply's own place, the id goes on it.
    run(["thread", "ref", addr, "--add", d.message_id]);
  }
  try { left.length ? writeFileSync(f, `${left.join("\n")}\n`) : unlinkSync(f); } catch { /* */ }
}

const e = readStdin();
if (e && typeof e === "object") {
  const extra = e.extra || {};
  const name = e.event || process.env.CC_HOOK_EVENT || "";
  if (name === "message.received" && extra.message_id) {
    const text = String(e.content || extra.content || "");
    if (text.trim()) {
      const args = ["thread", "new", "--text-file", "-", "--ref", String(extra.message_id), "--channel", "feishu"];
      if (extra.reply_to_message_id) args.push("--reply-to-ref", String(extra.reply_to_message_id));
      if (extra.platform_thread_id) { /* the topic is kept in the ref's record */ }
      if (process.env.FR_THREAD_SESSION) args.push("--via", process.env.FR_THREAD_SESSION);
      const r = spawnSync(bin, args, { timeout: TIMEOUT, encoding: "utf8", input: text, stdio: ["pipe", "pipe", "ignore"] });
      const key = r.status === 0 ? (r.stdout || "").trim() : "";
      if (/^t-/.test(key)) keep(`recv-${extra.message_id}`, key);
    }
  } else if (name === "message.sent" && extra.in_reply_to) {
    const text = String(e.content || extra.content || "");
    if (text.trim()) {
      const r = spawnSync(bin, ["thread", "reply", "--reply-to-ref", String(extra.in_reply_to), "--text-file", "-"], { timeout: TIMEOUT, encoding: "utf8", input: text, stdio: ["pipe", "pipe", "ignore"] });
      // The reply's own address (t-… or t-…#seq) for message.delivered to find.
      const m = (r.stdout || "").match(/added reply (\d+) to (t-[A-Za-z0-9_.:-]+)/);
      if (m) keep(`sent-${extra.in_reply_to}`, `${m[2]}#${m[1]}`);
    }
    flushDelivered();
  } else if (name === "message.delivered" && extra.message_id) {
    const addr = extra.in_reply_to ? keyOf(`sent-${extra.in_reply_to}`) : null;
    if (addr) run(["thread", "ref", addr, "--add", String(extra.message_id)]);
    else if (extra.in_reply_to) queueDelivered(String(extra.in_reply_to), String(extra.message_id));
    flushDelivered();
  }
}
process.exit(0);
