// scripts/fr-thread.mjs against a stand-in foldrender: each event maps to the
// right call, nothing ever fails the hook (exit 0, silent), a delivered id
// waits for the reply's address and is kept on it. Run: node test/fr-thread.test.mjs
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, readFileSync, rmSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const dir = mkdtempSync(join(tmpdir(), "fr-thread-test-"));
const callsFile = join(dir, "calls");
const fake = join(dir, "foldrender");
// A stand-in command: logs each call as JSON; answers thread new/reply/ref.
writeFileSync(fake, `#!/usr/bin/env node
const fs=require("fs");
const a=process.argv.slice(2);let input="";try{input=fs.readFileSync(0,"utf8")}catch{}
fs.appendFileSync(process.env.CALLS,JSON.stringify({a,input})+"\\n");
if(a[0]==="thread"&&a[1]==="new")console.log("t-10280000-abc");
if(a[1]==="reply")console.log("added reply 2 to t-10280000-abc");
if(a[1]==="ref")console.log("kept: x");
`);
try { spawnSync("chmod", ["+x", fake]); } catch {}
const env = { ...process.env, HOME: dir, CALLS: callsFile, FR_THREAD_BIN: fake, FR_THREAD_STATE_DIR: join(dir, "st") };
let failed = 0;
const check = (n, ok, d) => { if (!ok) failed++; console.log(`${ok ? "PASS" : "FAIL"}  ${n}${ok ? "" : `  ${d}`}`); };
const run = (event) => spawnSync("node", ["scripts/fr-thread.mjs"], { env, input: JSON.stringify(event), encoding: "utf8" });
const calls = () => existsSync(callsFile) ? readFileSync(callsFile, "utf8").trim().split("\n").map(JSON.parse) : [];

let r = run({ event: "message.received", content: "帮我看下", extra: { message_id: "om_1" } });
check("received → thread new --ref (words on stdin)", r.status === 0 && calls().some((c) => c.a[1] === "new" && c.a.includes("--ref") && c.a.includes("om_1") && c.input === "帮我看下"), JSON.stringify(calls()));
r = run({ event: "message.received", content: "回上条", extra: { message_id: "om_2", reply_to_message_id: "om_1" } });
check("received with a parent → --reply-to-ref too", calls().some((c) => c.a.includes("--reply-to-ref") && c.a.includes("om_1")), JSON.stringify(calls().at(-1)));
r = run({ event: "message.sent", content: "答复", extra: { in_reply_to: "om_1" } });
check("sent → reply --reply-to-ref; the reply's address kept for delivered", calls().some((c) => c.a[1] === "reply" && c.a.includes("om_1")) && readFileSync(join(dir, "st", "sent-om_1"), "utf8").trim() === "t-10280000-abc#2", JSON.stringify(calls().at(-1)));
r = run({ event: "message.delivered", extra: { message_id: "om_bot1", in_reply_to: "om_1" } });
check("delivered → ref --add on the reply's address", calls().some((c) => c.a[1] === "ref" && c.a[2] === "t-10280000-abc#2" && c.a.includes("om_bot1")), JSON.stringify(calls().at(-1)));
// delivered before the reply's address exists: queued, flushed later.
run({ event: "message.delivered", extra: { message_id: "om_bot9", in_reply_to: "om_9" } });
run({ event: "message.sent", content: "答复九", extra: { in_reply_to: "om_9" } });
check("delivered early is queued and kept once the address exists", calls().some((c) => c.a[1] === "ref" && c.a[2] === "t-10280000-abc#2" && c.a.includes("om_bot9")), JSON.stringify(calls().slice(-3)));
// The same reply replayed: the command is asked twice; the desk's own
// idempotency keeps one (the hook must still exit 0 and not crash).
const n0 = calls().length;
r = run({ event: "message.sent", content: "答复", extra: { in_reply_to: "om_1" } });
check("a replayed event stays a plain second call (exit 0; the desk dedupes by ref)", r.status === 0 && calls().length > n0, `${r.status} ${calls().length}`);
// Garbage and a dying command: exit 0, silent.
r = run(undefined);
const bad = spawnSync("node", ["scripts/fr-thread.mjs"], { env: { ...env, FR_THREAD_BIN: "/nonexistent" }, input: JSON.stringify({ event: "message.received", content: "x", extra: { message_id: "om_x" } }), encoding: "utf8" });
check("no event, and a command that fails: exit 0, nothing on stderr", r.status === 0 && bad.status === 0 && bad.stderr === "", `${r.status} ${bad.status} ${bad.stderr}`);
rmSync(dir, { recursive: true, force: true });
console.log(failed ? `${failed} failed` : "all passed");
process.exit(failed ? 1 : 0);
