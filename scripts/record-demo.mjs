#!/usr/bin/env node
// Records docs/demo.gif from the live dashboard: jobs flowing, a worker
// killed with the dashboard's own button, its jobs rescued by the others.
//
//   docker compose up -d --build
//   node scripts/record-demo.mjs [http://localhost:8080]
//
// Needs Google Chrome, ffmpeg, and Node 22+ (built-in WebSocket). Drives
// headless Chrome over the DevTools protocol and stitches PNG frames with
// ffmpeg. The relay stack should be idle and freshly reset for a clean take.
import { spawn, execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const BASE = process.argv[2] || "http://localhost:8080";
const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = join(ROOT, "docs", "demo.gif");
const CHROME = process.env.CHROME || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const FPS = 5;
const WIDTH = 1360, HEIGHT = 860;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const work = mkdtempSync(join(tmpdir(), "relay-demo-"));
const frames = join(work, "frames");
mkdirSync(frames);

const chrome = spawn(CHROME, [
  "--headless=new", "--disable-gpu", "--hide-scrollbars", "--remote-debugging-port=9333",
  `--user-data-dir=${join(work, "profile")}`, `--window-size=${WIDTH},${HEIGHT}`, "about:blank",
], { stdio: "ignore" });

async function devtoolsTarget() {
  for (let i = 0; i < 50; i++) {
    try {
      const list = await (await fetch("http://127.0.0.1:9333/json/list")).json();
      const page = list.find((t) => t.type === "page");
      if (page) return page.webSocketDebuggerUrl;
    } catch {}
    await sleep(200);
  }
  throw new Error("chrome devtools did not come up");
}

const ws = new WebSocket(await devtoolsTarget());
await new Promise((r) => ws.addEventListener("open", r, { once: true }));
let seq = 0;
const pending = new Map();
ws.addEventListener("message", (m) => {
  const msg = JSON.parse(m.data);
  if (msg.id && pending.has(msg.id)) {
    pending.get(msg.id)(msg);
    pending.delete(msg.id);
  }
});
const cdp = (method, params = {}) => new Promise((resolve, reject) => {
  const id = ++seq;
  pending.set(id, (msg) => (msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result)));
  ws.send(JSON.stringify({ id, method, params }));
});
const evaluate = (expression) => cdp("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });

async function post(path, body) {
  const res = await fetch(BASE + path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  return res.status;
}
async function burst(n, body) {
  const all = [];
  for (let i = 0; i < n; i++) all.push(post("/jobs", body));
  await Promise.all(all);
}

await cdp("Emulation.setDeviceMetricsOverride", { width: WIDTH, height: HEIGHT, deviceScaleFactor: 1, mobile: false });
await cdp("Page.enable");
await cdp("Page.navigate", { url: BASE + "/" });
await sleep(2500); // SSE connects, workers render

let frame = 0;
let recording = true;
const recorder = (async () => {
  while (recording) {
    const t0 = Date.now();
    const { data } = await cdp("Page.captureScreenshot", { format: "png" });
    writeFileSync(join(frames, `f${String(frame++).padStart(4, "0")}.png`), Buffer.from(data, "base64"));
    await sleep(Math.max(0, 1000 / FPS - (Date.now() - t0)));
  }
})();

// The script: steady work, then a kill mid-job, then recovery.
await sleep(1000);
await burst(90, { type: "sleep", payload: { ms: 3000 } });
await burst(150, { type: "flaky", payload: { fail_rate: 0.3, ms: 200 } });
await burst(5, { type: "flaky", payload: { fail_rate: 1 }, max_attempts: 2 });
await sleep(4000);
const killed = await evaluate(`(() => {
  const b = [...document.querySelectorAll('[data-kill]')].find((x) => !x.disabled);
  if (!b) return null;
  b.scrollIntoView({ block: "center" });
  b.click();
  return b.dataset.kill;
})()`);
console.log("killed", killed.result.value);
await burst(60, { type: "sleep", payload: { ms: 2000 } });
await sleep(14000);
recording = false;
await recorder;
ws.close();
chrome.kill();

console.log(`captured ${frame} frames; encoding ${OUT}`);
mkdirSync(dirname(OUT), { recursive: true });
const palette = join(work, "palette.png");
const scale = "fps=" + FPS + ",scale=1100:-1:flags=lanczos";
execFileSync("ffmpeg", ["-y", "-loglevel", "error", "-framerate", String(FPS), "-i", join(frames, "f%04d.png"),
  "-vf", `${scale},palettegen=max_colors=128:stats_mode=full`, palette]);
execFileSync("ffmpeg", ["-y", "-loglevel", "error", "-framerate", String(FPS), "-i", join(frames, "f%04d.png"), "-i", palette,
  "-lavfi", `${scale}[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle`, OUT]);
rmSync(work, { recursive: true, force: true });
console.log("wrote", OUT);
