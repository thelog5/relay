// Shared helpers for Relay's k6 scripts.
import http from 'k6/http';
import { sleep } from 'k6';

export const BASE = (__ENV.RELAY_URL || 'http://localhost:8080').replace(/\/$/, '');
export const LABEL = __ENV.LABEL || 'local';
// run.sh points this at one folder per run so every artifact lands together.
const OUT_DIR = (__ENV.OUT_DIR || 'loadtest/results').replace(/\/$/, '');

export const jsonParams = (name) => ({
  headers: { 'Content-Type': 'application/json' },
  tags: { name },
});

export function stats() {
  // responseType overrides discardResponseBodies, which the scripts set.
  const res = http.get(`${BASE}/stats`, { tags: { name: 'GET /stats' }, responseType: 'text' });
  if (res.status !== 200) throw new Error(`GET /stats -> ${res.status}`);
  return JSON.parse(res.body);
}

export function openJobs(s) {
  return s.statuses.queued + s.statuses.running + s.statuses.retry_scheduled;
}

// Wait until nothing is queued, running or waiting to retry.
export function waitDrained(timeoutSec) {
  const deadline = Date.now() + timeoutSec * 1000;
  for (;;) {
    const s = stats();
    if (openJobs(s) === 0) return s;
    if (Date.now() > deadline) throw new Error(`not drained after ${timeoutSec}s: ${JSON.stringify(s.statuses)}`);
    sleep(0.5);
  }
}

export function metricValue(data, name, stat) {
  const m = data.metrics[name];
  return m && m.values ? m.values[stat] : undefined;
}

export function writeSummary(data, name, extra) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const out = { label: LABEL, url: BASE, at: new Date().toISOString(), ...extra, k6: data.metrics };
  const lines = [
    '',
    `== ${name} (${LABEL}) ==`,
    ...Object.entries(extra).map(([k, v]) => `  ${k.padEnd(28)} ${v}`),
    '',
  ];
  return {
    stdout: lines.join('\n'),
    [`${OUT_DIR}/${name}-${LABEL}-${stamp}.json`]: JSON.stringify(out, null, 2),
  };
}
