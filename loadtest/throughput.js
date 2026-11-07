// Throughput: enqueue sleep jobs with ms=0 at a rising arrival rate until
// requests start failing, then measure how fast the workers drained them.
//
//   k6 run -e RELAY_URL=http://<host>:8080 -e LABEL=t3.small loadtest/throughput.js
//
// Reports two numbers:
//   accepted/s  - peak sustained rate POST /jobs returned 202 (k6)
//   processed/s - jobs completed per second end to end, from /stats deltas
import http from 'k6/http';
import { check } from 'k6';
import { Gauge } from 'k6/metrics';
import { BASE, jsonParams, stats, waitDrained, metricValue, writeSummary } from './lib.js';

const MAX_RATE = Number(__ENV.MAX_RATE || 6000);
const STAGE = __ENV.STAGE || '20s';
const body = JSON.stringify({ type: 'sleep', payload: { ms: 0 } });
const params = jsonParams('POST /jobs');

// Filled in by teardown (handleSummary does not receive teardown's return).
const processedJobs = new Gauge('relay_jobs_processed');
const processedRate = new Gauge('relay_processed_per_sec');
const drainSecs = new Gauge('relay_drain_secs');

export const options = {
  discardResponseBodies: true,
  setupTimeout: '30s',
  teardownTimeout: '10m',
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: Math.min(200, MAX_RATE),
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 3000,
      stages: [0.1, 0.2, 0.35, 0.5, 0.7, 0.85, 1].map((f) => ({ target: Math.round(MAX_RATE * f), duration: STAGE })),
    },
  },
  thresholds: {
    // "Ramp until errors": stop once more than 1% of enqueues fail.
    http_req_failed: [{ threshold: 'rate<0.01', abortOnFail: true, delayAbortEval: '5s' }],
  },
};

export function setup() {
  const s = stats();
  return { startSucceeded: s.statuses.succeeded, startTotal: s.total, startMs: Date.now() };
}

export default function () {
  const res = http.post(`${BASE}/jobs`, body, params);
  check(res, { '202 Accepted': (r) => r.status === 202 });
}

export function teardown(ctx) {
  const enqueueEnd = Date.now();
  const end = waitDrained(600);
  const doneMs = Date.now();
  const processed = end.statuses.succeeded - ctx.startSucceeded;
  const secs = (doneMs - ctx.startMs) / 1000;
  processedJobs.add(processed);
  drainSecs.add(secs);
  processedRate.add(processed / secs);
  console.log(`enqueue phase ${((enqueueEnd - ctx.startMs) / 1000).toFixed(1)}s, drained after ${secs.toFixed(1)}s`);
}

export function handleSummary(data) {
  const reqs = metricValue(data, 'http_reqs', 'count') || 0;
  const failed = metricValue(data, 'http_req_failed', 'rate') || 0;
  const dur = data.state.testRunDurationMs / 1000;
  return writeSummary(data, 'throughput', {
    'requests sent': reqs,
    'failure rate': `${(failed * 100).toFixed(2)}%`,
    'accepted/s (avg over run)': ((reqs * (1 - failed)) / dur).toFixed(0),
    'peak target rate': `${MAX_RATE}/s`,
    'jobs processed': metricValue(data, 'relay_jobs_processed', 'value'),
    'drained after (s)': (metricValue(data, 'relay_drain_secs', 'value') || 0).toFixed(1),
    'processed/s (end to end)': (metricValue(data, 'relay_processed_per_sec', 'value') || 0).toFixed(0),
    'POST /jobs p99 (ms)': (metricValue(data, 'http_req_duration{name:POST /jobs}', 'p(99)') || metricValue(data, 'http_req_duration', 'p(99)') || 0).toFixed(1),
  });
}
