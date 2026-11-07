// p99 enqueue latency: POST /jobs at a constant arrival rate.
//
//   k6 run -e RELAY_URL=http://<host>:8080 -e RATE=500 -e LABEL=t3.small loadtest/latency.js
import http from 'k6/http';
import { check } from 'k6';
import { BASE, jsonParams, metricValue, writeSummary } from './lib.js';

const RATE = Number(__ENV.RATE || 500);
const DURATION = __ENV.DURATION || '60s';
const body = JSON.stringify({ type: 'sleep', payload: { ms: 0 } });
const params = jsonParams('POST /jobs');

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(50, RATE / 5),
      maxVUs: Math.max(500, RATE),
    },
  },
  thresholds: {
    'http_req_duration{name:POST /jobs}': ['p(99)<100'],
    http_req_failed: ['rate<0.001'],
  },
};

export default function () {
  const res = http.post(`${BASE}/jobs`, body, params);
  check(res, { '202 Accepted': (r) => r.status === 202 });
}

export function handleSummary(data) {
  const m = 'http_req_duration{name:POST /jobs}';
  return writeSummary(data, 'latency', {
    'rate': `${RATE}/s for ${DURATION}`,
    'requests': metricValue(data, 'http_reqs', 'count'),
    'failure rate': `${((metricValue(data, 'http_req_failed', 'rate') || 0) * 100).toFixed(3)}%`,
    'p50 (ms)': (metricValue(data, m, 'p(50)') || 0).toFixed(2),
    'p95 (ms)': (metricValue(data, m, 'p(95)') || 0).toFixed(2),
    'p99 (ms)': (metricValue(data, m, 'p(99)') || 0).toFixed(2),
    'max (ms)': (metricValue(data, m, 'max') || 0).toFixed(2),
  });
}
