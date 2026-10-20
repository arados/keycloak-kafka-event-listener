#!/usr/bin/env node
// Load sanity check (not part of the Playwright suite): drives ~RATE token requests per second against the
// main compose stack, first with Kafka up, then with the broker stopped, and compares request latency and
// the listener's queue. Each request is a refresh_token grant: it emits a REFRESH_TOKEN event without the
// password hashing that caps password logins far below 1k/s on a laptop, so the listener is what is measured.
//
//   docker compose up --build -d && node e2e/load/load.js [rate=1000] [seconds=30] [concurrency=64]
//
// Restarts the broker at the end. Prints p50/p95/p99 per phase, achieved rate, errors, and the listener's
// queue size, drops and Keycloak's heap from :9000/metrics.

const { execFileSync } = require('child_process');
const path = require('path');

const [rate = 1000, seconds = 30, concurrency = 64] = process.argv.slice(2).map(Number);
const KEYCLOAK = 'http://localhost:8080/realms/kafka-demo/protocol/openid-connect/token';
const METRICS = 'http://localhost:9000/metrics';
const COMPOSE_DIR = path.resolve(__dirname, '..', '..');
const form = (fields) => ({ method: 'POST', body: new URLSearchParams(fields) });

async function refreshToken() {
  const res = await fetch(KEYCLOAK, form({
    grant_type: 'password', client_id: 'kafka-demo-client', username: 'testuser', password: 'password',
  }));
  if (!res.ok) throw new Error(`login failed: ${res.status}`);
  return (await res.json()).refresh_token;
}

async function metrics() {
  const text = await (await fetch(METRICS)).text();
  const pick = (re) => [...text.matchAll(re)].reduce((sum, m) => sum + Number(m[1]), 0);
  return {
    queue: pick(/^keycloak_kafka_events_queue_size(?:\{[^}]*\})? (\S+)$/gm),
    queueFull: pick(/^keycloak_kafka_events_dropped_total\{[^}]*reason="queue_full"[^}]*\} (\S+)$/gm),
    sent: pick(/^keycloak_kafka_events_sent_total\{[^}]*\} (\S+)$/gm),
    heapMb: Math.round(pick(/^jvm_memory_used_bytes\{[^}]*area="heap"[^}]*\} (\S+)$/gm) / 1048576),
  };
}

const pct = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];

async function phase(name, refresh) {
  const latencies = [];
  let errors = 0;
  let next = performance.now();
  const end = next + seconds * 1000;
  const interval = 1000 / rate;
  let peakQueue = 0;
  const sampler = setInterval(async () => {
    try { peakQueue = Math.max(peakQueue, (await metrics()).queue); } catch (_) { /* ignore */ }
  }, 1000);

  async function worker() {
    while (true) {
      const slot = next;
      next += interval;
      if (slot >= end) return;
      const wait = slot - performance.now();
      if (wait > 0) await new Promise((r) => setTimeout(r, wait));
      const start = performance.now();
      try {
        const res = await fetch(KEYCLOAK, form({
          grant_type: 'refresh_token', client_id: 'kafka-demo-client', refresh_token: refresh,
        }));
        await res.arrayBuffer();
        if (!res.ok) errors++;
      } catch (_) {
        errors++;
      }
      latencies.push(performance.now() - start);
    }
  }
  const started = performance.now();
  await Promise.all(Array.from({ length: concurrency }, worker));
  const elapsed = (performance.now() - started) / 1000;
  clearInterval(sampler);
  const sorted = latencies.sort((a, b) => a - b);
  const m = await metrics();
  const result = {
    phase: name,
    requests: sorted.length,
    rate: Math.round(sorted.length / elapsed),
    errors,
    p50: pct(sorted, 0.5).toFixed(1),
    p95: pct(sorted, 0.95).toFixed(1),
    p99: pct(sorted, 0.99).toFixed(1),
    peakQueue,
    queueFullTotal: m.queueFull,
    sentTotal: m.sent,
    heapMb: m.heapMb,
  };
  console.log(JSON.stringify(result));
  return result;
}

(async () => {
  console.log(`target ${rate} req/s for ${seconds}s per phase, concurrency ${concurrency}`);
  const refresh = await refreshToken();
  await phase('warmup', refresh);
  await phase('kafka-up', refresh);
  execFileSync('docker', ['compose', 'stop', 'kafka'], { cwd: COMPOSE_DIR, stdio: 'ignore' });
  try {
    await phase('kafka-down', refresh);
  } finally {
    execFileSync('docker', ['compose', 'start', 'kafka'], { cwd: COMPOSE_DIR, stdio: 'ignore' });
  }
})().catch((e) => { console.error(e); process.exit(1); });
