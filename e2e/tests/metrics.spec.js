const { test, expect } = require('@playwright/test');
const { TOPIC_EVENTS, passwordLogin, decodeJwt, waitForRecord } = require('./helpers');

// Keycloak runs with metrics enabled (docker-compose.yml); its management interface serves Prometheus text on
// :9000/metrics, including the listener's Micrometer meters.

const METRICS_URL = 'http://localhost:9000/metrics';

/** Sum of all samples of `name` whose labels include every `labels` entry. */
async function sample(request, name, labels = {}) {
  const res = await request.get(METRICS_URL);
  expect(res.ok()).toBeTruthy();
  let total = 0;
  for (const line of (await res.text()).split('\n')) {
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? (\S+)$/);
    if (!match || match[1] !== name) continue;
    const present = Object.fromEntries([...(match[2] || '').matchAll(/(\w+)="([^"]*)"/g)].map((m) => [m[1], m[2]]));
    if (Object.entries(labels).every(([k, v]) => present[k] === v)) total += Number(match[3]);
  }
  return total;
}

test('listener metrics are exposed and count published events', async ({ request }) => {
  const before = await sample(request, 'keycloak_kafka_events_sent_total', { kind: 'user', topic: TOPIC_EVENTS });

  const res = await passwordLogin(request);
  expect(res.status()).toBe(200);
  const sid = decodeJwt((await res.json()).access_token).sid;
  await waitForRecord(TOPIC_EVENTS, (r) => r.value.data?.sessionId === sid);

  await expect.poll(() => sample(request, 'keycloak_kafka_events_sent_total', { kind: 'user', topic: TOPIC_EVENTS }))
    .toBeGreaterThan(before);
  expect(await sample(request, 'keycloak_kafka_events_enqueued_total', { kind: 'user' })).toBeGreaterThan(0);
  expect(await sample(request, 'keycloak_kafka_events_queue_capacity')).toBe(10000);
  const text = await (await request.get(METRICS_URL)).text();
  expect(text).toContain('keycloak_kafka_events_queue_size');
});
