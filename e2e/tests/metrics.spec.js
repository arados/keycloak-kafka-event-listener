const { test, expect } = require('@playwright/test');
const { TOPIC_EVENTS, METRICS_URL, passwordLogin, decodeJwt, waitForRecord, metricValue } = require('./helpers');

// Keycloak runs with metrics enabled (docker-compose.yml); its management interface serves Prometheus text on
// :9000/metrics, including the listener's Micrometer meters.

test('listener metrics are exposed and count published events', async ({ request }) => {
  const sent = () => metricValue(request, 'keycloak_kafka_events_sent_total', { kind: 'user', topic: TOPIC_EVENTS });
  const before = await sent();

  const res = await passwordLogin(request);
  expect(res.status()).toBe(200);
  const sid = decodeJwt((await res.json()).access_token).sid;
  await waitForRecord(TOPIC_EVENTS, (r) => r.value.data?.sessionId === sid);

  await expect.poll(sent).toBeGreaterThan(before);
  expect(await metricValue(request, 'keycloak_kafka_events_enqueued_total', { kind: 'user' })).toBeGreaterThan(0);
  expect(await metricValue(request, 'keycloak_kafka_events_queue_capacity')).toBe(10000);
  const text = await (await request.get(METRICS_URL)).text();
  expect(text).toContain('keycloak_kafka_events_queue_size');
});
