const { test, expect } = require('@playwright/test');
const {
  TOPIC_EVENTS, SECURE, SECURE_2, passwordLogin, demoRealm, decodeJwt, consumeTopic, waitForRecord, topicCodecs,
  metricValue,
} = require('./helpers');

// The `secure` compose profile, set up like a production deployment: two Keycloak nodes started with
// `kc.sh start --optimized` on Postgres, clustered, delivery-mode=outbox, publishing to kafka-secure over
// SASL_SSL with SCRAM-SHA-512 (settings and secrets from producer-properties-file) and zstd compression (codec JAR
// in providers/ next to the listener, Dockerfile target keycloak-prod).

async function loginOn(request, stack) {
  const res = await passwordLogin(request, 'testuser', 'password', stack);
  expect(res.status()).toBe(200);
  return decodeJwt((await res.json()).access_token);
}

const loginRecords = (sid) => consumeTopic(TOPIC_EVENTS, { stack: SECURE })
  .filter((r) => r.value.type === 'org.keycloak.event.LOGIN' && r.value.data?.sessionId === sid);

test('both cluster nodes publish over SASL_SSL with SCRAM and zstd, each event exactly once', async ({ request }) => {
  const realm = await demoRealm(request, SECURE);
  const node1 = await loginOn(request, SECURE);
  const node2 = await loginOn(request, SECURE_2);

  for (const claims of [node1, node2]) {
    const record = await waitForRecord(TOPIC_EVENTS,
      (r) => r.value.type === 'org.keycloak.event.LOGIN' && r.value.data?.sessionId === claims.sid,
      { stack: SECURE });
    expect(record.key).toBe(`${realm.id}:${claims.sub}`);
    expect(record.value.kcrealmid).toBe(realm.id);
  }
  // Both nodes drain the shared outbox (one at a time, rows locked): without failures nothing is sent twice.
  expect(loginRecords(node1.sid)).toHaveLength(1);
  expect(loginRecords(node2.sid)).toHaveLength(1);
  expect(topicCodecs(TOPIC_EVENTS, SECURE)).toEqual(['zstd']);
});

test('outbox metrics are exposed by the optimized build', async ({ request }) => {
  // Whichever node holds the cluster lock drains, so `sent` grows on either node; `enqueued` on the writing one.
  const sent = async () => (await metricValue(request, 'keycloak_kafka_events_sent_total', { kind: 'user' }, SECURE))
    + (await metricValue(request, 'keycloak_kafka_events_sent_total', { kind: 'user' }, SECURE_2));
  const sentBefore = await sent();
  const enqueuedBefore = await metricValue(request, 'keycloak_kafka_events_enqueued_total', { kind: 'user' }, SECURE);
  await loginOn(request, SECURE);

  await expect.poll(sent, { timeout: 30_000 }).toBeGreaterThan(sentBefore);
  expect(await metricValue(request, 'keycloak_kafka_events_enqueued_total', { kind: 'user' }, SECURE))
    .toBeGreaterThan(enqueuedBefore);
  const text = await (await request.get(SECURE.metricsUrl)).text();
  for (const gauge of ['keycloak_kafka_events_outbox_pending', 'keycloak_kafka_events_outbox_dead_letters',
    'keycloak_kafka_events_outbox_oldest_pending_age_seconds']) {
    expect(text).toContain(gauge);
  }
  expect(await metricValue(request, 'keycloak_kafka_events_outbox_dead_letters', {}, SECURE)).toBe(0);
});
