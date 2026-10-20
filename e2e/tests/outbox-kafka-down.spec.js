const { test, expect } = require('@playwright/test');
const {
  TOPIC_EVENTS, SECURE, passwordLogin, decodeJwt, consumeTopic, waitForRecord, stopKafka, startKafka,
} = require('./helpers');

// keycloak-secure runs delivery-mode=outbox: events go into Keycloak's OUTBOX_ENTRY table in the request
// transaction and a drainer publishes them. Stops kafka-secure, so it is its own Playwright project that runs
// after the main tests, and always starts the broker again.

const LOGINS = 5;

test('outbox mode delivers every event made while Kafka was down, once it is back', async ({ request }) => {
  test.setTimeout(240_000);
  const sessions = [];

  stopKafka(SECURE);
  try {
    for (let i = 0; i < LOGINS; i++) {
      const start = performance.now();
      const res = await passwordLogin(request, 'testuser', 'password', SECURE);
      const ms = Math.round(performance.now() - start);
      expect(res.status()).toBe(200);
      expect(ms).toBeLessThan(1000);
      sessions.push(decodeJwt((await res.json()).access_token).sid);
    }
  } finally {
    await startKafka({ stack: SECURE });
  }

  for (const sid of sessions) {
    await waitForRecord(TOPIC_EVENTS,
      (r) => r.value.type === 'org.keycloak.event.LOGIN' && r.value.data?.sessionId === sid,
      { stack: SECURE, timeoutMs: 120_000 });
  }
  // At-least-once: a login may appear more than once, but only ever with the same event id.
  const records = consumeTopic(TOPIC_EVENTS, { stack: SECURE });
  for (const sid of sessions) {
    const ids = new Set(records
      .filter((r) => r.value.type === 'org.keycloak.event.LOGIN' && r.value.data?.sessionId === sid)
      .map((r) => r.value.id));
    expect(ids.size).toBe(1);
  }
});
