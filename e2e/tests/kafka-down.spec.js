const { test, expect } = require('@playwright/test');
const { TOPIC_EVENTS, passwordLogin, decodeJwt, waitForRecord, stopKafka, startKafka } = require('./helpers');

// Stops this stack's broker, so it runs in its own Playwright project after every other test
// (playwright.config.js), and always starts the broker again.

const LOGINS = 10;
const MAX_LOGIN_MS = 500;

async function timedLogins(request, count) {
  const timings = [];
  for (let i = 0; i < count; i++) {
    const start = performance.now();
    const res = await passwordLogin(request);
    timings.push(Math.round(performance.now() - start));
    expect(res.status()).toBe(200);
  }
  return timings;
}

test('logins stay fast while Kafka is down, and publishing resumes once it is back', async ({ request }) => {
  test.setTimeout(240_000);
  const up = await timedLogins(request, LOGINS);

  stopKafka();
  let down;
  try {
    down = await timedLogins(request, LOGINS);
  } finally {
    await startKafka();
  }
  console.log(`login ms with Kafka up:   ${up.join(', ')}`);
  console.log(`login ms with Kafka down: ${down.join(', ')}`);
  for (const ms of down) expect(ms).toBeLessThan(MAX_LOGIN_MS);

  // The sender survived the outage: a new login is published again.
  const res = await passwordLogin(request);
  expect(res.status()).toBe(200);
  const sid = decodeJwt((await res.json()).access_token).sid;
  await waitForRecord(TOPIC_EVENTS, (r) => r.value.data?.sessionId === sid, { timeoutMs: 90_000 });
});
