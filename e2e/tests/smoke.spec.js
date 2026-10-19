const { test, expect } = require('@playwright/test');
const { KEYCLOAK_URL, REALM, adminToken, passwordLogin } = require('./helpers');

// Scaffold-level checks: the shaded JAR loads into Keycloak and the demo realm uses it.
// Publishing assertions (records on keycloak.events / keycloak.admin-events) are added
// in milestone M1 — see docs/PLAN.md.

test('kafka-events listener is registered in Keycloak', async ({ request }) => {
  const token = await adminToken(request);
  const res = await request.get(`${KEYCLOAK_URL}/admin/serverinfo`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(res.ok()).toBeTruthy();
  const info = await res.json();
  expect(Object.keys(info.providers.eventsListener.providers)).toContain('kafka-events');
});

test('demo realm has kafka-events enabled as an event listener', async ({ request }) => {
  const token = await adminToken(request);
  const res = await request.get(`${KEYCLOAK_URL}/admin/realms/${REALM}/events/config`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(res.ok()).toBeTruthy();
  const cfg = await res.json();
  expect(cfg.eventsListeners).toContain('kafka-events');
  expect(cfg.eventsEnabled).toBe(true);
  expect(cfg.adminEventsEnabled).toBe(true);
});

test('login still succeeds with the listener enabled', async ({ request }) => {
  // The listener must never break the auth flow, whatever state Kafka is in.
  const res = await passwordLogin(request);
  expect(res.status()).toBe(200);
});
