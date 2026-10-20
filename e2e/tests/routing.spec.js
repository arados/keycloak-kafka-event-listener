const { test, expect } = require('@playwright/test');
const {
  KEYCLOAK_URL, REALM, CLIENT_ID, TOPIC_EVENTS, TOPIC_ADMIN_EVENTS,
  adminToken, passwordLogin, decodeJwt, consumeTopic, waitForRecord, metricValue,
} = require('./helpers');

// docker-compose.yml configures the main Keycloak with
//   topic-overrides=LOGOUT=keycloak.events.{realmName}.logout
//   exclude-event-types=USER_INFO_REQUEST
//   exclude-admin-resource-types=REALM_ROLE
// A filtered event is proven absent by waiting for a later "sentinel" event instead of sleeping.

const LOGOUT_TOPIC = `keycloak.events.${REALM}.logout`;

const filtered = (request, kind) =>
  metricValue(request, 'keycloak_kafka_events_dropped_total', { kind, reason: 'filtered' });

test('topic override sends LOGOUT to its own templated topic', async ({ request }) => {
  const tokens = await (await passwordLogin(request)).json();
  const sid = decodeJwt(tokens.access_token).sid;

  const res = await request.post(`${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/logout`, {
    form: { client_id: CLIENT_ID, refresh_token: tokens.refresh_token },
  });
  expect(res.status()).toBe(204);

  const record = await waitForRecord(LOGOUT_TOPIC,
    (r) => r.value.type === 'org.keycloak.event.LOGOUT' && r.value.data?.sessionId === sid);
  expect(record.headers['kc-event-type']).toBe('org.keycloak.event.LOGOUT');
  expect(consumeTopic(TOPIC_EVENTS).filter(
    (r) => r.value.type === 'org.keycloak.event.LOGOUT' && r.value.data?.sessionId === sid)).toHaveLength(0);
});

test('excluded user event type is not published', async ({ request }) => {
  const filteredBefore = await filtered(request, 'user');
  // userinfo needs the openid scope.
  const login = await request.post(`${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token`, {
    form: { grant_type: 'password', client_id: CLIENT_ID, username: 'testuser', password: 'password', scope: 'openid' },
  });
  expect(login.status()).toBe(200);
  const tokens = await login.json();
  const sid = decodeJwt(tokens.access_token).sid;

  const userinfo = await request.get(`${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/userinfo`, {
    headers: { Authorization: `Bearer ${tokens.access_token}` },
  });
  expect(userinfo.status()).toBe(200);
  // Sentinel: a refresh in the same session, published after USER_INFO_REQUEST would have been (same key,
  // same partition, so order is kept).
  const refreshed = await request.post(`${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token`, {
    form: { grant_type: 'refresh_token', client_id: CLIENT_ID, refresh_token: tokens.refresh_token },
  });
  expect(refreshed.status()).toBe(200);

  await waitForRecord(TOPIC_EVENTS,
    (r) => r.value.type === 'org.keycloak.event.REFRESH_TOKEN' && r.value.data?.sessionId === sid);
  expect(consumeTopic(TOPIC_EVENTS).filter((r) => r.value.data?.sessionId === sid).map((r) => r.value.type))
    .not.toContain('org.keycloak.event.USER_INFO_REQUEST');
  expect(await filtered(request, 'user')).toBeGreaterThan(filteredBefore);
});

test('excluded admin resource type is not published', async ({ request }) => {
  const filteredBefore = await filtered(request, 'admin');
  const auth = { Authorization: `Bearer ${await adminToken(request)}` };
  const roleName = `e2e-role-${Date.now()}`;
  const username = `e2e-sentinel-${Date.now()}`;

  const role = await request.post(`${KEYCLOAK_URL}/admin/realms/${REALM}/roles`, {
    headers: auth, data: { name: roleName },
  });
  expect(role.status()).toBe(201);
  const user = await request.post(`${KEYCLOAK_URL}/admin/realms/${REALM}/users`, {
    headers: auth, data: { username, enabled: true },
  });
  expect(user.status()).toBe(201);
  const userId = user.headers().location.split('/').pop();

  try {
    await waitForRecord(TOPIC_ADMIN_EVENTS, (r) => r.value.data?.resourcePath === `users/${userId}`);
    expect(consumeTopic(TOPIC_ADMIN_EVENTS).filter((r) => r.value.data?.resourcePath === `roles/${roleName}`))
      .toHaveLength(0);
    expect(await filtered(request, 'admin')).toBeGreaterThan(filteredBefore);
  } finally {
    await request.delete(`${KEYCLOAK_URL}/admin/realms/${REALM}/users/${userId}`, { headers: auth });
    await request.delete(`${KEYCLOAK_URL}/admin/realms/${REALM}/roles/${roleName}`, { headers: auth });
  }
});
