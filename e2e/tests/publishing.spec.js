const { test, expect } = require('@playwright/test');
const {
  KEYCLOAK_URL, REALM, CLIENT_ID, TOPIC_EVENTS, TOPIC_ADMIN_EVENTS,
  adminToken, passwordLogin, demoRealm, decodeJwt, consumeTopic, waitForRecord,
} = require('./helpers');

// Records are matched by something unique to this run (session id, new user id, unseen event id), since
// topics keep records from earlier runs against the same stack.

const CONTENT_TYPE = 'application/cloudevents+json; charset=UTF-8';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const RFC3339_MILLIS_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

let realm;

test.beforeAll(async ({ request }) => {
  realm = await demoRealm(request);
});

function expectHeaders(record, kind, type) {
  expect(record.headers).toEqual({
    'content-type': CONTENT_TYPE,
    'kc-kind': kind,
    'kc-event-type': type,
    'kc-realm-id': realm.id,
    'kc-schema-version': '1',
  });
}

test('password login publishes LOGIN with key, headers and envelope', async ({ request }) => {
  const res = await passwordLogin(request);
  expect(res.status()).toBe(200);
  const tokens = await res.json();
  const claims = decodeJwt(tokens.access_token);

  const record = await waitForRecord(TOPIC_EVENTS,
    (r) => r.value.type === 'org.keycloak.event.LOGIN' && r.value.data?.sessionId === claims.sid);
  console.log(`LOGIN record: key=${record.key} headers=${JSON.stringify(record.headers)}\n${record.raw}`);

  expect(record.key).toBe(`${realm.id}:${claims.sub}`);
  expectHeaders(record, 'user', 'org.keycloak.event.LOGIN');
  const envelope = record.value;
  expect(envelope).toMatchObject({
    specversion: '1.0',
    source: claims.iss,
    type: 'org.keycloak.event.LOGIN',
    subject: claims.sub,
    datacontenttype: 'application/json',
    kcschemaversion: 1,
    kcrealmid: realm.id,
    kcrealmname: REALM,
    kckind: 'user',
    data: { type: 'LOGIN', clientId: CLIENT_ID, userId: claims.sub, sessionId: claims.sid },
  });
  expect(claims.iss).toBe(realm.issuer);
  expect(envelope.id).toMatch(UUID);
  expect(envelope.time).toMatch(RFC3339_MILLIS_UTC);
  expect(envelope.data.ipAddress).toBeTruthy();
  expect(envelope.data.details.username).toBe('testuser');
  // No tokens in the payload.
  expect(record.raw).not.toContain(tokens.access_token);
  expect(record.raw).not.toContain(tokens.refresh_token);
});

test('wrong password publishes LOGIN_ERROR from the inner session with the issuer as source', async ({ request }) => {
  const userId = decodeJwt((await (await passwordLogin(request)).json()).access_token).sub;
  const seen = new Set(consumeTopic(TOPIC_EVENTS).map((r) => r.value.id));

  const res = await passwordLogin(request, 'testuser', 'not-the-password');
  expect(res.ok()).toBe(false);
  expect((await res.json()).error).toBe('invalid_grant');

  const record = await waitForRecord(TOPIC_EVENTS,
    (r) => r.value.type === 'org.keycloak.event.LOGIN_ERROR' && !seen.has(r.value.id));
  console.log(`LOGIN_ERROR record: key=${record.key}\n${record.raw}`);

  expect(record.key).toBe(`${realm.id}:${userId}`);
  expectHeaders(record, 'user', 'org.keycloak.event.LOGIN_ERROR');
  expect(record.value).toMatchObject({
    source: realm.issuer,
    type: 'org.keycloak.event.LOGIN_ERROR',
    kcrealmid: realm.id,
    kcrealmname: REALM,
    data: { type: 'LOGIN_ERROR', clientId: CLIENT_ID, userId, error: 'invalid_user_credentials' },
  });
  expect(record.raw).not.toContain('not-the-password');
});

test('admin user creation publishes USER.CREATE without representation', async ({ request }) => {
  const token = await adminToken(request);
  const auth = { Authorization: `Bearer ${token}` };
  const username = `e2e-user-${Date.now()}`;
  const res = await request.post(`${KEYCLOAK_URL}/admin/realms/${REALM}/users`, {
    headers: auth,
    data: { username, enabled: true, email: `${username}@example.com` },
  });
  expect(res.status()).toBe(201);
  const userId = res.headers().location.split('/').pop();

  try {
    const record = await waitForRecord(TOPIC_ADMIN_EVENTS,
      (r) => r.value.data?.resourcePath === `users/${userId}` && r.value.data?.operationType === 'CREATE');
    console.log(`USER.CREATE record: key=${record.key} headers=${JSON.stringify(record.headers)}\n${record.raw}`);

    expect(record.key).toBe(`${realm.id}:users/${userId}`);
    expectHeaders(record, 'admin', 'org.keycloak.admin.USER.CREATE');
    expect(record.value).toMatchObject({
      specversion: '1.0',
      source: realm.issuer,
      type: 'org.keycloak.admin.USER.CREATE',
      subject: `users/${userId}`,
      kcrealmid: realm.id,
      kcrealmname: REALM,
      kckind: 'admin',
      data: {
        operationType: 'CREATE',
        resourceType: 'USER',
        resourcePath: `users/${userId}`,
        authDetails: { realmName: 'master' },
      },
    });
    expect(record.value.id).toMatch(UUID);
    // The realm records admin event details (representations), but include-representation defaults to false.
    expect(record.value.data).not.toHaveProperty('representation');
    expect(record.raw).not.toContain(username);
  } finally {
    await request.delete(`${KEYCLOAK_URL}/admin/realms/${REALM}/users/${userId}`, { headers: auth });
  }
});
