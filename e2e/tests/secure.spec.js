const { test, expect } = require('@playwright/test');
const { TOPIC_EVENTS, SECURE, passwordLogin, demoRealm, decodeJwt, waitForRecord, topicCodecs } = require('./helpers');

// The `secure` compose profile: keycloak-secure publishes to kafka-secure over SASL_SSL with SCRAM-SHA-512
// (settings and secrets from producer-properties-file) and compresses with zstd, whose codec JAR sits in
// providers/ next to the listener (Dockerfile target keycloak-zstd).

test('publishes over SASL_SSL with SCRAM and zstd compression', async ({ request }) => {
  const realm = await demoRealm(request, SECURE);
  const res = await passwordLogin(request, 'testuser', 'password', SECURE);
  expect(res.status()).toBe(200);
  const claims = decodeJwt((await res.json()).access_token);

  const record = await waitForRecord(TOPIC_EVENTS,
    (r) => r.value.type === 'org.keycloak.event.LOGIN' && r.value.data?.sessionId === claims.sid,
    { stack: SECURE });

  expect(record.key).toBe(`${realm.id}:${claims.sub}`);
  expect(record.value.source).toBe(realm.issuer);
  expect(topicCodecs(TOPIC_EVENTS, SECURE)).toEqual(['zstd']);
});
