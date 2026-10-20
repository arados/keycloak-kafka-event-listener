const { execFileSync } = require('child_process');
const path = require('path');

const KEYCLOAK_URL = 'http://localhost:8080';
const REALM = 'kafka-demo';
const CLIENT_ID = 'kafka-demo-client';
const TOPIC_EVENTS = 'keycloak.events';
const TOPIC_ADMIN_EVENTS = 'keycloak.admin-events';
const COMPOSE_DIR = path.resolve(__dirname, '..', '..');
const METRICS_URL = 'http://localhost:9000/metrics';

/**
 * The Keycloak + Kafka setups of docker-compose.yml. Every helper takes an optional `stack` (default MAIN).
 * SECURE and SECURE_2 (compose profile `secure`) are the two nodes of a production-style cluster
 * (`start --optimized`, Postgres) publishing over SASL_SSL/SCRAM with zstd and delivery-mode=outbox; their broker's
 * in-network PLAINTEXT listener is only used by the console tools here.
 */
const MAIN = { keycloakUrl: KEYCLOAK_URL, kafkaService: 'kafka', metricsUrl: 'http://localhost:9000/metrics' };
const SECURE = {
  keycloakUrl: 'http://localhost:8082', kafkaService: 'kafka-secure', metricsUrl: 'http://localhost:9002/metrics',
};
const SECURE_2 = {
  keycloakUrl: 'http://localhost:8083', kafkaService: 'kafka-secure', metricsUrl: 'http://localhost:9003/metrics',
};

/** Admin access token from the master realm (admin/admin from docker-compose.yml). */
async function adminToken(request, stack = MAIN) {
  const res = await request.post(`${stack.keycloakUrl}/realms/master/protocol/openid-connect/token`, {
    form: { grant_type: 'password', client_id: 'admin-cli', username: 'admin', password: 'admin' },
  });
  if (!res.ok()) throw new Error(`admin token failed: ${res.status()} ${await res.text()}`);
  return (await res.json()).access_token;
}

/** Password grant for testuser; produces a LOGIN (or LOGIN_ERROR) event. */
async function passwordLogin(request, username = 'testuser', password = 'password', stack = MAIN) {
  return request.post(`${stack.keycloakUrl}/realms/${REALM}/protocol/openid-connect/token`, {
    form: { grant_type: 'password', client_id: CLIENT_ID, username, password },
  });
}

/** The demo realm's id and issuer (= the `iss` of its tokens and the envelope `source`). */
async function demoRealm(request, stack = MAIN) {
  const token = await adminToken(request, stack);
  const res = await request.get(`${stack.keycloakUrl}/admin/realms/${REALM}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok()) throw new Error(`realm lookup failed: ${res.status()} ${await res.text()}`);
  const discovery = await request.get(`${stack.keycloakUrl}/realms/${REALM}/.well-known/openid-configuration`);
  return { id: (await res.json()).id, issuer: (await discovery.json()).issuer };
}

/** Payload of a JWT (no signature check: only used to correlate records with the login). */
function decodeJwt(jwt) {
  return JSON.parse(Buffer.from(jwt.split('.')[1], 'base64url').toString('utf8'));
}

function docker(args, options = {}) {
  return execFileSync('docker', args, { cwd: COMPOSE_DIR, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], ...options });
}

/**
 * Reads all records of a topic with the broker's own console consumer, running inside the stack's
 * Kafka service container (no Node Kafka client needed). Starts from the beginning and stops after
 * `timeoutMs` without new records.
 *
 * The console consumer prints HEADERS<TAB>KEY<TAB>VALUE per record (verified with apache/kafka 4.3.1),
 * headers as `name:value` pairs separated by `,` (or `NO_HEADERS`). Returns
 * `{ headers: {name: value}, key, value, raw }`, where `value` is the parsed JSON envelope (the raw string
 * when it is not JSON) and `raw` the unparsed value.
 */
function consumeTopic(topic, { timeoutMs = 3000, stack = MAIN } = {}) {
  const args = [
    'compose', 'exec', '-T', stack.kafkaService,
    '/opt/kafka/bin/kafka-console-consumer.sh',
    '--bootstrap-server', 'localhost:9092',
    '--topic', topic,
    '--from-beginning',
    '--timeout-ms', String(timeoutMs),
    // --formatter-property, not the deprecated --property, which prints a warning to stdout.
    '--formatter-property', 'print.key=true',
    '--formatter-property', 'print.headers=true',
    '--formatter-property', 'key.separator=\t',
    '--formatter-property', 'headers.separator=,',
  ];

  let out;
  try {
    out = docker(args);
  } catch (e) {
    // The console consumer exits non-zero on --timeout-ms; whatever it printed is still valid.
    out = e.stdout || '';
  }
  return out
    .split('\n')
    .filter((line) => line.includes('\t'))
    .map((line) => {
      const [rawHeaders, key, ...rest] = line.split('\t');
      const raw = rest.join('\t');
      let value = raw;
      try { value = JSON.parse(raw); } catch (_) { /* keep raw */ }
      return { headers: parseHeaders(rawHeaders), key, value, raw };
    });
}

function parseHeaders(rawHeaders) {
  const headers = {};
  if (!rawHeaders || rawHeaders === 'NO_HEADERS') return headers;
  for (const pair of rawHeaders.split(',')) {
    const separator = pair.indexOf(':');
    headers[pair.slice(0, separator)] = pair.slice(separator + 1);
  }
  return headers;
}

/** Polls the topic until a record matches `predicate`; returns it or throws after `timeoutMs`. */
async function waitForRecord(topic, predicate, { timeoutMs = 30000, stack = MAIN } = {}) {
  const deadline = Date.now() + timeoutMs;
  let seen = 0;
  while (Date.now() < deadline) {
    const records = consumeTopic(topic, { stack });
    seen = records.length;
    const match = records.find((record) => predicate(record));
    if (match) return match;
  }
  throw new Error(`no matching record on ${topic} within ${timeoutMs} ms (${seen} records on the topic)`);
}

/** `docker compose stop <kafka service>`: only this compose project's broker, never other containers. */
function stopKafka(stack = MAIN) {
  docker(['compose', 'stop', stack.kafkaService]);
}

/** `docker compose start <kafka service>`, then waits for its health check. */
async function startKafka({ timeoutMs = 90000, stack = MAIN } = {}) {
  docker(['compose', 'start', stack.kafkaService]);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const line = docker(['compose', 'ps', '--format', 'json', stack.kafkaService]).split('\n').find((l) => l.trim());
    if (line && JSON.parse(line).Health === 'healthy') return;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error(`kafka did not become healthy within ${timeoutMs} ms`);
}

/**
 * Sum of the Prometheus samples of `name` whose labels include every entry of `labels`, scraped from the stack's
 * management interface (metrics are enabled in docker-compose.yml).
 */
async function metricValue(request, name, labels = {}, stack = MAIN) {
  const res = await request.get(stack.metricsUrl);
  if (!res.ok()) throw new Error(`metrics scrape failed: ${res.status()}`);
  let total = 0;
  for (const line of (await res.text()).split('\n')) {
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? (\S+)$/);
    if (!match || match[1] !== name) continue;
    const present = Object.fromEntries([...(match[2] || '').matchAll(/(\w+)="([^"]*)"/g)].map((m) => [m[1], m[2]]));
    if (Object.entries(labels).every(([k, v]) => present[k] === v)) total += Number(match[3]);
  }
  return total;
}

/**
 * Compression codecs of the record batches of a topic, read from the broker's log segments with
 * kafka-dump-log.sh (e.g. ['zstd']).
 */
function topicCodecs(topic, stack = MAIN) {
  const script = `for f in /tmp/kafka-logs/${topic}-*/*.log; do /opt/kafka/bin/kafka-dump-log.sh --files "$f"; done`;
  const out = docker(['compose', 'exec', '-T', stack.kafkaService, 'sh', '-c', script]);
  return [...new Set([...out.matchAll(/compresscodec: (\w+)/g)].map((m) => m[1]))];
}

module.exports = {
  KEYCLOAK_URL, REALM, CLIENT_ID, TOPIC_EVENTS, TOPIC_ADMIN_EVENTS, METRICS_URL, MAIN, SECURE, SECURE_2,
  adminToken, passwordLogin, demoRealm, decodeJwt, consumeTopic, waitForRecord, stopKafka, startKafka, topicCodecs,
  metricValue,
};
