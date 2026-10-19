const { execFileSync } = require('child_process');
const path = require('path');

const KEYCLOAK_URL = 'http://localhost:8080';
const REALM = 'kafka-demo';
const CLIENT_ID = 'kafka-demo-client';
const COMPOSE_DIR = path.resolve(__dirname, '..', '..');

/** Admin access token from the master realm (admin/admin from docker-compose.yml). */
async function adminToken(request) {
  const res = await request.post(`${KEYCLOAK_URL}/realms/master/protocol/openid-connect/token`, {
    form: { grant_type: 'password', client_id: 'admin-cli', username: 'admin', password: 'admin' },
  });
  if (!res.ok()) throw new Error(`admin token failed: ${res.status()} ${await res.text()}`);
  return (await res.json()).access_token;
}

/** Password grant for testuser; produces a LOGIN (or LOGIN_ERROR) event. */
async function passwordLogin(request, username = 'testuser', password = 'password') {
  return request.post(`${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token`, {
    form: { grant_type: 'password', client_id: CLIENT_ID, username, password },
  });
}

/**
 * Reads records from a topic with the broker's own console consumer, running inside the
 * kafka container (no Node Kafka client needed). Starts from the beginning, stops after
 * `timeoutMs` of inactivity, and returns parsed records: { key, headers, value }.
 *
 * Records are printed as KEY<TAB>HEADERS<TAB>VALUE. Values are JSON envelopes once M1
 * is implemented; unparseable values are returned as raw strings.
 */
function consumeTopic(topic, { timeoutMs = 10000, maxMessages } = {}) {
  const args = [
    'compose', 'exec', '-T', 'kafka',
    '/opt/kafka/bin/kafka-console-consumer.sh',
    '--bootstrap-server', 'localhost:9092',
    '--topic', topic,
    '--from-beginning',
    '--timeout-ms', String(timeoutMs),
    '--property', 'print.key=true',
    '--property', 'print.headers=true',
    '--property', 'key.separator=\t',
    '--property', 'headers.separator=,',
  ];
  if (maxMessages) args.push('--max-messages', String(maxMessages));

  let out;
  try {
    out = execFileSync('docker', args, { cwd: COMPOSE_DIR, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (e) {
    // The console consumer exits non-zero on --timeout-ms; whatever it printed is still valid.
    out = e.stdout || '';
  }
  return out
    .split('\n')
    .filter((line) => line.trim() !== '' && !line.startsWith('Processed a total of'))
    .map((line) => {
      const [headers, key, ...rest] = line.split('\t');
      const raw = rest.join('\t');
      let value = raw;
      try { value = JSON.parse(raw); } catch (_) { /* keep raw */ }
      return { headers, key, value };
    });
}

module.exports = { KEYCLOAK_URL, REALM, CLIENT_ID, adminToken, passwordLogin, consumeTopic };
