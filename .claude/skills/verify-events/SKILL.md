---
name: verify-events
description: Run the local Keycloak + Kafka stack, trigger user and admin events, and read them back from the Kafka topics to confirm the listener publishes correct envelopes, keys and headers. Use after changing listener code, routing, envelope or producer config, or when the user asks to "check it works", "see the events", or debug missing events.
---

# Verify events end to end

Stack: `docker-compose.yml` (Keycloak on :8080, Kafka in-network `kafka:9092`, host `localhost:9094`).
Realm `kafka-demo`, user `testuser`/`password`, public client `kafka-demo-client` (direct grants on),
admin `admin`/`admin`.

1. Build and start fresh (code changes need `--build`):
   ```bash
   docker compose down -v && docker compose up --build -d
   until curl -sf http://localhost:8080/realms/kafka-demo/.well-known/openid-configuration >/dev/null; do sleep 2; done
   ```
2. Trigger events:
   ```bash
   # user LOGIN
   curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/realms/kafka-demo/protocol/openid-connect/token \
     -d grant_type=password -d client_id=kafka-demo-client -d username=testuser -d password=password
   # user LOGIN_ERROR
   curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/realms/kafka-demo/protocol/openid-connect/token \
     -d grant_type=password -d client_id=kafka-demo-client -d username=testuser -d password=wrong
   # admin CREATE user
   TOKEN=$(curl -s -X POST http://localhost:8080/realms/master/protocol/openid-connect/token \
     -d grant_type=password -d client_id=admin-cli -d username=admin -d password=admin | jq -r .access_token)
   curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/admin/realms/kafka-demo/users \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"username":"probe-'$RANDOM'","enabled":true}'
   ```
3. Read the topics (inside the broker container, no local Kafka tools needed):
   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic keycloak.events --from-beginning --timeout-ms 10000 \
     --formatter-property print.key=true --formatter-property print.headers=true
   ```
   Repeat with `--topic keycloak.admin-events`. Pipe values through `jq` when checking envelope fields.
4. Check against README "Envelope" and "Topics, keys and ordering": key = `realmId:userId` (admin events: see README),
   headers `kc-event-type`, `kc-realm-id`, `kc-schema-version`, no secrets/credentials in payloads,
   admin `representation` absent unless the realm has "Include representation" on **and** the listener has
   `include-representation=true` (the compose stack leaves it at the default `false`).
5. Failure-mode check (the most important property): `docker compose stop kafka`, repeat a login →
   it must still return 200 quickly (well under 1s extra); `docker compose logs keycloak | grep -i kafka`
   shows a bounded warning, not a stack trace per event. `docker compose start kafka` afterwards.
6. Optional UI: `docker compose --profile ui up -d` → http://localhost:8081.
7. Clean up with `docker compose down -v` when done.

Report what you saw (actual records), not what you expected to see.
