#!/bin/sh
# One-shot setup for the `secure` compose profile (DEMO ONLY: fixed passwords, self-signed certificate).
# Writes into the shared `secure-secrets` volume:
#   broker.p12 + *_creds  — the broker's TLS key pair and its password files (apache/kafka image conventions)
#   broker_jaas.conf      — JAAS file the broker requires for its SASL listener (SCRAM, no static users)
#   truststore.p12        — trusts the broker certificate (issued for kafka-secure and localhost)
#   kafka-client.properties — Keycloak's producer-properties-file: SASL_SSL + SCRAM-SHA-512 + truststore
set -eu

dir=/secrets
password=changeit

if [ -f "$dir/kafka-client.properties" ]; then
  echo "secrets already present"
  exit 0
fi

keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 -validity 3650 -dname CN=kafka-secure \
  -ext SAN=dns:kafka-secure,dns:localhost -keystore "$dir/broker.p12" -storetype PKCS12 \
  -storepass "$password" -keypass "$password"
keytool -exportcert -alias broker -keystore "$dir/broker.p12" -storepass "$password" -rfc -file "$dir/broker.pem"
keytool -importcert -noprompt -alias broker -file "$dir/broker.pem" -keystore "$dir/truststore.p12" \
  -storetype PKCS12 -storepass "$password"
printf '%s' "$password" > "$dir/broker_keystore_creds"
printf '%s' "$password" > "$dir/broker_key_creds"

cat > "$dir/broker_jaas.conf" <<'JAAS'
KafkaServer {
  org.apache.kafka.common.security.scram.ScramLoginModule required;
};
JAAS

cat > "$dir/kafka-client.properties" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="keycloak" password="keycloak-secret";
ssl.truststore.location=/opt/keycloak/conf/kafka/truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=$password
PROPS

chmod 644 "$dir"/*
echo "secrets generated"
