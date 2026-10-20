FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src src
RUN mvn clean package -DskipTests -B

FROM quay.io/keycloak/keycloak:26.8.1 AS keycloak
# Match any version of the shaded JAR so version bumps don't require touching this file.
COPY --from=builder /build/target/keycloak-kafka-event-listener-*.jar /opt/keycloak/providers/
COPY realm-export.json /opt/keycloak/data/import/realm-export.json
RUN /opt/keycloak/bin/kc.sh build

# The zstd codec JAR is not bundled (README "Compression"); this image adds it to providers/ the way the README
# tells users to. Used by the `secure` compose profile. Keep the version in sync with KafkaProducers.ZSTD_JAR.
FROM builder AS codecs
ARG ZSTD_JNI_VERSION=1.5.7-17
RUN mvn -B -q dependency:copy -Dartifact=com.github.luben:zstd-jni:${ZSTD_JNI_VERSION} -DoutputDirectory=/codecs

FROM keycloak AS keycloak-zstd
COPY --from=codecs /codecs/ /opt/keycloak/providers/
RUN /opt/keycloak/bin/kc.sh build

# Production-style build of the same: Postgres, health and metrics are build options, so the image starts with
# `kc.sh start --optimized` like a real deployment. Used by the `secure` compose profile (a two-node cluster).
FROM keycloak-zstd AS keycloak-prod
RUN /opt/keycloak/bin/kc.sh build --db=postgres --health-enabled=true --metrics-enabled=true

# Default target (`docker build .`): the plain image.
FROM keycloak
