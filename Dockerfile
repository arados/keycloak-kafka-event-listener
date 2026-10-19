FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY pom.xml .
COPY listener/pom.xml listener/pom.xml
COPY dist/pom.xml dist/pom.xml
RUN mvn dependency:go-offline -B
COPY listener/src listener/src
RUN mvn clean package -DskipTests -B

FROM quay.io/keycloak/keycloak:26.8.1 AS keycloak
# Match any version of the shaded JAR so version bumps don't require touching this file.
COPY --from=builder /build/dist/target/keycloak-kafka-events-*.jar /opt/keycloak/providers/
COPY realm-export.json /opt/keycloak/data/import/realm-export.json
RUN /opt/keycloak/bin/kc.sh build
