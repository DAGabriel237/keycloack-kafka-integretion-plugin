# =========================================================
# Stage 1: Build the plugin JAR with Maven
# =========================================================
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

# Cache dependencies first
COPY pom.xml .
RUN mvn -B dependency:go-offline

# Build the shaded jar (tests are run — remove -DskipTests if you want them to run in CI instead)
COPY src ./src
RUN mvn -B clean package -DskipTests

# =========================================================
# Stage 2: Keycloak with the plugin baked in
# =========================================================
FROM quay.io/keycloak/keycloak:26.4 AS final

# Only the shaded jar is copied in — NOT original-keycloak-kafka-plugin-*.jar,
# which lacks the relocated kafka-clients/jackson classes and would break the SPI.
COPY --from=builder /build/target/keycloak-kafka-plugin-*.jar /opt/keycloak/providers/

# Bake the provider into the image (faster container startup than --optimized at runtime)
RUN /opt/keycloak/bin/kc.sh build

ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]
CMD ["start-dev"]

# =========================================================
# Runtime configuration (set these with -e or in docker-compose):
#
# Required:
#   KAFKA_BOOTSTRAP_SERVERS   e.g. kafka:9092
#   KAFKA_USER_EVENTS_TOPIC   e.g. user-events
#
# Optional (topic provisioning):
#   KAFKA_TOPIC_PARTITIONS    default: 3
#   KAFKA_TOPIC_REPLICATION   default: 1
#
# Optional (TLS / SASL, leave unset for plaintext):
#   KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_SASL_JAAS_CONFIG,
#   KAFKA_SSL_TRUSTSTORE_LOCATION, KAFKA_SSL_TRUSTSTORE_PASSWORD,
#   KAFKA_SSL_KEYSTORE_LOCATION, KAFKA_SSL_KEYSTORE_PASSWORD
#
# After startup, enable it per realm:
#   Admin Console -> Realm Settings -> Events -> Event listeners -> add "kafka-event-listener"
# =========================================================
