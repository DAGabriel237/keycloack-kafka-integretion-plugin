package com.danieldev.keycloak.kafka;

import com.danieldev.keycloak.kafka.dlq.DlqPublisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.List;
import java.util.Properties;

/**
 * Factory — a true singleton within a Keycloak node.
 *
 * ── Responsibilities ──────────────────────────────────────────────────────
 * 1. Build and own the shared KafkaProducer (thread-safe, expensive to create).
 * 2. Build and own the DlqPublisher (wraps the same producer for DLQ routing).
 * 3. Resolve and validate all configuration from environment variables at startup.
 * 4. Provision the required Kafka topics if they do not yet exist.
 * 5. Vend lightweight KeycloakKafkaEventListener instances per request.
 * 6. Gracefully drain and close the producer on Keycloak shutdown.
 *
 * ── Configuration (all env vars) ──────────────────────────────────────────
 *
 * Required:
 *   KAFKA_BOOTSTRAP_SERVERS   e.g. "kafka:9092" or "broker1:9092,broker2:9092"
 *   KAFKA_USER_EVENTS_TOPIC   e.g. "user-events"
 *
 * Optional — topic provisioning:
 *   KAFKA_TOPIC_PARTITIONS    default: 3
 *   KAFKA_TOPIC_REPLICATION   default: 1 (use 3 in production)
 *
 * Optional — TLS / SASL (leave unset for plaintext):
 *   KAFKA_SECURITY_PROTOCOL   e.g. "SASL_SSL"
 *   KAFKA_SASL_MECHANISM      e.g. "SCRAM-SHA-512"
 *   KAFKA_SASL_JAAS_CONFIG    e.g. "org.apache.kafka.common.security.scram.ScramLoginModule required username="..." password="...";"
 *   KAFKA_SSL_TRUSTSTORE_LOCATION
 *   KAFKA_SSL_TRUSTSTORE_PASSWORD
 *   KAFKA_SSL_KEYSTORE_LOCATION
 *   KAFKA_SSL_KEYSTORE_PASSWORD
 *
 * ── SPI registration ──────────────────────────────────────────────────────
 * Listed in:
 *   src/main/resources/META-INF/services/org.keycloak.events.EventListenerProviderFactory
 *
 * Registered in Keycloak admin UI:
 *   Realm Settings → Events → Event listeners → add "kafka-event-listener"
 */
public final class KeycloakKafkaEventListenerFactory implements EventListenerProviderFactory {

    private static final Logger log = Logger.getLogger(KeycloakKafkaEventListenerFactory.class);

    /** Must match the ID registered in Keycloak admin UI → Event listeners */
    private static final String PROVIDER_ID = "kafka-event-listener";

    private KafkaProducer<String, byte[]> producer;
    private DlqPublisher dlqPublisher;
    private String topic;

    // ── Lifecycle ─────────────────────────────────────────────────────────

    @Override
    public void init(Config.Scope config) {
        String bootstrapServers = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        topic = requireEnv("KAFKA_USER_EVENTS_TOPIC");

        Properties props = buildProducerProperties(bootstrapServers);

        // Build the shared producer — thread-safe, one instance per Keycloak node
        producer = new KafkaProducer<>(props);

        // Build the DLQ publisher — wraps the same producer, routes to <topic>-dlq
        dlqPublisher = new DlqPublisher(producer, topic);

        // Ensure topics exist — runs once at startup, never on the request path
        Properties securityOnlyProps = extractSecurityProps(props, bootstrapServers);
        TopicProvisioner provisioner = new TopicProvisioner(securityOnlyProps);
        provisioner.ensureTopicsExist(List.of(
            topic,
            topic + "-dlq"   // provision DLQ alongside the main topic
        ));

        log.infof("[KeycloakKafka] Plugin initialized. Topic: %s  DLQ: %s-dlq", topic, topic);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // Nothing needed here — all initialization is done in init()
    }

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        // Lightweight — called per Keycloak request (login, register, etc.)
        // All the expensive state (producer, dlqPublisher) is shared from init()
        return new KeycloakKafkaEventListener(producer, topic, dlqPublisher);
    }

    @Override
    public void close() {
        if (producer != null) {
            log.info("[KeycloakKafka] Flushing and closing producer...");
            producer.flush();   // drain any buffered messages before shutdown
            producer.close();
            log.info("[KeycloakKafka] Producer closed cleanly.");
        }
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    // ── Producer configuration ────────────────────────────────────────────

    private static Properties buildProducerProperties(String bootstrapServers) {
        Properties props = new Properties();

        // ── Connection ────────────────────────────────────────────────────
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // ── Reliability ───────────────────────────────────────────────────
        // acks=all: leader + all in-sync replicas must acknowledge before success
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        // Idempotent producer: prevents duplicate messages caused by producer retries
        // (e.g. broker received the message but network died before ACK returned)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     "true");
        props.put(ProducerConfig.RETRIES_CONFIG,                "3");
        // Required when idempotence is enabled — one in-flight batch at a time
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        // ── Performance ───────────────────────────────────────────────────
        // Linger 5ms: wait up to 5ms to batch more records before sending
        // (good trade-off — login events aren't latency-sensitive)
        props.put(ProducerConfig.LINGER_MS_CONFIG,              "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,             "16384");    // 16KB batch
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,       "snappy");  // fast, good ratio

        // ── Security (TLS + SASL) — opt-in via env vars ───────────────────
        applySecurityConfig(props);

        return props;
    }

    /**
     * Applies optional TLS/SASL security settings from environment variables.
     * Each setting is only added if its env var is present and non-blank —
     * unset vars are ignored, so plaintext brokers work with zero config.
     */
    private static void applySecurityConfig(Properties props) {
        putIfPresent(props, "security.protocol",         "KAFKA_SECURITY_PROTOCOL");
        putIfPresent(props, "sasl.mechanism",            "KAFKA_SASL_MECHANISM");
        putIfPresent(props, "sasl.jaas.config",          "KAFKA_SASL_JAAS_CONFIG");
        putIfPresent(props, "ssl.truststore.location",   "KAFKA_SSL_TRUSTSTORE_LOCATION");
        putIfPresent(props, "ssl.truststore.password",   "KAFKA_SSL_TRUSTSTORE_PASSWORD");
        putIfPresent(props, "ssl.keystore.location",     "KAFKA_SSL_KEYSTORE_LOCATION");
        putIfPresent(props, "ssl.keystore.password",     "KAFKA_SSL_KEYSTORE_PASSWORD");
    }

    /**
     * Extracts only bootstrap servers + security properties into a clean
     * Properties object suitable for AdminClient initialization.
     *
     * AdminClient rejects producer-only keys (serializers, acks, etc.) with
     * warnings or errors, so we strip them before passing to TopicProvisioner.
     */
    private static Properties extractSecurityProps(Properties producerProps, String bootstrapServers) {
        Properties admin = new Properties();
        admin.put("bootstrap.servers", bootstrapServers);
        for (String key : List.of(
                "security.protocol", "sasl.mechanism", "sasl.jaas.config",
                "ssl.truststore.location", "ssl.truststore.password",
                "ssl.keystore.location",   "ssl.keystore.password")) {
            if (producerProps.containsKey(key)) {
                admin.put(key, producerProps.get(key));
            }
        }
        return admin;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static void putIfPresent(Properties props, String kafkaKey, String envVar) {
        String value = System.getenv(envVar);
        if (value != null && !value.isBlank()) {
            props.put(kafkaKey, value);
        }
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                "[KeycloakKafka] Required environment variable not set: " + name
                + ". Keycloak will not start until this is configured.");
        }
        return value;
    }
}
