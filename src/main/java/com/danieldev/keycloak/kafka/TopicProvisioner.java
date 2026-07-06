package com.danieldev.keycloak.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Ensures required Kafka topics exist when the plugin starts.
 *
 * ── When is this called? ──────────────────────────────────────────────────
 * Once, from KeycloakKafkaEventListenerFactory.init(). Never on the hot path
 * of a login or registration request.
 *
 * ── TopicExistsException ─────────────────────────────────────────────────
 * Treated as success. On every Keycloak restart after the first boot, the
 * topic already exists — that is the normal case, not an error.
 *
 * ── Fail-fast strategy ───────────────────────────────────────────────────
 * Any other exception fails Keycloak startup immediately. This is intentional:
 * if Kafka is unreachable or misconfigured, you want to know at startup
 * (visible in logs, health checks fail) rather than silently dropping events
 * for hours before someone notices.
 *
 * All topic failures are collected before throwing so you see every broken
 * topic name in one error, not just the first.
 */
public final class TopicProvisioner {

    private static final Logger log = Logger.getLogger(TopicProvisioner.class);

    private final Properties adminProps;
    private final int        partitions;
    private final short      replicationFactor;

    public TopicProvisioner(Properties sharedSecurityProps) {
        this.adminProps        = buildAdminProps(sharedSecurityProps);
        this.partitions        = parseIntEnv("KAFKA_TOPIC_PARTITIONS",   3);
        this.replicationFactor = parseShortEnv("KAFKA_TOPIC_REPLICATION", (short) 1);
    }

    /**
     * Creates each topic in the list if it does not already exist.
     * All topics are submitted in a single AdminClient call for efficiency.
     *
     * @param topicNames list of topic names to ensure, e.g. ["user-events", "user-events-dlq"]
     * @throws RuntimeException if any topic cannot be created for a reason other than already existing
     */
    public void ensureTopicsExist(List<String> topicNames) {
        List<NewTopic> newTopics = topicNames.stream()
            .map(name -> new NewTopic(name, partitions, replicationFactor))
            .toList();

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Map<String, ? extends org.apache.kafka.common.KafkaFuture<Void>> results =
                admin.createTopics(newTopics).values();

            List<String> failures = new ArrayList<>();

            for (Map.Entry<String, ? extends org.apache.kafka.common.KafkaFuture<Void>> entry
                    : results.entrySet()) {
                String topicName = entry.getKey();
                try {
                    entry.getValue().get(); // blocks until the broker responds for this topic
                    log.infof("[KeycloakKafka] Topic created: %s (partitions=%d, replication=%d)",
                        topicName, partitions, replicationFactor);
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof TopicExistsException) {
                        log.infof("[KeycloakKafka] Topic already exists (OK): %s", topicName);
                    } else {
                        // Collect all failures before throwing — better diagnostics
                        failures.add(topicName + " (" + ex.getCause().getMessage() + ")");
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                        "[KeycloakKafka] Topic provisioning interrupted for: " + topicName, ex);
                }
            }

            if (!failures.isEmpty()) {
                throw new RuntimeException(
                    "[KeycloakKafka] Failed to create topics: " + String.join(", ", failures));
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /**
     * Builds AdminClient-safe properties from the shared security config.
     *
     * The factory builds one Properties object with all producer config (serializers,
     * acks, idempotence, etc.) plus security settings. AdminClient rejects unknown
     * producer-only keys, so we extract only what AdminClient accepts.
     */
    private static Properties buildAdminProps(Properties sharedSecurityProps) {
        Properties props = new Properties();
        props.putAll(sharedSecurityProps);

        // Remove producer-only keys — AdminClient will log warnings or reject them
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG);
        props.remove(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);

        // Fail fast if Kafka is unreachable at startup — 10s is more than enough
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,       "10000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,   "15000");
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG,     "500");
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "5000");

        return props;
    }

    private static int parseIntEnv(String name, int defaultValue) {
        try { return Integer.parseInt(System.getenv().getOrDefault(name, String.valueOf(defaultValue))); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    private static short parseShortEnv(String name, short defaultValue) {
        try { return Short.parseShort(System.getenv().getOrDefault(name, String.valueOf(defaultValue))); }
        catch (NumberFormatException e) { return defaultValue; }
    }
}
