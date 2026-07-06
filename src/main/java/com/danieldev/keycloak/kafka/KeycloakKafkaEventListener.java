package com.danieldev.keycloak.kafka;

import com.danieldev.keycloak.kafka.dlq.DlqPublisher;
import com.danieldev.keycloak.kafka.events.KeycloakUserEvent;
import com.danieldev.keycloak.kafka.security.EventSanitizer;
import com.danieldev.keycloak.kafka.serialization.EventSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

import java.util.UUID;

/**
 * Keycloak SPI EventListenerProvider.
 *
 * ── Lifecycle ─────────────────────────────────────────────────────────────
 * Keycloak creates a NEW instance of this class for every HTTP request
 * (login, registration, token refresh, etc.). It is NOT a singleton.
 *
 * The shared KafkaProducer and DlqPublisher are injected by the Factory,
 * which IS a singleton. Never create expensive resources here.
 *
 * ── What this class does ──────────────────────────────────────────────────
 * 1. Receives Keycloak user events (LOGIN, REGISTER, LOGOUT, etc.)
 * 2. Receives Keycloak admin events (user created/updated/deleted via admin API)
 * 3. Builds a KeycloakUserEvent payload with sanitized fields
 * 4. Serializes to JSON bytes
 * 5. Publishes to Kafka — or routes to DLQ on failure
 *
 * ── DLQ routing ──────────────────────────────────────────────────────────
 * Two failure modes are handled:
 *
 *   a) Serialization failure — the event object couldn't be converted to bytes.
 *      We route a minimal error payload to the DLQ with a descriptive header.
 *      The C# consumer sees it in the DLQ and can alert ops.
 *
 *   b) Kafka publish failure — broker unreachable, network error, etc.
 *      The send() callback fires asynchronously with an exception.
 *      We attempt a best-effort DLQ publish of the SAME bytes (they serialized
 *      fine — only the delivery failed).
 *
 * ── Thread safety ─────────────────────────────────────────────────────────
 * KafkaProducer is thread-safe. DlqPublisher is stateless. This class is
 * stateless. Multiple listener instances can call publish() concurrently.
 */
public final class KeycloakKafkaEventListener implements EventListenerProvider {

    private static final Logger log = Logger.getLogger(KeycloakKafkaEventListener.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String       topic;
    private final DlqPublisher dlq;

    KeycloakKafkaEventListener(
            KafkaProducer<String, byte[]> producer,
            String topic,
            DlqPublisher dlq) {
        this.producer = producer;
        this.topic    = topic;
        this.dlq      = dlq;
    }

    // ── User events: LOGIN, REGISTER, LOGOUT, UPDATE_PASSWORD, etc. ───────

    @Override
    public void onEvent(Event event) {
        if (event == null || event.getUserId() == null) return;

        KeycloakUserEvent payload = KeycloakUserEvent.builder()
            .eventId(UUID.randomUUID().toString())
            .eventType(event.getType() != null ? event.getType().name() : "UNKNOWN")
            .userId(event.getUserId())
            .realmId(event.getRealmId())
            .clientId(event.getClientId())
            .occurredAtEpochMs(event.getTime())
            .ipAddress(EventSanitizer.sanitizeIp(event.getIpAddress()))
            .details(EventSanitizer.sanitize(event.getDetails()))
            .adminEvent(false)
            .build();

        publish(event.getUserId(), payload);
    }

    // ── Admin events: user created/deleted/updated via admin API ──────────

    @Override
    public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
        if (adminEvent == null) return;

        // Only handle user-related admin operations.
        // resourcePath format: "users/{userId}" or "users/{userId}/reset-password" etc.
        String userId = extractUserId(adminEvent.getResourcePath());
        if (userId == null) return;

        KeycloakUserEvent payload = KeycloakUserEvent.builder()
            .eventId(UUID.randomUUID().toString())
            .eventType(resolveAdminEventType(adminEvent))
            .userId(userId)
            .realmId(adminEvent.getRealmId())
            .occurredAtEpochMs(adminEvent.getTime())
            .adminEvent(true)
            // Deliberately NOT including includeRepresentation:
            // the full user representation contains hashed credentials and
            // sensitive profile data — never publish it to Kafka.
            .build();

        publish(userId, payload);
    }

    @Override
    public void close() {
        // Do NOT close the producer here.
        // The producer is shared across ALL listener instances and is owned
        // by the Factory singleton, which closes it in Factory.close() during
        // Keycloak shutdown. Closing it here would break other concurrent requests.
    }

    // ── Core publish logic ────────────────────────────────────────────────

    private void publish(String messageKey, KeycloakUserEvent payload) {
        // Step 1: Serialize
        byte[] bytes = EventSerializer.toJsonBytes(payload);

        if (bytes == null) {
            // Serialization failed — we have no bytes to send to the main topic.
            // Route to DLQ with an error description so ops can investigate.
            // We use the eventId as a minimal "payload" so there's something on the DLQ record.
            String fallbackPayload = "{\"eventId\":\"" + payload.getEventId()
                + "\",\"userId\":\"" + payload.getUserId()
                + "\",\"error\":\"serialization_failed\"}";

            dlq.publish(
                messageKey,
                fallbackPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "Serialization failed for eventType=" + payload.getEventType(),
                topic
            );
            return;
        }

        // Step 2: Publish to main topic
        // The message KEY is the userId. This ensures all events for a given user
        // land on the same Kafka partition, guaranteeing ordered delivery per user.
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, messageKey, bytes);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                // Happy path — log at debug level to avoid flooding logs at scale
                log.debugf("[KeycloakKafka] Published eventType=%s userId=%s partition=%d offset=%d",
                    payload.getEventType(), messageKey,
                    metadata.partition(), metadata.offset());
            } else {
                // Publish failed (network error, broker unavailable, etc.)
                // The bytes are fine — route them to the DLQ so no event is lost.
                log.warnf(exception,
                    "[KeycloakKafka] Publish failed for eventType=%s userId=%s — routing to DLQ",
                    payload.getEventType(), messageKey);

                dlq.publish(
                    messageKey,
                    bytes,   // same bytes — serialization succeeded, delivery failed
                    "Kafka publish failed: " + exception.getMessage(),
                    topic
                );
            }
        });
    }

    // ── Static helpers ────────────────────────────────────────────────────

    /**
     * Extracts userId from Keycloak admin event resource paths.
     *
     * Examples:
     *   "users/abc-123"                 → "abc-123"   (create or delete)
     *   "users/abc-123/reset-password"  → "abc-123"   (password reset via admin)
     *   "clients/xyz"                   → null          (not a user event — ignored)
     *   null                            → null
     */
    static String extractUserId(String resourcePath) {
        if (resourcePath == null || !resourcePath.startsWith("users/")) return null;
        String[] parts = resourcePath.split("/");
        return parts.length >= 2 ? parts[1] : null;
    }

    /**
     * Maps Keycloak AdminEvent operation types to meaningful event type strings.
     * Includes the sub-path when it adds context.
     *
     * Examples:
     *   DELETE on "users/abc"                    → "ADMIN_DELETE"
     *   UPDATE on "users/abc/reset-password"     → "ADMIN_UPDATE_PASSWORD"
     *   UPDATE on "users/abc/send-verify-email"  → "ADMIN_UPDATE_VERIFY_EMAIL"
     */
    static String resolveAdminEventType(AdminEvent event) {
        if (event.getOperationType() == null) return "ADMIN_UNKNOWN";

        String path = event.getResourcePath() != null ? event.getResourcePath() : "";
        String base = "ADMIN_" + event.getOperationType().name(); // e.g. ADMIN_UPDATE

        if (path.contains("reset-password"))    return base + "_PASSWORD";
        if (path.contains("send-verify-email")) return base + "_VERIFY_EMAIL";

        return base; // e.g. ADMIN_CREATE, ADMIN_DELETE, ADMIN_UPDATE
    }
}
