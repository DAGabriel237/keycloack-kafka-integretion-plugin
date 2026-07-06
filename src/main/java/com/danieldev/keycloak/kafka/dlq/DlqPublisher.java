package com.danieldev.keycloak.kafka.dlq;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Routes failed events to the Dead Letter Queue topic.
 *
 * ── Why a separate class? ─────────────────────────────────────────────────
 * DLQ logic has its own concerns: attaching headers, handling publish failures
 * gracefully, and never throwing. Keeping it here makes the listener clean
 * and makes the DLQ behaviour easy to test in isolation.
 *
 * ── DLQ headers ──────────────────────────────────────────────────────────
 * Every DLQ message carries three headers the C# consumer can inspect to
 * understand why the message was routed here:
 *
 *   dlq-reason        — human-readable failure description
 *   dlq-timestamp-utc — ISO-8601 UTC timestamp of the failure
 *   dlq-source-topic  — the topic the message was originally destined for
 *
 * ── Failure modes handled ─────────────────────────────────────────────────
 * 1. Kafka publish failure (network error, broker unavailable, etc.)
 * 2. Serialization failure (Jackson can't serialize the event object)
 *
 * ── What this class never does ───────────────────────────────────────────
 * It never throws. A DLQ publish failure is logged and swallowed — crashing
 * the request thread because we can't write to the DLQ would be worse than
 * losing the event.
 */
public final class DlqPublisher {

    private static final Logger log = Logger.getLogger(DlqPublisher.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String dlqTopic;

    public DlqPublisher(KafkaProducer<String, byte[]> producer, String mainTopic) {
        this.producer = producer;
        this.dlqTopic = mainTopic + "-dlq";
    }

    /**
     * Publishes a raw byte payload to the DLQ with diagnostic headers.
     *
     * @param messageKey  the Kafka message key (usually userId) — preserved for traceability
     * @param payload     the serialized bytes to route to DLQ (may be the original payload)
     * @param reason      a short description of why this ended up in the DLQ
     * @param sourceTopic the topic this message was originally destined for
     */
    public void publish(String messageKey, byte[] payload, String reason, String sourceTopic) {
        if (payload == null) {
            // Nothing to route — the serialization failed completely.
            // Log and stop; there's no bytes to put on the DLQ.
            log.warnf("[DLQ] Skipping DLQ publish for key=%s — payload is null. Reason: %s",
                messageKey, reason);
            return;
        }

        try {
            ProducerRecord<String, byte[]> dlqRecord = new ProducerRecord<>(dlqTopic, messageKey, payload);

            // Attach diagnostic headers so the C# consumer (and ops team) can understand
            // what went wrong without having to decode the message body.
            dlqRecord.headers()
                .add(new RecordHeader("dlq-reason",
                    reason.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("dlq-timestamp-utc",
                    Instant.now().toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("dlq-source-topic",
                    sourceTopic.getBytes(StandardCharsets.UTF_8)));

            producer.send(dlqRecord, (metadata, exception) -> {
                if (exception != null) {
                    // DLQ publish itself failed. Log loudly — this means we're
                    // losing events and ops needs to know.
                    log.errorf(exception,
                        "[DLQ] Failed to publish to DLQ topic=%s key=%s reason=%s",
                        dlqTopic, messageKey, reason);
                } else {
                    log.debugf("[DLQ] Routed to DLQ topic=%s partition=%d offset=%d key=%s",
                        dlqTopic, metadata.partition(), metadata.offset(), messageKey);
                }
            });

        } catch (Exception e) {
            // Should not happen (send() itself is non-blocking), but never let a DLQ
            // failure propagate up to Keycloak's request thread.
            log.errorf(e, "[DLQ] Unexpected error routing to DLQ for key=%s", messageKey);
        }
    }
}
