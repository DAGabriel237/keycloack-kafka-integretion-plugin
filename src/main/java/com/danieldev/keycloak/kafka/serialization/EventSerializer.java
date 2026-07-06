package com.danieldev.keycloak.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Thread-safe JSON serializer for Kafka messages.
 *
 * ── Why a shared static ObjectMapper? ────────────────────────────────────
 * ObjectMapper is expensive to construct (it scans classpath, builds type
 * factories, etc). It is fully thread-safe after initial configuration.
 * One shared instance is correct — never create one per event or per request.
 *
 * ── Why FAIL_ON_EMPTY_BEANS disabled? ────────────────────────────────────
 * Prevents serialization from throwing if Keycloak adds a new event type
 * that contains a type Jackson doesn't recognize — we still get the rest
 * of the fields rather than losing the whole event.
 */
public final class EventSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private EventSerializer() {}

    /**
     * Serializes the given object to a UTF-8 JSON byte array.
     *
     * Returns null if serialization fails (exception already logged).
     * Callers must decide what to do with a null result — either skip the
     * event or route the raw failure to a DLQ with an error header.
     */
    public static byte[] toJsonBytes(Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (Exception e) {
            // Use System.err here because serialization failures are rare and
            // alarming — we want them visible even without a log config.
            System.err.println("[EventSerializer] Serialization failed for "
                + obj.getClass().getSimpleName() + ": " + e.getMessage());
            return null;
        }
    }
}
