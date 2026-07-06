package com.danieldev.keycloak.kafka.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

/**
 * Canonical event shape written to Kafka.
 *
 * ── Contract rules ────────────────────────────────────────────────────────
 * This class is a PUBLIC API shared between this Java plugin and the C# User
 * Service consumer. Any change here MUST be mirrored in the C# contracts package.
 *
 *   ✅ ADD new nullable fields freely — consumers ignore unknown fields.
 *   ❌ NEVER remove or rename an existing field — consumers will break silently.
 *   ❌ NEVER change the type of an existing field.
 *
 * @JsonInclude(NON_NULL) keeps the JSON compact: null fields are omitted rather
 * than written as "fieldName": null.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class KeycloakUserEvent {

    // ── Identity ──────────────────────────────────────────────────────────

    /**
     * Random UUID generated per event.
     * The C# consumer stores this in the idempotency table to detect and skip
     * duplicate deliveries (Kafka at-least-once delivery guarantee).
     */
    private String eventId;

    /**
     * What happened. Possible values:
     *
     * User-initiated:
     *   REGISTER, LOGIN, LOGIN_ERROR, LOGOUT,
     *   UPDATE_PASSWORD, UPDATE_PROFILE,
     *   VERIFY_EMAIL, SEND_RESET_PASSWORD,
     *   DELETE_ACCOUNT
     *
     * Admin-initiated (via Keycloak admin API or admin UI):
     *   ADMIN_CREATE, ADMIN_UPDATE, ADMIN_DELETE,
     *   ADMIN_UPDATE_PASSWORD, ADMIN_UPDATE_VERIFY_EMAIL,
     *   ADMIN_UNKNOWN (fallback)
     */
    private String eventType;

    /** Keycloak internal user UUID. Maps to UserId in the User Service domain. */
    private String userId;

    /** Keycloak realm. Useful if you run multiple realms (e.g. dev vs prod). */
    private String realmId;

    /** OAuth client ID that triggered the event (null for admin events). */
    private String clientId;

    // ── Timing ────────────────────────────────────────────────────────────

    /** Unix epoch milliseconds — when the event occurred in Keycloak. */
    private long occurredAtEpochMs;

    // ── Context ───────────────────────────────────────────────────────────

    /**
     * Client IP address — sanitized.
     * Internal/loopback addresses (127.x, 10.x, 192.168.x, 172.16-31.x) are
     * stripped and this field will be null. Proxy chains are resolved to the
     * first (leftmost) address.
     */
    private String ipAddress;

    /**
     * Allowlisted Keycloak event details only.
     * Credentials, tokens, and secrets are NEVER present — EventSanitizer
     * enforces this with a hard blocklist + allowlist before publish.
     * Null or empty when no safe details are available.
     */
    private Map<String, String> details;

    /** True when this event was triggered via the Keycloak Admin API. */
    private boolean adminEvent;

    // Private constructor — use builder()
    private KeycloakUserEvent() {}

    // ── Builder ───────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private final KeycloakUserEvent obj = new KeycloakUserEvent();

        public Builder eventId(String v)              { obj.eventId = v;           return this; }
        public Builder eventType(String v)            { obj.eventType = v;         return this; }
        public Builder userId(String v)               { obj.userId = v;            return this; }
        public Builder realmId(String v)              { obj.realmId = v;           return this; }
        public Builder clientId(String v)             { obj.clientId = v;          return this; }
        public Builder occurredAtEpochMs(long v)      { obj.occurredAtEpochMs = v; return this; }
        public Builder ipAddress(String v)            { obj.ipAddress = v;         return this; }
        public Builder details(Map<String, String> v) { obj.details = v;           return this; }
        public Builder adminEvent(boolean v)          { obj.adminEvent = v;        return this; }
        public KeycloakUserEvent build()              { return obj; }
    }

    // ── Getters (no setters — immutable after build) ──────────────────────

    public String              getEventId()           { return eventId; }
    public String              getEventType()         { return eventType; }
    public String              getUserId()            { return userId; }
    public String              getRealmId()           { return realmId; }
    public String              getClientId()          { return clientId; }
    public long                getOccurredAtEpochMs() { return occurredAtEpochMs; }
    public String              getIpAddress()         { return ipAddress; }
    public Map<String, String> getDetails()           { return details; }
    public boolean             isAdminEvent()         { return adminEvent; }
}
