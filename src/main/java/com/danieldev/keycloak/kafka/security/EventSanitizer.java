package com.danieldev.keycloak.kafka.security;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Sanitizes Keycloak event details before publishing to Kafka.
 *
 * ── Two-layer defense ─────────────────────────────────────────────────────
 *
 * Layer 1 — BLOCKLIST: credentials and tokens are always stripped, regardless
 *   of anything else. These keys NEVER leave Keycloak.
 *
 * Layer 2 — ALLOWLIST: only explicitly approved keys are forwarded.
 *   Unknown keys are denied by default.
 *
 * This means adding a new Keycloak detail key requires a conscious decision
 * here — not an accidental data leak from a Keycloak upgrade.
 *
 * ── Why both layers? ──────────────────────────────────────────────────────
 * The blocklist protects against a future developer who edits the allowlist
 * and accidentally re-adds a credential key.
 * The allowlist protects against Keycloak adding new detail keys in a future
 * version that we haven't reviewed yet.
 */
public final class EventSanitizer {

    // ── Layer 1: Hard blocklist — these NEVER leave Keycloak ─────────────
    private static final Set<String> BLOCKED_KEYS = Set.of(
        "password", "new_password", "confirm_password", "current_password",
        "token", "access_token", "refresh_token", "id_token", "session_state",
        "client_secret", "credential", "secret", "private_key",
        "code",              // OAuth authorization code
        "response",          // raw OAuth response blob
        "user_session_note"
    );

    // ── Layer 2: Allowlist — only these keys are forwarded ────────────────
    private static final Set<String> ALLOWED_KEYS = Set.of(
        "auth_method",
        "auth_type",
        "username",           // login identifier (not a credential)
        "redirect_uri",
        "remember_me",
        "custom_required_action",
        "updated_field",      // e.g. "email", "firstName" — what changed
        "register_method",
        "email_verified"
    );

    private static final int MAX_VALUE_LENGTH   = 256;
    private static final int MAX_DETAIL_ENTRIES = 10;

    private EventSanitizer() {}

    /**
     * Returns a sanitized, unmodifiable copy of the detail map.
     * Returns an empty map if input is null or empty.
     */
    public static Map<String, String> sanitize(Map<String, String> raw) {
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> safe = new HashMap<>();

        raw.entrySet().stream()
            .filter(e -> e.getKey() != null)
            .filter(e -> !BLOCKED_KEYS.contains(e.getKey().toLowerCase()))   // layer 1
            .filter(e -> ALLOWED_KEYS.contains(e.getKey().toLowerCase()))    // layer 2
            .limit(MAX_DETAIL_ENTRIES)
            .forEach(e -> {
                String value = e.getValue();
                if (value != null && value.length() > MAX_VALUE_LENGTH) {
                    value = value.substring(0, MAX_VALUE_LENGTH);
                }
                safe.put(e.getKey(), value);
            });

        return Collections.unmodifiableMap(safe);
    }

    /**
     * Sanitizes an IP address string from Keycloak.
     *
     * Keycloak may include proxy chains ("client, proxy1, proxy2").
     * We extract only the first (leftmost / originating) address.
     *
     * Internal/loopback/private addresses are stripped — no value publishing them.
     * Returns null when no publishable address is found.
     */
    public static String sanitizeIp(String raw) {
        if (raw == null || raw.isBlank()) return null;

        String first = raw.split(",")[0].trim();

        if (first.startsWith("127.")
                || first.equals("::1")
                || first.startsWith("10.")
                || first.startsWith("192.168.")
                || first.matches("^172\\.(1[6-9]|2[0-9]|3[01])\\..*")) {
            return null;
        }

        // Sanity-check length: max IPv6 = 39 chars, with port brackets ~45
        return first.length() <= 45 ? first : null;
    }
}
