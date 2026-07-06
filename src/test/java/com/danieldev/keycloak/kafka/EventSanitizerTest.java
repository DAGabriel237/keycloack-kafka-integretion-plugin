package com.danieldev.keycloak.kafka;

import com.danieldev.keycloak.kafka.security.EventSanitizer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventSanitizerTest {

    // ── sanitize() ────────────────────────────────────────────────────────

    @Test
    void sanitize_nullInput_returnsEmptyMap() {
        assertTrue(EventSanitizer.sanitize(null).isEmpty());
    }

    @Test
    void sanitize_emptyInput_returnsEmptyMap() {
        assertTrue(EventSanitizer.sanitize(new HashMap<>()).isEmpty());
    }

    @Test
    void sanitize_blockedKeys_areStripped() {
        Map<String, String> raw = new HashMap<>();
        raw.put("password",      "secret123");
        raw.put("access_token",  "tok_abc");
        raw.put("refresh_token", "ref_xyz");
        raw.put("client_secret", "cs_123");
        raw.put("code",          "oauth_code");

        Map<String, String> result = EventSanitizer.sanitize(raw);
        assertTrue(result.isEmpty(), "All credential keys must be stripped");
    }

    @Test
    void sanitize_allowedKeys_areKept() {
        Map<String, String> raw = new HashMap<>();
        raw.put("username",    "john.doe");
        raw.put("auth_method", "openid-connect");

        Map<String, String> result = EventSanitizer.sanitize(raw);
        assertEquals("john.doe",       result.get("username"));
        assertEquals("openid-connect", result.get("auth_method"));
    }

    @Test
    void sanitize_unknownKey_isStripped() {
        Map<String, String> raw = Map.of("some_new_unknown_key", "value");
        assertTrue(EventSanitizer.sanitize(raw).isEmpty(),
            "Unknown keys must be denied by default (allowlist)");
    }

    @Test
    void sanitize_longValue_isTruncated() {
        String longValue = "x".repeat(300);
        Map<String, String> raw = Map.of("username", longValue);
        Map<String, String> result = EventSanitizer.sanitize(raw);
        assertEquals(256, result.get("username").length());
    }

    @Test
    void sanitize_resultIsUnmodifiable() {
        Map<String, String> raw = Map.of("username", "alice");
        Map<String, String> result = EventSanitizer.sanitize(raw);
        assertThrows(UnsupportedOperationException.class, () -> result.put("x", "y"));
    }

    @Test
    void sanitize_mixedKeys_onlyAllowedAreKept() {
        Map<String, String> raw = new HashMap<>();
        raw.put("username",     "alice");          // allowed
        raw.put("password",     "secret");          // blocked
        raw.put("redirect_uri", "https://app.io"); // allowed
        raw.put("random_field", "something");       // unknown — denied

        Map<String, String> result = EventSanitizer.sanitize(raw);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("username"));
        assertTrue(result.containsKey("redirect_uri"));
        assertFalse(result.containsKey("password"));
        assertFalse(result.containsKey("random_field"));
    }

    // ── sanitizeIp() ──────────────────────────────────────────────────────

    @Test
    void sanitizeIp_loopback_returnsNull() {
        assertNull(EventSanitizer.sanitizeIp("127.0.0.1"));
        assertNull(EventSanitizer.sanitizeIp("::1"));
    }

    @Test
    void sanitizeIp_privateRange_returnsNull() {
        assertNull(EventSanitizer.sanitizeIp("10.0.0.5"));
        assertNull(EventSanitizer.sanitizeIp("192.168.1.1"));
        assertNull(EventSanitizer.sanitizeIp("172.16.0.1"));
        assertNull(EventSanitizer.sanitizeIp("172.31.255.255"));
    }

    @Test
    void sanitizeIp_publicIp_isReturned() {
        assertEquals("8.8.8.8", EventSanitizer.sanitizeIp("8.8.8.8"));
    }

    @Test
    void sanitizeIp_proxyChain_returnsFirstSegment() {
        assertEquals("203.0.113.5",
            EventSanitizer.sanitizeIp("203.0.113.5, 10.0.0.1, 172.16.0.1"));
    }

    @Test
    void sanitizeIp_null_returnsNull() {
        assertNull(EventSanitizer.sanitizeIp(null));
        assertNull(EventSanitizer.sanitizeIp("   "));
    }

    @Test
    void sanitizeIp_proxyChainWithPrivateFirst_returnsNull() {
        // If the first address is private (e.g. all hops are internal), return null
        assertNull(EventSanitizer.sanitizeIp("192.168.1.1, 10.0.0.1"));
    }
}
