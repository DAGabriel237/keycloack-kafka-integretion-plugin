package com.danieldev.keycloak.kafka;

import com.danieldev.keycloak.kafka.dlq.DlqPublisher;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KeycloakKafkaEventListenerTest {

    private KafkaProducer<String, byte[]> mockProducer;
    private DlqPublisher mockDlq;
    private KeycloakKafkaEventListener listener;

    private static final String TOPIC = "user-events";

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockProducer = mock(KafkaProducer.class);
        mockDlq      = mock(DlqPublisher.class);
        listener     = new KeycloakKafkaEventListener(mockProducer, TOPIC, mockDlq);

        // Default: producer.send() succeeds — returns a completed future
        RecordMetadata meta = new RecordMetadata(
            new TopicPartition(TOPIC, 0), 0, 0, 0, 10, 10);
        when(mockProducer.send(any(), any())).thenAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(meta, null); // null exception = success
            return mock(Future.class);
        });
    }

    // ── extractUserId ─────────────────────────────────────────────────────

    @Test
    void extractUserId_simplePath_returnsId() {
        assertEquals("abc-123", KeycloakKafkaEventListener.extractUserId("users/abc-123"));
    }

    @Test
    void extractUserId_withSubPath_returnsId() {
        // Admin password reset: "users/{id}/reset-password"
        assertEquals("abc-123",
            KeycloakKafkaEventListener.extractUserId("users/abc-123/reset-password"));
    }

    @Test
    void extractUserId_nonUserPath_returnsNull() {
        assertNull(KeycloakKafkaEventListener.extractUserId("clients/some-client"));
        assertNull(KeycloakKafkaEventListener.extractUserId("roles/admin"));
    }

    @Test
    void extractUserId_null_returnsNull() {
        assertNull(KeycloakKafkaEventListener.extractUserId(null));
    }

    @Test
    void extractUserId_emptyUsersPath_returnsNull() {
        // "users/" with nothing after — should not crash
        assertNull(KeycloakKafkaEventListener.extractUserId("users/"));
    }

    // ── resolveAdminEventType ─────────────────────────────────────────────

    @Test
    void resolveAdminEventType_create_returnsAdminCreate() {
        AdminEvent event = adminEvent(OperationType.CREATE, "users/abc");
        assertEquals("ADMIN_CREATE", KeycloakKafkaEventListener.resolveAdminEventType(event));
    }

    @Test
    void resolveAdminEventType_delete_returnsAdminDelete() {
        AdminEvent event = adminEvent(OperationType.DELETE, "users/abc");
        assertEquals("ADMIN_DELETE", KeycloakKafkaEventListener.resolveAdminEventType(event));
    }

    @Test
    void resolveAdminEventType_updatePassword_returnsAdminUpdatePassword() {
        AdminEvent event = adminEvent(OperationType.ACTION, "users/abc/reset-password");
        assertEquals("ADMIN_ACTION_PASSWORD",
            KeycloakKafkaEventListener.resolveAdminEventType(event));
    }

    @Test
    void resolveAdminEventType_verifyEmail_returnsAdminUpdateVerifyEmail() {
        AdminEvent event = adminEvent(OperationType.ACTION, "users/abc/send-verify-email");
        assertEquals("ADMIN_ACTION_VERIFY_EMAIL",
            KeycloakKafkaEventListener.resolveAdminEventType(event));
    }

    @Test
    void resolveAdminEventType_nullOperation_returnsAdminUnknown() {
        AdminEvent event = adminEvent(null, "users/abc");
        assertEquals("ADMIN_UNKNOWN", KeycloakKafkaEventListener.resolveAdminEventType(event));
    }

    // ── onEvent(Event) — happy path ───────────────────────────────────────

    @Test
    void onEvent_validLoginEvent_publishesToMainTopic() {
        Event event = loginEvent("user-1");
        listener.onEvent(event);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = recordCaptor();
        verify(mockProducer).send(captor.capture(), any());

        ProducerRecord<String, byte[]> record = captor.getValue();
        assertEquals(TOPIC, record.topic());
        assertEquals("user-1", record.key());
        assertNotNull(record.value());
        // DLQ must NOT be called on success
        verifyNoInteractions(mockDlq);
    }

    @Test
    void onEvent_nullEvent_doesNothing() {
        listener.onEvent((Event) null);
        verifyNoInteractions(mockProducer, mockDlq);
    }

    @Test
    void onEvent_eventWithNullUserId_doesNothing() {
        Event event = loginEvent(null);
        listener.onEvent(event);
        verifyNoInteractions(mockProducer, mockDlq);
    }

    // ── onEvent(Event) — DLQ on publish failure ───────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void onEvent_publishFails_routesToDlq() {
        // Simulate Kafka broker unavailable
        RuntimeException kafkaError = new RuntimeException("Broker not available");
        when(mockProducer.send(any(), any())).thenAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(null, kafkaError);
            return mock(Future.class);
        });

        Event event = loginEvent("user-2");
        listener.onEvent(event);

        // Main topic attempted
        verify(mockProducer).send(any(), any());
        // DLQ must be called with the same bytes and reason
        verify(mockDlq).publish(eq("user-2"), any(), contains("Broker not available"), eq(TOPIC));
    }

    // ── onEvent(AdminEvent) ───────────────────────────────────────────────

    @Test
    void onAdminEvent_userCreate_publishesToMainTopic() {
        AdminEvent event = adminEvent(OperationType.CREATE, "users/user-99");
        listener.onEvent(event, false);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = recordCaptor();
        verify(mockProducer).send(captor.capture(), any());
        assertEquals("user-99", captor.getValue().key());
        verifyNoInteractions(mockDlq);
    }

    @Test
    void onAdminEvent_nonUserPath_isIgnored() {
        AdminEvent event = adminEvent(OperationType.CREATE, "clients/some-client");
        listener.onEvent(event, false);
        verifyNoInteractions(mockProducer, mockDlq);
    }

    @Test
    void onAdminEvent_null_doesNothing() {
        listener.onEvent((AdminEvent) null, false);
        verifyNoInteractions(mockProducer, mockDlq);
    }

    // ── close() ───────────────────────────────────────────────────────────

    @Test
    void close_doesNotCloseSharedProducer() {
        // The producer is shared and owned by the Factory — listener.close() must
        // NOT close it, or concurrent requests on other listener instances will fail.
        listener.close();
        verify(mockProducer, never()).close();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static Event loginEvent(String userId) {
        Event e = new Event();
        e.setType(EventType.LOGIN);
        e.setUserId(userId);
        e.setRealmId("master");
        e.setClientId("frontend");
        e.setTime(System.currentTimeMillis());
        return e;
    }

    private static AdminEvent adminEvent(OperationType operation, String resourcePath) {
        AdminEvent e = mock(AdminEvent.class);
        when(e.getOperationType()).thenReturn(operation);
        when(e.getResourcePath()).thenReturn(resourcePath);
        when(e.getRealmId()).thenReturn("master");
        when(e.getTime()).thenReturn(System.currentTimeMillis());
        return e;
    }

    @SuppressWarnings("unchecked")
    private static ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor() {
        return ArgumentCaptor.forClass(ProducerRecord.class);
    }
}
