package com.danieldev.keycloak.kafka;

import com.danieldev.keycloak.kafka.dlq.DlqPublisher;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DlqPublisherTest {

    private KafkaProducer<String, byte[]> mockProducer;
    private DlqPublisher dlq;

    private static final String MAIN_TOPIC = "user-events";
    private static final String DLQ_TOPIC  = "user-events-dlq";

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockProducer = mock(KafkaProducer.class);
        dlq          = new DlqPublisher(mockProducer, MAIN_TOPIC);

        // Default: DLQ send() succeeds
        RecordMetadata meta = new RecordMetadata(
            new TopicPartition(DLQ_TOPIC, 0), 0, 0, 0, 10, 10);
        when(mockProducer.send(any(), any())).thenAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(meta, null);
            return mock(Future.class);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void publish_routesToDlqTopic() {
        byte[] payload = "{\"eventId\":\"abc\"}".getBytes();
        dlq.publish("user-1", payload, "test reason", MAIN_TOPIC);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
            ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture(), any());

        ProducerRecord<String, byte[]> record = captor.getValue();
        assertEquals(DLQ_TOPIC, record.topic());
        assertEquals("user-1", record.key());
        assertArrayEquals(payload, record.value());
    }

    @Test
    @SuppressWarnings("unchecked")
    void publish_attachesRequiredHeaders() {
        byte[] payload = "{}".getBytes();
        dlq.publish("user-2", payload, "broker unavailable", MAIN_TOPIC);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
            ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture(), any());

        ProducerRecord<String, byte[]> record = captor.getValue();
        // All three headers must be present
        assertNotNull(record.headers().lastHeader("dlq-reason"));
        assertNotNull(record.headers().lastHeader("dlq-timestamp-utc"));
        assertNotNull(record.headers().lastHeader("dlq-source-topic"));

        String reason = new String(record.headers().lastHeader("dlq-reason").value());
        assertEquals("broker unavailable", reason);

        String sourceTopic = new String(record.headers().lastHeader("dlq-source-topic").value());
        assertEquals(MAIN_TOPIC, sourceTopic);
    }

    @Test
    void publish_nullPayload_doesNotCallProducer() {
        // If serialization totally failed, there are no bytes to route
        dlq.publish("user-3", null, "serialization failed", MAIN_TOPIC);
        verifyNoInteractions(mockProducer);
    }

    @Test
    @SuppressWarnings("unchecked")
    void publish_dlqPublishFails_doesNotThrow() {
        // DLQ publish itself failing must never propagate to the caller
        when(mockProducer.send(any(), any())).thenAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(null, new RuntimeException("DLQ broker down"));
            return mock(Future.class);
        });

        assertDoesNotThrow(() ->
            dlq.publish("user-4", "{}".getBytes(), "original failure", MAIN_TOPIC));
    }
}
