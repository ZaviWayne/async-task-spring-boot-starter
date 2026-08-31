package com.zaviwayne.asynctask.kafka;

import com.zaviwayne.asynctask.core.*;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaAsyncTaskTransportTest {
    @Test
    void shouldSendSerializedEnvelopeWithStableIdempotencyKey() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskEnvelope envelope = envelope();
        when(serializer.serialize(envelope)).thenReturn("envelope-json");
        when(kafkaOperations.send("task-events", "resume.parse:1", "envelope-json"))
                .thenReturn(CompletableFuture.completedFuture(null));
        KafkaAsyncTaskTransport transport = new KafkaAsyncTaskTransport(
                kafkaOperations, serializer, Duration.ofSeconds(1));

        transport.send(envelope);

        verify(kafkaOperations).send("task-events", "resume.parse:1", "envelope-json");
    }

    @Test
    void shouldWrapKafkaSendFailure() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskEnvelope envelope = envelope();
        when(serializer.serialize(envelope)).thenReturn("envelope-json");
        when(kafkaOperations.send("task-events", "resume.parse:1", "envelope-json"))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        KafkaAsyncTaskTransport transport = new KafkaAsyncTaskTransport(
                kafkaOperations, serializer, Duration.ofSeconds(1));

        assertThatThrownBy(() -> transport.send(envelope))
                .isInstanceOf(AsyncTaskTransportException.class)
                .hasMessage("Kafka 异步任务发送失败");
    }

    @Test
    void shouldMarkKafkaTimeoutAsUncertainDelivery() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskEnvelope envelope = envelope();
        when(serializer.serialize(envelope)).thenReturn("envelope-json");
        when(kafkaOperations.send("task-events", "resume.parse:1", "envelope-json"))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("确认超时")));
        KafkaAsyncTaskTransport transport = new KafkaAsyncTaskTransport(
                kafkaOperations, serializer, Duration.ofSeconds(1));

        assertThatThrownBy(() -> transport.send(envelope))
                .isInstanceOfSatisfying(AsyncTaskTransportException.class,
                        exception -> assertThat(exception.isDeliveryUncertain()).isTrue());
    }

    @Test
    void shouldSendInsideKafkaTransactionWhenEnabled() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskEnvelope envelope = envelope();
        when(kafkaOperations.isTransactional()).thenReturn(true);
        when(serializer.serialize(envelope)).thenReturn("envelope-json");
        when(kafkaOperations.send("task-events", "resume.parse:1", "envelope-json"))
                .thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(invocation -> {
            KafkaOperations.OperationsCallback<Object, Object, Object> callback = invocation.getArgument(0);
            return callback.doInOperations(kafkaOperations);
        }).when(kafkaOperations).executeInTransaction(any());
        KafkaAsyncTaskTransport transport = new KafkaAsyncTaskTransport(
                kafkaOperations, serializer, Duration.ofSeconds(1), true);

        transport.send(envelope);

        verify(kafkaOperations).executeInTransaction(any());
        verify(kafkaOperations).send("task-events", "resume.parse:1", "envelope-json");
    }

    @Test
    void shouldRejectTransactionModeWithoutTransactionalKafkaOperations() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);

        assertThatThrownBy(() -> new KafkaAsyncTaskTransport(
                kafkaOperations, mock(TaskPayloadSerializer.class), Duration.ofSeconds(1), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("启用 Kafka 事务发送时必须配置事务型 KafkaTemplate");
    }

    @Test
    void shouldRejectNonPositiveSendTimeout() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);

        assertThatThrownBy(() -> new KafkaAsyncTaskTransport(
                kafkaOperations, serializer, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("发送确认超时时间必须大于 0");
        assertThatThrownBy(() -> new KafkaAsyncTaskTransport(
                kafkaOperations, serializer, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("发送确认超时时间必须大于 0");
        assertThatThrownBy(() -> new KafkaAsyncTaskTransport(
                kafkaOperations, serializer, Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("发送确认超时时间必须至少为 1 毫秒");
    }

    @Test
    void shouldRejectOversizedEnvelopeBeforeSending() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskEnvelope envelope = envelope();
        when(serializer.serialize(envelope)).thenReturn("123456");
        KafkaAsyncTaskTransport transport = new KafkaAsyncTaskTransport(
                kafkaOperations,
                serializer,
                Duration.ofSeconds(1),
                false,
                new AsyncTaskContentLimits(5, 5));

        assertThatThrownBy(() -> transport.send(envelope))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("任务信封 JSON 不能超过 5 个 UTF-8 字节，实际为 6 字节");
        verify(kafkaOperations, never()).send(any(), any(), any());
    }

    private static AsyncTaskEnvelope envelope() {
        return new AsyncTaskEnvelope(
                UUID.randomUUID(),
                "task-events",
                "resume.parse",
                1,
                "{\"resumeId\":1}",
                AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":1}"),
                "resume.parse:1",
                null,
                null,
                Map.of(),
                Instant.parse("2026-08-26T00:00:00Z"));
    }
}
