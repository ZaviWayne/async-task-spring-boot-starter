package com.zaviwayne.asynctask.kafka;

import com.zaviwayne.asynctask.core.*;
import com.zaviwayne.asynctask.jdbc.AsyncTaskProcessor;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class AsyncTaskKafkaMessageHandlerTest {
    @Test
    void shouldRejectOversizedEnvelopeBeforeDeserialization() {
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskProcessor taskProcessor = mock(AsyncTaskProcessor.class);
        AsyncTaskKafkaMessageHandler handler = new AsyncTaskKafkaMessageHandler(
                serializer, taskProcessor, new AsyncTaskContentLimits(5, 5));

        assertThatThrownBy(() -> handler.handle("123456"))
                .isInstanceOf(InvalidAsyncTaskMessageException.class)
                .hasMessage("任务信封 JSON 不能超过 5 个 UTF-8 字节，实际为 6 字节");
        assertThatThrownBy(() -> handler.handleDeadLetter("123456", "测试死信"))
                .isInstanceOf(InvalidAsyncTaskMessageException.class)
                .hasMessage("任务信封 JSON 不能超过 5 个 UTF-8 字节，实际为 6 字节");
        verifyNoInteractions(serializer, taskProcessor);
    }

    @Test
    void shouldProcessMessageWhenTopicAndKeyMatchEnvelope() {
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskProcessor taskProcessor = mock(AsyncTaskProcessor.class);
        AsyncTaskEnvelope envelope = envelope();
        when(serializer.deserialize("{}", AsyncTaskEnvelope.class)).thenReturn(envelope);
        AsyncTaskKafkaMessageHandler handler = new AsyncTaskKafkaMessageHandler(serializer, taskProcessor);

        handler.handle("task-events", "resume.parse:1", "{}");
        handler.handleDeadLetter("task-events", "resume.parse:1", "{}", "测试死信");

        verify(taskProcessor).process(envelope);
        verify(taskProcessor).processDeadLetter(envelope, "测试死信");
    }

    @Test
    void shouldRejectMismatchedTopicBeforeProcessing() {
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskProcessor taskProcessor = mock(AsyncTaskProcessor.class);
        when(serializer.deserialize("{}", AsyncTaskEnvelope.class)).thenReturn(envelope());
        AsyncTaskKafkaMessageHandler handler = new AsyncTaskKafkaMessageHandler(serializer, taskProcessor);

        assertThatThrownBy(() -> handler.handle("other-events", "resume.parse:1", "{}"))
                .isInstanceOf(InvalidAsyncTaskMessageException.class)
                .hasMessage(
                        "Kafka 业务主题与任务信封目标通道不一致: "
                                + "topic=other-events, destination=task-events");
        verifyNoInteractions(taskProcessor);
    }

    @Test
    void shouldRejectNonStringOrMismatchedKeyBeforeProcessing() {
        TaskPayloadSerializer serializer = mock(TaskPayloadSerializer.class);
        AsyncTaskProcessor taskProcessor = mock(AsyncTaskProcessor.class);
        when(serializer.deserialize("{}", AsyncTaskEnvelope.class)).thenReturn(envelope());
        AsyncTaskKafkaMessageHandler handler = new AsyncTaskKafkaMessageHandler(serializer, taskProcessor);

        assertThatThrownBy(() -> handler.handle("task-events", 1L, "{}"))
                .isInstanceOf(InvalidAsyncTaskMessageException.class)
                .hasMessage("Kafka 异步任务消息 Key 必须是字符串");
        assertThatThrownBy(() -> handler.handleDeadLetter(
                "task-events", "other-key", "{}", "测试死信"))
                .isInstanceOf(InvalidAsyncTaskMessageException.class)
                .hasMessage("Kafka 消息 Key 与任务信封幂等键不一致");
        verifyNoInteractions(taskProcessor);
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
