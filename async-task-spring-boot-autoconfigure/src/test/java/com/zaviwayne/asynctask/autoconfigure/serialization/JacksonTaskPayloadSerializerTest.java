package com.zaviwayne.asynctask.autoconfigure.serialization;

import com.zaviwayne.asynctask.core.AsyncTaskEnvelope;
import com.zaviwayne.asynctask.core.AsyncTaskMessageValidator;
import com.zaviwayne.asynctask.core.AsyncTaskProcessingException;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JacksonTaskPayloadSerializerTest {
    @Test
    void shouldRejectEnvelopeWithInvalidPayloadHashDuringDeserialization() {
        ObjectMapper objectMapper = new ObjectMapper();
        JacksonTaskPayloadSerializer serializer = new JacksonTaskPayloadSerializer(objectMapper);
        String payloadHash = AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":1}");
        AsyncTaskEnvelope envelope = new AsyncTaskEnvelope(
                UUID.randomUUID(),
                "task-events",
                "resume.parse",
                1,
                "{\"resumeId\":1}",
                payloadHash,
                "resume:1",
                null,
                null,
                Map.of(),
                Instant.parse("2026-08-31T00:00:00Z"));
        String invalidEnvelopeJson = serializer.serialize(envelope)
                .replace(payloadHash, "invalid");

        assertThatThrownBy(() -> serializer.deserialize(invalidEnvelopeJson, AsyncTaskEnvelope.class))
                .isInstanceOf(AsyncTaskProcessingException.class)
                .hasMessage("异步任务载荷反序列化失败")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("载荷摘要必须是 64 位小写 SHA-256 十六进制字符串");
    }

    @Test
    void shouldRejectEnvelopeWithMismatchedPayloadHashDuringDeserialization() {
        ObjectMapper objectMapper = new ObjectMapper();
        JacksonTaskPayloadSerializer serializer = new JacksonTaskPayloadSerializer(objectMapper);
        String payloadHash = AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":1}");
        AsyncTaskEnvelope envelope = new AsyncTaskEnvelope(
                UUID.randomUUID(),
                "task-events",
                "resume.parse",
                1,
                "{\"resumeId\":1}",
                payloadHash,
                "resume:1",
                null,
                null,
                Map.of(),
                Instant.parse("2026-08-31T00:00:00Z"));
        String invalidEnvelopeJson = serializer.serialize(envelope)
                .replace(payloadHash, "0".repeat(64));

        assertThatThrownBy(() -> serializer.deserialize(invalidEnvelopeJson, AsyncTaskEnvelope.class))
                .isInstanceOf(AsyncTaskProcessingException.class)
                .hasMessage("异步任务载荷反序列化失败")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("载荷摘要与 JSON 载荷内容不一致");
    }
}
