package com.zaviwayne.asynctask.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTaskEnvelopeTest {
    private static final Instant CREATED_AT = Instant.parse("2026-08-31T00:00:00Z");

    @Test
    void shouldNormalizeTextAndCopyHeaders() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(" trace-id ", " trace-1 ");

        AsyncTaskEnvelope envelope = envelope(
                " task-events ", payloadHash(), " resume ", " 1 ", headers);
        headers.put("other", "value");

        assertThat(envelope.destination()).isEqualTo("task-events");
        assertThat(envelope.taskType()).isEqualTo("resume.parse");
        assertThat(envelope.idempotencyKey()).isEqualTo("resume:1");
        assertThat(envelope.referenceType()).isEqualTo("resume");
        assertThat(envelope.referenceId()).isEqualTo("1");
        assertThat(envelope.headers()).containsExactly(Map.entry("trace-id", "trace-1"));
        assertThat(envelope.generation()).isZero();
        assertThat(envelope.nextGeneration().generation()).isEqualTo(1);
    }

    @Test
    void shouldRejectInvalidPayloadHash() {
        assertThatThrownBy(() -> envelope("task-events", "invalid", null, null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("载荷摘要必须是 64 位小写 SHA-256 十六进制字符串");
    }

    @Test
    void shouldRejectPayloadHashThatDoesNotMatchPayloadJson() {
        assertThatThrownBy(() -> envelope(
                "task-events", "0".repeat(64), null, null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("载荷摘要与 JSON 载荷内容不一致");
    }

    @Test
    void shouldRejectExcessiveHeaders() {
        Map<String, String> headers = new LinkedHashMap<>(65);
        for (int index = 0; index < 65; index++) {
            headers.put("header-" + index, "value");
        }

        assertThatThrownBy(() -> envelope(
                "task-events", payloadHash(), null, null, headers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("扩展请求头数量不能超过 64");
    }

    @Test
    void shouldRejectDuplicateNormalizedHeaderNames() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("trace-id", "trace-1");
        headers.put(" trace-id ", "trace-2");

        assertThatThrownBy(() -> envelope(
                "task-events", payloadHash(), null, null, headers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("规范化后的请求头名称不能重复: trace-id");
    }

    @Test
    void shouldRejectOversizedDestination() {
        assertThatThrownBy(() -> envelope(
                "a".repeat(250), payloadHash(), null, null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("目标通道长度不能超过 249");
    }

    private static AsyncTaskEnvelope envelope(String destination,
                                              String payloadHash,
                                              String referenceType,
                                              String referenceId,
                                              Map<String, String> headers) {
        return new AsyncTaskEnvelope(
                UUID.randomUUID(),
                destination,
                " resume.parse ",
                1,
                "{\"resumeId\":1}",
                payloadHash,
                " resume:1 ",
                referenceType,
                referenceId,
                headers,
                CREATED_AT);
    }

    private static String payloadHash() {
        return AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":1}");
    }
}
