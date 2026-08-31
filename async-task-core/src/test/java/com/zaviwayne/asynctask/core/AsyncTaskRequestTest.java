package com.zaviwayne.asynctask.core;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTaskRequestTest {
    @Test
    void shouldNormalizeTextAndCopyHeaders() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(" trace-id ", " trace-1 ");

        AsyncTaskRequest<String> request = new AsyncTaskRequest<>(
            " task-events ", " resume.parse ", 1, "payload", " resume:1 ", null, null, headers);
        headers.put("other", "value");

        assertThat(request.destination()).isEqualTo("task-events");
        assertThat(request.taskType()).isEqualTo("resume.parse");
        assertThat(request.idempotencyKey()).isEqualTo("resume:1");
        assertThat(request.headers()).containsExactly(Map.entry("trace-id", "trace-1"));
    }

    @Test
    void shouldRejectInvalidSchemaVersion() {
        assertThatThrownBy(() -> AsyncTaskRequest.of("task-events", "resume.parse", 0,
            "payload", "resume:1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("消息结构版本必须大于 0");
    }

    @Test
    void shouldRejectBlankIdempotencyKey() {
        assertThatThrownBy(() -> AsyncTaskRequest.of("task-events", "resume.parse", 1,
            "payload", " "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("幂等键不能为空");
    }

    @Test
    void shouldNormalizeBusinessReference() {
        AsyncTaskRequest<String> request = AsyncTaskRequest.referenced(
            "task-events", "resume.parse", 1, "payload", "resume:1", " resume ", " 1 ");

        assertThat(request.referenceType()).isEqualTo("resume");
        assertThat(request.referenceId()).isEqualTo("1");
    }

    @Test
    void shouldRejectIncompleteBusinessReference() {
        assertThatThrownBy(() -> new AsyncTaskRequest<>(
            "task-events", "resume.parse", 1, "payload", "resume:1", "resume", null, Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("业务关联类型和业务关联标识必须同时填写或同时留空");
    }
}
