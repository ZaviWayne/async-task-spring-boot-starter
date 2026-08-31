package com.zaviwayne.asynctask.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTaskContentLimitsTest {
    @Test
    void shouldCountUtf8BytesInsteadOfCharacters() {
        AsyncTaskContentLimits limits = new AsyncTaskContentLimits(6, 6);

        assertThatCode(() -> limits.validateEnvelopeJson("中文"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> limits.validateProgressJson("中文a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("任务进度 JSON 不能超过 6 个 UTF-8 字节，实际为 7 字节");
    }

    @Test
    void shouldRejectNonPositiveLimits() {
        assertThatThrownBy(() -> new AsyncTaskContentLimits(0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("任务信封 JSON 最大字节数必须大于 0");
        assertThatThrownBy(() -> new AsyncTaskContentLimits(1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("任务进度 JSON 最大字节数必须大于 0");
    }
}
