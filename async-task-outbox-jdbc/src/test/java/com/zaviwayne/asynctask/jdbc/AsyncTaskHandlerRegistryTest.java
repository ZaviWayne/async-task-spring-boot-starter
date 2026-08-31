package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskContext;
import com.zaviwayne.asynctask.core.AsyncTaskHandler;
import com.zaviwayne.asynctask.core.NoAsyncTaskHandlerException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTaskHandlerRegistryTest {
    @Test
    void shouldResolveHandlerByTaskTypeAndVersion() {
        TestHandler handler = new TestHandler();
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of(handler));

        assertThat(registry.getRequired("test.task", 1)).isSameAs(handler);
    }

    @Test
    void shouldRejectDuplicateRegistration() {
        assertThatThrownBy(() -> new AsyncTaskHandlerRegistry(
            List.of(new TestHandler(), new TestHandler())))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("异步任务处理器重复注册");
    }

    @Test
    void shouldRejectNullHandler() {
        assertThatThrownBy(() -> new AsyncTaskHandlerRegistry(Collections.singletonList(null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("异步任务处理器不能为空");
    }

    @Test
    void shouldRejectBlankTaskType() {
        assertThatThrownBy(() -> new AsyncTaskHandlerRegistry(
            List.of(new TestHandler(" ", 1, String.class))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("任务类型不能为空");
    }

    @Test
    void shouldRejectNonPositiveSchemaVersion() {
        assertThatThrownBy(() -> new AsyncTaskHandlerRegistry(
            List.of(new TestHandler("test.task", 0, String.class))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("异步任务处理器消息结构版本必须大于 0");
    }

    @Test
    void shouldRejectNullPayloadType() {
        assertThatThrownBy(() -> new AsyncTaskHandlerRegistry(
            List.of(new TestHandler("test.task", 1, null))))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("异步任务处理器载荷类型不能为空: taskType=test.task");
    }

    @Test
    void shouldRejectMissingHandler() {
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of());

        assertThatThrownBy(() -> registry.getRequired("missing", 1))
            .isInstanceOf(NoAsyncTaskHandlerException.class);
    }

    private static final class TestHandler implements AsyncTaskHandler<String> {
        private final String taskType;

        private final int schemaVersion;

        private final Class<String> payloadType;

        private TestHandler() {
            this("test.task", 1, String.class);
        }

        private TestHandler(String taskType, int schemaVersion, Class<String> payloadType) {
            this.taskType = taskType;
            this.schemaVersion = schemaVersion;
            this.payloadType = payloadType;
        }

        @Override
        public String taskType() {
            return taskType;
        }

        @Override
        public int schemaVersion() {
            return schemaVersion;
        }

        @Override
        public Class<String> payloadType() {
            return payloadType;
        }

        @Override
        public void handle(AsyncTaskContext context, String payload) {
        }
    }
}
