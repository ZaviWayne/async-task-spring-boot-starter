package com.zaviwayne.asynctask.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncTaskContextTest {
    @Test
    void shouldExposeOperationsWithoutExposingInternalCallbacks() {
        AtomicBoolean heartbeatCalled = new AtomicBoolean();
        AtomicReference<Object> progress = new AtomicReference<>();
        AsyncTaskContext context = new AsyncTaskContext(
                UUID.randomUUID(),
                "resume.parse",
                1,
                "resume:1",
                Map.of("trace-id", "trace-1"),
                () -> heartbeatCalled.set(true),
                progress::set);

        context.heartbeat();
        context.updateProgress("50%");

        assertThat(heartbeatCalled).isTrue();
        assertThat(progress).hasValue("50%");
        assertThat(Arrays.stream(AsyncTaskContext.class.getMethods()).map(method -> method.getName()))
                .doesNotContain("heartbeatCallback", "progressReporter");
    }
}
