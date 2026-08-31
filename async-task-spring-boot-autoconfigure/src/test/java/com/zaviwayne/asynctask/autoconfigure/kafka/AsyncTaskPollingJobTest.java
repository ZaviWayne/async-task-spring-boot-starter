package com.zaviwayne.asynctask.autoconfigure.kafka;

import com.zaviwayne.asynctask.jdbc.AsyncTaskDispatcher;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class AsyncTaskPollingJobTest {
    @Test
    void shouldRejectSubMillisecondPollInterval() {
        AsyncTaskDispatcher dispatcher = mock(AsyncTaskDispatcher.class);

        assertThatThrownBy(() -> new AsyncTaskPollingJob(dispatcher, Duration.ofNanos(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Outbox 轮询间隔必须至少为 1 毫秒");
    }

    @Test
    void shouldAcceptOneMillisecondPollInterval() {
        AsyncTaskDispatcher dispatcher = mock(AsyncTaskDispatcher.class);

        assertThatCode(() -> new AsyncTaskPollingJob(dispatcher, Duration.ofMillis(1)))
            .doesNotThrowAnyException();
    }
}
