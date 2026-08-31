package com.zaviwayne.asynctask.autoconfigure.observability;

import com.zaviwayne.asynctask.core.AsyncTaskAdmin;
import com.zaviwayne.asynctask.core.AsyncTaskObserver;
import com.zaviwayne.asynctask.core.AsyncTaskStatistics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AsyncTaskObservabilityTest {
    @Test
    @SuppressWarnings("unchecked")
    void shouldRecordLifecycleCounters() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ObjectProvider<MeterRegistry> meterRegistries = mock(ObjectProvider.class);
        when(meterRegistries.getIfAvailable()).thenReturn(meterRegistry);
        AsyncTaskObserver observer = new AsyncTaskObservabilityAutoConfiguration()
            .asyncTaskObserver(meterRegistries);

        observer.onEnqueued("resume-events", "resume.parse");
        observer.onDispatchFailed("resume-events", "resume.parse", false);
        observer.onExecutionSucceeded("resume-events", "resume.parse");
        observer.onCleaned(3);

        assertThat(meterRegistry.get("async.task.enqueued").counter().count()).isEqualTo(1);
        assertThat(meterRegistry.get("async.task.dispatch").tag("outcome", "retry").counter().count())
            .isEqualTo(1);
        assertThat(meterRegistry.get("async.task.execution").tag("outcome", "success").counter().count())
            .isEqualTo(1);
        assertThat(meterRegistry.get("async.task.cleaned").counter().count()).isEqualTo(3);
    }

    @Test
    void shouldExposeQueueGaugesWithSharedStatisticsSnapshot() {
        Instant now = Instant.parse("2026-08-27T00:00:10Z");
        AsyncTaskAdmin taskAdmin = mock(AsyncTaskAdmin.class);
        when(taskAdmin.statistics()).thenReturn(new AsyncTaskStatistics(
            5, 2, 1, now.minusSeconds(10)));
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MeterBinder metrics = new AsyncTaskMetrics(
            taskAdmin, Clock.fixed(now, ZoneOffset.UTC), Duration.ofSeconds(30));
        metrics.bindTo(meterRegistry);

        assertThat(meterRegistry.get("async.task.backlog").gauge().value()).isEqualTo(5);
        assertThat(meterRegistry.get("async.task.running").gauge().value()).isEqualTo(2);
        assertThat(meterRegistry.get("async.task.dead").gauge().value()).isEqualTo(1);
        assertThat(meterRegistry.get("async.task.oldest.backlog.age").gauge().value()).isEqualTo(10);
        verify(taskAdmin).statistics();
    }

    @Test
    void shouldRefreshQueueStatisticsAfterCacheExpires() {
        Instant now = Instant.parse("2026-08-27T00:00:10Z");
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now, now.plusSeconds(29), now.plusSeconds(30));
        AsyncTaskAdmin taskAdmin = mock(AsyncTaskAdmin.class);
        when(taskAdmin.statistics()).thenReturn(
            new AsyncTaskStatistics(5, 2, 1, now.minusSeconds(10)),
            new AsyncTaskStatistics(6, 3, 2, now.minusSeconds(20)));
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MeterBinder metrics = new AsyncTaskMetrics(taskAdmin, clock, Duration.ofSeconds(30));
        metrics.bindTo(meterRegistry);

        assertThat(meterRegistry.get("async.task.backlog").gauge().value()).isEqualTo(5);
        assertThat(meterRegistry.get("async.task.backlog").gauge().value()).isEqualTo(5);
        assertThat(meterRegistry.get("async.task.backlog").gauge().value()).isEqualTo(6);
        verify(taskAdmin, times(2)).statistics();
    }

    @Test
    void shouldThrottleStatisticsFailuresAndExposeUnavailableGauges() {
        Instant now = Instant.parse("2026-08-27T00:00:10Z");
        AsyncTaskAdmin taskAdmin = mock(AsyncTaskAdmin.class);
        when(taskAdmin.statistics()).thenThrow(new IllegalStateException("数据库暂不可用"));
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MeterBinder metrics = new AsyncTaskMetrics(
            taskAdmin, Clock.fixed(now, ZoneOffset.UTC), Duration.ofSeconds(30));
        metrics.bindTo(meterRegistry);

        assertThat(meterRegistry.get("async.task.backlog").gauge().value()).isNaN();
        assertThat(meterRegistry.get("async.task.running").gauge().value()).isNaN();
        assertThat(meterRegistry.get("async.task.dead").gauge().value()).isNaN();
        assertThat(meterRegistry.get("async.task.oldest.backlog.age").gauge().value()).isNaN();
        verify(taskAdmin).statistics();
    }

    @Test
    void shouldKeepLastSnapshotWhenStatisticsRefreshFails() {
        Instant now = Instant.parse("2026-08-27T00:00:10Z");
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now, now.plusSeconds(30));
        AsyncTaskAdmin taskAdmin = mock(AsyncTaskAdmin.class);
        when(taskAdmin.statistics())
            .thenReturn(new AsyncTaskStatistics(5, 2, 1, now.minusSeconds(10)))
            .thenThrow(new IllegalStateException("数据库暂不可用"));
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MeterBinder metrics = new AsyncTaskMetrics(taskAdmin, clock, Duration.ofSeconds(30));
        metrics.bindTo(meterRegistry);

        assertThat(meterRegistry.get("async.task.backlog").gauge().value()).isEqualTo(5);
        assertThat(meterRegistry.get("async.task.backlog").gauge().value()).isEqualTo(5);
        verify(taskAdmin, times(2)).statistics();
    }

}
