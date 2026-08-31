package com.zaviwayne.asynctask.autoconfigure.retention;

import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskRetentionProperties;
import com.zaviwayne.asynctask.jdbc.JdbcTaskStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.Mockito.*;

class AsyncTaskCleanupJobTest {
    private static final Instant CURRENT_TIME = Instant.parse("2026-08-30T00:00:00Z");

    @Test
    void shouldContinueCleaningUntilBatchIsNotFull() {
        JdbcTaskStore taskStore = mock(JdbcTaskStore.class);
        Instant cutoff = CURRENT_TIME.minus(Duration.ofDays(30));
        when(taskStore.deleteTerminalTasks(cutoff, 2)).thenReturn(2, 2, 1);
        AsyncTaskCleanupJob cleanupJob = cleanupJob(taskStore, 10);

        cleanupJob.clean();

        verify(taskStore, times(3)).deleteTerminalTasks(cutoff, 2);
    }

    @Test
    void shouldStopCleaningAtConfiguredBatchLimit() {
        JdbcTaskStore taskStore = mock(JdbcTaskStore.class);
        Instant cutoff = CURRENT_TIME.minus(Duration.ofDays(30));
        when(taskStore.deleteTerminalTasks(cutoff, 2)).thenReturn(2);
        AsyncTaskCleanupJob cleanupJob = cleanupJob(taskStore, 3);

        cleanupJob.clean();

        verify(taskStore, times(3)).deleteTerminalTasks(cutoff, 2);
    }

    private static AsyncTaskCleanupJob cleanupJob(JdbcTaskStore taskStore, int maxBatchesPerRun) {
        AsyncTaskRetentionProperties retention = new AsyncTaskRetentionProperties(
                true, Duration.ofDays(30), Duration.ofHours(1), 2, maxBatchesPerRun);
        Clock clock = Clock.fixed(CURRENT_TIME, ZoneOffset.UTC);
        return new AsyncTaskCleanupJob(taskStore, clock, retention);
    }
}
