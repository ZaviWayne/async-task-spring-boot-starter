package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.TaskExecutionInProgressException;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 异步任务执行租约心跳。
 *
 * @since 2026-08-31
 */
final class ExecutionLeaseHeartbeat implements AutoCloseable {
    /**
     * 心跳状态锁。
     */
    private final Object monitor = new Object();

    /**
     * 任务 ID。
     */
    private final UUID taskId;

    /**
     * 租约刷新操作。
     */
    private final Runnable heartbeatAction;

    /**
     * 定时心跳任务。
     */
    private final ScheduledFuture<?> future;

    /**
     * 最近一次心跳失败。
     */
    private RuntimeException failure;

    /**
     * 是否已经停止心跳。
     */
    private boolean stopped;

    /**
     * 创建并启动异步任务执行租约心跳。
     *
     * @param taskId            任务 ID
     * @param heartbeatExecutor 心跳调度器
     * @param heartbeatInterval 心跳间隔
     * @param heartbeatAction   租约刷新操作
     */
    public ExecutionLeaseHeartbeat(UUID taskId,
                                   ScheduledExecutorService heartbeatExecutor,
                                   Duration heartbeatInterval,
                                   Runnable heartbeatAction) {
        this.taskId = Objects.requireNonNull(taskId, "任务 ID 不能为空");
        this.heartbeatAction = Objects.requireNonNull(heartbeatAction, "租约刷新操作不能为空");
        ScheduledExecutorService requiredExecutor = Objects.requireNonNull(
                heartbeatExecutor, "心跳调度器不能为空");
        Duration requiredInterval = requirePositive(heartbeatInterval);
        long intervalNanos = requiredInterval.toNanos();
        this.future = requiredExecutor.scheduleWithFixedDelay(
                this::heartbeat, intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * 停止心跳，并在心跳失败时传递租约异常。
     */
    @Override
    public void close() {
        RuntimeException heartbeatFailure;
        synchronized (monitor) {
            stopped = true;
            future.cancel(false);
            heartbeatFailure = failure;
        }
        if (heartbeatFailure != null) {
            throw heartbeatFailure;
        }
    }

    private void heartbeat() {
        synchronized (monitor) {
            if (stopped || failure != null) {
                return;
            }
            try {
                heartbeatAction.run();
            } catch (RuntimeException exception) {
                failure = heartbeatFailure(exception);
            }
        }
    }

    private TaskExecutionInProgressException heartbeatFailure(RuntimeException cause) {
        TaskExecutionInProgressException exception = new TaskExecutionInProgressException(
                "异步任务自动刷新执行租约失败: " + taskId);
        exception.addSuppressed(cause);
        return exception;
    }

    private static Duration requirePositive(Duration duration) {
        Objects.requireNonNull(duration, "自动心跳间隔不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("自动心跳间隔必须大于 0");
        }
        return duration;
    }
}
