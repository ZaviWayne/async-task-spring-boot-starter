package com.zaviwayne.asynctask.autoconfigure.retention;

import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskRetentionProperties;

import com.zaviwayne.asynctask.jdbc.JdbcTaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 异步终态任务定时清理作业。
 *
 * @since 2026-08-27
 */
public final class AsyncTaskCleanupJob implements SmartLifecycle, AutoCloseable {
    /**
     * 日志记录器。
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTaskCleanupJob.class);

    /**
     * 清理线程名称。
     */
    private static final String CLEANUP_THREAD_NAME = "async-task-retention-cleanup";

    /**
     * JDBC 任务状态存储。
     */
    private final JdbcTaskStore taskStore;

    /**
     * 系统时钟。
     */
    private final Clock clock;

    /**
     * 终态任务保留配置。
     */
    private final AsyncTaskRetentionProperties retention;

    /**
     * 终态任务清理调度器。
     */
    private volatile ScheduledExecutorService scheduler;

    /**
     * 生命周期运行状态。
     */
    private volatile boolean running;

    /**
     * 创建异步终态任务定时清理作业。
     *
     * @param taskStore JDBC 任务状态存储
     * @param clock     系统时钟
     * @param retention 终态任务保留配置
     */
    public AsyncTaskCleanupJob(JdbcTaskStore taskStore,
                               Clock clock,
                               AsyncTaskRetentionProperties retention) {
        this.taskStore = Objects.requireNonNull(taskStore, "JDBC 任务状态存储不能为空");
        this.clock = Objects.requireNonNull(clock, "系统时钟不能为空");
        this.retention = Objects.requireNonNull(retention, "终态任务保留配置不能为空");
    }

    /**
     * 启动终态任务清理作业。
     */
    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        ScheduledExecutorService createdScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, CLEANUP_THREAD_NAME);
            thread.setDaemon(true);
            return thread;
        });
        scheduler = createdScheduler;
        running = true;
        createdScheduler.scheduleWithFixedDelay(
                this::clean, 0L, retention.cleanupInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 停止终态任务清理作业。
     */
    @Override
    public synchronized void stop() {
        ScheduledExecutorService currentScheduler = scheduler;
        scheduler = null;
        running = false;
        if (currentScheduler != null) {
            currentScheduler.shutdownNow();
        }
    }

    /**
     * 判断终态任务清理作业是否正在运行。
     *
     * @return 正在运行时返回 true
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * 关闭终态任务清理作业。
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * 按配置清理一批过期终态任务。
     */
    public void clean() {
        try {
            Instant cutoff = clock.instant().minus(retention.retentionPeriod());
            int totalDeleted = 0;
            for (int batchIndex = 0; batchIndex < retention.maxBatchesPerRun(); batchIndex++) {
                int deleted = taskStore.deleteTerminalTasks(cutoff, retention.batchSize());
                totalDeleted += deleted;
                if (deleted < retention.batchSize()) {
                    break;
                }
            }
            if (totalDeleted > 0) {
                LOGGER.info("异步终态任务清理完成: deleted={}", totalDeleted);
            }
        } catch (RuntimeException exception) {
            LOGGER.error("异步终态任务清理失败", exception);
        }
    }
}
