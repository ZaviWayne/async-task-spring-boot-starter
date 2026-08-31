package com.zaviwayne.asynctask.autoconfigure.kafka;

import com.zaviwayne.asynctask.jdbc.AsyncTaskDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Outbox 定时轮询任务。
 *
 * @since 2026-08-26
 */
public final class AsyncTaskPollingJob implements SmartLifecycle, AutoCloseable {
    /**
     * 日志记录器。
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTaskPollingJob.class);

    /**
     * 轮询线程名称。
     */
    private static final String POLLING_THREAD_NAME = "async-task-outbox-polling";

    /**
     * 轮询作业生命周期阶段。
     */
    private static final int LIFECYCLE_PHASE = 100;

    /**
     * Outbox 投递器。
     */
    private final AsyncTaskDispatcher dispatcher;

    /**
     * Outbox 轮询间隔。
     */
    private final Duration pollInterval;

    /**
     * Outbox 轮询调度器。
     */
    private volatile ScheduledExecutorService scheduler;

    /**
     * 生命周期运行状态。
     */
    private volatile boolean running;

    /**
     * 创建 outbox 定时轮询任务。
     *
     * @param dispatcher   Outbox 投递器
     * @param pollInterval Outbox 轮询间隔，至少为 1 毫秒
     */
    public AsyncTaskPollingJob(AsyncTaskDispatcher dispatcher, Duration pollInterval) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "Outbox 投递器不能为空");
        this.pollInterval = requireAtLeastOneMillisecond(pollInterval);
    }

    /**
     * 启动 Outbox 轮询作业。
     */
    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        ScheduledExecutorService createdScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, POLLING_THREAD_NAME);
            thread.setDaemon(true);
            return thread;
        });
        scheduler = createdScheduler;
        running = true;
        createdScheduler.scheduleWithFixedDelay(
                this::poll, 0L, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 停止 Outbox 轮询作业。
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
     * 判断 Outbox 轮询作业是否正在运行。
     *
     * @return 正在运行时返回 true
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * 获取 Outbox 轮询作业的生命周期阶段。
     *
     * @return 生命周期阶段
     */
    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    /**
     * 关闭 Outbox 轮询作业。
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * 按配置间隔投递一批 outbox 消息。
     */
    public void poll() {
        try {
            dispatcher.dispatchBatch();
        } catch (RuntimeException exception) {
            LOGGER.error("异步任务 outbox 轮询失败", exception);
        }
    }

    private static Duration requireAtLeastOneMillisecond(Duration duration) {
        Objects.requireNonNull(duration, "Outbox 轮询间隔不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("Outbox 轮询间隔必须大于 0");
        }
        if (duration.toMillis() == 0L) {
            throw new IllegalArgumentException("Outbox 轮询间隔必须至少为 1 毫秒");
        }
        return duration;
    }
}
