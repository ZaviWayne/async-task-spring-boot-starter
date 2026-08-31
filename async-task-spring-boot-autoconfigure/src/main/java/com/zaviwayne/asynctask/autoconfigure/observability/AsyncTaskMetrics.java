package com.zaviwayne.asynctask.autoconfigure.observability;

import com.zaviwayne.asynctask.core.AsyncTaskAdmin;
import com.zaviwayne.asynctask.core.AsyncTaskStatistics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * 异步任务队列 Micrometer 指标。
 *
 * @since 2026-08-27
 */
public final class AsyncTaskMetrics implements MeterBinder {
    /**
     * 日志记录器。
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTaskMetrics.class);

    /**
     * 异步任务管理门面。
     */
    private final AsyncTaskAdmin taskAdmin;

    /**
     * 系统时钟。
     */
    private final Clock clock;

    /**
     * 运行统计缓存时长。
     */
    private final Duration statisticsCacheDuration;

    /**
     * 最近一次读取的任务统计。
     */
    private AsyncTaskStatistics cachedStatistics;

    /**
     * 统计缓存过期时间。
     */
    private Instant statisticsExpiresAt = Instant.MIN;

    /**
     * 创建异步任务队列指标。
     *
     * @param taskAdmin               异步任务管理门面
     * @param clock                   系统时钟
     * @param statisticsCacheDuration 运行统计缓存时长
     */
    public AsyncTaskMetrics(AsyncTaskAdmin taskAdmin,
                            Clock clock,
                            Duration statisticsCacheDuration) {
        this.taskAdmin = Objects.requireNonNull(taskAdmin, "异步任务管理门面不能为空");
        this.clock = Objects.requireNonNull(clock, "系统时钟不能为空");
        this.statisticsCacheDuration = requirePositive(statisticsCacheDuration);
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("async.task.backlog", this, AsyncTaskMetrics::backlogCount)
                .description("尚未完成投递的异步任务数量")
                .register(registry);
        Gauge.builder("async.task.running", this, AsyncTaskMetrics::runningCount)
                .description("正在执行的异步任务数量")
                .register(registry);
        Gauge.builder("async.task.dead", this, AsyncTaskMetrics::deadCount)
                .description("死亡状态异步任务数量")
                .register(registry);
        Gauge.builder("async.task.oldest.backlog.age", this, AsyncTaskMetrics::oldestBacklogAgeSeconds)
                .baseUnit("seconds")
                .description("最老积压异步任务的等待秒数")
                .register(registry);
    }

    private double backlogCount() {
        AsyncTaskStatistics statistics = statistics();
        return statistics == null ? Double.NaN : statistics.backlogCount();
    }

    private double runningCount() {
        AsyncTaskStatistics statistics = statistics();
        return statistics == null ? Double.NaN : statistics.runningCount();
    }

    private double deadCount() {
        AsyncTaskStatistics statistics = statistics();
        return statistics == null ? Double.NaN : statistics.deadCount();
    }

    private double oldestBacklogAgeSeconds() {
        AsyncTaskStatistics statistics = statistics();
        if (statistics == null) {
            return Double.NaN;
        }
        Instant oldestBacklogAt = statistics.oldestBacklogAt();
        if (oldestBacklogAt == null) {
            return 0;
        }
        long ageSeconds = Duration.between(oldestBacklogAt, clock.instant()).toSeconds();
        return Math.max(ageSeconds, 0);
    }

    private synchronized AsyncTaskStatistics statistics() {
        Instant now = clock.instant();
        if (!now.isBefore(statisticsExpiresAt)) {
            refreshStatistics(now);
        }
        return cachedStatistics;
    }

    private void refreshStatistics(Instant now) {
        try {
            cachedStatistics = taskAdmin.statistics();
        } catch (RuntimeException exception) {
            if (cachedStatistics == null) {
                LOGGER.warn("刷新异步任务运行统计失败，当前指标暂不可用", exception);
            } else {
                LOGGER.warn("刷新异步任务运行统计失败，继续使用最近一次成功快照", exception);
            }
        } finally {
            statisticsExpiresAt = now.plus(statisticsCacheDuration);
        }
    }

    private static Duration requirePositive(Duration duration) {
        Objects.requireNonNull(duration, "运行统计缓存时长不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("运行统计缓存时长必须大于 0");
        }
        return duration;
    }
}
