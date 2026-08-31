package com.zaviwayne.asynctask.autoconfigure.properties;

import com.zaviwayne.asynctask.core.AsyncTaskContentLimits;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Objects;

/**
 * 异步任务 Outbox 配置。
 *
 * @param enabled                    是否启动轮询投递
 * @param pollInterval               轮询间隔，至少为 1 毫秒
 * @param batchSize                  每次抢占数量
 * @param leaseDuration              投递租约时长
 * @param executionLeaseDuration     执行租约时长
 * @param executionHeartbeatInterval 自动刷新执行租约的间隔
 * @param executionHeartbeatThreads  自动刷新执行租约的线程数
 * @param maxAttempts                最大投递次数
 * @param initialBackoff             首次失败退避时间
 * @param maxBackoff                 最大退避时间
 * @param maxEnvelopeBytes           任务信封 JSON 最大 UTF-8 字节数
 * @param maxProgressBytes           任务进度 JSON 最大 UTF-8 字节数
 * @since 2026-08-26
 */
public record AsyncTaskOutboxProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("1s") Duration pollInterval,
        @DefaultValue("20") int batchSize,
        @DefaultValue("5m") Duration leaseDuration,
        @DefaultValue("5m") Duration executionLeaseDuration,
        @DefaultValue("30s") Duration executionHeartbeatInterval,
        @DefaultValue("2") int executionHeartbeatThreads,
        @DefaultValue("8") int maxAttempts,
        @DefaultValue("2s") Duration initialBackoff,
        @DefaultValue("1h") Duration maxBackoff,
        @DefaultValue("1000000") int maxEnvelopeBytes,
        @DefaultValue("1000000") int maxProgressBytes) {
    /**
     * 校验 Outbox 配置。
     */
    @ConstructorBinding
    public AsyncTaskOutboxProperties {
        requireAtLeastOneMillisecond(pollInterval, "Outbox 轮询间隔");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Outbox 批量大小必须大于 0");
        }
        requirePositive(leaseDuration, "Outbox 租约时长");
        requirePositive(executionLeaseDuration, "Outbox 执行租约时长");
        requirePositive(executionHeartbeatInterval, "Outbox 执行心跳间隔");
        if (executionHeartbeatInterval.compareTo(executionLeaseDuration) >= 0) {
            throw new IllegalArgumentException("Outbox 执行心跳间隔必须小于执行租约时长");
        }
        if (executionHeartbeatThreads <= 0) {
            throw new IllegalArgumentException("Outbox 执行心跳线程数必须大于 0");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("Outbox 最大投递次数必须大于 0");
        }
        requirePositive(initialBackoff, "Outbox 首次退避时间");
        requirePositive(maxBackoff, "Outbox 最大退避时间");
        if (initialBackoff.compareTo(maxBackoff) > 0) {
            throw new IllegalArgumentException("Outbox 首次退避时间不能大于最大退避时间");
        }
        if (maxEnvelopeBytes <= 0) {
            throw new IllegalArgumentException("任务信封 JSON 最大字节数必须大于 0");
        }
        if (maxProgressBytes <= 0) {
            throw new IllegalArgumentException("任务进度 JSON 最大字节数必须大于 0");
        }
    }

    /**
     * 使用默认内容大小限制创建 Outbox 配置。
     *
     * @param enabled                    是否启动轮询投递
     * @param pollInterval               轮询间隔
     * @param batchSize                  每次抢占数量
     * @param leaseDuration              投递租约时长
     * @param executionLeaseDuration     执行租约时长
     * @param executionHeartbeatInterval 自动刷新执行租约的间隔
     * @param executionHeartbeatThreads  自动刷新执行租约的线程数
     * @param maxAttempts                最大投递次数
     * @param initialBackoff             首次失败退避时间
     * @param maxBackoff                 最大退避时间
     */
    public AsyncTaskOutboxProperties(boolean enabled,
                                     Duration pollInterval,
                                     int batchSize,
                                     Duration leaseDuration,
                                     Duration executionLeaseDuration,
                                     Duration executionHeartbeatInterval,
                                     int executionHeartbeatThreads,
                                     int maxAttempts,
                                     Duration initialBackoff,
                                     Duration maxBackoff) {
        this(enabled, pollInterval, batchSize, leaseDuration, executionLeaseDuration,
                executionHeartbeatInterval, executionHeartbeatThreads, maxAttempts,
                initialBackoff, maxBackoff,
                AsyncTaskContentLimits.DEFAULT_MAX_ENVELOPE_BYTES,
                AsyncTaskContentLimits.DEFAULT_MAX_PROGRESS_BYTES);
    }

    private static void requirePositive(Duration duration, String fieldName) {
        Objects.requireNonNull(duration, fieldName + "不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(fieldName + "必须大于 0");
        }
    }

    private static void requireAtLeastOneMillisecond(Duration duration, String fieldName) {
        requirePositive(duration, fieldName);
        if (duration.toMillis() == 0L) {
            throw new IllegalArgumentException(fieldName + "必须至少为 1 毫秒");
        }
    }
}
