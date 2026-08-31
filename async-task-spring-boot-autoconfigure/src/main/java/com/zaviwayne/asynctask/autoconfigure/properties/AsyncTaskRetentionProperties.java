package com.zaviwayne.asynctask.autoconfigure.properties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Objects;

/**
 * 异步任务终态数据保留配置。
 *
 * @param enabled          是否启用自动清理
 * @param retentionPeriod  终态任务保留时长
 * @param cleanupInterval  自动清理间隔，至少为 1 毫秒
 * @param batchSize        单批清理数量
 * @param maxBatchesPerRun 单次运行最大清理批数
 * @since 2026-08-27
 */
public record AsyncTaskRetentionProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("720h") Duration retentionPeriod,
        @DefaultValue("1h") Duration cleanupInterval,
        @DefaultValue("500") int batchSize,
        @DefaultValue("200") int maxBatchesPerRun) {
    /**
     * 默认单次运行最大清理批数。
     */
    private static final int DEFAULT_MAX_BATCHES_PER_RUN = 200;

    /**
     * 校验终态数据保留配置。
     */
    @ConstructorBinding
    public AsyncTaskRetentionProperties {
        requirePositive(retentionPeriod, "终态任务保留时长");
        requireAtLeastOneMillisecond(cleanupInterval, "终态任务清理间隔");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("终态任务清理批量大小必须大于 0");
        }
        if (maxBatchesPerRun <= 0) {
            throw new IllegalArgumentException("终态任务单次运行最大清理批数必须大于 0");
        }
    }

    /**
     * 使用默认单次最大清理批数创建终态任务保留配置。
     *
     * @param enabled         是否启用自动清理
     * @param retentionPeriod 终态任务保留时长
     * @param cleanupInterval 自动清理间隔
     * @param batchSize       单批清理数量
     */
    public AsyncTaskRetentionProperties(boolean enabled,
                                        Duration retentionPeriod,
                                        Duration cleanupInterval,
                                        int batchSize) {
        this(enabled, retentionPeriod, cleanupInterval, batchSize, DEFAULT_MAX_BATCHES_PER_RUN);
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
