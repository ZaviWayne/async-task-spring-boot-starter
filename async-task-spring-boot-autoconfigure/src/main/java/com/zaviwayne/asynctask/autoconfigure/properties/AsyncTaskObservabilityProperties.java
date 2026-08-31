package com.zaviwayne.asynctask.autoconfigure.properties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Objects;

/**
 * 异步任务可观测性配置。
 *
 * @param statisticsCacheDuration 运行统计缓存时长
 * @since 2026-08-31
 */
public record AsyncTaskObservabilityProperties(
        @DefaultValue("30s") Duration statisticsCacheDuration) {
    /**
     * 校验异步任务可观测性配置。
     */
    @ConstructorBinding
    public AsyncTaskObservabilityProperties {
        Objects.requireNonNull(statisticsCacheDuration, "运行统计缓存时长不能为空");
        if (statisticsCacheDuration.isZero() || statisticsCacheDuration.isNegative()) {
            throw new IllegalArgumentException("运行统计缓存时长必须大于 0");
        }
    }
}
