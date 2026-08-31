package com.zaviwayne.asynctask.autoconfigure.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Objects;

/**
 * 异步任务 starter 配置。
 *
 * @param enabled       是否启用 starter
 * @param database      数据库配置
 * @param outbox        生产侧 outbox 配置
 * @param retention     终态任务保留配置
 * @param kafka         Kafka 配置
 * @param observability 可观测性配置
 * @since 2026-08-26
 */
@ConfigurationProperties("async-task")
public record AsyncTaskProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue @NestedConfigurationProperty AsyncTaskDatabaseProperties database,
        @DefaultValue @NestedConfigurationProperty AsyncTaskOutboxProperties outbox,
        @DefaultValue @NestedConfigurationProperty AsyncTaskRetentionProperties retention,
        @DefaultValue @NestedConfigurationProperty AsyncTaskKafkaProperties kafka,
        @DefaultValue @NestedConfigurationProperty AsyncTaskObservabilityProperties observability) {
    /**
     * 校验异步任务配置。
     */
    public AsyncTaskProperties {
        Objects.requireNonNull(database, "数据库配置不能为空");
        Objects.requireNonNull(outbox, "Outbox 配置不能为空");
        Objects.requireNonNull(retention, "终态任务保留配置不能为空");
        Objects.requireNonNull(kafka, "Kafka 配置不能为空");
        Objects.requireNonNull(observability, "可观测性配置不能为空");
        validateDispatchLease(outbox, kafka);
    }

    private static void validateDispatchLease(AsyncTaskOutboxProperties outbox,
                                              AsyncTaskKafkaProperties kafka) {
        if (!outbox.enabled() || !kafka.enabled()) {
            return;
        }
        Duration maximumBatchSendTime;
        try {
            maximumBatchSendTime = kafka.sendTimeout().multipliedBy(outbox.batchSize());
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("Kafka 整批最大发送时间超出可配置范围", exception);
        }
        if (outbox.leaseDuration().compareTo(maximumBatchSendTime) <= 0) {
            throw new IllegalArgumentException(
                    "Outbox 投递租约时长必须大于批量大小与 Kafka 发送超时时间的乘积");
        }
    }
}
