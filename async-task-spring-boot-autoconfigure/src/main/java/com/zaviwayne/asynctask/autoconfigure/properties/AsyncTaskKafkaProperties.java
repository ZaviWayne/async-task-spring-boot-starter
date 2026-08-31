package com.zaviwayne.asynctask.autoconfigure.properties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.*;

/**
 * 异步任务 Kafka 配置。
 *
 * @param enabled              是否启用 Kafka 适配器
 * @param autoCreateTopics     是否自动声明业务、死信和停放主题
 * @param transactionEnabled   是否使用 Kafka 事务发送
 * @param topics               使用全局默认项的业务主题
 * @param bindings             单个业务主题的独立配置
 * @param consumerGroup        默认消费组
 * @param concurrency          默认消费并发数
 * @param partitions           默认分区数
 * @param replicas             默认副本数
 * @param sendTimeout          生产发送确认超时时间，至少为 1 毫秒
 * @param retryInterval        消费失败重试间隔，至少为 1 毫秒
 * @param maxRetries           进入 DLT 前的重试次数
 * @param leaseRetryInterval   执行租约仍有效时的消息重试间隔，至少为 1 毫秒
 * @param deadLetterSuffix     默认死信主题后缀
 * @param deadLetterMaxRetries DLT 处理失败后进入停放主题前的重试次数
 * @param parkingSuffix        默认停放主题后缀
 * @since 2026-08-26
 */
public record AsyncTaskKafkaProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("false") boolean autoCreateTopics,
        @DefaultValue("false") boolean transactionEnabled,
        @DefaultValue List<String> topics,
        @DefaultValue List<AsyncTaskTopicBinding> bindings,
        @DefaultValue("async-task") String consumerGroup,
        @DefaultValue("1") int concurrency,
        @DefaultValue("3") int partitions,
        @DefaultValue("1") int replicas,
        @DefaultValue("10s") Duration sendTimeout,
        @DefaultValue("2s") Duration retryInterval,
        @DefaultValue("3") long maxRetries,
        @DefaultValue("5s") Duration leaseRetryInterval,
        @DefaultValue(".DLT") String deadLetterSuffix,
        @DefaultValue("10") long deadLetterMaxRetries,
        @DefaultValue(".PARKING") String parkingSuffix) {
    /**
     * 默认 DLT 处理重试次数。
     */
    private static final long DEFAULT_DEAD_LETTER_MAX_RETRIES = 10L;

    /**
     * 默认停放主题后缀。
     */
    private static final String DEFAULT_PARKING_SUFFIX = ".PARKING";

    /**
     * 校验 Kafka 配置并解析单主题配置。
     */
    @ConstructorBinding
    public AsyncTaskKafkaProperties {
        requireText(consumerGroup, "Kafka 消费组");
        if (concurrency <= 0) {
            throw new IllegalArgumentException("Kafka 消费并发数必须大于 0");
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("Kafka 默认分区数必须大于 0");
        }
        if (replicas <= 0) {
            throw new IllegalArgumentException("Kafka 默认副本数必须大于 0");
        }
        requireAtLeastOneMillisecond(sendTimeout, "Kafka 发送确认超时时间");
        requireAtLeastOneMillisecond(retryInterval, "Kafka 消费重试间隔");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Kafka 最大重试次数不能小于 0");
        }
        requireAtLeastOneMillisecond(leaseRetryInterval, "Kafka 租约冲突重试间隔");
        requireText(deadLetterSuffix, "Kafka 死信主题后缀");
        if (deadLetterMaxRetries < 0) {
            throw new IllegalArgumentException("Kafka DLT 最大重试次数不能小于 0");
        }
        requireText(parkingSuffix, "Kafka 停放主题后缀");
        bindings = resolveBindings(
                topics, bindings, consumerGroup, concurrency, partitions, replicas,
                deadLetterSuffix, parkingSuffix);
        topics = bindings.stream().map(AsyncTaskTopicBinding::topic).toList();
    }

    /**
     * 使用默认 DLT 重试次数和停放主题后缀创建 Kafka 配置。
     *
     * @param enabled            是否启用 Kafka 适配器
     * @param autoCreateTopics   是否自动声明主题
     * @param transactionEnabled 是否使用 Kafka 事务发送
     * @param topics             使用全局默认项的业务主题
     * @param bindings           单个业务主题的独立配置
     * @param consumerGroup      默认消费组
     * @param concurrency        默认消费并发数
     * @param partitions         默认分区数
     * @param replicas           默认副本数
     * @param sendTimeout        生产发送确认超时时间
     * @param retryInterval      消费失败重试间隔
     * @param maxRetries         进入 DLT 前的重试次数
     * @param leaseRetryInterval 执行租约冲突重试间隔
     * @param deadLetterSuffix   默认死信主题后缀
     */
    public AsyncTaskKafkaProperties(boolean enabled,
                                    boolean autoCreateTopics,
                                    boolean transactionEnabled,
                                    List<String> topics,
                                    List<AsyncTaskTopicBinding> bindings,
                                    String consumerGroup,
                                    int concurrency,
                                    int partitions,
                                    int replicas,
                                    Duration sendTimeout,
                                    Duration retryInterval,
                                    long maxRetries,
                                    Duration leaseRetryInterval,
                                    String deadLetterSuffix) {
        this(enabled, autoCreateTopics, transactionEnabled, topics, bindings, consumerGroup,
                concurrency, partitions, replicas, sendTimeout, retryInterval, maxRetries,
                leaseRetryInterval, deadLetterSuffix, DEFAULT_DEAD_LETTER_MAX_RETRIES,
                DEFAULT_PARKING_SUFFIX);
    }

    private static List<AsyncTaskTopicBinding> resolveBindings(List<String> topics,
                                                               List<AsyncTaskTopicBinding> bindings,
                                                               String consumerGroup,
                                                               int concurrency,
                                                               int partitions,
                                                               int replicas,
                                                               String deadLetterSuffix,
                                                               String parkingSuffix) {
        Map<String, AsyncTaskTopicBinding> resolvedBindings = new LinkedHashMap<>();
        if (topics != null) {
            for (String topic : topics) {
                AsyncTaskTopicBinding binding = new AsyncTaskTopicBinding(topic, null, null, 0, 0, 0);
                AsyncTaskTopicBinding resolvedBinding = resolveBinding(
                        binding, consumerGroup, concurrency, partitions, replicas,
                        deadLetterSuffix, parkingSuffix);
                resolvedBindings.putIfAbsent(resolvedBinding.topic(), resolvedBinding);
            }
        }
        Set<String> explicitlyConfiguredTopics = new LinkedHashSet<>();
        if (bindings != null) {
            for (AsyncTaskTopicBinding binding : bindings) {
                Objects.requireNonNull(binding, "Kafka 单主题配置不能为空");
                if (!explicitlyConfiguredTopics.add(binding.topic())) {
                    throw new IllegalArgumentException("Kafka 主题配置重复: " + binding.topic());
                }
                resolvedBindings.put(binding.topic(), resolveBinding(
                        binding, consumerGroup, concurrency, partitions, replicas,
                        deadLetterSuffix, parkingSuffix));
            }
        }
        validateTopicNames(resolvedBindings);
        return List.copyOf(resolvedBindings.values());
    }

    private static AsyncTaskTopicBinding resolveBinding(AsyncTaskTopicBinding binding,
                                                        String defaultConsumerGroup,
                                                        int defaultConcurrency,
                                                        int defaultPartitions,
                                                        int defaultReplicas,
                                                        String deadLetterSuffix,
                                                        String parkingSuffix) {
        String resolvedDeadLetterTopic = binding.deadLetterTopic().isEmpty()
                ? binding.topic() + deadLetterSuffix
                : binding.deadLetterTopic();
        String resolvedConsumerGroup = binding.consumerGroup().isEmpty()
                ? defaultConsumerGroup
                : binding.consumerGroup();
        String resolvedParkingTopic = binding.parkingTopic().isEmpty()
                ? resolvedDeadLetterTopic + parkingSuffix
                : binding.parkingTopic();
        return new AsyncTaskTopicBinding(
                binding.topic(),
                resolvedDeadLetterTopic,
                resolvedParkingTopic,
                resolvedConsumerGroup,
                binding.concurrency() == 0 ? defaultConcurrency : binding.concurrency(),
                binding.partitions() == 0 ? defaultPartitions : binding.partitions(),
                binding.replicas() == 0 ? defaultReplicas : binding.replicas());
    }

    private static void validateTopicNames(Map<String, AsyncTaskTopicBinding> bindings) {
        Set<String> businessTopics = bindings.keySet();
        Set<String> deadLetterTopics = new LinkedHashSet<>();
        for (AsyncTaskTopicBinding binding : bindings.values()) {
            if (businessTopics.contains(binding.deadLetterTopic())) {
                throw new IllegalArgumentException(
                        "Kafka 死信主题不能与业务主题重名: " + binding.deadLetterTopic());
            }
            if (!deadLetterTopics.add(binding.deadLetterTopic())) {
                throw new IllegalArgumentException(
                        "Kafka 死信主题配置重复: " + binding.deadLetterTopic());
            }
        }
        Set<String> parkingTopics = new LinkedHashSet<>();
        for (AsyncTaskTopicBinding binding : bindings.values()) {
            if (businessTopics.contains(binding.parkingTopic())) {
                throw new IllegalArgumentException(
                        "Kafka 停放主题不能与业务主题重名: " + binding.parkingTopic());
            }
            if (deadLetterTopics.contains(binding.parkingTopic())) {
                throw new IllegalArgumentException(
                        "Kafka 停放主题不能与死信主题重名: " + binding.parkingTopic());
            }
            if (!parkingTopics.add(binding.parkingTopic())) {
                throw new IllegalArgumentException(
                        "Kafka 停放主题配置重复: " + binding.parkingTopic());
            }
        }
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

    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + "不能为空");
        }
    }
}
